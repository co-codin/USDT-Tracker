// Package app wires configuration, the TRON source, filter, sinks, cursor
// store, metrics and the HTTP server into a runnable application. It is the
// composition root used by cmd/listener and by end-to-end tests.
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"slices"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton/jetton"
	"github.com/co-codin/USDT-Tracker/internal/chain/tron"
	"github.com/co-codin/USDT-Tracker/internal/chain/tron/trc20"
	"github.com/co-codin/USDT-Tracker/internal/chainaddr"
	"github.com/co-codin/USDT-Tracker/internal/config"
	"github.com/co-codin/USDT-Tracker/internal/cursor"
	"github.com/co-codin/USDT-Tracker/internal/filter"
	"github.com/co-codin/USDT-Tracker/internal/listener"
	"github.com/co-codin/USDT-Tracker/internal/metrics"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/retry"
	"github.com/co-codin/USDT-Tracker/internal/server"
	"github.com/co-codin/USDT-Tracker/internal/sink"
	"github.com/co-codin/USDT-Tracker/internal/sink/postgres"
	"github.com/co-codin/USDT-Tracker/internal/watch"
)

// Options are runtime knobs that are not part of the user configuration.
type Options struct {
	Version  string
	Backfill bool          // use an in-memory cursor and no HTTP server
	Stdout   io.Writer     // stdout sink destination (default os.Stdout)
	Backoff  retry.Backoff // backoff for chain API and loop retries (zero = defaults)
	// BackfillChain selects the chain to backfill when several are enabled
	// (default: the only enabled chain).
	BackfillChain string
}

// Pipeline is one chain's listener with its filter and cursor.
type Pipeline struct {
	Chain    string
	Listener *listener.Listener
	dyn      *filter.Dynamic
	cursor   string
}

// App is a fully wired listener.
type App struct {
	cfg     config.Config
	opts    Options
	log     *slog.Logger
	Metrics *metrics.Metrics
	// Listener is the first enabled chain's listener (kept for single-chain callers).
	Listener  *listener.Listener
	Pipelines []*Pipeline
	disp      *sink.Dispatcher
	pg        *postgres.Store
	srv       *server.Server
}

// New builds the application. Call Run, then Close.
func New(ctx context.Context, cfg config.Config, log *slog.Logger, opts Options) (*App, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if opts.Stdout == nil {
		opts.Stdout = os.Stdout
	}
	if opts.Version == "" {
		opts.Version = "dev"
	}
	a := &App{cfg: cfg, opts: opts, log: log, Metrics: metrics.New()}

	chains := cfg.Chains
	if len(chains) == 0 {
		chains = []string{model.ChainTron}
	}
	if opts.Backfill {
		switch {
		case opts.BackfillChain != "":
			if !slices.Contains(chains, opts.BackfillChain) {
				return nil, fmt.Errorf("backfill chain %q is not enabled (chains: %s)", opts.BackfillChain, strings.Join(chains, ","))
			}
			chains = []string{opts.BackfillChain}
		case len(chains) > 1:
			return nil, fmt.Errorf("several chains are enabled (%s): pick the one to backfill with -chain", strings.Join(chains, ","))
		}
	}

	// Build sources and validate filters before connecting to anything.
	sources := make(map[string]chain.Source, len(chains))
	for _, c := range chains {
		src, err := a.buildSource(c)
		if err != nil {
			return nil, err
		}
		sources[c] = src
		minAmount, err := chainMinAmount(cfg, c)
		if err != nil {
			return nil, err
		}
		if _, err := filter.New(staticFilterConfig(cfg, c, minAmount)); err != nil {
			return nil, err
		}
	}

	entries, pg, err := buildSinks(ctx, cfg, opts.Stdout)
	if err != nil {
		return nil, err
	}
	a.pg = pg
	a.disp = sink.NewDispatcher(entries, cfg.Listener.DedupeCacheSize, a.Metrics, log)

	var watchSrc watch.Source // nil interface when Postgres is off
	if pg != nil {
		watchSrc = pg.Watchlist()
	}
	for _, c := range chains {
		minAmount, _ := chainMinAmount(cfg, c)
		matcher, dyn, err := buildFilter(ctx, cfg, c, minAmount, watchSrc, log)
		if err != nil {
			return nil, errors.Join(err, a.disp.Close())
		}
		store, desc := a.cursorStore(c)
		lc := listenerConfig(cfg, c, opts.Backoff)
		p := &Pipeline{Chain: c, dyn: dyn, cursor: desc,
			Listener: listener.New(lc, sources[c], matcher, a.disp, store, a.Metrics, log)}
		a.Pipelines = append(a.Pipelines, p)
	}
	a.Listener = a.Pipelines[0].Listener

	if cfg.HTTP.Addr != "" && !opts.Backfill {
		a.srv = server.New(cfg.HTTP.Addr, a.Metrics.Handler(), a.health, log)
		var ws watch.Store // nil interface (not a typed nil) when Postgres is off → 503
		if pg != nil {
			ws = pg.Watchlist()
		}
		a.srv.Handle("/v1/", watch.NewAPI(watch.APIConfig{
			Token: cfg.API.Token, Store: ws, Static: staticAddresses(cfg, chains), Chains: chains,
			OnChange: func() {
				for _, p := range a.Pipelines {
					if p.dyn != nil {
						p.dyn.Invalidate()
					}
				}
			},
			Log: log,
		}))
	}

	if cfg.HasChain(model.ChainTron) && cfg.Tron.APIKey == "" && sources[model.ChainTron] != nil {
		log.Warn("TRON_PRO_API_KEY not set: using public rate limits, keep tron.rps low")
	}
	if sources[model.ChainTON] != nil && cfg.TON.APIKey == "" {
		log.Warn("TONCENTER_API_KEY not set: keyless toncenter allows ~1 req/s, which cannot keep up with mainnet head (2 requests per masterchain block); get a key from @tonapibot and raise TON_RPS")
	}
	cursors := make([]string, 0, len(a.Pipelines))
	for _, p := range a.Pipelines {
		cursors = append(cursors, p.Chain+"="+p.cursor)
	}
	log.Info("usdt-tracker configured",
		"version", opts.Version,
		"chains", strings.Join(chains, ","),
		"sinks", strings.Join(a.disp.Names(), ","),
		"cursor", strings.Join(cursors, " "),
		"watch_addresses", len(cfg.Filter.WatchAddresses),
		"ton_watch_addresses", len(cfg.TON.WatchAddresses),
		"runtime_watch_list", pg != nil,
		"admin_api", cfg.API.Token != "" && cfg.HTTP.Addr != "",
		"min_amount", cfg.Filter.MinAmount,
	)
	return a, nil
}

func (a *App) buildSource(c string) (chain.Source, error) {
	cfg, opts, log := a.cfg, a.opts, a.log
	cm := a.Metrics.For(c)
	switch c {
	case model.ChainTron:
		contract, err := tron.ParseAddress(cfg.Token.Contract)
		if err != nil {
			return nil, err
		}
		client := tron.NewClient(tron.Options{
			BaseURL: cfg.Tron.APIURL, APIKey: cfg.Tron.APIKey, Timeout: cfg.Tron.Timeout.D(),
			RPS: cfg.Tron.RPS, MaxRetries: cfg.Tron.MaxRetries, Backoff: opts.Backoff,
			UserAgent: "tron-usdt-listener/" + opts.Version, Observer: cm, Logger: log,
		})
		log.Info("tron source", "api_url", cfg.Tron.APIURL, "api_key_set", cfg.Tron.APIKey != "",
			"token", cfg.Token.Symbol, "contract", contract.String())
		dec := trc20.New(trc20.Token{Contract: contract, Symbol: cfg.Token.Symbol, Decimals: cfg.Token.Decimals})
		return trc20.NewSource(client, dec, log, cm.DecodeErrors.Inc), nil
	case model.ChainTON:
		master, err := ton.ParseAddress(cfg.TON.JettonMaster)
		if err != nil {
			return nil, fmt.Errorf("ton.jetton_master: %w", err)
		}
		client := ton.NewClient(ton.Options{
			BaseURL: cfg.TON.APIURL, APIKey: cfg.TON.APIKey, Timeout: cfg.TON.Timeout.D(),
			RPS: cfg.TON.RPS, MaxRetries: cfg.TON.MaxRetries, Backoff: opts.Backoff,
			UserAgent: "usdt-tracker/" + opts.Version, Observer: cm, Logger: log,
		})
		log.Info("ton source", "api_url", cfg.TON.APIURL, "api_key_set", cfg.TON.APIKey != "",
			"token", cfg.TON.Symbol, "jetton_master", master.Friendly(true, false))
		return jetton.NewSource(client, jetton.Token{Master: master, Symbol: cfg.TON.Symbol, Decimals: cfg.TON.Decimals},
			log.With("chain", c), cm.DecodeErrors.Inc), nil
	default:
		return nil, fmt.Errorf("unknown chain %q", c)
	}
}

func (a *App) cursorStore(c string) (cursor.Store, string) {
	switch {
	case a.opts.Backfill:
		return &cursor.MemoryStore{}, "memory (backfill)"
	case a.pg != nil:
		if c == model.ChainTron {
			return a.pg, "postgres:" + a.pg.CursorName()
		}
		name := a.pg.CursorName() + ":" + c
		return a.pg.Cursor(name), "postgres:" + name
	case c == model.ChainTON:
		return cursor.NewFileStore(a.cfg.TON.CursorFile), "file:" + a.cfg.TON.CursorFile
	default:
		return cursor.NewFileStore(a.cfg.Cursor.File), "file:" + a.cfg.Cursor.File
	}
}

func listenerConfig(cfg config.Config, c string, b retry.Backoff) listener.Config {
	l := cfg.Listener
	lc := listener.Config{
		Chain: c, Confirmations: l.Confirmations, PollInterval: l.PollInterval.D(), StartBlock: l.StartBlock,
		BatchSize: l.BatchSize, Concurrency: l.Concurrency, DispatchTimeout: l.DispatchTimeout.D(), Backoff: b,
	}
	if c == model.ChainTON {
		t := cfg.TON
		lc.Confirmations, lc.PollInterval, lc.StartBlock = t.Confirmations, t.PollInterval.D(), t.StartBlock
		lc.BatchSize, lc.Concurrency = t.BatchSize, t.Concurrency
	}
	return lc
}

func chainMinAmount(cfg config.Config, c string) (*big.Int, error) {
	if c == model.ChainTON {
		return cfg.TONMinAmount()
	}
	return cfg.MinAmount()
}

// Run starts the HTTP server (if enabled) and processes blocks on every
// enabled chain until ctx is cancelled. With from > 0 it backfills
// [from, to] on the single selected chain and returns.
func (a *App) Run(ctx context.Context, from, to int64) error {
	if a.srv != nil {
		if err := a.srv.Start(); err != nil {
			return fmt.Errorf("http server: %w", err)
		}
		defer func() {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = a.srv.Shutdown(sctx)
		}()
	}
	if from > 0 {
		if to == 0 {
			to = from
		}
		return a.Listener.Backfill(ctx, from, to)
	}
	if len(a.Pipelines) == 1 {
		return a.Listener.Run(ctx)
	}
	// One chain failing fatally stops the others so the process restarts as a whole.
	g, gctx := errgroup.WithContext(ctx)
	for _, p := range a.Pipelines {
		g.Go(func() error {
			if err := p.Listener.Run(gctx); err != nil {
				return fmt.Errorf("%s: %w", p.Chain, err)
			}
			return nil
		})
	}
	return g.Wait()
}

// HTTPAddr returns the bound ops server address ("" if disabled/not started).
func (a *App) HTTPAddr() string {
	if a.srv == nil || a.srv.Addr() == nil {
		return ""
	}
	return a.srv.Addr().String()
}

// Close releases sinks (and the Postgres pool).
func (a *App) Close() error { return a.disp.Close() }

func (a *App) health(ctx context.Context) (bool, any) {
	ok := true
	statuses := make(map[string]listener.Status, len(a.Pipelines))
	for _, p := range a.Pipelines {
		st := p.Listener.Status()
		if time.Since(st.LastProgressAt) > a.cfg.HTTP.StaleAfter.D() {
			ok = false
		}
		statuses[p.Chain] = st
	}
	var pgErr string
	if a.pg != nil {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := a.pg.Ping(pctx); err != nil {
			ok = false
			pgErr = "postgres: " + err.Error()
		}
	}
	if len(a.Pipelines) == 1 { // unchanged single-chain shape
		st := statuses[a.Pipelines[0].Chain]
		if pgErr != "" {
			st.LastError = pgErr
		}
		return ok, st
	}
	out := map[string]any{"chains": statuses}
	if pgErr != "" {
		out["last_error"] = pgErr
	}
	return ok, out
}

// staticAddresses returns the canonical static watch addresses of the enabled chains.
func staticAddresses(cfg config.Config, chains []string) []string {
	var out []string
	for _, c := range chains {
		list := cfg.Filter.WatchAddresses
		if c == model.ChainTON {
			list = cfg.TON.WatchAddresses
		}
		for _, s := range list {
			if n, err := chainaddr.Normalize(c, s); err == nil {
				out = append(out, n)
			}
		}
	}
	return out
}

func staticFilterConfig(cfg config.Config, c string, minAmount *big.Int) filter.Config {
	addrs := cfg.Filter.WatchAddresses
	if c == model.ChainTON {
		addrs = cfg.TON.WatchAddresses
	}
	return filter.Config{
		Chain:          c,
		WatchAddresses: addrs,
		Direction:      filter.Direction(cfg.Filter.Direction),
		MinAmount:      minAmount,
		Mode:           filter.Mode(cfg.Filter.Mode),
	}
}

// buildFilter returns the static filter, or — when a watch source (Postgres) is set — a
// Dynamic filter that merges the static addresses with the enabled rows of
// watch_addresses. With the admin API enabled the watch rule stays active
// even when the list is empty, so removing the last deposit address never
// turns the listener into a firehose.
func buildFilter(ctx context.Context, cfg config.Config, c string, minAmount *big.Int, src watch.Source, log *slog.Logger) (listener.Matcher, *filter.Dynamic, error) {
	fCfg := staticFilterConfig(cfg, c, minAmount)
	if src == nil {
		f, err := filter.New(fCfg)
		return f, nil, err
	}
	fCfg.Dynamic = cfg.API.Token != ""
	dyn, err := filter.NewDynamic(fCfg, src, cfg.Filter.ReloadInterval.D())
	if err != nil {
		return nil, nil, err
	}
	dyn.SetLogger(log)
	if err := dyn.Refresh(ctx); err != nil {
		return nil, nil, err
	}
	return dyn, dyn, nil
}

func buildSinks(ctx context.Context, cfg config.Config, stdout io.Writer) ([]sink.Entry, *postgres.Store, error) {
	var (
		entries []sink.Entry
		pg      *postgres.Store
	)
	fail := func(err error) ([]sink.Entry, *postgres.Store, error) {
		if pg != nil {
			err = errors.Join(err, pg.Close())
		}
		return nil, nil, err
	}
	s := cfg.Sinks
	if s.Stdout.Enabled {
		entries = append(entries, sink.Entry{Sink: sink.NewStdout(stdout)})
	}
	if s.Postgres.Enabled {
		var err error
		pg, err = postgres.Open(ctx, s.Postgres.DSN, s.Postgres.CursorName)
		if err != nil {
			return nil, nil, err
		}
		entries = append(entries, sink.Entry{Sink: pg, BestEffort: s.Postgres.BestEffort})
	}
	if s.Webhook.Enabled {
		w, err := sink.NewWebhook(sink.WebhookConfig{
			URL: s.Webhook.URL, Secret: s.Webhook.Secret, Timeout: s.Webhook.Timeout.D(),
			MaxRetries: s.Webhook.MaxRetries, Headers: s.Webhook.Headers,
		})
		if err != nil {
			return fail(err)
		}
		entries = append(entries, sink.Entry{Sink: w, BestEffort: s.Webhook.BestEffort})
	}
	if s.Telegram.Enabled {
		minAmt, err := model.ParseUnits(s.Telegram.MinAmount, cfg.Token.Decimals)
		if err != nil {
			return fail(err)
		}
		if minAmt.Sign() == 0 {
			minAmt = nil
		}
		t, err := sink.NewTelegram(sink.TelegramConfig{
			BotToken: s.Telegram.BotToken, ChatID: s.Telegram.ChatID, APIBase: s.Telegram.APIBase,
			MinAmount: minAmt, MaxPerBlock: s.Telegram.MaxPerBlock,
		})
		if err != nil {
			return fail(err)
		}
		entries = append(entries, sink.Entry{Sink: t, BestEffort: s.Telegram.BestEffort})
	}
	return entries, pg, nil
}
