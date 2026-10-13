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

	"github.com/co-codin/USDT-Tracker/internal/alert"
	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/chainaddr"
	"github.com/co-codin/USDT-Tracker/internal/config"
	"github.com/co-codin/USDT-Tracker/internal/cursor"
	"github.com/co-codin/USDT-Tracker/internal/export"
	"github.com/co-codin/USDT-Tracker/internal/filter"
	"github.com/co-codin/USDT-Tracker/internal/listener"
	"github.com/co-codin/USDT-Tracker/internal/metrics"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/notify"
	"github.com/co-codin/USDT-Tracker/internal/retry"
	"github.com/co-codin/USDT-Tracker/internal/server"
	"github.com/co-codin/USDT-Tracker/internal/sink"
	"github.com/co-codin/USDT-Tracker/internal/sink/postgres"
	"github.com/co-codin/USDT-Tracker/internal/tgbot"
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
	// Alerts is the large-transfer channel alerter (nil when disabled).
	Alerts *alert.Alerter
	// NotifyBot is the self-service notify bot (nil when disabled).
	NotifyBot *notify.Bot
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
	if la := cfg.Alerts.LargeTransfers; la.Enabled && !opts.Backfill {
		bot, err := tgbot.New(tgbot.Options{Token: la.Token(&cfg), APIBase: la.APIBase, Backoff: opts.Backoff})
		if err != nil {
			return nil, errors.Join(err, a.disp.Close())
		}
		a.Alerts, err = alert.New(alert.Config{
			ChatID: la.ChatID, Threshold: la.Threshold, Interval: la.Interval.D(),
			MaxPerMessage: la.MaxPerMessage, QueueSize: la.QueueSize, Footer: la.Footer,
		}, bot, log.With("component", "large_alerts"))
		if err != nil {
			return nil, errors.Join(err, a.disp.Close())
		}
		a.Alerts.OnSent(func(status string, n int) { a.Metrics.LargeAlerts.WithLabelValues(status).Add(float64(n)) })
	}

	if nb := cfg.NotifyBot; nb.Enabled && !opts.Backfill {
		if pg == nil {
			return nil, errors.Join(errors.New("notify_bot needs the postgres sink"), a.disp.Close())
		}
		api, err := tgbot.New(tgbot.Options{Token: nb.BotToken, APIBase: nb.APIBase, Backoff: opts.Backoff})
		if err != nil {
			return nil, errors.Join(err, a.disp.Close())
		}
		a.NotifyBot, err = notify.New(notify.Config{
			FreeLimit: nb.FreeLimit, PremiumLimit: nb.PremiumLimit, MinAmount: nb.MinAmount,
			Chains: chains, PollTimeout: nb.PollTimeout.D(),
			MaxUsers: nb.MaxUsers, MaxTotalSubs: nb.MaxTotalSubs, CommandsPerMinute: nb.CommandsPerMinute,
		}, pg.Notify(), api, log.With("component", "notify_bot"))
		if err != nil {
			return nil, errors.Join(err, a.disp.Close())
		}
	}

	var watchSrc watch.Source // nil interface when Postgres is off
	if pg != nil {
		watchSrc = pg.Watchlist()
	}
	for _, c := range chains {
		minAmount, _ := chainMinAmount(cfg, c) // already validated above
		matcher, dyn, err := buildFilter(ctx, cfg, c, minAmount, watchSrc, log)
		if err != nil {
			return nil, errors.Join(err, a.disp.Close())
		}
		store, desc := a.cursorStore(c)
		lc := listenerConfig(cfg, c, opts.Backoff)
		if a.Alerts != nil {
			lc.Taps = append(lc.Taps, a.Alerts.Observe)
		}
		if a.NotifyBot != nil {
			lc.Taps = append(lc.Taps, a.NotifyBot.Observe)
		}
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
		var es export.Source // nil interface when Postgres is off → 503
		if pg != nil {
			es = pg
		}
		a.srv.Handle("/v1/export.csv", export.NewHandler(cfg.API.Token, es, log.With("component", "export")))
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

	for c := range sources {
		if warn := chainDefs[c].warn; warn != nil {
			warn(cfg, log)
		}
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
		"eth_watch_addresses", len(cfg.ETH.WatchAddresses),
		"runtime_watch_list", pg != nil,
		"admin_api", cfg.API.Token != "" && cfg.HTTP.Addr != "",
		"min_amount", cfg.Filter.MinAmount,
		"large_alerts", a.Alerts != nil,
		"notify_bot", a.NotifyBot != nil,
	)
	return a, nil
}

func (a *App) buildSource(c string) (chain.Source, error) {
	def, ok := chainDefs[c]
	if !ok {
		return nil, fmt.Errorf("unknown chain %q", c)
	}
	return def.newSource(a, a.Metrics.For(c))
}

func (a *App) cursorStore(c string) (cursor.Store, string) {
	switch {
	case a.opts.Backfill:
		return &cursor.MemoryStore{}, "memory (backfill)"
	case a.pg != nil:
		if chainDefs[c].bareCursorName {
			return a.pg, "postgres:" + a.pg.CursorName()
		}
		name := a.pg.CursorName() + ":" + c
		return a.pg.Cursor(name), "postgres:" + name
	default:
		f := chainDefs[c].cursorFile(a.cfg)
		return cursor.NewFileStore(f), "file:" + f
	}
}

func listenerConfig(cfg config.Config, c string, b retry.Backoff) listener.Config {
	l := cfg.Listener
	lc := listener.Config{
		Chain: c, Confirmations: l.Confirmations, PollInterval: l.PollInterval.D(), StartBlock: l.StartBlock,
		BatchSize: l.BatchSize, Concurrency: l.Concurrency, DispatchTimeout: l.DispatchTimeout.D(), Backoff: b,
	}
	if tune := chainDefs[c].tuneListener; tune != nil {
		tune(cfg, &lc)
	}
	return lc
}

func chainMinAmount(cfg config.Config, c string) (*big.Int, error) {
	def, ok := chainDefs[c]
	if !ok {
		return nil, fmt.Errorf("unknown chain %q", c)
	}
	return def.minAmount(cfg)
}

// Run starts the HTTP server (if enabled) and processes blocks on every
// enabled chain until ctx is cancelled. With from > 0 it backfills
// [from, to] on the single selected chain and returns.
func (a *App) Run(ctx context.Context, from, to int64) error {
	if a.srv != nil {
		if err := a.srv.Start(ctx); err != nil {
			return fmt.Errorf("http server: %w", err)
		}
		defer func() {
			// ctx is already cancelled here: keep its values, drop the cancel.
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = a.srv.Shutdown(sctx)
		}()
	}
	if a.Alerts != nil && from == 0 {
		actx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { defer close(done); a.Alerts.Run(actx, 5*time.Second) }()
		defer func() { cancel(); <-done }()
	}
	if a.NotifyBot != nil && from == 0 {
		nctx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { defer close(done); _ = a.NotifyBot.Run(nctx) }()
		defer func() { cancel(); <-done }()
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
		if st.LastError != "" && !a.cfg.HTTP.HealthDetails {
			st.LastError = "error (details in the logs; set HTTP_HEALTH_DETAILS=true to show here)"
		}
		statuses[p.Chain] = st
	}
	var pgErr string
	if a.pg != nil {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := a.pg.Ping(pctx); err != nil {
			ok = false
			// Driver errors can name hosts/users: details go to the log only.
			a.log.Warn("health: postgres ping failed", "err", err)
			pgErr = "postgres: unavailable"
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
		list := chainWatchAddresses(cfg, c)
		for _, s := range list {
			if n, err := chainaddr.Normalize(c, s); err == nil {
				out = append(out, n)
			}
		}
	}
	return out
}

// chainWatchAddresses returns the static watch list configured for chain c.
func chainWatchAddresses(cfg config.Config, c string) []string {
	if def, ok := chainDefs[c]; ok {
		return def.watchAddresses(cfg)
	}
	return nil
}

func staticFilterConfig(cfg config.Config, c string, minAmount *big.Int) filter.Config {
	addrs := chainWatchAddresses(cfg, c)
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
