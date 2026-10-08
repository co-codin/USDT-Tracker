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
	"strings"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain/tron"
	"github.com/co-codin/USDT-Tracker/internal/chain/tron/trc20"
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
	Backoff  retry.Backoff // backoff for TRON API and loop retries (zero = defaults)
}

// App is a fully wired listener.
type App struct {
	cfg      config.Config
	opts     Options
	log      *slog.Logger
	Metrics  *metrics.Metrics
	Listener *listener.Listener
	disp     *sink.Dispatcher
	pg       *postgres.Store
	srv      *server.Server
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

	contract, err := tron.ParseAddress(cfg.Token.Contract)
	if err != nil {
		return nil, err
	}
	client := tron.NewClient(tron.Options{
		BaseURL:    cfg.Tron.APIURL,
		APIKey:     cfg.Tron.APIKey,
		Timeout:    cfg.Tron.Timeout.D(),
		RPS:        cfg.Tron.RPS,
		MaxRetries: cfg.Tron.MaxRetries,
		Backoff:    opts.Backoff,
		UserAgent:  "tron-usdt-listener/" + opts.Version,
		Observer:   a.Metrics,
		Logger:     log,
	})
	dec := trc20.New(trc20.Token{Contract: contract, Symbol: cfg.Token.Symbol, Decimals: cfg.Token.Decimals})
	src := trc20.NewSource(client, dec, log, a.Metrics.DecodeErrors.Inc)

	minAmount, err := cfg.MinAmount()
	if err != nil {
		return nil, err
	}
	if _, err := filter.New(staticFilterConfig(cfg, minAmount)); err != nil { // fail fast, before connecting
		return nil, err
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
	matcher, dyn, err := buildFilter(ctx, cfg, minAmount, watchSrc, log)
	if err != nil {
		return nil, errors.Join(err, a.disp.Close())
	}

	var store cursor.Store
	switch {
	case opts.Backfill:
		store = &cursor.MemoryStore{}
	case pg != nil:
		store = pg
	default:
		store = cursor.NewFileStore(cfg.Cursor.File)
	}

	a.Listener = listener.New(listener.Config{
		Confirmations:   cfg.Listener.Confirmations,
		PollInterval:    cfg.Listener.PollInterval.D(),
		StartBlock:      cfg.Listener.StartBlock,
		BatchSize:       cfg.Listener.BatchSize,
		Concurrency:     cfg.Listener.Concurrency,
		DispatchTimeout: cfg.Listener.DispatchTimeout.D(),
		Backoff:         opts.Backoff,
	}, src, matcher, a.disp, store, a.Metrics, log)

	if cfg.HTTP.Addr != "" && !opts.Backfill {
		a.srv = server.New(cfg.HTTP.Addr, a.Metrics.Handler(), a.health, log)
		var ws watch.Store // nil interface (not a typed nil) when Postgres is off → 503
		if pg != nil {
			ws = pg.Watchlist()
		}
		a.srv.Handle("/v1/", watch.NewAPI(watch.APIConfig{
			Token: cfg.API.Token, Store: ws, Static: canonicalAddresses(cfg.Filter.WatchAddresses),
			OnChange: func() {
				if dyn != nil {
					dyn.Invalidate()
				}
			},
			Log: log,
		}))
	}

	if cfg.Tron.APIKey == "" {
		log.Warn("TRON_PRO_API_KEY not set: using public rate limits, keep tron.rps low")
	}
	log.Info("tron-usdt-listener configured",
		"version", opts.Version,
		"api_url", cfg.Tron.APIURL,
		"api_key_set", cfg.Tron.APIKey != "",
		"token", cfg.Token.Symbol,
		"contract", contract.String(),
		"sinks", strings.Join(a.disp.Names(), ","),
		"cursor", a.cursorDesc(),
		"watch_addresses", len(cfg.Filter.WatchAddresses),
		"runtime_watch_list", pg != nil,
		"admin_api", cfg.API.Token != "" && cfg.HTTP.Addr != "",
		"min_amount", cfg.Filter.MinAmount,
	)
	return a, nil
}

// Run starts the HTTP server (if enabled) and processes blocks until ctx is
// cancelled. With from > 0 it backfills [from, to] and returns.
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
	return a.Listener.Run(ctx)
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
	st := a.Listener.Status()
	ok := time.Since(st.LastProgressAt) <= a.cfg.HTTP.StaleAfter.D()
	if a.pg != nil {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := a.pg.Ping(pctx); err != nil {
			ok = false
			st.LastError = "postgres: " + err.Error()
		}
	}
	return ok, st
}

func (a *App) cursorDesc() string {
	switch {
	case a.opts.Backfill:
		return "memory (backfill)"
	case a.pg != nil:
		return "postgres:" + a.cfg.Sinks.Postgres.CursorName
	default:
		return "file:" + a.cfg.Cursor.File
	}
}

// canonicalAddresses converts validated config addresses to base58.
func canonicalAddresses(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if a, err := tron.ParseAddress(s); err == nil {
			out = append(out, a.String())
		}
	}
	return out
}

func staticFilterConfig(cfg config.Config, minAmount *big.Int) filter.Config {
	return filter.Config{
		WatchAddresses: cfg.Filter.WatchAddresses,
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
func buildFilter(ctx context.Context, cfg config.Config, minAmount *big.Int, src watch.Source, log *slog.Logger) (listener.Matcher, *filter.Dynamic, error) {
	fCfg := staticFilterConfig(cfg, minAmount)
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
