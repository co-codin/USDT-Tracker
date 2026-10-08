// Package listener runs the block-range processing loop:
//
//	head → safe head (head - confirmations) → fetch blocks [cursor+1 .. safe]
//	→ decode → filter → dispatch to sinks → persist cursor
//
// Blocks are fetched concurrently but delivered strictly in order, and the
// cursor only advances after every required sink accepted a block, which
// gives at-least-once delivery without gaps across restarts.
package listener

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/cursor"
	"github.com/co-codin/USDT-Tracker/internal/metrics"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/retry"
	"github.com/co-codin/USDT-Tracker/internal/sink"
)

// Config tunes the processing loop.
type Config struct {
	Confirmations   int64         // blocks to stay behind head (default 20 ≈ 60s, i.e. solidified)
	PollInterval    time.Duration // how often to poll head when caught up (default 3s = TRON block time)
	StartBlock      int64         // used only when no cursor exists; see cursor.NextBlock
	BatchSize       int           // max blocks fetched per iteration (default 20)
	Concurrency     int           // parallel block fetches (default 2)
	DispatchTimeout time.Duration // per delivery attempt; in-flight blocks finish on shutdown (default 30s)
	Backoff         retry.Backoff // backoff for loop-level errors
}

func (c *Config) defaults() {
	if c.Confirmations < 0 {
		c.Confirmations = 0
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 3 * time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 20
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 2
	}
	if c.DispatchTimeout <= 0 {
		c.DispatchTimeout = 30 * time.Second
	}
}

// Matcher selects the transfers to deliver (*filter.Filter or *filter.Dynamic).
type Matcher interface {
	Apply(in []model.Transfer) []model.Transfer
	MatchesEverything() bool
}

// Refresher is implemented by matchers whose rules change at runtime. Refresh
// is called before every block; an error fails the block (it is retried and
// the cursor does not advance), so no block is filtered with a watch list
// that could not be reloaded.
type Refresher interface {
	Refresh(ctx context.Context) error
}

// Dispatcher delivers a batch to sinks.
type Dispatcher interface {
	Dispatch(ctx context.Context, b sink.Batch) error
}

// Status is a snapshot used by /healthz.
type Status struct {
	Head           int64     `json:"head"`
	SafeHead       int64     `json:"safe_head"`
	Cursor         int64     `json:"cursor"`
	Lag            int64     `json:"lag"`
	LastProgressAt time.Time `json:"last_progress_at"`
	LastError      string    `json:"last_error,omitempty"`
	StartedAt      time.Time `json:"started_at"`
}

// Listener is the main processing loop.
type Listener struct {
	cfg    Config
	src    chain.Source
	filter Matcher
	disp   Dispatcher
	store  cursor.Store
	m      *metrics.Metrics
	log    *slog.Logger

	mu     sync.RWMutex
	status Status
}

// New creates a listener. m may be nil.
func New(cfg Config, src chain.Source, f Matcher, d Dispatcher, store cursor.Store, m *metrics.Metrics, log *slog.Logger) *Listener {
	cfg.defaults()
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	now := time.Now()
	return &Listener{cfg: cfg, src: src, filter: f, disp: d, store: store, m: m, log: log,
		status: Status{StartedAt: now, LastProgressAt: now}}
}

// Status returns a copy of the current status.
func (l *Listener) Status() Status {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.status
}

// Run processes blocks until ctx is cancelled, resuming from the persisted
// cursor. It returns nil on graceful shutdown.
func (l *Listener) Run(ctx context.Context) error {
	st, err := l.store.Load(ctx)
	if err != nil {
		return fmt.Errorf("load cursor: %w", err)
	}
	safe, err := l.waitSafeHead(ctx)
	if err != nil {
		return nil // context cancelled
	}
	next := cursor.NextBlock(st, l.cfg.StartBlock, safe)
	l.log.Info("listener starting",
		"resumed", st.LastBlock > 0, "cursor", st.LastBlock, "next_block", next, "safe_head", safe,
		"confirmations", l.cfg.Confirmations, "filter_all", l.filter.MatchesEverything())
	return l.loop(ctx, next, 0, true)
}

// Backfill processes the inclusive range [from, to] once without touching
// the persisted cursor, then returns.
func (l *Listener) Backfill(ctx context.Context, from, to int64) error {
	if from <= 0 || to < from {
		return fmt.Errorf("invalid backfill range %d..%d", from, to)
	}
	l.log.Info("backfill starting", "from", from, "to", to)
	return l.loop(ctx, from, to, false)
}

func (l *Listener) loop(ctx context.Context, next, end int64, persist bool) error {
	failures := 0
	for {
		if ctx.Err() != nil {
			return nil
		}
		if end > 0 && next > end {
			l.log.Info("backfill done", "last_block", end)
			return nil
		}
		safe, err := l.safeHead(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			l.fail(err)
			l.log.Error("fetch head failed", "err", err)
			if retry.Sleep(ctx, l.cfg.Backoff.Delay(failures)) != nil {
				return nil
			}
			failures++
			continue
		}
		if next > safe {
			if end > 0 {
				return fmt.Errorf("backfill range end %d is beyond safe head %d", end, safe)
			}
			l.progress(next - 1)
			if retry.Sleep(ctx, l.cfg.PollInterval) != nil {
				return nil
			}
			continue
		}
		to := min(safe, next+int64(l.cfg.BatchSize)-1)
		if end > 0 {
			to = min(to, end)
		}

		blocks, ferr := l.fetchRange(ctx, next, to)
		for _, b := range blocks {
			if err := l.process(ctx, b, persist); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				ferr = err
				break
			}
			next = b.Number + 1
		}
		if ferr != nil {
			if ctx.Err() != nil {
				return nil
			}
			failures++
			l.fail(ferr)
			if l.m != nil {
				l.m.BlockErrors.Inc()
			}
			l.log.Error("block processing failed, will retry", "block", next, "err", ferr)
			if retry.Sleep(ctx, l.cfg.Backoff.Delay(failures-1)) != nil {
				return nil
			}
			continue
		}
		failures = 0
		if next > safe && end == 0 {
			if retry.Sleep(ctx, l.cfg.PollInterval) != nil {
				return nil
			}
		}
	}
}

// fetchRange fetches [from, to] concurrently and returns the longest
// contiguous prefix that succeeded, plus the first error (if any).
func (l *Listener) fetchRange(ctx context.Context, from, to int64) ([]chain.Block, error) {
	n := int(to - from + 1)
	blocks := make([]chain.Block, n)
	errs := make([]error, n)
	var g errgroup.Group
	g.SetLimit(l.cfg.Concurrency)
	for i := range n {
		g.Go(func() error {
			b, err := l.src.Block(ctx, from+int64(i))
			blocks[i], errs[i] = b, err
			return nil
		})
	}
	_ = g.Wait()
	for i, err := range errs {
		if err != nil {
			return blocks[:i], fmt.Errorf("block %d: %w", from+int64(i), err)
		}
	}
	return blocks, nil
}

// process filters and delivers one block, then persists the cursor. Delivery
// is retried until all required sinks succeed; on shutdown an in-flight block
// is allowed to finish (bounded by DispatchTimeout).
func (l *Listener) process(ctx context.Context, b chain.Block, persist bool) error {
	if r, ok := l.filter.(Refresher); ok {
		if err := r.Refresh(ctx); err != nil {
			return fmt.Errorf("block %d: %w", b.Number, err)
		}
	}
	matched := l.filter.Apply(b.Transfers)
	if l.m != nil {
		l.m.TransfersDecoded.Add(float64(len(b.Transfers)))
		for _, t := range matched {
			for _, r := range t.Reasons {
				l.m.TransfersMatched.WithLabelValues(r).Inc()
			}
		}
	}
	batch := sink.Batch{BlockNumber: b.Number, BlockTime: b.Time, Transfers: matched}
	for attempt := 0; ; attempt++ {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), l.cfg.DispatchTimeout)
		err := l.disp.Dispatch(dctx, batch)
		cancel()
		if err == nil {
			break
		}
		if l.m != nil {
			l.m.DispatchRetries.Inc()
		}
		l.fail(err)
		l.log.Error("delivery failed, retrying block", "block", b.Number, "attempt", attempt+1, "err", err)
		if err := retry.Sleep(ctx, l.cfg.Backoff.Delay(attempt)); err != nil {
			return err
		}
	}
	if persist {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		err := l.store.Save(sctx, cursor.State{LastBlock: b.Number, UpdatedAt: time.Now().UTC()})
		cancel()
		if err != nil {
			return fmt.Errorf("save cursor: %w", err)
		}
	}
	l.progress(b.Number)
	if l.m != nil {
		l.m.BlocksProcessed.Inc()
		l.m.LastProcessedTime.SetToCurrentTime()
	}
	l.log.Debug("block processed", "block", b.Number, "txs", b.TxCount, "transfers", len(b.Transfers), "matched", len(matched))
	return nil
}

func (l *Listener) safeHead(ctx context.Context) (int64, error) {
	head, err := l.src.Head(ctx)
	if err != nil {
		return 0, err
	}
	safe := head - l.cfg.Confirmations
	l.mu.Lock()
	l.status.Head, l.status.SafeHead = head, safe
	l.status.Lag = max(0, safe-l.status.Cursor)
	l.mu.Unlock()
	if l.m != nil {
		l.m.HeadBlock.Set(float64(head))
		l.m.SafeHeadBlock.Set(float64(safe))
	}
	return safe, nil
}

func (l *Listener) waitSafeHead(ctx context.Context) (int64, error) {
	for attempt := 0; ; attempt++ {
		safe, err := l.safeHead(ctx)
		if err == nil {
			return safe, nil
		}
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		l.fail(err)
		l.log.Error("fetch head failed", "err", err)
		if err := retry.Sleep(ctx, l.cfg.Backoff.Delay(attempt)); err != nil {
			return 0, err
		}
	}
}

func (l *Listener) progress(cursorBlock int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if cursorBlock > l.status.Cursor {
		l.status.Cursor = cursorBlock
	}
	l.status.Lag = max(0, l.status.SafeHead-l.status.Cursor)
	l.status.LastProgressAt = time.Now()
	l.status.LastError = ""
	if l.m != nil {
		l.m.CursorBlock.Set(float64(l.status.Cursor))
		l.m.LagBlocks.Set(float64(l.status.Lag))
	}
}

func (l *Listener) fail(err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	l.mu.Lock()
	l.status.LastError = err.Error()
	l.mu.Unlock()
}
