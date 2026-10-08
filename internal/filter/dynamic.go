package filter

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/watch"
)

// DefaultReloadInterval is how long a loaded runtime watch list is reused.
const DefaultReloadInterval = 5 * time.Second

// Dynamic is a Filter whose watch list is the static config merged with the
// enabled rows of a watch.Source, reloaded at most every interval (or on
// the next Refresh after Invalidate). Apply/Match never block on I/O: they
// use the last successfully loaded snapshot. It is safe for concurrent use.
type Dynamic struct {
	cfg      Config
	src      watch.Source
	interval time.Duration
	now      func() time.Time
	log      *slog.Logger

	cur   atomic.Pointer[Filter]
	dirty atomic.Bool // set by Invalidate

	mu       sync.Mutex // serialises reloads
	loadedAt time.Time  // zero = never loaded from src
	lastN    int
}

// NewDynamic validates cfg and returns a Dynamic filter that starts with the
// static addresses only; the first Refresh loads src. interval <= 0 means
// DefaultReloadInterval.
func NewDynamic(cfg Config, src watch.Source, interval time.Duration) (*Dynamic, error) {
	if src == nil {
		return nil, fmt.Errorf("filter: dynamic watch list needs a source")
	}
	f, err := New(cfg)
	if err != nil {
		return nil, err
	}
	if interval <= 0 {
		interval = DefaultReloadInterval
	}
	d := &Dynamic{cfg: cfg, src: src, interval: interval, now: time.Now, lastN: -1, log: slog.New(slog.DiscardHandler)}
	d.cur.Store(f)
	return d, nil
}

// Refresh reloads the watch list if it was never loaded, was invalidated,
// or is older than the reload interval. On error the previous snapshot is
// kept and the error returned, so callers can retry before using a list
// that may be stale.
func (d *Dynamic) Refresh(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	invalidated := d.dirty.Swap(false)
	if !invalidated && !d.loadedAt.IsZero() && d.now().Sub(d.loadedAt) < d.interval {
		return nil
	}
	if err := d.reload(ctx); err != nil {
		if invalidated {
			d.dirty.Store(true)
		}
		return err
	}
	return nil
}

func (d *Dynamic) reload(ctx context.Context) error {
	rows, err := d.src.EnabledWatches(ctx)
	if err != nil {
		return fmt.Errorf("filter: load watch list: %w", err)
	}
	extra := make([]Watch, 0, len(rows))
	for _, r := range rows {
		extra = append(extra, Watch{Address: r.Address, Direction: Direction(r.Direction)})
	}
	f, err := NewWithWatches(d.cfg, extra)
	if err != nil {
		return err
	}
	d.cur.Store(f)
	d.loadedAt = d.now()
	if len(rows) != d.lastN {
		d.log.Info("watch list reloaded", "runtime_addresses", len(rows), "total_watched", f.WatchCount())
	}
	d.lastN = len(rows)
	return nil
}

// SetLogger sets the logger used to report watch-list changes. Call it
// before the filter is shared.
func (d *Dynamic) SetLogger(l *slog.Logger) {
	if l != nil {
		d.log = l
	}
}

// Invalidate forces the next Refresh to reload (called after API writes so a
// newly added address applies to the very next processed block).
func (d *Dynamic) Invalidate() { d.dirty.Store(true) }

// Loaded returns the number of enabled runtime rows in the current snapshot
// (-1 before the first successful load).
func (d *Dynamic) Loaded() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastN
}

// Current returns the snapshot used by Apply/Match.
func (d *Dynamic) Current() *Filter { return d.cur.Load() }

// MatchesEverything reports whether the current snapshot is in firehose mode.
func (d *Dynamic) MatchesEverything() bool { return d.cur.Load().MatchesEverything() }

// Match matches t against the current snapshot.
func (d *Dynamic) Match(t model.Transfer) ([]string, bool) { return d.cur.Load().Match(t) }

// Apply filters in against the current snapshot.
func (d *Dynamic) Apply(in []model.Transfer) []model.Transfer { return d.cur.Load().Apply(in) }
