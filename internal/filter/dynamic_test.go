package filter

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/watch"
)

// countingSource wraps a MemoryStore and counts loads.
type countingSource struct {
	*watch.MemoryStore
	mu    sync.Mutex
	loads int
}

func (c *countingSource) EnabledWatches(ctx context.Context) ([]watch.Entry, error) {
	c.mu.Lock()
	c.loads++
	c.mu.Unlock()
	return c.MemoryStore.EnabledWatches(ctx)
}

func (c *countingSource) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loads
}

type fakeClock struct{ t time.Time }

func (f *fakeClock) now() time.Time      { return f.t }
func (f *fakeClock) add(d time.Duration) { f.t = f.t.Add(d) }
func newDyn(t *testing.T, cfg Config, src watch.Source) (*Dynamic, *fakeClock) {
	t.Helper()
	d, err := NewDynamic(cfg, src, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	d.now = clk.now
	return d, clk
}

func matched(f interface {
	Apply([]model.Transfer) []model.Transfer
}, ts ...model.Transfer) int {
	return len(f.Apply(ts))
}

// The core requirement: an address added to the store at runtime is picked
// up by the same filter instance (no restart), merged with static config.
func TestDynamicPicksUpNewAddressWithoutRestart(t *testing.T) {
	ctx := context.Background()
	store := &countingSource{MemoryStore: watch.NewMemoryStore()}
	d, clk := newDyn(t, Config{WatchAddresses: []string{alice}, Dynamic: true}, store)

	if err := d.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if matched(d, tr(carol, bob, 5)) != 0 {
		t.Fatal("bob is not watched yet")
	}
	if matched(d, tr(carol, alice, 5)) != 1 {
		t.Fatal("static address must match")
	}

	if _, err := store.AddWatch(ctx, watch.Entry{Address: bob, Direction: "incoming", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	// Within the reload interval the cached snapshot is reused...
	clk.add(time.Second)
	_ = d.Refresh(ctx)
	if store.count() != 1 || matched(d, tr(carol, bob, 5)) != 0 {
		t.Fatalf("expected cached snapshot, loads=%d", store.count())
	}
	// ...and after it the new address applies.
	clk.add(5 * time.Second)
	if err := d.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if store.count() != 2 {
		t.Fatalf("loads = %d, want 2", store.count())
	}
	out := d.Apply([]model.Transfer{tr(carol, bob, 5), tr(bob, carol, 5), tr(carol, alice, 5)})
	if len(out) != 2 || out[0].To != bob || !reflect.DeepEqual(out[0].Reasons, []string{model.ReasonIncoming}) || out[1].To != alice {
		t.Fatalf("merged filter output %+v (bob is incoming-only, alice static)", out)
	}
	if d.Loaded() != 1 || d.Current().WatchCount() != 2 {
		t.Fatalf("loaded=%d watched=%d", d.Loaded(), d.Current().WatchCount())
	}
}

func TestDynamicInvalidateReloadsImmediately(t *testing.T) {
	ctx := context.Background()
	store := &countingSource{MemoryStore: watch.NewMemoryStore()}
	d, _ := newDyn(t, Config{Dynamic: true}, store)
	_ = d.Refresh(ctx)
	_, _ = store.AddWatch(ctx, watch.Entry{Address: dave, Enabled: true})
	d.Invalidate()
	if err := d.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if matched(d, tr(dave, carol, 1)) != 1 {
		t.Fatal("invalidated filter must see the new address without waiting for the interval")
	}
	// Removal and disabling are picked up the same way.
	_ = store.DeleteWatch(ctx, dave)
	d.Invalidate()
	_ = d.Refresh(ctx)
	if matched(d, tr(dave, carol, 1)) != 0 {
		t.Fatal("removed address still matches")
	}
}

func TestDynamicEmptyListIsNotFirehose(t *testing.T) {
	ctx := context.Background()
	d, _ := newDyn(t, Config{Dynamic: true}, watch.NewMemoryStore())
	_ = d.Refresh(ctx)
	if d.MatchesEverything() || matched(d, tr(carol, dave, 1)) != 0 {
		t.Fatal("dynamic mode with an empty list must match nothing")
	}
	// Large-transfer rule still works in any mode.
	d2, _ := newDyn(t, Config{Dynamic: true, MinAmount: usdt(1000)}, watch.NewMemoryStore())
	_ = d2.Refresh(ctx)
	if matched(d2, tr(carol, dave, 5000)) != 1 {
		t.Fatal("large transfer must match")
	}
	// Mode all with an empty dynamic list: nothing is watched → nothing matches.
	d3, _ := newDyn(t, Config{Dynamic: true, MinAmount: usdt(1000), Mode: All}, watch.NewMemoryStore())
	_ = d3.Refresh(ctx)
	if matched(d3, tr(carol, dave, 5000)) != 0 {
		t.Fatal("mode all with empty dynamic list must match nothing")
	}
	// Without Dynamic an empty merged list keeps the legacy firehose behaviour.
	d4, _ := newDyn(t, Config{}, watch.NewMemoryStore())
	_ = d4.Refresh(ctx)
	if !d4.MatchesEverything() {
		t.Fatal("non-dynamic empty filter should stay in firehose mode")
	}
}

func TestDynamicDisabledRowsIgnored(t *testing.T) {
	ctx := context.Background()
	store := watch.NewMemoryStore()
	_, _ = store.AddWatch(ctx, watch.Entry{Address: bob, Enabled: true})
	_ = store.SetEnabled(bob, false)
	d, _ := newDyn(t, Config{Dynamic: true}, store)
	_ = d.Refresh(ctx)
	if matched(d, tr(alice, bob, 1)) != 0 {
		t.Fatal("disabled row must not match")
	}
}

func TestDynamicDirectionMerge(t *testing.T) {
	ctx := context.Background()
	store := watch.NewMemoryStore()
	_, _ = store.AddWatch(ctx, watch.Entry{Address: alice, Direction: "outgoing", Enabled: true})
	// Static alice is incoming-only; the DB row adds outgoing → union = both.
	d, _ := newDyn(t, Config{WatchAddresses: []string{alice}, Direction: Incoming}, store)
	_ = d.Refresh(ctx)
	if matched(d, tr(carol, alice, 1)) != 1 || matched(d, tr(alice, carol, 1)) != 1 {
		t.Fatal("static incoming + runtime outgoing must match both directions")
	}
}

func TestDynamicRefreshErrorKeepsSnapshotAndRetries(t *testing.T) {
	ctx := context.Background()
	store := &countingSource{MemoryStore: watch.NewMemoryStore()}
	_, _ = store.AddWatch(ctx, watch.Entry{Address: bob, Enabled: true})
	d, clk := newDyn(t, Config{Dynamic: true}, store)
	if err := d.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	store.Err = errors.New("db down")
	clk.add(10 * time.Second)
	if err := d.Refresh(ctx); err == nil {
		t.Fatal("expected error")
	}
	if matched(d, tr(alice, bob, 1)) != 1 {
		t.Fatal("previous snapshot must be kept on error")
	}
	// Invalidation survives a failed reload.
	d.Invalidate()
	if err := d.Refresh(ctx); err == nil {
		t.Fatal("expected error")
	}
	store.Err = nil
	n := store.count()
	if err := d.Refresh(ctx); err != nil || store.count() != n+1 {
		t.Fatalf("expected a reload after recovery: %v loads=%d", err, store.count())
	}
}

func TestNewDynamicValidation(t *testing.T) {
	if _, err := NewDynamic(Config{}, nil, 0); err == nil {
		t.Error("nil source must be rejected")
	}
	if _, err := NewDynamic(Config{Mode: "some"}, watch.NewMemoryStore(), 0); err == nil {
		t.Error("invalid config must be rejected")
	}
	d, err := NewDynamic(Config{}, watch.NewMemoryStore(), 0)
	if err != nil || d.interval != DefaultReloadInterval || d.Loaded() != -1 {
		t.Fatalf("defaults: %v %+v", err, d)
	}
	// A bad row (should never happen: the API and DB constraint validate) fails the reload.
	bad := watch.NewMemoryStore()
	_, _ = bad.AddWatch(context.Background(), watch.Entry{Address: "Tbad", Enabled: true})
	d, _ = NewDynamic(Config{}, bad, 0)
	if err := d.Refresh(context.Background()); err == nil {
		t.Error("invalid stored address must fail the reload")
	}
	if _, ok := d.Match(tr(alice, bob, 1)); !ok {
		t.Error("initial static snapshot (firehose) should still be in place")
	}
}

func TestConcurrentApplyAndRefresh(t *testing.T) {
	ctx := context.Background()
	store := watch.NewMemoryStore()
	d, err := NewDynamic(Config{Dynamic: true}, store, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				d.Invalidate()
				_ = d.Refresh(ctx)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = d.Apply([]model.Transfer{tr(alice, bob, 1)})
			}
		}
	}()
	for _, a := range []string{alice, bob, carol, dave} {
		_, _ = store.AddWatch(ctx, watch.Entry{Address: a, Enabled: true})
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	d.Invalidate()
	_ = d.Refresh(ctx)
	if d.Current().WatchCount() != 4 {
		t.Fatalf("watched = %d", d.Current().WatchCount())
	}
}

func TestDynamicLogsReloadAndRejectsBadRows(t *testing.T) {
	ctx := context.Background()
	var buf strings.Builder
	store := watch.NewMemoryStore()
	d, _ := newDyn(t, Config{Dynamic: true}, store)
	d.SetLogger(nil) // ignored
	d.SetLogger(slog.New(slog.NewTextHandler(&buf, nil)))
	_, _ = store.AddWatch(ctx, watch.Entry{Address: alice, Enabled: true})
	_ = d.Refresh(ctx)
	if !strings.Contains(buf.String(), "watch list reloaded") || !strings.Contains(buf.String(), "runtime_addresses=1") {
		t.Fatalf("log: %q", buf.String())
	}
	// A stored row with an unknown direction fails the reload (DB has a CHECK; defence in depth).
	_, _ = store.AddWatch(ctx, watch.Entry{Address: bob, Direction: "sideways", Enabled: true})
	d.Invalidate()
	if err := d.Refresh(ctx); err == nil || !strings.Contains(err.Error(), "direction") {
		t.Fatalf("want direction error, got %v", err)
	}
}

func TestNewWithWatchesSkipsBlankAddresses(t *testing.T) {
	f, err := NewWithWatches(Config{WatchAddresses: []string{" ", ""}}, []Watch{{Address: " "}})
	if err != nil || f.WatchCount() != 0 || !f.MatchesEverything() {
		t.Fatalf("blank addresses must be ignored: %v %d", err, f.WatchCount())
	}
}
