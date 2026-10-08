package listener

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/cursor"
	"github.com/co-codin/USDT-Tracker/internal/filter"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/retry"
	"github.com/co-codin/USDT-Tracker/internal/sink"
	"github.com/co-codin/USDT-Tracker/internal/watch"
)

// fakeSource serves one transfer per block; failOnce makes the first fetch
// of the listed blocks fail.
type fakeSource struct {
	mu       sync.Mutex
	head     int64
	failOnce map[int64]bool
	fetched  map[int64]int
}

func (f *fakeSource) Head(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.head, nil
}

func (f *fakeSource) Block(_ context.Context, n int64) (chain.Block, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fetched == nil {
		f.fetched = map[int64]int{}
	}
	f.fetched[n]++
	if n > f.head {
		return chain.Block{}, errors.New("block not produced yet")
	}
	if f.failOnce[n] {
		delete(f.failOnce, n)
		return chain.Block{}, errors.New("transient")
	}
	return chain.Block{Number: n, Transfers: []model.Transfer{{TxID: "tx", LogIndex: int(n), BlockNumber: n, Amount: big.NewInt(n)}}}, nil
}

// recorder collects delivered block numbers and stops the run at stopAt.
type recorder struct {
	mu     sync.Mutex
	blocks []int64
	stopAt int64
	cancel context.CancelFunc
	failAt map[int64]int
}

func (r *recorder) Dispatch(_ context.Context, b sink.Batch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failAt[b.BlockNumber] > 0 {
		r.failAt[b.BlockNumber]--
		return errors.New("sink down")
	}
	r.blocks = append(r.blocks, b.BlockNumber)
	if b.BlockNumber >= r.stopAt && r.cancel != nil {
		r.cancel()
	}
	return nil
}

func fastCfg() Config {
	return Config{Confirmations: 5, PollInterval: time.Millisecond, BatchSize: 4, Concurrency: 3,
		Backoff: retry.Backoff{Initial: time.Millisecond, Max: 2 * time.Millisecond}}
}

func allFilter(t *testing.T) *filter.Filter {
	f, err := filter.New(filter.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func assertContiguous(t *testing.T, got []int64, from, to int64) {
	t.Helper()
	if len(got) != int(to-from+1) {
		t.Fatalf("delivered %v, want contiguous %d..%d", got, from, to)
	}
	for i, b := range got {
		if b != from+int64(i) {
			t.Fatalf("delivered %v, want contiguous %d..%d", got, from, to)
		}
	}
}

func TestRunResumesFromCursorWithoutGaps(t *testing.T) {
	src := &fakeSource{head: 125, failOnce: map[int64]bool{103: true, 110: true}}
	store := &cursor.MemoryStore{}
	_ = store.Save(context.Background(), cursor.State{LastBlock: 99})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rec := &recorder{stopAt: 120, cancel: cancel, failAt: map[int64]int{105: 2}}

	l := New(fastCfg(), src, allFilter(t), rec, store, nil, nil)
	if err := l.Run(ctx); err != nil {
		t.Fatal(err)
	}
	// safe head = 125 - 5 = 120; must deliver 100..120 in order exactly once,
	// despite fetch failures (103, 110) and a sink failure (105).
	assertContiguous(t, rec.blocks, 100, 120)
	st, _ := store.Load(context.Background())
	if st.LastBlock != 120 {
		t.Fatalf("cursor = %d, want 120", st.LastBlock)
	}
	if s := l.Status(); s.Cursor != 120 || s.Head != 125 || s.SafeHead != 120 {
		t.Fatalf("unexpected status %+v", s)
	}
}

func TestRunRespectsConfirmations(t *testing.T) {
	src := &fakeSource{head: 50}
	store := &cursor.MemoryStore{}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	rec := &recorder{stopAt: 1 << 62}
	cfg := fastCfg()
	cfg.StartBlock = 40
	l := New(cfg, src, allFilter(t), rec, store, nil, nil)
	_ = l.Run(ctx)
	assertContiguous(t, rec.blocks, 40, 45) // never beyond head-confirmations
	for n := range src.fetched {
		if n > 45 {
			t.Fatalf("fetched block %d beyond safe head", n)
		}
	}
}

func TestBackfillDoesNotTouchCursor(t *testing.T) {
	src := &fakeSource{head: 1000}
	rec := &recorder{stopAt: 1 << 62}
	store := &cursor.MemoryStore{}
	l := New(fastCfg(), src, allFilter(t), rec, store, nil, nil)
	if err := l.Backfill(context.Background(), 10, 19); err != nil {
		t.Fatal(err)
	}
	assertContiguous(t, rec.blocks, 10, 19)
	if st, _ := store.Load(context.Background()); st.LastBlock != 0 {
		t.Fatal("backfill must not persist the cursor")
	}
	if err := l.Backfill(context.Background(), 990, 999); err == nil {
		t.Fatal("expected error for range beyond safe head")
	}
}

func TestFilterIsApplied(t *testing.T) {
	src := &fakeSource{head: 100}
	f, _ := filter.New(filter.Config{MinAmount: big.NewInt(93)}) // fake amount == block number
	var got []sink.Batch
	disp := dispatchFunc(func(_ context.Context, b sink.Batch) error { got = append(got, b); return nil })
	l := New(fastCfg(), src, f, disp, &cursor.MemoryStore{}, nil, nil)
	if err := l.Backfill(context.Background(), 90, 95); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, b := range got {
		matched += len(b.Transfers)
	}
	if matched != 3 { // blocks 93, 94, 95
		t.Fatalf("matched %d transfers, want 3", matched)
	}
}

type dispatchFunc func(context.Context, sink.Batch) error

func (f dispatchFunc) Dispatch(ctx context.Context, b sink.Batch) error { return f(ctx, b) }

// refreshingMatcher passes everything through and fails Refresh failFirst times.
type refreshingMatcher struct {
	mu        sync.Mutex
	failFirst int
	calls     int
}

func (m *refreshingMatcher) Refresh(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.failFirst > 0 {
		m.failFirst--
		return errors.New("watch list unavailable")
	}
	return nil
}

func (m *refreshingMatcher) Apply(in []model.Transfer) []model.Transfer { return in }
func (m *refreshingMatcher) MatchesEverything() bool                    { return true }

func TestRefreshCalledPerBlockAndFailureRetriesBlock(t *testing.T) {
	src := &fakeSource{head: 100}
	rec := &recorder{stopAt: 1 << 62}
	m := &refreshingMatcher{failFirst: 2}
	l := New(fastCfg(), src, m, rec, &cursor.MemoryStore{}, nil, nil)
	if err := l.Backfill(context.Background(), 10, 15); err != nil {
		t.Fatal(err)
	}
	// Block 10 is retried until the watch list loads; nothing is skipped or duplicated.
	assertContiguous(t, rec.blocks, 10, 15)
	if m.calls != 6+2 {
		t.Fatalf("Refresh calls = %d, want 8 (6 blocks + 2 failures)", m.calls)
	}
}

func TestDynamicFilterAddressAddedWhileRunning(t *testing.T) {
	// Transfers in fakeSource have empty From/To; give block >= 30 a watched recipient.
	const bob = "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv"
	src := &recipientSource{fakeSource: &fakeSource{head: 100}, to: bob}
	store := watch.NewMemoryStore()
	dyn, err := filter.NewDynamic(filter.Config{Dynamic: true}, store, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var matched []int64
	disp := dispatchFunc(func(_ context.Context, b sink.Batch) error {
		mu.Lock()
		defer mu.Unlock()
		if len(b.Transfers) > 0 {
			matched = append(matched, b.BlockNumber)
		}
		if b.BlockNumber == 24 { // simulate an API call between blocks 24 and 25
			_, _ = store.AddWatch(context.Background(), watch.Entry{Address: bob, Enabled: true})
			dyn.Invalidate()
		}
		return nil
	})
	l := New(fastCfg(), src, dyn, disp, &cursor.MemoryStore{}, nil, nil)
	if err := l.Backfill(context.Background(), 20, 30); err != nil {
		t.Fatal(err)
	}
	assertContiguous(t, matched, 25, 30)
}

type recipientSource struct {
	*fakeSource
	to string
}

func (r *recipientSource) Block(ctx context.Context, n int64) (chain.Block, error) {
	b, err := r.fakeSource.Block(ctx, n)
	for i := range b.Transfers {
		b.Transfers[i].To = r.to
	}
	return b, err
}
