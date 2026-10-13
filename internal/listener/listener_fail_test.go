package listener

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/cursor"
	"github.com/co-codin/USDT-Tracker/internal/metrics"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/sink"
)

// flakyHead fails Head for the first n calls.
type flakyHead struct {
	fakeSource
	hmu   sync.Mutex
	fails int
}

func (f *flakyHead) Head(ctx context.Context) (int64, error) {
	f.hmu.Lock()
	if f.fails > 0 {
		f.fails--
		f.hmu.Unlock()
		return 0, errors.New("node unreachable")
	}
	f.hmu.Unlock()
	return f.fakeSource.Head(ctx)
}

func TestHeadFailuresAreRetriedWithMetrics(t *testing.T) {
	src := &flakyHead{fakeSource: fakeSource{head: 20}, fails: 3}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rec := &recorder{stopAt: 15, cancel: cancel}
	m := metrics.New()
	cfg := fastCfg()
	cfg.Chain = "eth"
	l := New(cfg, src, allFilter(t), rec, &cursor.MemoryStore{}, m, nil)
	cfg.StartBlock = 0
	if err := l.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if st := l.Status(); st.Head != 20 || st.SafeHead != 15 || st.Cursor < 15 || st.LastError != "" {
		t.Fatalf("status %+v", st)
	}
}

func TestHeadFailsMidLoop(t *testing.T) {
	src := &flakyHead{fakeSource: fakeSource{head: 12}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var once sync.Once
	rec := &recorder{stopAt: 1 << 40}
	store := &cursor.MemoryStore{}
	_ = store.Save(ctx, cursor.State{LastBlock: 5})
	d := dispatchFunc(func(ctx context.Context, b sink.Batch) error {
		once.Do(func() {
			src.hmu.Lock()
			src.fails = 2
			src.hmu.Unlock()
		})
		if b.BlockNumber == 7 {
			cancel()
		}
		return rec.Dispatch(ctx, b)
	})
	l := New(fastCfg(), src, allFilter(t), d, store, metrics.New(), nil)
	if err := l.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if st, _ := store.Load(context.Background()); st.LastBlock != 7 {
		t.Fatalf("cursor %d", st.LastBlock)
	}
}

type errStore struct{ cursor.MemoryStore }

func (errStore) Load(context.Context) (cursor.State, error) {
	return cursor.State{}, errors.New("db down")
}

func TestRunCursorLoadErrorAndCancelledStart(t *testing.T) {
	l := New(fastCfg(), &fakeSource{head: 10}, allFilter(t), &recorder{}, &errStore{}, nil, nil)
	if err := l.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "load cursor") {
		t.Fatal(err)
	}
	// Head never answers and ctx ends: Run returns nil (graceful).
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	src := &flakyHead{fakeSource: fakeSource{head: 10}, fails: 1 << 30}
	l = New(fastCfg(), src, allFilter(t), &recorder{}, &cursor.MemoryStore{}, nil, nil)
	if err := l.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if l.Status().LastError != "node unreachable" {
		t.Fatalf("status %+v", l.Status())
	}
}

func TestBackfillErrors(t *testing.T) {
	l := New(fastCfg(), &fakeSource{head: 10}, allFilter(t), &recorder{}, &cursor.MemoryStore{}, nil, nil)
	if err := l.Backfill(context.Background(), 0, 5); err == nil {
		t.Fatal("from 0 must fail")
	}
	if err := l.Backfill(context.Background(), 5, 4); err == nil {
		t.Fatal("to<from must fail")
	}
	if err := l.Backfill(context.Background(), 1, 9); err == nil || !strings.Contains(err.Error(), "beyond safe head") {
		t.Fatal(err)
	}
}

// rangeSrc implements chain.RangeSource.
type rangeSrc struct {
	fakeSource
	mu    sync.Mutex
	short int // return one block too few this many times
	errs  int
	calls int
}

func (r *rangeSrc) Blocks(_ context.Context, from, to int64) ([]chain.Block, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.errs > 0 {
		r.errs--
		return nil, errors.New("getLogs failed")
	}
	var out []chain.Block
	for n := from; n <= to; n++ {
		out = append(out, chain.Block{Number: n, Transfers: []model.Transfer{{TxID: "r", LogIndex: int(n), BlockNumber: n, Amount: big.NewInt(1)}}})
	}
	if r.short > 0 {
		r.short--
		out = out[:len(out)-1]
	}
	return out, nil
}

func TestRangeSourceErrorsAndInconsistency(t *testing.T) {
	src := &rangeSrc{fakeSource: fakeSource{head: 30}, errs: 1, short: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rec := &recorder{stopAt: 25, cancel: cancel}
	store := &cursor.MemoryStore{}
	_ = store.Save(ctx, cursor.State{LastBlock: 9})
	l := New(fastCfg(), src, allFilter(t), rec, store, metrics.New(), nil)
	if err := l.Run(ctx); err != nil {
		t.Fatal(err)
	}
	assertContiguous(t, rec.blocks, 10, 25)
	if src.calls < 3 {
		t.Fatalf("calls %d: error and short range must be retried", src.calls)
	}
	if src.fetched != nil {
		t.Fatal("per-block Block must not be used for a RangeSource")
	}
}

func TestDefaults(t *testing.T) {
	c := Config{Confirmations: -3}
	c.defaults()
	if c.Confirmations != 0 || c.PollInterval != 3*time.Second || c.BatchSize != 20 || c.Concurrency != 2 || c.DispatchTimeout != 30*time.Second || c.chainName() != "tron" {
		t.Fatalf("%+v", c)
	}
}

// A tap that keeps failing while ctx ends: Run returns without saving.
func TestTapFailureUntilShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &cursor.MemoryStore{}
	_ = store.Save(ctx, cursor.State{LastBlock: 9})
	cfg := fastCfg()
	n := 0
	cfg.Taps = []Tap{func(context.Context, chain.Block) error {
		n++
		if n == 3 {
			cancel()
		}
		return errors.New("outbox down")
	}}
	l := New(cfg, &fakeSource{head: 30}, allFilter(t), &recorder{}, store, metrics.New(), nil)
	if err := l.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if st, _ := store.Load(context.Background()); st.LastBlock != 9 {
		t.Fatalf("cursor advanced to %d past an unrecorded block", st.LastBlock)
	}
	if !strings.Contains(l.Status().LastError, "outbox down") {
		t.Fatalf("status %+v", l.Status())
	}
}
