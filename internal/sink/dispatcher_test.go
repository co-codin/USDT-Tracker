package sink

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"

	"github.com/co-codin/USDT-Tracker/internal/model"
)

type fakeSink struct {
	name  string
	mu    sync.Mutex
	fails int // fail this many times before succeeding
	got   []string
	calls int
}

func (f *fakeSink) Name() string { return f.name }
func (f *fakeSink) Close() error { return nil }
func (f *fakeSink) Send(_ context.Context, b Batch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fails > 0 {
		f.fails--
		return errors.New("boom")
	}
	for _, t := range b.Transfers {
		f.got = append(f.got, t.Key())
	}
	return nil
}

func batchOf(keys ...int) Batch {
	b := Batch{BlockNumber: 1}
	for _, k := range keys {
		b.Transfers = append(b.Transfers, model.Transfer{TxID: "tx", LogIndex: k, Amount: big.NewInt(1)})
	}
	return b
}

func TestDispatcherRetriesOnlyFailedSink(t *testing.T) {
	good := &fakeSink{name: "good"}
	flaky := &fakeSink{name: "flaky", fails: 1}
	d := NewDispatcher([]Entry{{Sink: good}, {Sink: flaky}}, 100, nil, nil)

	if err := d.Dispatch(context.Background(), batchOf(0, 1)); err == nil {
		t.Fatal("expected error from flaky required sink")
	}
	// Retry of the same block: good must not receive duplicates.
	if err := d.Dispatch(context.Background(), batchOf(0, 1)); err != nil {
		t.Fatal(err)
	}
	if len(good.got) != 2 || good.calls != 1 {
		t.Fatalf("good sink got %v in %d calls, want exactly 2 keys once", good.got, good.calls)
	}
	if len(flaky.got) != 2 || flaky.calls != 2 {
		t.Fatalf("flaky sink got %v in %d calls", flaky.got, flaky.calls)
	}
	// Overlapping replay (e.g. restart mid-block) is deduped by tx id + log index.
	if err := d.Dispatch(context.Background(), batchOf(1, 2)); err != nil {
		t.Fatal(err)
	}
	if want := []string{"tx:0", "tx:1", "tx:2"}; len(good.got) != 3 || good.got[2] != want[2] {
		t.Fatalf("good sink got %v, want %v", good.got, want)
	}
}

func TestDispatcherBestEffortDoesNotBlock(t *testing.T) {
	broken := &fakeSink{name: "broken", fails: 100}
	d := NewDispatcher([]Entry{{Sink: broken, BestEffort: true}}, 100, nil, nil)
	if err := d.Dispatch(context.Background(), batchOf(0)); err != nil {
		t.Fatalf("best-effort failure must not fail the block: %v", err)
	}
	_ = d.Dispatch(context.Background(), batchOf(0))
	if broken.calls != 1 {
		t.Fatalf("best-effort sink should not be retried for the same transfer, calls=%d", broken.calls)
	}
}

func TestDispatcherSkipsEmptyBatches(t *testing.T) {
	s := &fakeSink{name: "s"}
	d := NewDispatcher([]Entry{{Sink: s}}, 10, nil, nil)
	if err := d.Dispatch(context.Background(), Batch{BlockNumber: 5}); err != nil || s.calls != 0 {
		t.Fatalf("empty batch should not call sinks (calls=%d, err=%v)", s.calls, err)
	}
}
