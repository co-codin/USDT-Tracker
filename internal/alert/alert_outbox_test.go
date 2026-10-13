package alert

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/outbox"
	"github.com/co-codin/USDT-Tracker/internal/tgbot"
)

type flakySender struct {
	mu    sync.Mutex
	fails int
	sent  int
}

func (f *flakySender) SendMessage(context.Context, string, string, tgbot.SendOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fails > 0 {
		f.fails--
		return errors.New("telegram down")
	}
	f.sent++
	return nil
}

type brokenBox struct{ outbox.Store }

func (brokenBox) Enqueue(context.Context, string, []outbox.Item) (int, error) {
	return 0, errors.New("db down")
}
func (brokenBox) Pending(context.Context, string, int) ([]outbox.Item, error) {
	return nil, errors.New("db down")
}
func (brokenBox) Count(context.Context, string) (int, error) { return 0, errors.New("db down") }

func TestAlertRetriesThenGivesUp(t *testing.T) {
	box := outbox.NewMemory(0)
	s := &flakySender{fails: 2}
	a, err := New(Config{ChatID: "@c", Threshold: "1", MaxAttempts: 3, Outbox: box}, s, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	b := chain.Block{Number: 1, Transfers: []model.Transfer{usdt("tron", 5, 1)}}
	if err := a.Observe(ctx, b); err != nil {
		t.Fatal(err)
	}
	a.sendNext(ctx) // fail 1 → retry later
	a.sendNext(ctx) // fail 2 → retry later
	if a.Pending() != 1 {
		t.Fatalf("pending %d", a.Pending())
	}
	a.sendNext(ctx) // ok
	if s.sent != 1 || a.Pending() != 0 || a.Stats().Sent != 1 {
		t.Fatalf("sent %d stats %+v", s.sent, a.Stats())
	}
	// A replayed block (restart before the cursor save) adds nothing.
	if err := a.Observe(ctx, b); err != nil || a.Pending() != 0 {
		t.Fatal("replay must not re-queue", err)
	}
	// Permanent failure: given up after MaxAttempts.
	s.fails = 10
	_ = a.Observe(ctx, chain.Block{Number: 2, Transfers: []model.Transfer{usdt("eth", 9, 2)}})
	for range 3 {
		a.sendNext(ctx)
	}
	if a.Pending() != 0 || a.Stats().Failed != 1 {
		t.Fatalf("pending %d stats %+v", a.Pending(), a.Stats())
	}
	// Bad payload rows are discarded.
	_, _ = box.Enqueue(ctx, outbox.KindAlert, []outbox.Item{{Key: "junk", Payload: []byte("{")}})
	a.sendNext(ctx)
	if a.Pending() != 0 {
		t.Fatal("junk row kept")
	}
}

func TestAlertOutboxErrors(t *testing.T) {
	a, err := New(Config{ChatID: "@c", Threshold: "1", Outbox: brokenBox{}}, &flakySender{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := a.Observe(ctx, chain.Block{Transfers: []model.Transfer{usdt("tron", 5, 1)}}); err == nil {
		t.Fatal("enqueue error must reach the listener (block retried)")
	}
	if a.Pending() != 0 {
		t.Fatal("count error → 0")
	}
	a.sendNext(ctx) // logs, no panic
	rctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	a.Run(rctx, 0)
}

func usdt(chainName string, whole int64, i int) model.Transfer {
	return model.Transfer{Chain: chainName, TxID: strings.Repeat("ab", 32), LogIndex: i, Symbol: "USDT", Decimals: 6,
		Amount: new(big.Int).Mul(big.NewInt(whole), big.NewInt(1_000_000)), From: "a", To: "b", BlockTime: time.Unix(1, 0)}
}
