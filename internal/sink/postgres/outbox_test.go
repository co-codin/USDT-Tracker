package postgres

import (
	"context"
	"math/big"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/notify"
	"github.com/co-codin/USDT-Tracker/internal/outbox"
	"github.com/co-codin/USDT-Tracker/internal/tgbot"
)

func TestOutboxIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, isolatedDSN(t, dsn), "ob")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	o := s.Outbox()
	if n, err := o.Enqueue(ctx, outbox.KindAlert, nil); n != 0 || err != nil {
		t.Fatal(n, err)
	}
	n, err := o.Enqueue(ctx, outbox.KindAlert, []outbox.Item{{Key: "a", Payload: []byte(`{}`)}, {Key: "b", ChatID: 5, Payload: []byte(`{"x":1}`)}, {Key: "a", Payload: []byte(`{}`)}})
	if err != nil || n != 2 {
		t.Fatalf("added %d err %v", n, err)
	}
	// Another scope (deployment) has its own queue.
	other := &Outbox{pool: s.pool, scope: "other"}
	if c, _ := other.Count(ctx, outbox.KindAlert); c != 0 {
		t.Fatal("scope leak")
	}
	if err := o.Failed(ctx, outbox.KindAlert, []string{"b"}); err != nil {
		t.Fatal(err)
	}
	p, err := o.Pending(ctx, outbox.KindAlert, 0)
	if err != nil || len(p) != 2 || p[1].Key != "b" || p[1].Attempts != 1 || p[1].ChatID != 5 || !strings.Contains(string(p[1].Payload), `"x"`) {
		t.Fatalf("pending %+v %v", p, err)
	}
	if err := o.Done(ctx, outbox.KindAlert, nil, outbox.StatusSent); err != nil {
		t.Fatal(err)
	}
	if err := o.Done(ctx, outbox.KindAlert, []string{"a"}, outbox.StatusSent); err != nil {
		t.Fatal(err)
	}
	if n, _ := o.Enqueue(ctx, outbox.KindAlert, []outbox.Item{{Key: "a", Payload: []byte(`{}`)}}); n != 0 {
		t.Fatal("a sent key must not be re-queued")
	}
	if c, _ := o.Count(ctx, outbox.KindAlert); c != 1 {
		t.Fatalf("count %d", c)
	}
	// Retention pruning removes "a" (done long enough ago with 1ns retention).
	o.Retention = time.Nanosecond
	o.lastPrune.Store(0)
	if err := o.Done(ctx, outbox.KindAlert, []string{"nope"}, outbox.StatusSent); err != nil {
		t.Fatal(err)
	}
	var rows int
	_ = s.pool.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE key = 'a'`).Scan(&rows)
	if rows != 0 {
		t.Fatalf("prune: %d rows", rows)
	}
	s.Close()
	if _, err := o.Pending(ctx, outbox.KindAlert, 1); err == nil {
		t.Fatal("want error on closed pool")
	}
	if _, err := o.Count(ctx, outbox.KindAlert); err == nil {
		t.Fatal("want error")
	}
	if _, err := o.Enqueue(ctx, outbox.KindAlert, []outbox.Item{{Key: "z", Payload: []byte(`{}`)}}); err == nil {
		t.Fatal("want error")
	}
	if err := o.Done(ctx, outbox.KindAlert, []string{"b"}, outbox.StatusSent); err == nil {
		t.Fatal("want error")
	}
	if err := o.Failed(ctx, outbox.KindAlert, []string{"b"}); err == nil {
		t.Fatal("want error")
	}
}

type recAPI struct {
	mu   sync.Mutex
	sent []string
	fail bool
}

func (r *recAPI) SendMessage(_ context.Context, chatID, text string, _ tgbot.SendOptions) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return context.DeadlineExceeded
	}
	r.sent = append(r.sent, chatID+":"+text)
	return nil
}

func (r *recAPI) GetUpdates(ctx context.Context, _ int64, _ time.Duration) ([]tgbot.Update, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (r *recAPI) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sent)
}

// A crash after the block was recorded but before the DM went out must not
// drop it, and replaying the block after restart must not duplicate it.
func TestNotifyBotRestartNeitherDropsNorDuplicates(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, isolatedDSN(t, dsn), "nbr")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	addr := "0x" + strings.Repeat("ab", 20)
	if err := s.Notify().AddSub(ctx, 42, "u", "eth", addr, notify.Limits{PerPlan: func(string) int { return 5 }}); err != nil {
		t.Fatal(err)
	}
	blk := chain.Block{Number: 7, Transfers: []model.Transfer{{Chain: "eth", TxID: "0x01", BlockNumber: 7, From: "0xdead", To: addr,
		Amount: big.NewInt(5_000_000), Decimals: 6, Symbol: "USDT", BlockTime: time.Unix(1700000000, 0)}}}

	newBot := func(api *recAPI) *notify.Bot {
		b, err := notify.New(notify.Config{Chains: []string{"eth"}, Outbox: s.Outbox(), PollTimeout: time.Second}, s.Notify(), api, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
		return b
	}
	// Run 1: the block is tapped, then the process "crashes" before sending.
	api1 := &recAPI{fail: true}
	if err := newBot(api1).Observe(ctx, blk); err != nil {
		t.Fatal(err)
	}
	// Run 2: the cursor was not saved, so the block is replayed; the
	// pending DM is delivered exactly once.
	api2 := &recAPI{}
	b2 := newBot(api2)
	if err := b2.Observe(ctx, blk); err != nil {
		t.Fatal(err)
	}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = b2.Run(rctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for api2.count() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if err := b2.Observe(ctx, blk); err != nil { // another replay
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done
	if api2.count() != 1 || api1.count() != 0 {
		t.Fatalf("sent %d (want 1): %v", api2.count(), api2.sent)
	}
	if c, _ := s.Outbox().Count(ctx, outbox.KindNotify); c != 0 {
		t.Fatalf("pending %d", c)
	}
}
