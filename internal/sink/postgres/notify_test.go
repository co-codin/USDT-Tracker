package postgres

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/co-codin/USDT-Tracker/internal/notify"
)

func TestNotifyStoreIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, isolatedDSN(t, dsn), "nb")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	n := s.Notify()
	limit := notify.Limits{PerPlan: func(plan string) int {
		if plan == notify.PlanPremium {
			return 10
		}
		return 2
	}}
	if _, ok, err := n.GetUser(ctx, 7); err != nil || ok {
		t.Fatal("GetUser must not create users", ok, err)
	}
	// Concurrent adds must not exceed the plan limit.
	var wg sync.WaitGroup
	errs := make([]error, 6)
	for i := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = n.AddSub(ctx, 7, "alice", "eth", "0x"+string(rune('a'+i))+"000000000000000000000000000000000000000", limit)
		}()
	}
	wg.Wait()
	ok, lim := 0, 0
	for _, e := range errs {
		switch {
		case e == nil:
			ok++
		case errors.Is(e, notify.ErrLimit):
			lim++
		default:
			t.Fatal(e)
		}
	}
	if ok != 2 || lim != 4 {
		t.Fatalf("ok %d limit %d", ok, lim)
	}
	subs, _ := n.ListSubs(ctx, 7)
	if err := n.AddSub(ctx, 7, "", subs[0].Chain, subs[0].Address, notify.Limits{PerPlan: func(string) int { return 99 }}); !errors.Is(err, notify.ErrExists) {
		t.Fatal(err)
	}
	if err := n.SetPlan(ctx, 7, notify.PlanPremium); err != nil {
		t.Fatal(err)
	}
	if err := n.AddSub(ctx, 7, "", "tron", "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv", limit); err != nil {
		t.Fatal(err)
	}
	if err := n.AddSub(ctx, 8, "", "sol", "x", limit); err == nil {
		t.Fatal("unknown chain accepted by the check constraint")
	}
	act, _ := n.ActiveSubs(ctx)
	if len(act) != 3 {
		t.Fatalf("active %d", len(act))
	}
	_ = n.SetBlocked(ctx, 7, true)
	if act, _ := n.ActiveSubs(ctx); len(act) != 0 {
		t.Fatal("blocked user still active")
	}
	if u, ok, _ := n.GetUser(ctx, 7); !ok || !u.Blocked || u.Plan != notify.PlanPremium {
		t.Fatalf("%+v", u)
	}
	_ = n.SetBlocked(ctx, 7, false)
	// Bot-wide caps.
	capLim := notify.Limits{PerPlan: limit.PerPlan, MaxUsers: 2, MaxTotalSubs: 4}
	if err := n.AddSub(ctx, 9, "", "tron", "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx", capLim); err != nil {
		t.Fatal(err)
	}
	if err := n.AddSub(ctx, 10, "", "tron", "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx", capLim); !errors.Is(err, notify.ErrCapacity) {
		t.Fatalf("max users: %v", err)
	}
	if err := n.AddSub(ctx, 9, "", "tron", "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv", capLim); !errors.Is(err, notify.ErrCapacity) {
		t.Fatalf("max subs: %v", err)
	}
	if err := n.RemoveSub(ctx, 7, "tron", "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv"); err != nil {
		t.Fatal(err)
	}
	if err := n.RemoveSub(ctx, 7, "tron", "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv"); !errors.Is(err, notify.ErrNotFound) {
		t.Fatal(err)
	}
	if off, err := n.LoadOffset(ctx); err != nil || off != 0 {
		t.Fatal(off, err)
	}
	_ = n.SaveOffset(ctx, 41)
	_ = n.SaveOffset(ctx, 42)
	if off, _ := n.LoadOffset(ctx); off != 42 {
		t.Fatal(off)
	}
}
