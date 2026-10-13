package outbox

import (
	"context"
	"fmt"
	"testing"
)

func TestMemoryLifecycle(t *testing.T) {
	ctx := context.Background()
	m := NewMemory(0)
	n, _ := m.Enqueue(ctx, KindNotify, []Item{{Key: "a"}, {Key: "b"}, {Key: "a"}})
	if n != 2 {
		t.Fatalf("added %d", n)
	}
	if n, _ := m.Enqueue(ctx, KindNotify, []Item{{Key: "a"}}); n != 0 {
		t.Fatal("pending key re-added")
	}
	if c, _ := m.Count(ctx, KindAlert); c != 0 {
		t.Fatal("kinds must be separate")
	}
	_ = m.Failed(ctx, KindNotify, []string{"a"})
	p, _ := m.Pending(ctx, KindNotify, 1)
	if len(p) != 1 || p[0].Key != "a" || p[0].Attempts != 1 {
		t.Fatalf("pending %+v", p)
	}
	_ = m.Done(ctx, KindNotify, []string{"a", "zz"}, StatusSent)
	if n, _ := m.Enqueue(ctx, KindNotify, []Item{{Key: "a"}}); n != 0 {
		t.Fatal("done key re-added (would duplicate)")
	}
	if c, _ := m.Count(ctx, KindNotify); c != 1 {
		t.Fatalf("count %d", c)
	}
}

func TestMemoryBoundsAndEviction(t *testing.T) {
	ctx := context.Background()
	m := NewMemory(2)
	m.MaxKeys = 3
	for i := 0; i < 5; i++ {
		_, _ = m.Enqueue(ctx, KindAlert, []Item{{Key: fmt.Sprint(i)}})
	}
	p, _ := m.Pending(ctx, KindAlert, 0)
	if len(p) != 2 || p[0].Key != "3" || p[1].Key != "4" || m.Dropped() != 3 {
		t.Fatalf("pending %+v dropped %d", p, m.Dropped())
	}
	_ = m.Done(ctx, KindAlert, []string{"3", "4"}, StatusSent)
	// done keys are bounded: the oldest is forgotten
	if n, _ := m.Enqueue(ctx, KindAlert, []Item{{Key: "0"}}); n != 1 {
		t.Fatal("evicted done key should be accepted again")
	}
}
