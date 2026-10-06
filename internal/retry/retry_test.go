package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoffGrowsAndCaps(t *testing.T) {
	b := Backoff{Initial: 100 * time.Millisecond, Max: time.Second, Multiplier: 2, Jitter: 0.0001}
	prev := time.Duration(0)
	for i := 0; i < 4; i++ {
		d := b.Delay(i)
		if d < prev {
			t.Fatalf("delay decreased at attempt %d: %s < %s", i, d, prev)
		}
		prev = d
	}
	if d := b.Delay(50); d > time.Second {
		t.Fatalf("delay not capped: %s", d)
	}
}

type hinted struct{ d time.Duration }

func (h hinted) Error() string             { return "slow down" }
func (h hinted) RetryAfter() time.Duration { return h.d }

func TestDo(t *testing.T) {
	fast := Backoff{Initial: time.Millisecond, Max: time.Millisecond}
	n := 0
	err := Do(context.Background(), 5, fast, func(context.Context) error {
		n++
		if n < 3 {
			return errors.New("transient")
		}
		return nil
	}, nil)
	if err != nil || n != 3 {
		t.Fatalf("err=%v n=%d", err, n)
	}

	n = 0
	err = Do(context.Background(), 5, fast, func(context.Context) error { n++; return Permanent(errors.New("fatal")) }, nil)
	if err == nil || n != 1 || !IsPermanent(err) {
		t.Fatalf("permanent error must stop retries: err=%v n=%d", err, n)
	}

	n = 0
	err = Do(context.Background(), 3, fast, func(context.Context) error { n++; return errors.New("x") }, nil)
	if err == nil || n != 3 {
		t.Fatalf("max attempts: err=%v n=%d", err, n)
	}

	var delays []time.Duration
	_ = Do(context.Background(), 2, fast, func(context.Context) error { return hinted{20 * time.Millisecond} },
		func(_ int, d time.Duration, _ error) { delays = append(delays, d) })
	if len(delays) != 1 || delays[0] != 20*time.Millisecond {
		t.Fatalf("Retry-After hint not honoured: %v", delays)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Do(ctx, 0, Backoff{Initial: time.Hour}, func(context.Context) error { return errors.New("x") }, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}
