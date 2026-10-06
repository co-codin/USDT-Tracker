// Package retry implements exponential backoff with jitter.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// Backoff describes an exponential backoff schedule.
type Backoff struct {
	Initial    time.Duration // first delay (default 500ms)
	Max        time.Duration // cap for a single delay (default 30s)
	Multiplier float64       // growth factor (default 2)
	Jitter     float64       // 0..1, fraction of the delay that is randomised (default 0.2)
}

// DefaultBackoff is a sensible default for HTTP calls.
var DefaultBackoff = Backoff{Initial: 500 * time.Millisecond, Max: 30 * time.Second, Multiplier: 2, Jitter: 0.2}

func (b Backoff) withDefaults() Backoff {
	if b.Initial <= 0 {
		b.Initial = DefaultBackoff.Initial
	}
	if b.Max <= 0 {
		b.Max = DefaultBackoff.Max
	}
	if b.Multiplier < 1 {
		b.Multiplier = DefaultBackoff.Multiplier
	}
	if b.Jitter < 0 || b.Jitter > 1 {
		b.Jitter = DefaultBackoff.Jitter
	}
	return b
}

// Delay returns the delay before retry number attempt (0-based).
func (b Backoff) Delay(attempt int) time.Duration {
	b = b.withDefaults()
	d := float64(b.Initial) * math.Pow(b.Multiplier, float64(attempt))
	if d > float64(b.Max) || math.IsInf(d, 0) {
		d = float64(b.Max)
	}
	if b.Jitter > 0 {
		// Randomise within [d*(1-j), d].
		d -= rand.Float64() * b.Jitter * d //nolint:gosec // G404: jitter does not need a CSPRNG
	}
	return time.Duration(d)
}

type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent marks err as non-retryable.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// AfterHint can be implemented by errors carrying a server-provided delay
// (e.g. HTTP 429 Retry-After). The longer of the hint and the backoff wins.
type AfterHint interface {
	RetryAfter() time.Duration
}

// Do calls fn until it succeeds, returns a permanent error, the context is
// done, or maxAttempts is reached (maxAttempts <= 0 means unlimited).
// onRetry, if non-nil, is called before each sleep.
func Do(ctx context.Context, maxAttempts int, b Backoff, fn func(ctx context.Context) error, onRetry func(attempt int, delay time.Duration, err error)) error {
	for attempt := 0; ; attempt++ {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if IsPermanent(err) {
			return err
		}
		if maxAttempts > 0 && attempt+1 >= maxAttempts {
			return err
		}
		delay := b.Delay(attempt)
		var ra AfterHint
		if errors.As(err, &ra) && ra.RetryAfter() > delay {
			delay = ra.RetryAfter()
		}
		if onRetry != nil {
			onRetry(attempt+1, delay, err)
		}
		if serr := Sleep(ctx, delay); serr != nil {
			return fmt.Errorf("%w (last error: %w)", serr, err)
		}
	}
}

// Sleep waits for d or until ctx is done.
func Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
