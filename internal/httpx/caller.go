package httpx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"

	"github.com/co-codin/USDT-Tracker/internal/retry"
)

// Response is a fully read HTTP response handed to a Caller's handler.
type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// RetryAfter returns the parsed Retry-After header (0 when absent/invalid).
func (r *Response) RetryAfter() time.Duration { return ParseRetryAfter(r.Header.Get("Retry-After")) }

// OK reports whether the status is 2xx.
func (r *Response) OK() bool { return r.StatusCode >= 200 && r.StatusCode <= 299 }

// Caller is the single retry/rate-limit/telemetry loop used by every outbound
// HTTP client (chain clients, Telegram, webhook, notify bot). Protocol
// specifics live in the handler passed to Do, which decides — usually via
// Classify — whether a response is success, retryable or permanent.
type Caller struct {
	Name      string        // error prefix, e.g. "tron"
	HTTP      *http.Client  // default NewClient(15s): same-host redirects only
	Limiter   *rate.Limiter // optional client-side rate limit, waited before every attempt
	Attempts  int           // per call; <=0 means unlimited
	Backoff   retry.Backoff // exponential backoff; server Retry-After hints win when longer
	BodyLimit int64         // max bytes read per response (default 8 MiB)
	// Observe, if set, gets one sample per attempt; status is the HTTP code
	// or "network_error".
	Observe func(endpoint, status string, d time.Duration)
	// OnRetry, if set, is called before each backoff sleep.
	OnRetry func(endpoint string, attempt int, delay time.Duration, err error)
	// ScrubErrors strips URLs from transport errors (use when the URL carries
	// a secret, e.g. Telegram bot tokens or webhook paths).
	ScrubErrors bool
}

// Do builds a request with newReq, sends it with retries and passes each
// fully read response to handle. A handler error marked retry.Permanent
// stops the loop; any other error is retried. Transport errors are retried
// unless the context is done.
func (c *Caller) Do(ctx context.Context, endpoint string, newReq func(ctx context.Context) (*http.Request, error), handle func(*Response) error) error {
	return retry.Do(ctx, c.Attempts, c.Backoff, func(ctx context.Context) error {
		return c.once(ctx, endpoint, newReq, handle)
	}, func(attempt int, delay time.Duration, err error) {
		if c.OnRetry != nil {
			c.OnRetry(endpoint, attempt, delay, err)
		}
	})
}

func (c *Caller) once(ctx context.Context, endpoint string, newReq func(ctx context.Context) (*http.Request, error), handle func(*Response) error) error {
	if c.Limiter != nil {
		if err := c.Limiter.Wait(ctx); err != nil {
			return retry.Permanent(err)
		}
	}
	req, err := newReq(ctx)
	if err != nil {
		return retry.Permanent(err)
	}
	hc := c.HTTP
	if hc == nil {
		hc = NewClient(15 * time.Second)
	}
	start := time.Now()
	resp, err := hc.Do(req) //nolint:bodyclose // closed by ReadBody
	if err != nil {
		c.observe(endpoint, "network_error", start)
		if ctx.Err() != nil {
			return retry.Permanent(ctx.Err())
		}
		if c.ScrubErrors {
			err = ScrubURLError(err)
		}
		return fmt.Errorf("%s: %w", c.label(endpoint), err)
	}
	limit := c.BodyLimit
	if limit <= 0 {
		limit = 8 << 20
	}
	body, rerr := ReadBody(resp, limit)
	c.observe(endpoint, strconv.Itoa(resp.StatusCode), start)
	if rerr != nil {
		return fmt.Errorf("%s: read body: %w", c.label(endpoint), rerr)
	}
	return handle(&Response{StatusCode: resp.StatusCode, Header: resp.Header, Body: body})
}

func (c *Caller) label(endpoint string) string {
	if c.Name == "" {
		return endpoint
	}
	return c.Name + " " + endpoint
}

func (c *Caller) observe(endpoint, status string, start time.Time) {
	if c.Observe != nil {
		c.Observe(endpoint, status, time.Since(start))
	}
}

// RetryableStatus reports the shared policy: 408, 429 and 5xx are transient.
func RetryableStatus(code int) bool {
	return code == http.StatusTooManyRequests || code == http.StatusRequestTimeout || code >= 500
}

// Classify wraps err for the retry loop: retryable statuses keep err (with
// hint attached when err carries none); everything else becomes permanent.
func Classify(code int, err error, hint time.Duration) error {
	if !RetryableStatus(code) {
		return retry.Permanent(err)
	}
	var ah retry.AfterHint
	if hint > 0 && !errors.As(err, &ah) {
		return WithRetryAfter(err, hint)
	}
	return err
}

// WithRetryAfter attaches a server-provided delay to err (see retry.AfterHint).
func WithRetryAfter(err error, d time.Duration) error {
	if err == nil || d <= 0 {
		return err
	}
	return afterErr{err: err, d: d}
}

type afterErr struct {
	err error
	d   time.Duration
}

func (e afterErr) Error() string             { return e.err.Error() }
func (e afterErr) Unwrap() error             { return e.err }
func (e afterErr) RetryAfter() time.Duration { return e.d }

// RetryReason labels a retry for metrics: "rate_limited" for 429 (or when
// rateLimited says so), the status code for other HTTP errors, else "error".
func RetryReason(code int, rateLimited bool) string {
	switch {
	case code == http.StatusTooManyRequests || rateLimited:
		return "rate_limited"
	case code > 0:
		return strconv.Itoa(code)
	default:
		return "error"
	}
}

// ParseRetryAfter parses a Retry-After header (delta-seconds or HTTP date).
func ParseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// Truncate trims s and cuts it to n bytes (appending "…").
func Truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// TronRateLimitBody reports whether a TronGrid 403 body is its "frequency
// limit" ban rather than a real authorization failure.
func TronRateLimitBody(b string) bool {
	l := strings.ToLower(b)
	return strings.Contains(l, "frequency") || strings.Contains(l, "rate limit") || strings.Contains(l, "exceed")
}
