package httpx

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/retry"
)

func fastCaller(srv *httptest.Server) *Caller {
	return &Caller{Name: "test", HTTP: srv.Client(), Attempts: 4, Backoff: retry.Backoff{Initial: time.Millisecond, Max: time.Millisecond}}
}

func get(url string) func(ctx context.Context) (*http.Request, error) {
	return func(ctx context.Context) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	}
}

func classifyHandler(r *Response) error {
	if r.OK() {
		return nil
	}
	return Classify(r.StatusCode, errors.New("bad status"), r.RetryAfter())
}

func TestCallerRetriesTransientThenSucceeds(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch n.Add(1) {
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
		case 2:
			w.WriteHeader(http.StatusBadGateway)
		default:
			_, _ = w.Write([]byte("ok"))
		}
	}))
	defer srv.Close()
	c := fastCaller(srv)
	var obs, retries atomic.Int32
	c.Observe = func(string, string, time.Duration) { obs.Add(1) }
	c.OnRetry = func(string, int, time.Duration, error) { retries.Add(1) }
	if err := c.Do(context.Background(), "/x", get(srv.URL), classifyHandler); err != nil {
		t.Fatal(err)
	}
	if n.Load() != 3 || obs.Load() != 3 || retries.Load() != 2 {
		t.Fatalf("calls=%d obs=%d retries=%d", n.Load(), obs.Load(), retries.Load())
	}
}

func TestCallerPermanentStops(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	err := fastCaller(srv).Do(context.Background(), "/x", get(srv.URL), classifyHandler)
	if !retry.IsPermanent(err) || n.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, n.Load())
	}
}

func TestCallerScrubsTransportErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	u := srv.URL + "/botSECRET/x"
	srv.Close()
	c := fastCaller(srv)
	c.Attempts = 1
	c.ScrubErrors = true
	err := c.Do(context.Background(), "ep", get(u), classifyHandler)
	if err == nil || strings.Contains(err.Error(), "SECRET") || !strings.HasPrefix(err.Error(), "test ep:") {
		t.Fatalf("err=%v", err)
	}
}

func TestCallerBadRequestBuilderAndCancel(t *testing.T) {
	c := &Caller{Attempts: 3}
	err := c.Do(context.Background(), "ep", func(context.Context) (*http.Request, error) { return nil, errors.New("boom") }, classifyHandler)
	if !retry.IsPermanent(err) {
		t.Fatalf("want permanent, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Do(ctx, "ep", get("http://127.0.0.1:1/"), classifyHandler); !errors.Is(err, context.Canceled) {
		t.Fatalf("want canceled, got %v", err)
	}
}

func TestClassifyAndHints(t *testing.T) {
	base := errors.New("x")
	if !retry.IsPermanent(Classify(404, base, 0)) {
		t.Fatal("404 should be permanent")
	}
	for _, c := range []int{408, 429, 500, 503} {
		if retry.IsPermanent(Classify(c, base, 0)) {
			t.Fatalf("%d should retry", c)
		}
	}
	var ah retry.AfterHint
	if err := Classify(429, base, 3*time.Second); !errors.As(err, &ah) || ah.RetryAfter() != 3*time.Second || !errors.Is(err, base) {
		t.Fatalf("hint lost: %v", err)
	}
	if WithRetryAfter(nil, time.Second) != nil || !errors.Is(WithRetryAfter(base, 0), base) {
		t.Fatal("WithRetryAfter passthrough")
	}
	if RetryReason(429, false) != "rate_limited" || RetryReason(403, true) != "rate_limited" || RetryReason(502, false) != "502" || RetryReason(0, false) != "error" {
		t.Fatal("RetryReason")
	}
	if ParseRetryAfter("7") != 7*time.Second || ParseRetryAfter("-1") != 0 || ParseRetryAfter("") != 0 || ParseRetryAfter("junk") != 0 {
		t.Fatal("ParseRetryAfter")
	}
	if d := ParseRetryAfter(time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)); d < 59*time.Minute {
		t.Fatalf("http-date: %v", d)
	}
	if Truncate(" abc ", 2) != "ab…" || Truncate("a", 5) != "a" {
		t.Fatal("Truncate")
	}
	if !TronRateLimitBody(`{"Error":"request rate exceeded the allowed frequency"}`) || TronRateLimitBody("forbidden") {
		t.Fatal("TronRateLimitBody")
	}
}
