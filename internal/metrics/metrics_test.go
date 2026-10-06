package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObservers(t *testing.T) {
	m := New()
	m.ObserveRequest("/wallet/getblock", "200", 10*time.Millisecond)
	m.ObserveRetry("/wallet/getblock", "rate_limited")
	m.ObserveSink("webhook", "ok", 3, time.Millisecond)

	if v := testutil.ToFloat64(m.RPCRequests.WithLabelValues("/wallet/getblock", "200")); v != 1 {
		t.Fatalf("rpc requests = %v", v)
	}
	if v := testutil.ToFloat64(m.SinkTransfers.WithLabelValues("webhook", "ok")); v != 3 {
		t.Fatalf("sink transfers = %v", v)
	}
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, s := range []string{"tron_listener_rpc_retries_total", "tron_listener_sink_delivery_duration_seconds", "go_goroutines"} {
		if !strings.Contains(string(body), s) {
			t.Errorf("metrics output missing %s", s)
		}
	}
}
