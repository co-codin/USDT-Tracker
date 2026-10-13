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
	tr, tn := m.For("tron"), m.For("ton")
	tr.ObserveRequest("/wallet/getblock", "200", 10*time.Millisecond)
	tr.ObserveRetry("/wallet/getblock", "rate_limited")
	tn.ObserveRequest("/masterchainInfo", "200", time.Millisecond)
	tn.HeadBlock.Set(42)
	tn.TransfersMatched.WithLabelValues("watch_incoming").Inc()
	m.ObserveSink("webhook", "ok", 3, time.Millisecond)

	if v := testutil.ToFloat64(m.RPCRequests.WithLabelValues("tron", "/wallet/getblock", "200")); v != 1 {
		t.Fatalf("rpc requests = %v", v)
	}
	if v := testutil.ToFloat64(m.RPCRequests.WithLabelValues("ton", "/masterchainInfo", "200")); v != 1 {
		t.Fatalf("ton rpc requests = %v", v)
	}
	if v := testutil.ToFloat64(m.HeadBlock.WithLabelValues("ton")); v != 42 {
		t.Fatalf("ton head = %v", v)
	}
	if v := testutil.ToFloat64(m.SinkTransfers.WithLabelValues("webhook", "ok")); v != 3 {
		t.Fatalf("sink transfers = %v", v)
	}
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, s := range []string{"usdt_tracker_rpc_retries_total", "usdt_tracker_sink_delivery_duration_seconds", "go_goroutines",
		`usdt_tracker_head_block{chain="ton"} 42`, `usdt_tracker_transfers_matched_total{chain="ton",reason="watch_incoming"} 1`} {
		if !strings.Contains(string(body), s) {
			t.Errorf("metrics output missing %s", s)
		}
	}
}
