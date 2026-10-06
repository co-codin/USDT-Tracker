package sink

import (
	"context"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/co-codin/tron-usdt-listener/internal/model"
	"github.com/co-codin/tron-usdt-listener/internal/retry"
	"github.com/co-codin/tron-usdt-listener/pkg/webhooksig"
)

func sampleBatch() Batch {
	bt := time.Unix(1791324426, 0).UTC()
	return Batch{BlockNumber: 86883900, BlockTime: bt, Transfers: []model.Transfer{{
		TxID: "3e5cb8d14a2063cdcd35510db2510c64da5df29b1293634ffe2895ce822c5d82", LogIndex: 0,
		BlockNumber: 86883900, BlockTime: bt, Contract: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", Symbol: "USDT", Decimals: 6,
		From: "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx", To: "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv",
		Amount: big.NewInt(400_000_000), Reasons: []string{model.ReasonIncoming},
	}}}
}

func TestWebhookSignsAndRetries(t *testing.T) {
	const secret = "topsecret"
	fixedNow := time.Unix(1_800_000_000, 0)
	var calls atomic.Int32
	var gotPayload WebhookPayload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		err := webhooksig.Verify([]byte(secret), r.Header.Get(webhooksig.HeaderTimestamp),
			r.Header.Get(webhooksig.HeaderSignature), body, time.Minute, fixedNow)
		if err != nil {
			t.Errorf("signature verification failed: %v", err)
		}
		if r.Header.Get(HeaderDelivery) == "" || r.Header.Get(HeaderEvent) != WebhookEvent {
			t.Errorf("missing delivery/event headers")
		}
		if r.Header.Get("X-Custom") != "1" {
			t.Errorf("custom header not forwarded")
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if err := json.Unmarshal(body, &gotPayload); err != nil {
			t.Errorf("bad payload: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	wh, err := NewWebhook(WebhookConfig{
		URL: srv.URL, Secret: secret, MaxRetries: 3, Headers: map[string]string{"X-Custom": "1"},
		Backoff: retry.Backoff{Initial: time.Millisecond, Max: 2 * time.Millisecond},
		Now:     func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := wh.Send(context.Background(), sampleBatch()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2 (one 503 retry)", calls.Load())
	}
	if gotPayload.BlockNumber != 86883900 || len(gotPayload.Transfers) != 1 {
		t.Fatalf("unexpected payload %+v", gotPayload)
	}
	tr := gotPayload.Transfers[0]
	if tr.AmountDecimal() != "400" || tr.Key() != "3e5cb8d14a2063cdcd35510db2510c64da5df29b1293634ffe2895ce822c5d82:0" {
		t.Fatalf("unexpected transfer %+v", tr)
	}
	if !strings.HasPrefix(gotPayload.DeliveryID, "86883900-") || gotPayload.DeliveryID != DeliveryID(sampleBatch()) {
		t.Fatalf("delivery id not deterministic: %s", gotPayload.DeliveryID)
	}
}

func TestWebhookClientErrorIsPermanent(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "nope", http.StatusBadRequest)
	}))
	defer srv.Close()
	wh, _ := NewWebhook(WebhookConfig{URL: srv.URL, MaxRetries: 5, Backoff: retry.Backoff{Initial: time.Millisecond}})
	if err := wh.Send(context.Background(), sampleBatch()); err == nil {
		t.Fatal("expected error")
	}
	if calls.Load() != 1 {
		t.Fatalf("4xx must not be retried, calls = %d", calls.Load())
	}
}

func TestWebhookInvalidURL(t *testing.T) {
	if _, err := NewWebhook(WebhookConfig{URL: "ftp://x"}); err == nil {
		t.Fatal("expected error")
	}
}
