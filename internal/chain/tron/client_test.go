package tron

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/retry"
)

func newTestClient(url string) *Client {
	return NewClient(Options{
		BaseURL: url, APIKey: "test-key", RPS: 1000, MaxRetries: 4,
		Backoff: retry.Backoff{Initial: time.Millisecond, Max: 5 * time.Millisecond},
	})
}

func TestClientRetriesOn429AndSendsAPIKey(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("TRON-PRO-API-KEY") != "test-key" {
			t.Errorf("missing api key header")
		}
		if r.URL.Path != "/wallet/gettransactioninfobyblocknum" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"num":123`) {
			t.Errorf("unexpected body %s", body)
		}
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`[{"id":"ab","blockNumber":123,"blockTimeStamp":1000,"log":[{"address":"a614f803b6fd780986a42c78ec9c7f77e6ded13c","topics":["x"],"data":"00"}]}]`))
	}))
	defer srv.Close()

	infos, err := newTestClient(srv.URL).TransactionInfoByBlockNum(context.Background(), 123)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3", calls.Load())
	}
	if len(infos) != 1 || infos[0].BlockNumber != 123 || len(infos[0].Log) != 1 {
		t.Fatalf("unexpected infos %+v", infos)
	}
}

func TestClientEmptyObjectMeansNoTransactions(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	infos, err := newTestClient(srv.URL).TransactionInfoByBlockNum(context.Background(), 1)
	if err != nil || infos != nil {
		t.Fatalf("got %v, %v", infos, err)
	}
}

func TestClientPermanentErrorNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer srv.Close()
	_, err := newTestClient(srv.URL).TransactionInfoByBlockNum(context.Background(), 1)
	var he *HTTPError
	if !errors.As(err, &he) || he.StatusCode != 400 {
		t.Fatalf("want HTTP 400 error, got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

func TestClientAPIErrorField(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"Error":"something broke"}`))
	}))
	defer srv.Close()
	_, err := newTestClient(srv.URL).BlockByNum(context.Background(), 1)
	var ae *APIError
	if !errors.As(err, &ae) {
		t.Fatalf("want APIError, got %v", err)
	}
}

func TestNowBlockFallsBackToGetNowBlock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/wallet/getblock":
			http.NotFound(w, r)
		case "/wallet/getnowblock":
			_, _ = w.Write([]byte(`{"blockID":"00ab","block_header":{"raw_data":{"number":86883926,"timestamp":1791324504000}}}`))
		}
	}))
	defer srv.Close()
	h, err := newTestClient(srv.URL).NowBlock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if h.Number != 86883926 || h.Timestamp.UnixMilli() != 1791324504000 {
		t.Fatalf("unexpected header %+v", h)
	}
}

func TestRateLimitDetection(t *testing.T) {
	e := &HTTPError{StatusCode: 403, Body: `{"Error":"The key exceeds the frequency limit(15), and the query server will be suspended for 30s"}`}
	if !e.RateLimited() {
		t.Fatal("403 frequency-limit body should be treated as rate limit")
	}
	if (&HTTPError{StatusCode: 403, Body: "forbidden"}).RateLimited() {
		t.Fatal("plain 403 is not a rate limit")
	}
	if parseRetryAfter("7") != 7*time.Second {
		t.Fatal("Retry-After seconds not parsed")
	}
}
