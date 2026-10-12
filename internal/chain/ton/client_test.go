package ton_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain/ton"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton/tontest"
	"github.com/co-codin/USDT-Tracker/internal/retry"
)

var fast = retry.Backoff{Initial: time.Millisecond, Max: 2 * time.Millisecond}

type obs struct{ req, retries atomic.Int32 }

func (o *obs) ObserveRequest(string, string, time.Duration) { o.req.Add(1) }
func (o *obs) ObserveRetry(string, string)                  { o.retries.Add(1) }

func newClient(url, key string, o *obs) *ton.Client {
	opts := ton.Options{BaseURL: url, APIKey: key, RPS: 1000, MaxRetries: 4, Backoff: fast}
	if o != nil {
		opts.Observer = o
	}
	return ton.NewClient(opts)
}

func TestClientEndpoints(t *testing.T) {
	srv := tontest.New(30)
	defer srv.Close()
	srv.APIKey = "secret-key"
	c := newClient(srv.BaseURL(), "secret-key", nil)
	ctx := context.Background()
	info, err := c.MasterchainInfo(ctx)
	if err != nil || info.Last.Seqno != 30 || info.Last.Time().IsZero() {
		t.Fatalf("info %+v %v", info, err)
	}
	blocks, err := c.MasterchainBlockShards(ctx, 15)
	if err != nil || len(blocks) != 3 || blocks[1].StartLt != 15000 {
		t.Fatalf("shards %+v %v", blocks, err)
	}
	items, err := c.JettonTransfers(ctx, ton.USDTMaster, 15000, 16100, 100, 0)
	if err != nil || len(items) == 0 {
		t.Fatalf("transfers %v %v", items, err)
	}
	if _, err := c.MasterchainBlockShards(ctx, 31); !errors.Is(err, ton.ErrBlockNotFound) {
		t.Errorf("future block err = %v", err)
	}
}

func TestClientAPIKeyRequired(t *testing.T) {
	srv := tontest.New(10)
	defer srv.Close()
	srv.APIKey = "secret-key"
	var he *ton.HTTPError
	if _, err := newClient(srv.BaseURL(), "", nil).MasterchainInfo(context.Background()); !errors.As(err, &he) || he.StatusCode != 401 {
		t.Fatalf("err = %v", err)
	}
	if srv.Requests("/masterchainInfo") != 1 {
		t.Errorf("401 must not be retried, got %d requests", srv.Requests("/masterchainInfo"))
	}
}

func TestClientRetries(t *testing.T) {
	srv := tontest.New(10)
	defer srv.Close()
	o := &obs{}
	c := newClient(srv.BaseURL(), "", o)
	srv.Fail("/masterchainInfo", 429, 502, 503)
	if _, err := c.MasterchainInfo(context.Background()); err != nil {
		t.Fatal(err)
	}
	if srv.Requests("/masterchainInfo") != 4 || o.retries.Load() != 3 || o.req.Load() != 4 {
		t.Errorf("requests=%d retries=%d observed=%d", srv.Requests("/masterchainInfo"), o.retries.Load(), o.req.Load())
	}
	srv.Fail("/jetton/transfers", 500, 500, 500, 500)
	if _, err := c.JettonTransfers(context.Background(), ton.USDTMaster, 1, 2, 10, 0); err == nil {
		t.Error("want error after max retries")
	}
	srv.Fail("/jetton/transfers", 400)
	if _, err := c.JettonTransfers(context.Background(), ton.USDTMaster, 1, 2, 10, 0); !retry.IsPermanent(err) {
		t.Errorf("400 should be permanent, got %v", err)
	}
}

func TestClientRetryAfterAndDecodeError(t *testing.T) {
	var n atomic.Int32
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method %s: client must be read-only GET", r.Method)
		}
		if n.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			return
		}
		_, _ = w.Write([]byte("not json"))
	}))
	defer hs.Close()
	start := time.Now()
	_, err := newClient(hs.URL, "", nil).MasterchainInfo(context.Background())
	if err == nil {
		t.Fatal("want decode error")
	}
	if time.Since(start) < 900*time.Millisecond {
		t.Errorf("Retry-After not honoured")
	}
}
