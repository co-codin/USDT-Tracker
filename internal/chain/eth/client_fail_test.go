package eth_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain/eth"
	"github.com/co-codin/USDT-Tracker/internal/retry"
)

type rpcReq struct {
	ID     int64  `json:"id"`
	Method string `json:"method"`
	Params []any  `json:"params"`
}

// scripted serves JSON-RPC; fn gets every request and returns the raw
// response body (or an HTTP status when status != 0).
func scripted(t *testing.T, fn func(n int, body []byte) (status int, resp string)) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		st, resp := fn(int(n.Add(1)), b)
		if st != 0 {
			w.WriteHeader(st)
		}
		_, _ = io.WriteString(w, resp)
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

func single(b []byte) rpcReq {
	var r rpcReq
	_ = json.Unmarshal(b, &r)
	return r
}

func result(id int64, v string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":%s}`, id, v)
}

type obs struct {
	mu      sync.Mutex
	retries []string
	reqs    int
}

func (o *obs) ObserveRequest(string, string, time.Duration) { o.mu.Lock(); o.reqs++; o.mu.Unlock() }
func (o *obs) ObserveRetry(_ string, reason string) {
	o.mu.Lock()
	o.retries = append(o.retries, reason)
	o.mu.Unlock()
}

func client(url string, o *obs) *eth.Client {
	opts := eth.Options{URL: url, RPS: 1000, MaxRetries: 4, Backoff: retry.Backoff{Initial: time.Millisecond, Max: time.Millisecond}}
	if o != nil {
		opts.Observer = o
	}
	return eth.NewClient(opts)
}

func TestBlockTimeAndHeaders(t *testing.T) {
	srv, _ := scripted(t, func(_ int, b []byte) (int, string) {
		r := single(b)
		switch {
		case r.Method == "eth_getBlockByNumber" && r.Params[0] == "0x1":
			return 0, result(r.ID, `{"number":"0x1","hash":"0xh","parentHash":"0xp","timestamp":"0x10"}`)
		case r.Method == "eth_getBlockByNumber" && r.Params[0] == "finalized":
			return 0, result(r.ID, `{"number":"0x64","timestamp":"0x1"}`)
		case r.Method == "eth_getBlockByNumber" && r.Params[0] == "0x2":
			return 0, result(r.ID, `{"number":"0x2","timestamp":"zz"}`)
		case r.Method == "eth_blockNumber":
			return 0, result(r.ID, `"0x7"`)
		default:
			return 0, result(r.ID, `null`)
		}
	})
	c := client(srv.URL, nil)
	ctx := context.Background()
	if ts, err := c.BlockTime(ctx, 1); err != nil || ts.Unix() != 16 {
		t.Fatal(ts, err)
	}
	if _, err := c.BlockTime(ctx, 2); err == nil {
		t.Fatal("bad timestamp must fail")
	}
	if _, err := c.BlockTime(ctx, 3); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatal(err)
	}
	if n, err := c.HeaderByTag(ctx, eth.HeadFinalized); err != nil || n != 100 {
		t.Fatal(n, err)
	}
	if n, err := c.HeaderByTag(ctx, eth.HeadLatest); err != nil || n != 7 {
		t.Fatal(n, err)
	}
	if _, err := c.HeaderByTag(ctx, eth.HeadSafe); err == nil {
		t.Fatal("nil safe header must fail")
	}
	if h, err := c.HeaderByNumber(ctx, 1); err != nil || h.Hash != "0xh" {
		t.Fatal(h, err)
	}
	if h, err := c.HeaderByNumber(ctx, 9); err != nil || h != nil {
		t.Fatal("missing block must be nil", h, err)
	}
}

func batchResp(b []byte, mutate func(reqs []rpcReq, out []string) []string) string {
	var reqs []rpcReq
	_ = json.Unmarshal(b, &reqs)
	out := make([]string, 0, len(reqs))
	for _, r := range reqs {
		if r.Method == "eth_getLogs" {
			out = append(out, result(r.ID, `[{"address":"0xa","blockNumber":"0x1","logIndex":"0x0"}]`))
		} else {
			out = append(out, result(r.ID, fmt.Sprintf(`{"number":%q,"hash":"0xh"}`, r.Params[0])))
		}
	}
	if mutate != nil {
		out = mutate(reqs, out)
	}
	return "[" + strings.Join(out, ",") + "]"
}

func TestLogsWithHeaders(t *testing.T) {
	f := eth.LogFilter{FromBlock: 1, ToBlock: 3, Address: "0xa", Topics: []string{eth.TransferTopic}}
	ctx := context.Background()
	cases := []struct {
		name    string
		fn      func(n int, b []byte) (int, string)
		wantErr string
		perm    bool
		calls   int32
	}{
		{name: "ok", fn: func(_ int, b []byte) (int, string) { return 0, batchResp(b, nil) }, calls: 1},
		{name: "unsupported", fn: func(int, []byte) (int, string) {
			return 0, `{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"batch not supported"}}`
		}, wantErr: "batch requests not supported", perm: true, calls: 1},
		{name: "batch rate limited then ok", fn: func(n int, b []byte) (int, string) {
			if n == 1 {
				return 0, `{"jsonrpc":"2.0","id":null,"error":{"code":429,"message":"rate limit"}}`
			}
			return 0, batchResp(b, nil)
		}, calls: 2},
		{name: "item error", fn: func(_ int, b []byte) (int, string) {
			return 0, batchResp(b, func(reqs []rpcReq, out []string) []string {
				out[1] = fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":-32602,"message":"invalid params"}}`, reqs[1].ID)
				return out
			})
		}, wantErr: "invalid params", perm: true, calls: 1},
		{name: "missing logs", fn: func(_ int, b []byte) (int, string) {
			return 0, batchResp(b, func(_ []rpcReq, out []string) []string { return out[1:] })
		}, wantErr: "eth_getLogs response missing", calls: 4},
		{name: "missing header", fn: func(_ int, b []byte) (int, string) {
			return 0, batchResp(b, func(_ []rpcReq, out []string) []string { return out[:3] })
		}, wantErr: "header 3 response missing", calls: 4},
		{name: "unexpected id", fn: func(_ int, b []byte) (int, string) {
			return 0, batchResp(b, func(_ []rpcReq, out []string) []string { return append(out, result(999999, `null`)) })
		}, wantErr: "unexpected response id", calls: 4},
		{name: "bad id", fn: func(_ int, b []byte) (int, string) {
			return 0, batchResp(b, func(_ []rpcReq, out []string) []string { return append(out, `{"id":"x","result":null}`) })
		}, wantErr: "bad response id", calls: 4},
		{name: "garbage", fn: func(int, []byte) (int, string) { return 0, `<html>` }, wantErr: "decode response", calls: 4},
		{name: "bad logs", fn: func(_ int, b []byte) (int, string) {
			return 0, batchResp(b, func(reqs []rpcReq, out []string) []string { out[0] = result(reqs[0].ID, `{}`); return out })
		}, wantErr: "eth_getLogs: decode result", calls: 4},
		{name: "bad header", fn: func(_ int, b []byte) (int, string) {
			return 0, batchResp(b, func(reqs []rpcReq, out []string) []string { out[1] = result(reqs[1].ID, `[]`); return out })
		}, wantErr: "eth_getBlockByNumber: decode result", calls: 4},
		{name: "413", fn: func(int, []byte) (int, string) { return http.StatusRequestEntityTooLarge, "too big" }, wantErr: "range too large", perm: true, calls: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, n := scripted(t, tc.fn)
			logs, hdrs, err := client(srv.URL, nil).LogsWithHeaders(ctx, f)
			if n.Load() != tc.calls {
				t.Fatalf("calls %d, want %d (err %v)", n.Load(), tc.calls, err)
			}
			if tc.wantErr == "" {
				if err != nil || len(logs) != 1 || len(hdrs) != 3 || hdrs[2].Number != "0x3" {
					t.Fatalf("logs %v hdrs %v err %v", logs, hdrs, err)
				}
				return
			}
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.wantErr)) {
				t.Fatalf("err %v, want %q", err, tc.wantErr)
			}
			if tc.perm && !retry.IsPermanent(err) {
				t.Fatalf("want permanent: %v", err)
			}
		})
	}
}

func TestRPCErrorClassification(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		code       int
		msg        string
		method     string
		retryable  bool
		rangeLarge bool
	}{
		{-32005, "limit exceeded", "eth_blockNumber", true, false},
		{-32000, "header not found", "eth_blockNumber", true, false},
		{-32000, "unknown block", "eth_blockNumber", true, false},
		{-32603, "internal", "eth_blockNumber", true, false},
		{-32000, "request timeout", "eth_blockNumber", true, false},
		{-32601, "method not found", "eth_blockNumber", false, false},
		{-32005, "query returned more than 10000 results", "eth_getLogs", false, true},
		{-32000, "rate limit exceeded", "eth_getLogs", true, false},
	}
	for _, tc := range cases {
		o := &obs{}
		srv, n := scripted(t, func(_ int, b []byte) (int, string) {
			r := single(b)
			return 0, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":%d,"message":%q}}`, r.ID, tc.code, tc.msg)
		})
		c := client(srv.URL, o)
		var err error
		if tc.method == "eth_getLogs" {
			_, err = c.GetLogs(ctx, eth.LogFilter{FromBlock: 1, ToBlock: 2})
		} else {
			_, err = c.BlockNumber(ctx)
		}
		var re *eth.RPCError
		if !errors.As(err, &re) || re.Code != tc.code {
			t.Fatalf("%q: err %v", tc.msg, err)
		}
		if got := n.Load() > 1; got != tc.retryable {
			t.Fatalf("%q: retried=%v want %v", tc.msg, got, tc.retryable)
		}
		if errors.Is(err, eth.ErrRangeTooLarge) != tc.rangeLarge {
			t.Fatalf("%q: range=%v", tc.msg, !tc.rangeLarge)
		}
		if tc.retryable && (len(o.retries) == 0 || !strings.HasPrefix(o.retries[0], "rpc_")) {
			t.Fatalf("%q: retry reasons %v", tc.msg, o.retries)
		}
	}
}

func TestHTTPFailures(t *testing.T) {
	ctx := context.Background()
	o := &obs{}
	srv, n := scripted(t, func(i int, b []byte) (int, string) {
		switch i {
		case 1:
			return http.StatusServiceUnavailable, "down"
		case 2:
			return 0, `not json`
		case 3:
			return 0, result(single(b).ID, `"0xzz"`)
		default:
			return 0, result(single(b).ID, `"0x10"`)
		}
	})
	c := client(srv.URL, o)
	// 503 and a non-JSON body are retried; the bad quantity then fails the call.
	if _, err := c.BlockNumber(ctx); err == nil || !strings.Contains(err.Error(), "invalid quantity") {
		t.Fatal(err)
	}
	if n.Load() != 3 || o.reqs < 3 || len(o.retries) < 2 || o.retries[0] != "503" {
		t.Fatalf("calls %d reqs %d retries %v", n.Load(), o.reqs, o.retries)
	}
	// Result decode error into the wrong type.
	srv2, _ := scripted(t, func(_ int, b []byte) (int, string) { return 0, result(single(b).ID, `{}`) })
	if _, err := client(srv2.URL, nil).BlockNumber(ctx); err == nil || !strings.Contains(err.Error(), "decode result") {
		t.Fatal(err)
	}
	// Invalid URL is permanent.
	if _, err := client("http://[::1", nil).BlockNumber(ctx); err == nil || !retry.IsPermanent(err) {
		t.Fatal(err)
	}
	// Cancelled context.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := client(srv.URL, nil).BlockNumber(cctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if eth.IsAddress("0x123") || !eth.IsAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7") {
		t.Fatal("IsAddress")
	}
}
