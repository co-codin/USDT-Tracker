package sink

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/retry"
)

var fastB = retry.Backoff{Initial: time.Millisecond, Max: 2 * time.Millisecond}

type flakySink struct {
	name     string
	fail     int
	closeErr error
	mu       sync.Mutex
	got      int
}

func (f *flakySink) Name() string { return f.name }
func (f *flakySink) Send(_ context.Context, b Batch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail > 0 {
		f.fail--
		return errors.New(f.name + " down")
	}
	f.got += len(b.Transfers)
	return nil
}
func (f *flakySink) Close() error { return f.closeErr }

type sinkRec struct {
	mu  sync.Mutex
	obs []string
}

func (r *sinkRec) ObserveSink(name, status string, _ int, _ time.Duration) {
	r.mu.Lock()
	r.obs = append(r.obs, name+":"+status)
	r.mu.Unlock()
}

func TestDispatcherFailurePaths(t *testing.T) {
	req := &flakySink{name: "req", fail: 1}
	be := &flakySink{name: "be", fail: 1}
	ok := &flakySink{name: "ok", closeErr: errors.New("flush failed")}
	r := &sinkRec{}
	d := NewDispatcher([]Entry{{Sink: req}, {Sink: be, BestEffort: true}, {Sink: ok}}, 100, r, nil)
	if got := strings.Join(d.Names(), ","); got != "req,be,ok" {
		t.Fatal(got)
	}
	if err := d.Dispatch(context.Background(), Batch{}); err != nil {
		t.Fatal("empty batch must be a no-op")
	}
	b := sampleBatch()
	err := d.Dispatch(context.Background(), b)
	if err == nil || !strings.Contains(err.Error(), "sink req: req down") {
		t.Fatal(err)
	}
	// Retry: only the failed required sink gets the batch again; the
	// best-effort failure is not retried and the ok sink is deduped.
	if err := d.Dispatch(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	n := len(b.Transfers)
	if req.got != n || be.got != 0 || ok.got != n {
		t.Fatalf("req %d be %d ok %d", req.got, be.got, ok.got)
	}
	joined := strings.Join(r.obs, " ")
	for _, w := range []string{"req:error", "be:dropped", "ok:ok", "req:ok"} {
		if !strings.Contains(joined, w) {
			t.Fatalf("missing %s in %s", w, joined)
		}
	}
	if err := d.Close(); err == nil || !strings.Contains(err.Error(), "close ok: flush failed") {
		t.Fatal(err)
	}
}

func TestSinkNamesAndClose(t *testing.T) {
	var buf bytes.Buffer
	s := NewStdout(&buf)
	tr := sampleBatch().Transfers[0]
	tr.Memo = "inv-1"
	if err := s.Send(context.Background(), Batch{Transfers: []model.Transfer{tr}}); err != nil || !strings.Contains(buf.String(), `"memo":"inv-1"`) {
		t.Fatal(err, buf.String())
	}
	tg, err := NewTelegram(TelegramConfig{BotToken: "t", ChatID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	wh, err := NewWebhook(WebhookConfig{URL: "https://example.invalid/h"})
	if err != nil {
		t.Fatal(err)
	}
	if s.Name() != "stdout" || tg.Name() != "telegram" || wh.Name() != "webhook" || s.Close() != nil || tg.Close() != nil || wh.Close() != nil {
		t.Fatal("names/close")
	}
	if _, err := NewTelegram(TelegramConfig{BotToken: "t"}); err == nil {
		t.Fatal("chat id required")
	}
	if _, err := NewWebhook(WebhookConfig{URL: "ftp://x"}); err == nil {
		t.Fatal("scheme")
	}
	if short("abc", 5) != "abc" || tail("abc", 5) != "abc" || truncRunes("абв", 5) != "абв" {
		t.Fatal("helpers")
	}
}

func TestWebhookFailurePaths(t *testing.T) {
	var calls atomic.Int32
	var mode atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		switch mode.Load() {
		case 0: // 429 with Retry-After 0 then 503 then ok
			switch calls.Load() {
			case 1:
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusTooManyRequests)
			case 2:
				w.WriteHeader(http.StatusServiceUnavailable)
			default:
				w.WriteHeader(http.StatusNoContent)
			}
		case 1:
			http.Error(w, "bad signature", http.StatusUnauthorized)
		case 2:
			w.WriteHeader(http.StatusBadGateway)
		}
	}))
	defer srv.Close()
	newWH := func() *Webhook {
		w, err := NewWebhook(WebhookConfig{URL: srv.URL + "/hook?token=SECRET", MaxRetries: 3, Backoff: fastB, Headers: map[string]string{"X-A": "1"}})
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	ctx := context.Background()
	if err := newWH().Send(ctx, sampleBatch()); err != nil || calls.Load() != 3 {
		t.Fatalf("err %v calls %d", err, calls.Load())
	}
	mode.Store(1)
	calls.Store(0)
	err := newWH().Send(ctx, sampleBatch())
	if !retry.IsPermanent(err) || calls.Load() != 1 || !strings.Contains(err.Error(), "HTTP 401: bad signature") {
		t.Fatalf("err %v calls %d", err, calls.Load())
	}
	mode.Store(2)
	calls.Store(0)
	if err := newWH().Send(ctx, sampleBatch()); err == nil || calls.Load() != 3 {
		t.Fatalf("err %v calls %d", err, calls.Load())
	}
	// Transport error: the secret in the URL never shows up.
	w, _ := NewWebhook(WebhookConfig{URL: "http://127.0.0.1:1/hook?token=SECRET", MaxRetries: 1, Backoff: fastB})
	if err := w.Send(ctx, sampleBatch()); err == nil || strings.Contains(err.Error(), "SECRET") || !strings.Contains(err.Error(), "webhook http://127.0.0.1:1/***") {
		t.Fatalf("err %v", err)
	}
	// Cancelled context.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := newWH().Send(cctx, sampleBatch()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestTelegramFailurePaths(t *testing.T) {
	var calls atomic.Int32
	var mode atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		switch mode.Load() {
		case 0:
			if calls.Load() == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = io.WriteString(w, `{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":0}}`)
				return
			}
			_, _ = io.WriteString(w, `{"ok":true,"result":{}}`)
		case 1:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"ok":false,"error_code":400,"description":"chat not found"}`)
		case 2:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()
	tg, err := NewTelegram(TelegramConfig{BotToken: "SECRET", ChatID: "c", APIBase: srv.URL, MaxRetries: 3, Backoff: fastB})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	b := sampleBatch()
	b.Transfers[0].Amount = big.NewInt(1)
	if err := tg.Send(ctx, b); err != nil || calls.Load() != 2 {
		t.Fatalf("err %v calls %d", err, calls.Load())
	}
	mode.Store(1)
	calls.Store(0)
	if err := tg.Send(ctx, b); err == nil || calls.Load() != 1 || !strings.Contains(err.Error(), "chat not found") || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("err %v calls %d", err, calls.Load())
	}
	mode.Store(2)
	calls.Store(0)
	if err := tg.Send(ctx, b); err == nil || calls.Load() != 3 {
		t.Fatalf("err %v calls %d", err, calls.Load())
	}
	dead, _ := NewTelegram(TelegramConfig{BotToken: "SECRET", ChatID: "c", APIBase: "http://127.0.0.1:1", MaxRetries: 1, Backoff: fastB})
	if err := dead.Send(ctx, b); err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("err %v", err)
	}
}
