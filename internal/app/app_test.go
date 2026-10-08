package app_test

// End-to-end tests: the real composition root (config → tron client → source
// → decoder → filter → dispatcher → sinks → cursor) against an in-process mock
// TRON node (trontest) and mock webhook / Telegram servers. No internet needed.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/co-codin/USDT-Tracker/internal/app"
	"github.com/co-codin/USDT-Tracker/internal/chain/tron/trontest"
	"github.com/co-codin/USDT-Tracker/internal/config"
	"github.com/co-codin/USDT-Tracker/internal/retry"
	"github.com/co-codin/USDT-Tracker/internal/sink"
	"github.com/co-codin/USDT-Tracker/pkg/webhooksig"
)

var fastBackoff = retry.Backoff{Initial: time.Millisecond, Max: 5 * time.Millisecond}

const txInfoPath = "/wallet/gettransactioninfobyblocknum"

func baseCfg(t *testing.T, tronURL string) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Tron.APIURL = tronURL
	cfg.Tron.RPS = 1000
	cfg.Tron.MaxRetries = 5
	cfg.Listener.Confirmations = 5
	cfg.Listener.PollInterval = config.Duration(10 * time.Millisecond)
	cfg.Listener.BatchSize = 7
	cfg.Listener.Concurrency = 3
	cfg.Cursor.File = filepath.Join(t.TempDir(), "cursor.json")
	cfg.HTTP.Addr = "127.0.0.1:0"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// syncBuffer is a goroutine-safe stdout capture.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

type stdoutLine struct {
	ID        string `json:"id"`
	Block     int64  `json:"block"`
	From      string `json:"from"`
	To        string `json:"to"`
	Amount    string `json:"amount"`
	AmountRaw string `json:"amount_raw"`
	Reasons   string `json:"reasons"`
}

func (s *syncBuffer) lines(t *testing.T) []stdoutLine {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []stdoutLine
	sc := bufio.NewScanner(bytes.NewReader(s.b.Bytes()))
	for sc.Scan() {
		var l stdoutLine
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			t.Fatalf("stdout line is not JSON: %q", sc.Text())
		}
		out = append(out, l)
	}
	return out
}

// hookReceiver is a mock webhook endpoint that verifies signatures.
type hookReceiver struct {
	*httptest.Server
	secret     string
	mu         sync.Mutex
	failFirst  int
	requests   int
	badSig     int
	keys       map[string]int
	deliveries map[string][]string // delivery id -> keys
	blocks     []int64
}

func newHookReceiver(secret string, failFirst int) *hookReceiver {
	h := &hookReceiver{secret: secret, failFirst: failFirst, keys: map[string]int{}, deliveries: map[string][]string{}}
	h.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		h.mu.Lock()
		defer h.mu.Unlock()
		h.requests++
		if h.failFirst > 0 {
			h.failFirst--
			http.Error(w, "temporarily down", http.StatusInternalServerError)
			return
		}
		if err := webhooksig.Verify([]byte(h.secret), r.Header.Get(webhooksig.HeaderTimestamp),
			r.Header.Get(webhooksig.HeaderSignature), body, time.Minute, time.Now()); err != nil {
			h.badSig++
			http.Error(w, "bad signature", http.StatusUnauthorized)
			return
		}
		var p sink.WebhookPayload
		if err := json.Unmarshal(body, &p); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		if r.Header.Get(sink.HeaderDelivery) != p.DeliveryID {
			http.Error(w, "delivery header mismatch", http.StatusBadRequest)
			return
		}
		var keys []string
		for _, tr := range p.Transfers {
			h.keys[tr.Key()]++
			keys = append(keys, tr.Key())
		}
		h.deliveries[p.DeliveryID] = keys
		h.blocks = append(h.blocks, p.BlockNumber)
		w.WriteHeader(http.StatusOK)
	}))
	return h
}

func (h *hookReceiver) snapshot() (map[string]int, []int64, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	keys := make(map[string]int, len(h.keys))
	for k, v := range h.keys {
		keys[k] = v
	}
	return keys, append([]int64(nil), h.blocks...), h.badSig
}

// runUntil runs a until its cursor reaches target, calls during (if any)
// while the app is still running, then shuts it down gracefully.
func runUntil(t *testing.T, a *app.App, target int64, during func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx, 0, 0) }()
	deadline := time.After(15 * time.Second)
	for a.Listener.Status().Cursor < target {
		select {
		case err := <-done:
			t.Fatalf("app exited early: %v", err)
		case <-deadline:
			t.Fatalf("timeout: cursor %d, want %d (last error: %s)", a.Listener.Status().Cursor, target, a.Listener.Status().LastError)
		case <-time.After(5 * time.Millisecond):
		}
	}
	if during != nil {
		during()
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("app did not shut down")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
}

func newApp(t *testing.T, cfg config.Config, stdout io.Writer, backfill bool) *app.App {
	t.Helper()
	a, err := app.New(context.Background(), cfg, nil, app.Options{Stdout: stdout, Backoff: fastBackoff, Backfill: backfill})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func expectedKeys(from, to int64) map[string]trontest.Expected {
	m := map[string]trontest.Expected{}
	for _, e := range trontest.ExpectedTransfers(from, to) {
		m[e.Key] = e
	}
	return m
}

func assertExactlyOnce(t *testing.T, what string, got map[string]int, want map[string]trontest.Expected) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: got %d distinct transfers, want %d", what, len(got), len(want))
	}
	for k := range want {
		if got[k] != 1 {
			t.Errorf("%s: transfer %s delivered %d times, want 1", what, k, got[k])
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("%s: unexpected transfer %s", what, k)
		}
	}
}

func assertBlocksContiguous(t *testing.T, blocks []int64, from, to int64) {
	t.Helper()
	// Empty blocks produce no webhook call, so compare against non-empty ones.
	var want []int64
	for n := from; n <= to; n++ {
		if !trontest.IsEmptyBlock(n) {
			want = append(want, n)
		}
	}
	if len(blocks) != len(want) {
		t.Fatalf("delivered blocks %v, want %v", blocks, want)
	}
	for i := range want {
		if blocks[i] != want[i] {
			t.Fatalf("blocks out of order or missing: got %v, want %v", blocks, want)
		}
	}
}

func readCursor(t *testing.T, path string) int64 {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var st struct {
		LastBlock int64 `json:"last_block"`
	}
	_ = json.Unmarshal(b, &st)
	return st.LastBlock
}

func TestE2E_RangeConfirmationsRetriesAndOutputs(t *testing.T) {
	node := trontest.NewServer(1000)
	defer node.Close()
	node.FailBlock(983, 429, 429)
	node.FailBlock(987, 500, 502)
	node.EmptyOnce(990) // lagging node: [] although the block has txs

	hook := newHookReceiver("e2e-secret", 0)
	defer hook.Close()

	cfg := baseCfg(t, node.URL)
	cfg.Listener.StartBlock = 980
	cfg.Sinks.Webhook = config.WebhookSink{Enabled: true, URL: hook.URL, Secret: "e2e-secret", MaxRetries: 3}
	out := &syncBuffer{}
	a := newApp(t, cfg, out, false)

	var health, metricsText string
	runUntil(t, a, 995, func() {
		base := "http://" + a.HTTPAddr()
		resp, err := http.Get(base + "/healthz")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("/healthz status %d: %s", resp.StatusCode, b)
		}
		health = string(b)
		resp, err = http.Get(base + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		b, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		metricsText = string(b)
	})

	want := expectedKeys(980, 995)
	// stdout sink: every expected transfer exactly once with correct fields.
	got := map[string]int{}
	for _, l := range out.lines(t) {
		got[l.ID]++
		e, ok := want[l.ID]
		if !ok {
			continue
		}
		if l.Block != e.Block || l.From != e.From || l.To != e.To || l.AmountRaw != e.Amount.String() {
			t.Errorf("stdout %s: got %+v, want %+v", l.ID, l, e)
		}
	}
	assertExactlyOnce(t, "stdout", got, want)

	// webhook sink: signed, in block order, exactly once.
	keys, blocks, badSig := hook.snapshot()
	if badSig != 0 {
		t.Errorf("%d webhook deliveries had bad signatures", badSig)
	}
	assertExactlyOnce(t, "webhook", keys, want)
	assertBlocksContiguous(t, blocks, 980, 995)

	// confirmations: never asked for blocks above head - confirmations.
	if m := node.MaxRequestedBlock(); m > 995 {
		t.Errorf("requested block %d beyond safe head 995", m)
	}
	// retries actually happened.
	if n := node.TxInfoRequests(983); n != 3 {
		t.Errorf("block 983 requested %d times, want 3 (2x429 + ok)", n)
	}
	if n := node.TxInfoRequests(987); n != 3 {
		t.Errorf("block 987 requested %d times, want 3 (500, 502, ok)", n)
	}
	if n := node.TxInfoRequests(990); n < 2 {
		t.Errorf("block 990 requested %d times, want >= 2 (empty response must be re-fetched)", n)
	}
	m := a.Metrics
	if v := testutil.ToFloat64(m.RPCRetries.WithLabelValues(txInfoPath, "rate_limited")); v != 2 {
		t.Errorf("rate_limited retries = %v, want 2", v)
	}
	if v := testutil.ToFloat64(m.RPCRetries.WithLabelValues(txInfoPath, "500")) + testutil.ToFloat64(m.RPCRetries.WithLabelValues(txInfoPath, "502")); v != 2 {
		t.Errorf("5xx retries = %v, want 2", v)
	}
	if v := testutil.ToFloat64(m.BlockErrors); v < 1 {
		t.Errorf("block_fetch_errors_total = %v, want >= 1 (inconsistent empty block)", v)
	}
	if v := testutil.ToFloat64(m.TransfersDecoded); int(v) != len(want) {
		t.Errorf("transfers_decoded_total = %v, want %d", v, len(want))
	}
	if readCursor(t, cfg.Cursor.File) < 995 {
		t.Errorf("cursor file not advanced")
	}
	if !strings.Contains(health, `"ok":true`) {
		t.Errorf("unexpected /healthz body: %s", health)
	}
	for _, s := range []string{"tron_listener_cursor_block", "tron_listener_rpc_requests_total", "tron_listener_sink_deliveries_total{sink=\"webhook\",status=\"ok\"}"} {
		if !strings.Contains(metricsText, s) {
			t.Errorf("/metrics missing %s", s)
		}
	}
}

func TestE2E_RestartResumesFromCursorWithoutGaps(t *testing.T) {
	node := trontest.NewServer(500)
	defer node.Close()
	hook := newHookReceiver("s", 0)
	defer hook.Close()

	cfg := baseCfg(t, node.URL)
	cfg.Listener.StartBlock = -10 // first run: safe head (495) - 10 = 485
	cfg.Sinks.Stdout.Enabled = false
	cfg.Sinks.Webhook = config.WebhookSink{Enabled: true, URL: hook.URL, Secret: "s", MaxRetries: 3}

	// Run 1: 485..495.
	runUntil(t, newApp(t, cfg, io.Discard, false), 495, nil)
	if c := readCursor(t, cfg.Cursor.File); c != 495 {
		t.Fatalf("cursor after run 1 = %d, want 495", c)
	}

	// Chain advances while we're down; run 2 must resume at 496, not at head.
	node.SetHead(530)
	cfg.Listener.StartBlock = 0 // ignored: the cursor wins
	runUntil(t, newApp(t, cfg, io.Discard, false), 525, nil)

	keys, blocks, _ := hook.snapshot()
	assertExactlyOnce(t, "webhook after restart", keys, expectedKeys(485, 525))
	assertBlocksContiguous(t, blocks, 485, 525)

	// Crash simulation: blocks 521..525 were delivered but the process died
	// before persisting the cursor. On restart they are re-delivered
	// (at-least-once) with identical idempotency keys / delivery ids.
	hook.mu.Lock()
	before := map[string][]string{}
	for id, k := range hook.deliveries {
		before[id] = k
	}
	hook.mu.Unlock()
	if err := os.WriteFile(cfg.Cursor.File, []byte(`{"last_block":520}`), 0o644); err != nil {
		t.Fatal(err)
	}
	runUntil(t, newApp(t, cfg, io.Discard, false), 525, nil)
	keys, _, _ = hook.snapshot()
	for k, e := range expectedKeys(485, 525) {
		want := 1
		if e.Block > 520 {
			want = 2
		}
		if keys[k] != want {
			t.Errorf("after crash replay: %s delivered %d times, want %d", k, keys[k], want)
		}
	}
	hook.mu.Lock()
	defer hook.mu.Unlock()
	if len(hook.deliveries) != len(before) {
		t.Errorf("replayed deliveries must reuse delivery ids: %d ids before, %d after", len(before), len(hook.deliveries))
	}
}

func TestE2E_DedupeWhenOneSinkFails(t *testing.T) {
	node := trontest.NewServer(400)
	defer node.Close()
	hook := newHookReceiver("s", 3) // first 3 webhook calls fail with 500
	defer hook.Close()

	cfg := baseCfg(t, node.URL)
	cfg.Sinks.Webhook = config.WebhookSink{Enabled: true, URL: hook.URL, Secret: "s", MaxRetries: 1}
	out := &syncBuffer{}
	a := newApp(t, cfg, out, true)
	if err := a.Run(context.Background(), 300, 312); err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	want := expectedKeys(300, 312)
	got := map[string]int{}
	for _, l := range out.lines(t) {
		got[l.ID]++
	}
	// The block is retried until the webhook succeeds; stdout already
	// succeeded and must not see duplicates (dedupe by tx id + log index).
	assertExactlyOnce(t, "stdout", got, want)
	keys, _, _ := hook.snapshot()
	assertExactlyOnce(t, "webhook", keys, want)
	if v := testutil.ToFloat64(a.Metrics.DispatchRetries); v != 3 {
		t.Errorf("dispatch_retries_total = %v, want 3", v)
	}
	if v := testutil.ToFloat64(a.Metrics.SinkDeliveries.WithLabelValues("webhook", "error")); v != 3 {
		t.Errorf("webhook errors = %v, want 3", v)
	}
}

func TestE2E_FilterWatchListThresholdAndTelegram(t *testing.T) {
	node := trontest.NewServer(200)
	defer node.Close()

	var mu sync.Mutex
	var messages []string
	tg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/botTEST:TOKEN/sendMessage" {
			http.NotFound(w, r)
			return
		}
		var body struct {
			ChatID string `json:"chat_id"`
			Text   string `json:"text"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.ChatID != "-100123" {
			http.Error(w, `{"ok":false,"description":"chat not found"}`, http.StatusBadRequest)
			return
		}
		mu.Lock()
		messages = append(messages, body.Text)
		mu.Unlock()
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer tg.Close()

	cfg := baseCfg(t, node.URL)
	cfg.Filter.WatchAddresses = []string{trontest.Bob}
	cfg.Filter.Direction = "incoming"
	cfg.Filter.MinAmount = "100000"
	cfg.Sinks.Telegram = config.TelegramSink{Enabled: true, BotToken: "TEST:TOKEN", ChatID: "-100123", APIBase: tg.URL, MaxPerBlock: 10}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	out := &syncBuffer{}
	a := newApp(t, cfg, out, true)
	if err := a.Run(context.Background(), 101, 102); err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	// Per block: Alice->Bob (incoming to watched) and Carol->Dave (>=100k);
	// Dave->Alice (1 unit, unwatched) is filtered out.
	lines := out.lines(t)
	if len(lines) != 4 {
		t.Fatalf("got %d matched transfers, want 4: %+v", len(lines), lines)
	}
	reasons := map[string]int{}
	for _, l := range lines {
		reasons[l.Reasons]++
		if l.From == trontest.Dave {
			t.Errorf("unwatched small transfer leaked: %+v", l)
		}
	}
	if reasons["watch_incoming"] != 2 || reasons["large_transfer"] != 2 {
		t.Errorf("unexpected reasons %v", reasons)
	}

	mu.Lock()
	defer mu.Unlock()
	sort.Strings(messages)
	if len(messages) != 4 {
		t.Fatalf("got %d telegram messages, want 4", len(messages))
	}
	joined := strings.Join(messages, "\n---\n")
	for _, s := range []string{"📥 Incoming USDT", "🐋 Large USDT transfer", "100,101 USDT", "101.5 USDT", trontest.Bob} {
		if !strings.Contains(joined, s) {
			t.Errorf("telegram messages missing %q:\n%s", s, joined)
		}
	}
}

func TestE2E_BackfillBeyondSafeHeadFails(t *testing.T) {
	node := trontest.NewServer(100)
	defer node.Close()
	a := newApp(t, baseCfg(t, node.URL), io.Discard, true)
	defer a.Close()
	if err := a.Run(context.Background(), 90, 99); err == nil {
		t.Fatal("expected error: 99 is above safe head 95")
	}
}

func TestE2E_UnhealthyWhenNodeDown(t *testing.T) {
	node := trontest.NewServer(100)
	cfg := baseCfg(t, node.URL)
	cfg.HTTP.StaleAfter = config.Duration(50 * time.Millisecond)
	node.Close() // node unreachable from the start
	a := newApp(t, cfg, io.Discard, false)
	defer a.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx, 0, 0) }()
	defer func() { cancel(); <-done }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if addr := a.HTTPAddr(); addr != "" {
			resp, err := http.Get("http://" + addr + "/healthz")
			if err == nil {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusServiceUnavailable {
					if !strings.Contains(string(b), "last_error") {
						t.Errorf("503 body should include last_error: %s", b)
					}
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("/healthz never reported unhealthy while the node was down")
}
