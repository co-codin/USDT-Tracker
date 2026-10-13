package app_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/co-codin/USDT-Tracker/internal/app"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton/tontest"
	"github.com/co-codin/USDT-Tracker/internal/chain/tron/trontest"
	"github.com/co-codin/USDT-Tracker/internal/config"
	"github.com/co-codin/USDT-Tracker/internal/listener"
)

func tonCfg(t *testing.T, cfg config.Config, url string) config.Config {
	t.Helper()
	cfg.TON.APIURL = url
	cfg.TON.RPS = 1000
	cfg.TON.MaxRetries = 5
	cfg.TON.Confirmations = 3
	cfg.TON.PollInterval = config.Duration(10 * time.Millisecond)
	cfg.TON.BatchSize = 6
	cfg.TON.Concurrency = 3
	cfg.TON.CursorFile = filepath.Join(t.TempDir(), "cursor-ton.json")
	return cfg
}

func pipeline(a *app.App, chain string) *listener.Listener {
	for _, p := range a.Pipelines {
		if p.Chain == chain {
			return p.Listener
		}
	}
	return nil
}

func waitChain(t *testing.T, l *listener.Listener, target int64) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for l.Status().Cursor < target {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: cursor %d, want %d (last error: %s)", l.Status().Cursor, target, l.Status().LastError)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func tonExpected(srv *tontest.Server, from, to int64) map[string]tontest.Expected {
	m := map[string]tontest.Expected{}
	for _, e := range srv.Expected(from, to) {
		m[e.Key] = e
	}
	return m
}

// TON only, webhook + stdout, with API faults and a restart that resumes
// from the TON cursor file: every USD₮ jetton transfer exactly once.
func TestE2E_TONOnly(t *testing.T) {
	srv := tontest.New(80)
	defer srv.Close()
	srv.APIKey = "ton-secret"
	srv.Fail("/jetton/transfers", 429, 502)
	srv.Fail("/masterchainBlockShards", 500)
	hook := newHookReceiver("s3cret", 1)
	defer hook.Close()

	cfg := tonCfg(t, baseCfg(t, "http://unused.invalid"), srv.BaseURL())
	cfg.Chains = []string{"ton"}
	cfg.TON.APIKey = "ton-secret"
	cfg.TON.StartBlock = 10
	cfg.Sinks.Webhook = config.WebhookSink{Enabled: true, URL: hook.URL, Secret: "s3cret", Timeout: config.Duration(time.Second), MaxRetries: 1}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	out := &syncBuffer{}
	a := newApp(t, cfg, out, false)
	if a.Listener != pipeline(a, "ton") || len(a.Pipelines) != 1 {
		t.Fatal("ton-only app must have one ton pipeline")
	}
	stop := startApp(t, a)
	waitChain(t, a.Listener, 50)
	_, body := apiCall(t, a, "GET", "/healthz", "", "")
	resp, _ := http.Get("http://" + a.HTTPAddr() + "/metrics")
	mb, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	stop()
	if !strings.Contains(body, `"ok":true`) || strings.Contains(body, `"chains"`) {
		t.Errorf("single-chain health shape changed: %s", body)
	}
	for _, s := range []string{`usdt_tracker_cursor_block{chain="ton"}`, `usdt_tracker_rpc_requests_total{chain="ton",endpoint="/jetton/transfers"`, `usdt_tracker_rpc_retries_total{chain="ton",endpoint="/jetton/transfers",reason="rate_limited"} 1`} {
		if !strings.Contains(string(mb), s) {
			t.Errorf("metrics missing %s", s)
		}
	}
	if v := testutil.ToFloat64(a.Metrics.BlocksProcessed.WithLabelValues("ton")); v < 40 {
		t.Errorf("ton blocks processed %v", v)
	}

	srv.SetHead(120)
	a2 := newApp(t, cfg, out, false)
	stop = startApp(t, a2)
	waitChain(t, a2.Listener, 110)
	stop()
	last := readCursor(t, cfg.TON.CursorFile)
	if last < 110 {
		t.Fatalf("ton cursor %d", last)
	}
	want := tonExpected(srv, 10, last)
	keys, _, bad := hook.snapshot()
	if bad != 0 {
		t.Errorf("bad signatures %d", bad)
	}
	assertOnce(t, "webhook", keys, want)
	lines := map[string]int{}
	for _, l := range out.lines(t) {
		lines[l.ID]++
		if e, ok := want[l.ID]; ok && (l.From != e.From || l.To != e.To || l.AmountRaw != e.Amount.String()) {
			t.Errorf("stdout %s mismatch %+v", l.ID, l)
		}
	}
	assertOnce(t, "stdout", lines, want)
	if _, err := os.Stat(cfg.Cursor.File); err == nil {
		t.Error("tron cursor file must not be written when tron is disabled")
	}
}

func assertOnce(t *testing.T, what string, got map[string]int, want map[string]tontest.Expected) {
	t.Helper()
	if len(got) != len(want) || len(want) == 0 {
		t.Errorf("%s: got %d distinct transfers, want %d", what, len(got), len(want))
	}
	for k := range want {
		if got[k] != 1 {
			t.Errorf("%s: %s delivered %d times", what, k, got[k])
		}
	}
}

// Both chains run concurrently with shared sinks and separate cursors; the
// watch filter on TON uses a user-friendly address from config.
func TestE2E_TronAndTONConcurrently(t *testing.T) {
	node := trontest.NewServer(400)
	defer node.Close()
	srv := tontest.New(200)
	defer srv.Close()

	cfg := tonCfg(t, baseCfg(t, node.URL), srv.BaseURL())
	cfg.Chains = []string{"tron", "ton"}
	cfg.Listener.StartBlock = 360
	cfg.TON.StartBlock = 150
	bob, _ := ton.ParseAddress(tontest.Bob)
	cfg.TON.WatchAddresses = []string{bob.Friendly(false, false)} // Bob receives in blocks %3 == 0
	cfg.Filter.Direction = "incoming"
	cfg.Filter.WatchAddresses = []string{trontest.Bob}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	out := &syncBuffer{}
	a := newApp(t, cfg, out, false)
	stop := startApp(t, a)
	waitChain(t, pipeline(a, "tron"), 390)
	waitChain(t, pipeline(a, "ton"), 190)
	_, body := apiCall(t, a, "GET", "/healthz", "", "")
	stop()

	var h struct {
		OK     bool `json:"ok"`
		Detail struct {
			Chains map[string]listener.Status `json:"chains"`
		} `json:"detail"`
	}
	_ = json.Unmarshal([]byte(body), &h)
	if !strings.Contains(body, `"chains"`) || !strings.Contains(body, `"ton"`) || !strings.Contains(body, `"tron"`) {
		t.Errorf("multi-chain health: %s", body)
	}

	var tonLines, tronLines int
	for _, l := range out.lines(t) {
		switch {
		case strings.HasPrefix(l.ID, "ton:"):
			tonLines++
			if l.To != tontest.Lower(tontest.Bob) || l.Reasons != "watch_incoming" {
				t.Errorf("ton line not for bob: %+v", l)
			}
		default:
			tronLines++
			if l.To != trontest.Bob {
				t.Errorf("tron line not for bob: %+v", l)
			}
		}
	}
	tonWant := 0
	for _, e := range srv.Expected(150, readCursor(t, cfg.TON.CursorFile)) {
		if e.To == tontest.Lower(tontest.Bob) {
			tonWant++
		}
	}
	if tonLines != tonWant || tonWant == 0 || tronLines == 0 {
		t.Errorf("ton lines %d (want %d), tron lines %d", tonLines, tonWant, tronLines)
	}
	if readCursor(t, cfg.Cursor.File) < 390 || readCursor(t, cfg.TON.CursorFile) < 190 {
		t.Error("cursors not advanced independently")
	}
}

func TestE2E_TONBackfillNeedsChain(t *testing.T) {
	srv := tontest.New(50)
	defer srv.Close()
	node := trontest.NewServer(50)
	defer node.Close()
	cfg := tonCfg(t, baseCfg(t, node.URL), srv.BaseURL())
	cfg.Chains = []string{"tron", "ton"}
	if _, err := app.New(context.Background(), cfg, nil, app.Options{Backfill: true, Stdout: io.Discard}); err == nil || !strings.Contains(err.Error(), "-chain") {
		t.Fatalf("want -chain hint, got %v", err)
	}
	out := &syncBuffer{}
	a, err := app.New(context.Background(), cfg, nil, app.Options{Backfill: true, BackfillChain: "ton", Stdout: out, Backoff: fastBackoff})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Run(context.Background(), 20, 30); err != nil {
		t.Fatal(err)
	}
	_ = a.Close()
	got := map[string]int{}
	for _, l := range out.lines(t) {
		got[l.ID]++
	}
	assertOnce(t, "backfill", got, tonExpected(srv, 20, 30))
	if _, err := app.New(context.Background(), cfg, nil, app.Options{Backfill: true, BackfillChain: "eth", Stdout: io.Discard}); err == nil {
		t.Error("unknown backfill chain accepted")
	}
}
