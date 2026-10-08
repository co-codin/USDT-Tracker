package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/co-codin/USDT-Tracker/internal/chain/tron/trontest"
	"github.com/co-codin/USDT-Tracker/internal/config"
)

func TestVersionAndFlagErrors(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-version"}, &out, &errb); code != 0 || !strings.Contains(out.String(), "tron-usdt-listener") {
		t.Fatalf("-version: code=%d out=%q", code, out.String())
	}
	if code := run([]string{"-to", "5"}, &out, &errb); code != 2 {
		t.Fatalf("-to without -from: code=%d", code)
	}
	if code := run([]string{"-nope"}, &out, &errb); code != 2 {
		t.Fatalf("unknown flag: code=%d", code)
	}
}

func TestConfigErrorExitCode(t *testing.T) {
	t.Setenv("FILTER_WATCH_ADDRESSES", "not-an-address")
	var out, errb bytes.Buffer
	if code := run([]string{"-config", ""}, &out, &errb); code != 2 || !strings.Contains(errb.String(), "configuration error") {
		t.Fatalf("code=%d stderr=%q", code, errb.String())
	}
}

func TestBackfillAgainstMockNode(t *testing.T) {
	node := trontest.NewServer(100)
	defer node.Close()
	t.Setenv("TRON_API_URL", node.URL)
	t.Setenv("LISTENER_CONFIRMATIONS", "5")
	t.Setenv("LOG_FORMAT", "text")
	var out, errb bytes.Buffer
	if code := run([]string{"-config", "", "-from", "90", "-to", "91"}, &out, &errb); code != 0 {
		t.Fatalf("code=%d stderr=%s", code, errb.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != len(trontest.ExpectedTransfers(90, 91)) {
		t.Fatalf("got %d stdout lines, want %d:\n%s", len(lines), len(trontest.ExpectedTransfers(90, 91)), out.String())
	}
	var first map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("stdout is not JSON: %v", err)
	}
	if first["msg"] != "transfer" || first["token"] != "USDT" {
		t.Fatalf("unexpected line %v", first)
	}
	if !strings.Contains(errb.String(), "backfill done") {
		t.Fatalf("expected text logs on stderr, got %q", errb.String())
	}
}

func TestProbeHealth(t *testing.T) {
	healthy := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" || !healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	var errb bytes.Buffer
	if code := probeHealth(addr, &errb); code != 0 {
		t.Fatalf("healthy probe: code=%d %s", code, errb.String())
	}
	healthy = false
	if code := probeHealth(addr, &errb); code != 1 {
		t.Fatal("unhealthy probe should exit 1")
	}
	if probeHealth("", &errb) != 1 || probeHealth("bad", &errb) != 1 || probeHealth("127.0.0.1:1", &errb) != 1 {
		t.Fatal("invalid/unreachable addresses should exit 1")
	}
}

func TestNewLoggerLevels(t *testing.T) {
	var b bytes.Buffer
	newLogger(config.Log{Level: "warn", Format: "json"}, &b).Info("hidden")
	newLogger(config.Log{Level: "bogus", Format: "text"}, &b).Info("shown")
	if strings.Contains(b.String(), "hidden") || !strings.Contains(b.String(), "shown") {
		t.Fatalf("unexpected log output %q", b.String())
	}
}
