package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestServerEndpoints(t *testing.T) {
	healthy := true
	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "my_metric 1\n") })
	s := New("127.0.0.1:0", metrics, func(context.Context) (bool, any) {
		return healthy, map[string]int{"cursor": 7}
	}, slog.New(slog.DiscardHandler))
	if s.Addr() != nil {
		t.Fatal("Addr must be nil before Start")
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	}()
	base := "http://" + s.Addr().String()

	get := func(path string) (int, string) {
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, body := get("/healthz"); code != 200 || !strings.Contains(body, `"ok":true`) || !strings.Contains(body, `"cursor":7`) {
		t.Fatalf("healthy: %d %s", code, body)
	}
	healthy = false
	if code, body := get("/healthz"); code != 503 || !strings.Contains(body, `"ok":false`) {
		t.Fatalf("unhealthy: %d %s", code, body)
	}
	if code, body := get("/metrics"); code != 200 || !strings.Contains(body, "my_metric 1") {
		t.Fatalf("metrics: %d %s", code, body)
	}
	if code, _ := get("/nope"); code != 404 {
		t.Fatalf("unknown path: %d", code)
	}
	// Port already in use.
	if err := New(s.Addr().String(), nil, nil, slog.New(slog.DiscardHandler)).Start(); err == nil {
		t.Fatal("expected error when the port is taken")
	}
}

func TestServerHandleMountsExtraRoutes(t *testing.T) {
	s := New("127.0.0.1:0", nil, func(context.Context) (bool, any) { return true, nil }, slog.New(slog.DiscardHandler))
	s.Handle("/v1/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "api:"+r.URL.Path)
	}))
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	}()
	resp, err := http.Get("http://" + s.Addr().String() + "/v1/addresses")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(b) != "api:/v1/addresses" {
		t.Fatalf("mounted handler: %d %s", resp.StatusCode, b)
	}
}
