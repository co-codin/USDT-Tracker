package app

import (
	"context"
	"errors"
	"io"
	"math/big"
	"strings"
	"testing"

	"github.com/co-codin/USDT-Tracker/internal/config"
	"github.com/co-codin/USDT-Tracker/internal/filter"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/watch"
)

const (
	alice = "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx"
	bob   = "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv"
)

func transfer(from, to string) model.Transfer {
	return model.Transfer{From: from, To: to, Amount: big.NewInt(1)}
}

func TestBuildFilterStaticWithoutSource(t *testing.T) {
	cfg := config.Default()
	cfg.Filter.WatchAddresses = []string{alice}
	m, dyn, err := buildFilter(context.Background(), cfg, nil, nil, nil)
	if err != nil || dyn != nil {
		t.Fatalf("static: %v %v", dyn, err)
	}
	if _, ok := m.(*filter.Filter); !ok {
		t.Fatalf("want *filter.Filter, got %T", m)
	}
	if len(m.Apply([]model.Transfer{transfer(bob, alice), transfer(bob, bob)})) != 1 {
		t.Fatal("static filter not applied")
	}
}

func TestBuildFilterDynamicMergesSource(t *testing.T) {
	ctx := context.Background()
	store := watch.NewMemoryStore()
	_, _ = store.AddWatch(ctx, watch.Entry{Address: bob, Direction: "incoming", Enabled: true})
	cfg := config.Default()
	cfg.Filter.WatchAddresses = []string{alice}
	cfg.API.Token = "0123456789abcdef0123"

	m, dyn, err := buildFilter(ctx, cfg, nil, store, nil)
	if err != nil || dyn == nil || m != dyn {
		t.Fatalf("dynamic: %T %v", m, err)
	}
	// Loaded eagerly at startup: static alice + runtime bob (incoming only).
	if got := m.Apply([]model.Transfer{transfer(bob, alice), transfer(alice, bob), transfer(bob, alice)}); len(got) != 3 {
		t.Fatalf("merged filter matched %d, want 3", len(got))
	}
	if got := m.Apply([]model.Transfer{transfer(bob, "TEHwL3F2kpkJExYAZYziHGLFR2CR8Bq3bo")}); len(got) != 0 {
		t.Fatal("bob is incoming-only")
	}
	// API enabled → empty list is not a firehose.
	if m.MatchesEverything() {
		t.Fatal("dynamic filter must not be firehose")
	}

	// No token: an empty merged list keeps the legacy firehose behaviour.
	cfg.API.Token = ""
	cfg.Filter.WatchAddresses = nil
	m, _, err = buildFilter(ctx, cfg, nil, watch.NewMemoryStore(), nil)
	if err != nil || !m.MatchesEverything() {
		t.Fatalf("no token, empty list: firehose expected (%v)", err)
	}

	// Source unavailable at startup → error (fail fast, don't run blind).
	bad := watch.NewMemoryStore()
	bad.Err = errors.New("db down")
	if _, _, err := buildFilter(ctx, cfg, nil, bad, nil); err == nil {
		t.Fatal("expected startup error when the watch list cannot be loaded")
	}
	// Invalid static config is rejected for the dynamic filter too.
	cfg.Filter.Mode = "most"
	if _, _, err := buildFilter(ctx, cfg, nil, store, nil); err == nil {
		t.Fatal("expected config error")
	}
}

func TestNewStartupErrors(t *testing.T) {
	ctx := context.Background()
	cases := map[string]func(c *config.Config){
		"bad contract":      func(c *config.Config) { c.Token.Contract = "nope" },
		"bad min amount":    func(c *config.Config) { c.Filter.MinAmount = "x" },
		"bad watch address": func(c *config.Config) { c.Filter.WatchAddresses = []string{"Tnope"} },
		"postgres down": func(c *config.Config) {
			c.Sinks.Postgres = config.PostgresSink{Enabled: true, DSN: "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1"}
		},
		"bad webhook": func(c *config.Config) {
			c.Sinks.Webhook = config.WebhookSink{Enabled: true, URL: "::not a url"}
		},
	}
	for name, mutate := range cases {
		cfg := config.Default()
		mutate(&cfg)
		if _, err := New(ctx, cfg, nil, Options{Stdout: io.Discard}); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestCursorDescAndCanonicalAddresses(t *testing.T) {
	a := &App{cfg: config.Default(), opts: Options{Backfill: true}}
	if !strings.Contains(a.cursorDesc(), "memory") {
		t.Fatal(a.cursorDesc())
	}
	a.opts.Backfill = false
	if !strings.HasPrefix(a.cursorDesc(), "file:") {
		t.Fatal(a.cursorDesc())
	}
	got := canonicalAddresses([]string{"41f4a3aa3c52cdc41e3c1a7b8f7493ae1164ee7f07", bob, "junk"})
	if len(got) != 2 || got[0] != alice || got[1] != bob {
		t.Fatalf("canonical = %v", got)
	}
}
