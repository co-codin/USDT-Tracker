package app_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain/eth"
	"github.com/co-codin/USDT-Tracker/internal/chain/eth/ethtest"
	"github.com/co-codin/USDT-Tracker/internal/config"
)

func ethCfg(t *testing.T, cfg config.Config, url string) config.Config {
	t.Helper()
	cfg.ETH.RPCURL = url
	cfg.ETH.RPS = 1000
	cfg.ETH.MaxRetries = 5
	cfg.ETH.Confirmations = 2
	cfg.ETH.PollInterval = config.Duration(10 * time.Millisecond)
	cfg.ETH.BatchSize = 7
	cfg.ETH.LogChunk = 3
	cfg.ETH.CursorFile = filepath.Join(t.TempDir(), "cursor-eth.json")
	return cfg
}

// Ethereum only, stdout firehose, 429s and node range limits: every USDT
// Transfer of the confirmed range exactly once, with the 0x tx hash.
func TestE2E_ETHOnly(t *testing.T) {
	node := ethtest.New(60)
	defer node.Close()
	node.MaxRange = 2
	node.Fail429(2)
	cfg := ethCfg(t, baseCfg(t, "http://unused.invalid"), node.URL)
	cfg.Chains = []string{"eth"}
	cfg.ETH.StartBlock = 10
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	out := &syncBuffer{}
	a := newApp(t, cfg, out, false)
	stop := startApp(t, a)
	waitChain(t, a.Listener, 58)
	stop()
	want := 0
	for b := int64(10); b <= 58; b++ {
		want += int(b % 3)
	}
	seen := map[string]int{}
	for _, l := range out.lines(t) {
		if !strings.HasPrefix(l.ID, "eth:0x") {
			t.Fatalf("unexpected id %q", l.ID)
		}
		seen[l.ID]++
		if seen[l.ID] > 1 {
			t.Fatalf("duplicate %s", l.ID)
		}
	}
	if len(seen) != want {
		t.Fatalf("got %d eth transfers, want %d", len(seen), want)
	}
	if seen["eth:"+ethtest.TxHash(10, 0)+":0"] != 1 {
		t.Fatal("missing first transfer")
	}
	if b, err := os.ReadFile(cfg.ETH.CursorFile); err != nil || !strings.Contains(string(b), "58") {
		t.Fatalf("cursor file: %s %v", b, err)
	}
}

// Postgres + admin API: an EIP-55 deposit address added at runtime is
// stored lowercase and only its transfers are persisted with chain='eth'.
func TestE2E_PostgresETHRuntimeWatch(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	sdsn, pool := isolatedSchema(t, dsn)
	node := ethtest.New(5)
	defer node.Close()
	cfg := ethCfg(t, baseCfg(t, "http://unused.invalid"), node.URL)
	cfg.Chains = []string{"eth"}
	cfg.ETH.StartBlock = 1
	cfg.API.Token = apiToken
	cfg.Sinks.Stdout.Enabled = false
	cfg.Sinks.Postgres = config.PostgresSink{Enabled: true, DSN: sdsn, CursorName: "main"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	a := newApp(t, cfg, nil, false)
	stop := startApp(t, a)
	code, body := apiCall(t, a, "POST", "/v1/addresses", apiToken, `{"address":"`+eth.Checksum(ethtest.Bob)+`","direction":"incoming"}`)
	if code != http.StatusCreated || !strings.Contains(body, `"chain":"eth"`) || !strings.Contains(body, ethtest.Bob) {
		t.Fatalf("add: %d %s", code, body)
	}
	node.Head.Store(40)
	waitChain(t, a.Listener, 38)
	stop()
	var cur int64
	if err := pool.QueryRow(ctx, `SELECT last_block FROM listener_cursor WHERE name='main:eth'`).Scan(&cur); err != nil || cur < 38 {
		t.Fatalf("eth cursor %d %v", cur, err)
	}
	var n, wrong int
	if err := pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE to_address <> $1 OR chain <> 'eth' OR tx_id NOT LIKE '0x%') FROM trc20_transfers`, ethtest.Bob).Scan(&n, &wrong); err != nil {
		t.Fatal(err)
	}
	if n == 0 || wrong != 0 {
		t.Fatalf("rows %d wrong %d", n, wrong)
	}
}
