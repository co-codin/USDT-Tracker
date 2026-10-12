package app_test

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/co-codin/USDT-Tracker/internal/chain/ton"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton/tontest"
	"github.com/co-codin/USDT-Tracker/internal/chain/tron/trontest"
	"github.com/co-codin/USDT-Tracker/internal/config"
)

// Both chains with the Postgres sink, per-chain DB cursors and a TON
// deposit address added at runtime through the admin API.
func TestE2E_PostgresTONRuntimeWatch(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	sdsn, pool := isolatedSchema(t, dsn)

	node := trontest.NewServer(300)
	defer node.Close()
	srv := tontest.New(60)
	defer srv.Close()
	cfg := tonCfg(t, baseCfg(t, node.URL), srv.BaseURL())
	cfg.Chains = []string{"tron", "ton"}
	cfg.Listener.StartBlock = 280
	cfg.TON.StartBlock = 1
	cfg.API.Token = apiToken
	cfg.Sinks.Stdout.Enabled = false
	cfg.Sinks.Postgres = config.PostgresSink{Enabled: true, DSN: sdsn, CursorName: "main"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	a := newApp(t, cfg, nil, false)
	stop := startApp(t, a)
	alice, _ := ton.ParseAddress(tontest.Alice)
	code, body := apiCall(t, a, "POST", "/v1/addresses", apiToken, `{"address":"`+alice.Friendly(true, false)+`","direction":"incoming","label":"ton deposit"}`)
	if code != http.StatusCreated || !strings.Contains(body, `"chain":"ton"`) {
		t.Fatalf("add: %d %s", code, body)
	}
	srv.SetHead(140)
	waitChain(t, pipeline(a, "ton"), 130)
	waitChain(t, pipeline(a, "tron"), 290)
	stop()

	var tonCur, tronCur int64
	if err := pool.QueryRow(ctx, `SELECT last_block FROM listener_cursor WHERE name='main:ton'`).Scan(&tonCur); err != nil || tonCur < 130 {
		t.Fatalf("ton cursor %d %v", tonCur, err)
	}
	if err := pool.QueryRow(ctx, `SELECT last_block FROM listener_cursor WHERE name='main'`).Scan(&tronCur); err != nil || tronCur < 290 {
		t.Fatalf("tron cursor %d %v", tronCur, err)
	}
	var n, wrong int
	if err := pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE to_address <> $1) FROM trc20_transfers WHERE chain='ton'`, tontest.Lower(tontest.Alice)).Scan(&n, &wrong); err != nil {
		t.Fatal(err)
	}
	if n == 0 || wrong != 0 {
		t.Fatalf("ton rows %d, not to alice %d", n, wrong)
	}
	var tronRows int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM trc20_transfers WHERE chain='tron'`).Scan(&tronRows)
	if tronRows != 0 { // tron has no static watch and the runtime list only has a TON address
		t.Errorf("tron rows %d, want 0 (dynamic watch rule with no tron addresses)", tronRows)
	}
}
