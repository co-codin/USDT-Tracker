package app_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/co-codin/USDT-Tracker/internal/app"
	"github.com/co-codin/USDT-Tracker/internal/chain/tron/trontest"
	"github.com/co-codin/USDT-Tracker/internal/config"
)

const apiToken = "e2e-admin-token-0123456789abcdef"

// apiCall performs an admin API request against a running app.
func apiCall(t *testing.T, a *app.App, method, path, token, body string) (int, string) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, "http://"+a.HTTPAddr()+path, r)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// startApp runs a in the background; the returned stop func shuts it down.
func startApp(t *testing.T, a *app.App) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx, 0, 0) }()
	deadline := time.Now().Add(5 * time.Second)
	for a.HTTPAddr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("http server did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("app did not shut down")
		}
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	}
}

func waitCursor(t *testing.T, a *app.App, target int64) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for a.Listener.Status().Cursor < target {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: cursor %d, want %d (last error: %s)", a.Listener.Status().Cursor, target, a.Listener.Status().LastError)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestE2E_AdminAPIWithoutPostgres(t *testing.T) {
	node := trontest.NewServer(200)
	defer node.Close()

	// Token set, Postgres off: API answers 503, static filter keeps working.
	cfg := baseCfg(t, node.URL)
	cfg.Listener.StartBlock = 180
	cfg.API.Token = apiToken
	cfg.Filter.WatchAddresses = []string{trontest.Bob}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	out := &syncBuffer{}
	a := newApp(t, cfg, out, false)
	stop := startApp(t, a)
	if code, body := apiCall(t, a, "GET", "/v1/addresses", "", ""); code != http.StatusUnauthorized {
		t.Errorf("no token: %d %s", code, body)
	}
	if code, body := apiCall(t, a, "GET", "/v1/addresses", apiToken, ""); code != http.StatusServiceUnavailable || !strings.Contains(body, "postgres") {
		t.Errorf("no postgres: %d %s", code, body)
	}
	if code, _ := apiCall(t, a, "POST", "/v1/addresses", apiToken, `{"address":"`+trontest.Dave+`"}`); code != http.StatusServiceUnavailable {
		t.Errorf("POST without postgres: %d", code)
	}
	if code, _ := apiCall(t, a, "GET", "/healthz", "", ""); code != http.StatusOK && code != http.StatusServiceUnavailable {
		t.Errorf("/healthz must stay unauthenticated: %d", code)
	}
	waitCursor(t, a, 190)
	stop()
	for _, l := range out.lines(t) {
		if l.From != trontest.Bob && l.To != trontest.Bob {
			t.Fatalf("static filter leaked %+v", l)
		}
	}
	if len(out.lines(t)) == 0 {
		t.Fatal("static watch address produced no output")
	}

	// No token: the API is disabled (404), never open.
	cfg.API.Token = ""
	a = newApp(t, cfg, io.Discard, false)
	stop = startApp(t, a)
	defer stop()
	if code, body := apiCall(t, a, "GET", "/v1/addresses", "", ""); code != http.StatusNotFound || !strings.Contains(body, "API_TOKEN") {
		t.Errorf("disabled API: %d %s", code, body)
	}
	if code, _ := apiCall(t, a, "POST", "/v1/addresses", "anything", `{"address":"`+trontest.Dave+`"}`); code != http.StatusNotFound {
		t.Errorf("disabled API POST: %d", code)
	}
}

// TestE2E_AdminAPIRuntimeWatchList adds and removes an address through the
// HTTP API while the listener runs and checks that exactly the blocks
// processed in between are filtered on it. Needs TEST_DATABASE_URL.
func TestE2E_AdminAPIRuntimeWatchList(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; see postgres_e2e_test.go")
	}
	dsn, pool := isolatedSchema(t, dsn)
	ctx := context.Background()

	node := trontest.NewServer(675) // safe head 670
	defer node.Close()
	cfg := baseCfg(t, node.URL)
	cfg.Listener.StartBlock = 650
	cfg.Sinks.Stdout.Enabled = false
	cfg.Sinks.Postgres = config.PostgresSink{Enabled: true, DSN: dsn, CursorName: "api-e2e"}
	cfg.API.Token = apiToken
	cfg.Filter.ReloadInterval = config.Duration(time.Hour) // prove API writes apply without waiting
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	a := newApp(t, cfg, io.Discard, false)
	stop := startApp(t, a)
	defer stop()

	rowsIn := func(from, to int64) map[string]bool {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT tx_id || ':' || log_index FROM trc20_transfers WHERE block_number BETWEEN $1 AND $2`, from, to)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		got := map[string]bool{}
		for rows.Next() {
			var k string
			if err := rows.Scan(&k); err != nil {
				t.Fatal(err)
			}
			got[k] = true
		}
		return got
	}

	// Phase 1: empty runtime list + API enabled → nothing matches (no firehose).
	waitCursor(t, a, 670)
	if got := rowsIn(0, 1<<40); len(got) != 0 {
		t.Fatalf("empty watch list stored %d rows; want 0 (no firehose)", len(got))
	}

	// Phase 2: add Dave (incoming only) at runtime.
	if code, body := apiCall(t, a, "POST", "/v1/addresses", apiToken, `{"address":"Tbad"}`); code != http.StatusBadRequest {
		t.Fatalf("invalid address: %d %s", code, body)
	}
	code, body := apiCall(t, a, "POST", "/v1/addresses", apiToken,
		`{"address":"`+trontest.Dave+`","label":"invoice 1001","direction":"incoming"}`)
	if code != http.StatusCreated {
		t.Fatalf("add: %d %s", code, body)
	}
	if code, body := apiCall(t, a, "GET", "/v1/addresses", apiToken, ""); code != 200 || !strings.Contains(body, trontest.Dave) || !strings.Contains(body, "invoice 1001") {
		t.Fatalf("list: %d %s", code, body)
	}
	added := a.Listener.Status().Cursor
	node.SetHead(700) // safe head 695
	waitCursor(t, a, 695)

	// Phase 3: remove it.
	if code, body := apiCall(t, a, "DELETE", "/v1/addresses/"+trontest.Dave, apiToken, ""); code != http.StatusNoContent {
		t.Fatalf("delete: %d %s", code, body)
	}
	removed := a.Listener.Status().Cursor
	node.SetHead(715)
	waitCursor(t, a, 710)

	want := map[string]bool{}
	for _, e := range trontest.ExpectedTransfers(added+1, removed) {
		if e.To == trontest.Dave { // incoming only: Dave -> Alice must not match
			want[e.Key] = true
		}
	}
	got := rowsIn(0, 1<<40)
	if len(want) == 0 || len(got) != len(want) {
		t.Fatalf("stored %d rows, want %d (blocks %d..%d, incoming to Dave)", len(got), len(want), added+1, removed)
	}
	for k := range want {
		if !got[k] {
			t.Errorf("missing %s", k)
		}
	}
	if extra := rowsIn(removed+1, 1<<40); len(extra) != 0 {
		t.Errorf("%d rows stored after the address was removed", len(extra))
	}
}

// isolatedSchema creates a throw-away schema (so watch_addresses rows don't
// affect other tests sharing the database) and returns a DSN using it plus a
// pool for assertions.
func isolatedSchema(t *testing.T, dsn string) (string, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("test_%d", time.Now().UnixNano())
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	pool, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return u.String(), pool
}
