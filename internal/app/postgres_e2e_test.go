package app_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/co-codin/USDT-Tracker/internal/chain/tron/trontest"
	"github.com/co-codin/USDT-Tracker/internal/config"
)

// TestE2E_PostgresSinkAndCursor runs the full pipeline with the Postgres sink
// (which is also the cursor store), restarts it, and checks the database.
//
// Skipped unless TEST_DATABASE_URL points at a disposable database, e.g.:
//
//	docker run --rm -d -p 55432:5432 -e POSTGRES_PASSWORD=test postgres:17-alpine
//	TEST_DATABASE_URL=postgres://postgres:test@localhost:55432/postgres?sslmode=disable go test ./...
func TestE2E_PostgresSinkAndCursor(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; see the test comment for how to run it against Docker Postgres")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	node := trontest.NewServer(700)
	defer node.Close()
	cursorName := fmt.Sprintf("e2e-%d", time.Now().UnixNano())

	cfg := baseCfg(t, node.URL)
	cfg.Listener.StartBlock = 650
	cfg.Sinks.Stdout.Enabled = false
	cfg.Sinks.Postgres = config.PostgresSink{Enabled: true, DSN: dsn, CursorName: cursorName}

	// Clean rows from previous runs (tx ids are deterministic).
	ids := map[string]bool{}
	for n := int64(650); n <= 700; n++ {
		ids[trontest.TxID(n, 0)], ids[trontest.TxID(n, 1)] = true, true
	}
	txIDs := make([]string, 0, len(ids))
	for id := range ids {
		txIDs = append(txIDs, id)
	}
	// Table may not exist yet on a fresh database; ignore that error.
	_, _ = pool.Exec(ctx, `DELETE FROM trc20_transfers WHERE tx_id = ANY($1)`, txIDs)

	runUntil(t, newApp(t, cfg, io.Discard, false), 670, nil)
	node.SetHead(700)
	runUntil(t, newApp(t, cfg, io.Discard, false), 690, nil) // resumes from DB cursor

	var last int64
	if err := pool.QueryRow(ctx, `SELECT last_block FROM listener_cursor WHERE name=$1`, cursorName).Scan(&last); err != nil {
		t.Fatal(err)
	}
	if last < 690 {
		t.Fatalf("db cursor = %d, want >= 690", last)
	}

	want := expectedKeys(650, last) // every block up to the persisted cursor
	rows, err := pool.Query(ctx, `SELECT tx_id, log_index, block_number, from_address, to_address, amount_raw::text
		FROM trc20_transfers WHERE tx_id = ANY($1) AND block_number <= $2`, txIDs, last)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]int{}
	for rows.Next() {
		var (
			tx, from, to, raw string
			idx               int
			block             int64
		)
		if err := rows.Scan(&tx, &idx, &block, &from, &to, &raw); err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("%s:%d", tx, idx)
		got[key]++
		if e, ok := want[key]; ok && (e.Block != block || e.From != from || e.To != to || e.Amount.String() != raw) {
			t.Errorf("row %s mismatch: block=%d from=%s to=%s amount=%s", key, block, from, to, raw)
		}
	}
	assertExactlyOnce(t, "postgres rows", got, want)
}
