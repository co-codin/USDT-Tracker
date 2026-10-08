package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/co-codin/USDT-Tracker/internal/watch"
)

// Integration test: runs only when TEST_DATABASE_URL is set (see postgres_test.go).
func TestWatchStoreIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	// Own schema: rows in watch_addresses change the filter of any listener
	// using the same database (e.g. the app e2e tests running in parallel).
	s, err := Open(ctx, isolatedDSN(t, dsn), "test-watch")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w := s.Watchlist()

	const (
		alice = "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx"
		bob   = "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv"
	)

	label := "order 42"
	before := time.Now().Add(-time.Minute)
	e, err := w.AddWatch(ctx, watch.Entry{Address: alice, Label: &label, Direction: "incoming", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if e.Address != alice || e.Label == nil || *e.Label != label || e.Direction != "incoming" || !e.Enabled || e.CreatedAt.Before(before) {
		t.Fatalf("inserted %+v", e)
	}
	if _, err := w.AddWatch(ctx, watch.Entry{Address: alice, Enabled: true}); !errors.Is(err, watch.ErrExists) {
		t.Fatalf("duplicate insert: %v", err)
	}
	e, err = w.AddWatch(ctx, watch.Entry{Address: bob, Enabled: false}) // direction defaults to both
	if err != nil || e.Direction != "both" || e.Label != nil || e.Enabled {
		t.Fatalf("bob: %+v %v", e, err)
	}

	all, err := w.ListWatches(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].Address != alice || all[1].Address != bob {
		t.Fatalf("list (oldest first): %+v", all)
	}
	en, err := w.EnabledWatches(ctx)
	if err != nil || len(en) != 1 || en[0].Address != alice {
		t.Fatalf("enabled: %+v %v", en, err)
	}

	// DB-level constraints back up API validation.
	if _, err := s.pool.Exec(ctx, `INSERT INTO watch_addresses(address) VALUES ('not-base58')`); err == nil {
		t.Error("address check constraint missing")
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO watch_addresses(address, direction) VALUES ($1, 'up')`, "TEHwL3F2kpkJExYAZYziHGLFR2CR8Bq3bo"); err == nil {
		t.Error("direction check constraint missing")
	}

	if err := w.DeleteWatch(ctx, alice); err != nil {
		t.Fatal(err)
	}
	if err := w.DeleteWatch(ctx, alice); !errors.Is(err, watch.ErrNotFound) {
		t.Fatalf("delete missing: %v", err)
	}
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE version='0002_watch_addresses'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("migration 0002 recorded %d times: %v", n, err)
	}
}

// isolatedDSN creates a throw-away schema and returns dsn with search_path
// pointing at it; the schema is dropped when the test ends.
func isolatedDSN(t *testing.T, dsn string) string {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("test_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		pool.Close()
	})
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}
