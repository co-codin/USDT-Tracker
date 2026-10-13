package postgres

import (
	"context"
	"os"
	"testing"
)

func TestSplitStatements(t *testing.T) {
	got := splitStatements("-- migrate:no-transaction\n-- c\nCREATE INDEX CONCURRENTLY IF NOT EXISTS a ON t (x);\n\n-- d\nCREATE INDEX CONCURRENTLY IF NOT EXISTS b\n  ON t (y);\n")
	if len(got) != 2 || got[0] != "CREATE INDEX CONCURRENTLY IF NOT EXISTS a ON t (x)" || got[1] != "CREATE INDEX CONCURRENTLY IF NOT EXISTS b\n  ON t (y)" {
		t.Fatalf("%q", got)
	}
}

// An interrupted CREATE INDEX CONCURRENTLY leaves an INVALID index; the
// no-transaction migration must drop and rebuild it.
func TestNoTxMigrationRebuildsInvalidIndex(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn, "migrate-test")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	valid := func() bool {
		var v bool
		if err := s.pool.QueryRow(ctx, `SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			WHERE c.relname = 'trc20_transfers_chain_to_time_idx'`).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	if !valid() {
		t.Fatal("index invalid after migrate")
	}
	if _, err := s.pool.Exec(ctx, `UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'trc20_transfers_chain_to_time_idx'::regclass`); err != nil {
		t.Skipf("cannot simulate an invalid index (needs superuser): %v", err)
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM schema_migrations WHERE version = '0005_export_indexes'`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if !valid() {
		t.Fatal("invalid index was not rebuilt")
	}
}
