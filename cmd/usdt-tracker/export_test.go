package main

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/sink"
	"github.com/co-codin/USDT-Tracker/internal/sink/postgres"
)

const exportAddr = "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv"

func isolatedDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("cmd_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		pool.Close()
	})
	u, _ := url.Parse(dsn)
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}

func TestExportCSVFromPostgres(t *testing.T) {
	dsn := isolatedDSN(t)
	ctx := context.Background()
	pg, err := postgres.Open(ctx, dsn, "default")
	if err != nil {
		t.Fatal(err)
	}
	bt := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	err = pg.Send(ctx, sink.Batch{BlockNumber: 10, BlockTime: bt, Transfers: []model.Transfer{
		{TxID: strings.Repeat("a", 64), BlockNumber: 10, BlockTime: bt, Contract: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", Symbol: "USDT", Decimals: 6,
			From: "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx", To: exportAddr, Amount: big.NewInt(12_500_000)},
	}})
	pg.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATABASE_URL", dsn)
	var out, errb bytes.Buffer
	if code := run([]string{"export", "-config", "", "-address", exportAddr, "-from", "2026-10-01", "-to", "2026-10-01", "-bom"}, &out, &errb); code != 0 {
		t.Fatalf("code %d: %s", code, errb.String())
	}
	if !strings.HasPrefix(out.String(), "\ufeff") || !strings.Contains(out.String(), "12.5") || !strings.Contains(errb.String(), "exported 1 transfers") {
		t.Fatalf("out %q err %q", out.String(), errb.String())
	}
	file := filepath.Join(t.TempDir(), "s.csv")
	out.Reset()
	if code := run([]string{"export", "-config", "", "-address", exportAddr, "-o", file}, &out, &errb); code != 0 || out.Len() != 0 {
		t.Fatalf("code %d: %s", code, errb.String())
	}
	if b, _ := os.ReadFile(file); !strings.Contains(string(b), strings.Repeat("a", 64)) {
		t.Fatalf("file %q", b)
	}
	errb.Reset()
	if code := run([]string{"export", "-config", "", "-address", exportAddr, "-o", filepath.Join(t.TempDir(), "no", "dir.csv")}, &out, &errb); code != 1 {
		t.Fatalf("bad output path: code %d", code)
	}
}

func TestExportErrors(t *testing.T) {
	var out, errb bytes.Buffer
	// Unreachable database.
	t.Setenv("DATABASE_URL", "postgres://u:p@127.0.0.1:1/x?connect_timeout=1")
	if code := run([]string{"export", "-config", "", "-address", exportAddr}, &out, &errb); code != 1 || !strings.Contains(errb.String(), "export:") {
		t.Fatalf("code %d: %s", code, errb.String())
	}
	// Config error.
	errb.Reset()
	t.Setenv("FILTER_DIRECTION", "sideways")
	if code := run([]string{"export", "-config", "", "-address", exportAddr}, &out, &errb); code != 2 || !strings.Contains(errb.String(), "configuration error") {
		t.Fatalf("code %d: %s", code, errb.String())
	}
	// Bad flag and bad date.
	if code := run([]string{"export", "-nope"}, &out, &errb); code != 2 {
		t.Fatal(code)
	}
	if code := run([]string{"export", "-address", exportAddr, "-from", "yesterday"}, &out, &errb); code != 2 {
		t.Fatal(code)
	}
}

func TestStartupFailureAndDefaultConfigPath(t *testing.T) {
	t.Setenv("SINK_POSTGRES_ENABLED", "true")
	t.Setenv("DATABASE_URL", "postgres://u:p@127.0.0.1:1/x?connect_timeout=1")
	var out, errb bytes.Buffer
	if code := run([]string{"-config", ""}, &out, &errb); code != 1 || !strings.Contains(errb.String(), "startup failed") {
		t.Fatalf("code %d: %s", code, errb.String())
	}
	t.Setenv("CONFIG_FILE", "")
	dir := t.TempDir()
	wd, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(wd) })
	_ = os.Chdir(dir)
	if p := defaultConfigPath(); p != "" {
		t.Fatalf("no config.yaml: %q", p)
	}
	_ = os.WriteFile("config.yaml", []byte("log:\n  level: info\n"), 0o600)
	if p := defaultConfigPath(); p != "config.yaml" {
		t.Fatalf("got %q", p)
	}
	t.Setenv("CONFIG_FILE", "/etc/x.yaml")
	if p := defaultConfigPath(); p != "/etc/x.yaml" {
		t.Fatalf("got %q", p)
	}
}
