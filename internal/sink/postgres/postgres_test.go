package postgres

import (
	"context"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/cursor"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/sink"
)

// Integration test: runs only when TEST_DATABASE_URL is set, e.g.
//
//	TEST_DATABASE_URL=postgres://listener:listener@localhost:5432/tron_test?sslmode=disable go test ./internal/sink/postgres/
func TestStoreIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn, "test-"+time.Now().Format("150405.000000"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil { // idempotent
		t.Fatal(err)
	}

	txID := "it" + time.Now().Format("20060102150405.000000000")
	b := sink.Batch{BlockNumber: 1, BlockTime: time.Now().UTC(), Transfers: []model.Transfer{
		{TxID: txID, LogIndex: 0, BlockNumber: 1, BlockTime: time.Now().UTC(), Contract: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
			Symbol: "USDT", Decimals: 6, From: "TA", To: "TB", Amount: big.NewInt(1_500_000), Reasons: []string{model.ReasonAll}},
		{TxID: txID, LogIndex: 1, BlockNumber: 1, BlockTime: time.Now().UTC(), Contract: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
			Symbol: "USDT", Decimals: 6, From: "TA", To: "TC", Amount: big.NewInt(7)},
	}}
	for i := 0; i < 2; i++ { // second insert must be a no-op (dedupe by tx id + log index)
		if err := s.Send(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	var amount string
	if err := s.pool.QueryRow(ctx, `SELECT count(*), max(amount)::text FROM trc20_transfers WHERE tx_id=$1`, txID).Scan(&n, &amount); err != nil {
		t.Fatal(err)
	}
	if n != 2 || amount != "1.500000000000000000" {
		t.Fatalf("rows=%d max(amount)=%s", n, amount)
	}

	st, err := s.Load(ctx)
	if err != nil || st.LastBlock != 0 {
		t.Fatalf("fresh cursor: %+v %v", st, err)
	}
	if err := s.Save(ctx, cursor.State{LastBlock: 42, UpdatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, cursor.State{LastBlock: 43, UpdatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if st, _ := s.Load(ctx); st.LastBlock != 43 {
		t.Fatalf("cursor = %d", st.LastBlock)
	}
}
