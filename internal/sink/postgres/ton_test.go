package postgres

import (
	"context"
	"errors"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/cursor"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/sink"
	"github.com/co-codin/USDT-Tracker/internal/watch"
)

// Integration test: runs only when TEST_DATABASE_URL is set (see postgres_test.go).
func TestTONIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, isolatedDSN(t, dsn), "multi")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const (
		hash = "540a39b8299cc5d0239eefa2be4268445a262b897a6cd970f1fb2ac4e70aef19"
		from = "0:0c8c0fb575238aaa844a89b36eb7546359e4e40297bcbe574c05611d884f36f1"
		to   = "0:852443f8599fe6a5da34fe43049ac4e0beb3071bb2bfb56635ea9421287c283a"
	)
	now := time.Now().UTC()
	b := sink.Batch{BlockNumber: 98632901, BlockTime: now, Transfers: []model.Transfer{
		{Chain: model.ChainTON, TxID: hash, BlockNumber: 98632901, BlockTime: now, Contract: "0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe",
			Symbol: "USDT", Decimals: 6, From: from, To: to, Amount: big.NewInt(11_250_000), Memo: "inv-42"},
		{TxID: "tronlegacy", BlockNumber: 5, BlockTime: now, Contract: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", Symbol: "USDT", Decimals: 6, From: "TA", To: "TB", Amount: big.NewInt(1)},
	}}
	for range 2 {
		if err := s.Send(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	var chain, memo, amount string
	if err := s.pool.QueryRow(ctx, `SELECT chain, memo, amount::text FROM trc20_transfers WHERE tx_id=$1`, hash).Scan(&chain, &memo, &amount); err != nil {
		t.Fatal(err)
	}
	if chain != "ton" || memo != "inv-42" || amount != "11.250000000000000000" {
		t.Fatalf("ton row chain=%s memo=%s amount=%s", chain, memo, amount)
	}
	var tronChain string
	var tronMemo *string
	if err := s.pool.QueryRow(ctx, `SELECT chain, memo FROM trc20_transfers WHERE tx_id='tronlegacy'`).Scan(&tronChain, &tronMemo); err != nil || tronChain != "tron" || tronMemo != nil {
		t.Fatalf("tron row chain=%s memo=%v err=%v", tronChain, tronMemo, err)
	}
	// legacy inserts without the chain column still work (default 'tron')
	if _, err := s.pool.Exec(ctx, `INSERT INTO trc20_transfers (tx_id, log_index, block_number, block_time, contract, symbol, from_address, to_address, amount_raw, amount)
		VALUES ('old', 0, 1, now(), 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t', 'USDT', 'TA', 'TB', 1, 0.000001)`); err != nil {
		t.Fatal(err)
	}

	// per-chain cursors are independent of the default one
	tc := s.Cursor("multi:ton")
	if err := tc.Save(ctx, cursor.State{LastBlock: 98632901, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, cursor.State{LastBlock: 7, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if st, _ := tc.Load(ctx); st.LastBlock != 98632901 {
		t.Fatalf("ton cursor %d", st.LastBlock)
	}
	if st, _ := s.Load(ctx); st.LastBlock != 7 {
		t.Fatalf("tron cursor %d", st.LastBlock)
	}

	// watch list with chain column
	w := s.Watchlist()
	e, err := w.AddWatch(ctx, watch.Entry{Chain: "ton", Address: to, Direction: "incoming", Enabled: true})
	if err != nil || e.Chain != "ton" || e.Address != to {
		t.Fatalf("add ton watch %+v %v", e, err)
	}
	e, err = w.AddWatch(ctx, watch.Entry{Address: "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx", Enabled: true})
	if err != nil || e.Chain != "tron" {
		t.Fatalf("tron default chain %+v %v", e, err)
	}
	if _, err := w.AddWatch(ctx, watch.Entry{Chain: "ton", Address: to, Enabled: true}); !errors.Is(err, watch.ErrExists) {
		t.Fatalf("dup: %v", err)
	}
	en, err := w.EnabledWatches(ctx)
	if err != nil || len(en) != 2 || en[0].Chain != "ton" {
		t.Fatalf("enabled %+v %v", en, err)
	}
	for _, bad := range [][2]string{
		{"ton", "EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs"}, // must be stored raw
		{"ton", "0:" + "AB"},
		{"tron", to},
		{"eth", "TEHwL3F2kpkJExYAZYziHGLFR2CR8Bq3bo"},
	} {
		if _, err := s.pool.Exec(ctx, `INSERT INTO watch_addresses(chain, address) VALUES ($1, $2)`, bad[0], bad[1]); err == nil {
			t.Errorf("constraint accepted %v", bad)
		}
	}
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE version='0003_ton_chain'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("migration 0003 recorded %d times: %v", n, err)
	}
}
