package postgres

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/export"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/sink"
)

func TestExportIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, isolatedDSN(t, dsn), "exp")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const (
		me    = "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed"
		other = "0xfb6916095ca1df60bb79ce92ce3ea74c37c5d359"
	)
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	// 3000 transfers, one per minute, alternating direction; plus noise on other chains/addresses.
	const n = 3000
	var ts []model.Transfer
	for i := range n {
		from, to := other, me
		if i%2 == 1 {
			from, to = me, other
		}
		ts = append(ts, model.Transfer{Chain: model.ChainETH, TxID: fmt.Sprintf("0x%064x", i), LogIndex: i % 7, BlockNumber: int64(1000 + i),
			BlockTime: base.Add(time.Duration(i) * time.Minute), Contract: "0xdac17f958d2ee523a2206206994597c13d831ec7",
			Symbol: "USDT", Decimals: 6, From: from, To: to, Amount: big.NewInt(int64(i+1) * 1_000_000)})
	}
	ts = append(ts,
		model.Transfer{Chain: model.ChainTON, TxID: "aa", BlockTime: base, Symbol: "USDT", Decimals: 6, From: me, To: other, Amount: big.NewInt(1), Contract: "x"},
		model.Transfer{Chain: model.ChainETH, TxID: "0xbb", BlockTime: base, Symbol: "DAI", Decimals: 18, From: other, To: "0x1111111111111111111111111111111111111111", Amount: big.NewInt(1), Contract: "y"},
	)
	if err := s.Send(ctx, sink.Batch{Transfers: ts}); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	cnt, err := export.Write(ctx, &buf, s, export.Query{Chain: "eth", Address: me}, nil)
	if err != nil || cnt != n {
		t.Fatalf("rows %d err %v", cnt, err)
	}
	recs, err := csv.NewReader(&buf).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if recs[1][6] != "in" || recs[1][7] != "1" || recs[2][6] != "out" || recs[n][7] != "3000" || recs[1][0] != "2026-10-01T00:00:00Z" {
		t.Fatalf("%q %q %q", recs[1], recs[2], recs[n])
	}
	// time window [01:00, 02:00)
	q := export.Query{Chain: "eth", Address: me, From: base.Add(time.Hour), To: base.Add(2 * time.Hour)}
	cnt, err = export.Write(ctx, &bytes.Buffer{}, s, q, nil)
	if err != nil || cnt != 60 {
		t.Fatalf("window rows %d %v", cnt, err)
	}
	if got := decimalsOf("1", "0.000000000000000001"); got != 18 {
		t.Fatal(got)
	}
	var dai []export.Row
	_ = s.StreamTransfers(ctx, export.Query{Chain: "eth", Address: "0x1111111111111111111111111111111111111111"}, func(r export.Row) error {
		dai = append(dai, r)
		return nil
	})
	if len(dai) != 1 || dai[0].Decimals != 18 || dai[0].Symbol != "DAI" {
		t.Fatalf("%+v", dai)
	}
}
