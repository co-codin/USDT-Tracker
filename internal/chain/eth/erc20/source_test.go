package erc20_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/chain/eth"
	"github.com/co-codin/USDT-Tracker/internal/chain/eth/erc20"
	"github.com/co-codin/USDT-Tracker/internal/chain/eth/ethtest"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/retry"
)

var tok = erc20.Token{Contract: eth.USDTContract, Symbol: "USDT", Decimals: 6}

func client(n *ethtest.Node) *eth.Client {
	return eth.NewClient(eth.Options{URL: n.URL, RPS: 1000, Backoff: retry.Backoff{Initial: time.Millisecond, Max: time.Millisecond}})
}

func TestBlocksChunkedAndDecoded(t *testing.T) {
	n := ethtest.New(1000)
	defer n.Close()
	src := erc20.NewSource(client(n), tok, 4, nil, nil)
	bs, err := src.Blocks(context.Background(), 100, 109)
	if err != nil {
		t.Fatal(err)
	}
	if len(bs) != 10 {
		t.Fatalf("got %d blocks", len(bs))
	}
	if r := n.Ranges(); len(r) != 3 || r[0] != [2]int64{100, 103} || r[2] != [2]int64{108, 109} {
		t.Fatalf("ranges %v", r)
	}
	total := 0
	for _, b := range bs {
		if len(b.Transfers) != int(b.Number%3) {
			t.Fatalf("block %d: %d transfers", b.Number, len(b.Transfers))
		}
		for i, tr := range b.Transfers {
			total++
			if tr.Chain != model.ChainETH || tr.BlockNumber != b.Number || tr.Amount.Cmp(ethtest.Amount(b.Number, i)) != 0 {
				t.Fatalf("bad transfer %+v", tr)
			}
			if tr.TxID != ethtest.TxHash(b.Number, i) || tr.LogIndex != i*2 {
				t.Fatalf("tx %s idx %d", tr.TxID, tr.LogIndex)
			}
			if !tr.BlockTime.Equal(time.Unix(ethtest.GenesisTime+12*b.Number, 0)) {
				t.Fatalf("time %v", tr.BlockTime)
			}
			if tr.Contract != ethtest.USDT || (i == 0 && (tr.From != ethtest.Alice || tr.To != ethtest.Bob)) {
				t.Fatalf("addr %+v", tr)
			}
		}
	}
	if total != 10 || n.Calls("eth_getBlockByNumber") != 0 {
		t.Fatalf("total %d header calls %d", total, n.Calls("eth_getBlockByNumber"))
	}
}

func TestBlocksSplitsRejectedRanges(t *testing.T) {
	n := ethtest.New(1000)
	defer n.Close()
	n.MaxRange = 3
	n.OmitTimestamp = true
	src := erc20.NewSource(client(n), tok, 10, nil, nil)
	bs, err := src.Blocks(context.Background(), 200, 209)
	if err != nil {
		t.Fatal(err)
	}
	cnt := 0
	for _, b := range bs {
		cnt += len(b.Transfers)
		if len(b.Transfers) > 0 && !b.Time.Equal(time.Unix(ethtest.GenesisTime+12*b.Number, 0)) {
			t.Fatalf("block %d time %v", b.Number, b.Time)
		}
	}
	if cnt != 11 {
		t.Fatalf("transfers %d", cnt)
	}
	if n.Calls("eth_getBlockByNumber") == 0 {
		t.Fatal("expected header lookups without blockTimestamp")
	}
	b, err := src.Block(context.Background(), 205)
	if err != nil || b.Number != 205 || len(b.Transfers) != 1 {
		t.Fatalf("%+v %v", b, err)
	}
}

func TestHead(t *testing.T) {
	n := ethtest.New(77)
	defer n.Close()
	h, err := erc20.NewSource(client(n), tok, 0, nil, nil).Head(context.Background())
	if err != nil || h != 77 {
		t.Fatal(h, err)
	}
}

type fakeAPI struct{ logs []eth.Log }

func (f fakeAPI) BlockNumber(context.Context) (int64, error) { return 0, nil }
func (f fakeAPI) GetLogs(context.Context, eth.LogFilter) ([]eth.Log, error) {
	return f.logs, nil
}
func (f fakeAPI) BlockTime(context.Context, int64) (time.Time, error) { return time.Unix(1, 0), nil }

// Real mainnet eth_getLogs response for block 26179072 (0x18f7600).
func TestDecodeRealMainnetLogs(t *testing.T) {
	raw, err := os.ReadFile("../testdata/getlogs_26179072.json")
	if err != nil {
		t.Fatal(err)
	}
	var resp struct {
		Result []eth.Log `json:"result"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	decErrs := 0
	src := erc20.NewSource(fakeAPI{resp.Result}, tok, 1, nil, func() { decErrs++ })
	b, err := src.Block(context.Background(), 26179072)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Transfers) != len(resp.Result) || decErrs != 0 {
		t.Fatalf("decoded %d of %d, errs %d", len(b.Transfers), len(resp.Result), decErrs)
	}
	first := b.Transfers[0]
	if first.From != "0x0000000aa232009084bd71a5797d089aa4edfad4" || first.To != "0x000000000004444c5dc75cb358380d2e3de08a90" ||
		first.AmountDecimal() != "1749.679936" || first.LogIndex != 5 {
		t.Fatalf("first = %+v amount %s", first, first.AmountDecimal())
	}
	if b.Time.UTC().Format(time.RFC3339) != time.Unix(0x6acd505b, 0).UTC().Format(time.RFC3339) {
		t.Fatalf("time %v", b.Time)
	}
}

func TestMalformedAndInconsistentLogs(t *testing.T) {
	good := eth.Log{Address: ethtest.USDT, Topics: []string{eth.TransferTopic,
		"0x000000000000000000000000" + "11111111111111111111111111111111111111111"[:40],
		"0x0000000000000000000000002222222222222222222222222222222222222222"},
		Data: "0x" + "0000000000000000000000000000000000000000000000000000000000000001", BlockNumber: "0x5",
		TransactionHash: "0x" + "ab" + "000000000000000000000000000000000000000000000000000000000000cd", LogIndex: "0x0"}
	bad := good
	bad.Topics = good.Topics[:2] // approval-like / anonymous shape
	removed := good
	removed.Removed = true
	decErrs := 0
	src := erc20.NewSource(fakeAPI{[]eth.Log{good, bad, removed}}, tok, 1, nil, func() { decErrs++ })
	b, err := src.Block(context.Background(), 5)
	if err != nil || len(b.Transfers) != 1 || decErrs != 1 {
		t.Fatalf("%v transfers=%d errs=%d", err, len(b.Transfers), decErrs)
	}
	wrongBlock := good
	wrongBlock.BlockNumber = "0x9"
	_, err = erc20.NewSource(fakeAPI{[]eth.Log{wrongBlock}}, tok, 1, nil, nil).Block(context.Background(), 5)
	if !errors.Is(err, chain.ErrInconsistent) {
		t.Fatalf("err = %v", err)
	}
}
