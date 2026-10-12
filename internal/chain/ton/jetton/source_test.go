package jetton_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton/jetton"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton/tontest"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/retry"
)

func usdt(t *testing.T) jetton.Token {
	t.Helper()
	m, err := ton.ParseAddress(ton.USDTMaster)
	if err != nil {
		t.Fatal(err)
	}
	return jetton.Token{Master: m, Symbol: "USDT", Decimals: 6}
}

// fixtureAPI replays real toncenter responses captured for masterchain block 98632901.
type fixtureAPI struct {
	shards    ton.BlocksResponse
	transfers ton.JettonTransfersResponse
	gotMaster string
	gotLo     uint64
	gotHi     uint64
}

func (f *fixtureAPI) MasterchainInfo(context.Context) (ton.MasterchainInfo, error) {
	return ton.MasterchainInfo{Last: ton.BlockRef{Seqno: 98632920}}, nil
}
func (f *fixtureAPI) MasterchainBlockShards(_ context.Context, n int64) ([]ton.BlockRef, error) {
	if n != 98632901 {
		return nil, ton.ErrBlockNotFound
	}
	return f.shards.Blocks, nil
}
func (f *fixtureAPI) JettonTransfers(_ context.Context, master string, lo, hi uint64, _, _ int) ([]ton.JettonTransfer, error) {
	f.gotMaster, f.gotLo, f.gotHi = master, lo, hi
	return f.transfers.JettonTransfers, nil
}

func load(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

func TestSourceRealFixture(t *testing.T) {
	f := &fixtureAPI{}
	load(t, "testdata/masterchainBlockShards_98632901.json", &f.shards)
	load(t, "testdata/jetton_transfers_98632901.json", &f.transfers)
	src := jetton.NewSource(f, usdt(t), nil, nil)
	head, err := src.Head(context.Background())
	if err != nil || head != 98632920 {
		t.Fatal(head, err)
	}
	b, err := src.Block(context.Background(), 98632901)
	if err != nil {
		t.Fatal(err)
	}
	if f.gotMaster != ton.USDTMaster || f.gotLo != 109558493000000 || f.gotHi != 109558493000018 {
		t.Errorf("query master=%s lt=[%d,%d]", f.gotMaster, f.gotLo, f.gotHi)
	}
	if b.Number != 98632901 || b.Time.Unix() != int64(f.shards.Blocks[0].GenUtime) {
		t.Errorf("block %+v", b)
	}
	if len(b.Transfers) != 1 {
		t.Fatalf("transfers = %d", len(b.Transfers))
	}
	tr := b.Transfers[0]
	want := model.Transfer{
		Chain: "ton", TxID: "540a39b8299cc5d0239eefa2be4268445a262b897a6cd970f1fb2ac4e70aef19", BlockNumber: 98632901,
		Contract: "0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe", Symbol: "USDT", Decimals: 6,
		From: "0:0c8c0fb575238aaa844a89b36eb7546359e4e40297bcbe574c05611d884f36f1",
		To:   "0:852443f8599fe6a5da34fe43049ac4e0beb3071bb2bfb56635ea9421287c283a",
	}
	if tr.Chain != want.Chain || tr.TxID != want.TxID || tr.Contract != want.Contract || tr.From != want.From || tr.To != want.To || tr.Symbol != "USDT" {
		t.Errorf("got %+v", tr)
	}
	if tr.AmountDecimal() != "11.25" || tr.Memo != "750 Telegram Stars \n\nRef#zopsXyorO" {
		t.Errorf("amount %s memo %q", tr.AmountDecimal(), tr.Memo)
	}
	if tr.Key() != "ton:"+want.TxID+":0" {
		t.Errorf("key %s", tr.Key())
	}
	if _, err := src.Block(context.Background(), 98632902); !errors.Is(err, chain.ErrInconsistent) || !errors.Is(err, ton.ErrBlockNotFound) {
		t.Errorf("not indexed err = %v", err)
	}
}

func newSrc(t *testing.T, srv *tontest.Server, page int, onErr func()) *jetton.Source {
	c := ton.NewClient(ton.Options{BaseURL: srv.BaseURL(), RPS: 1000, Backoff: retry.Backoff{Initial: time.Millisecond}})
	s := jetton.NewSource(c, usdt(t), nil, onErr)
	s.SetPageSize(page)
	return s
}

// Every expected transfer is reported exactly once, in the right block, even
// though shard lt ranges overlap across masterchain blocks.
func TestSourceSyntheticAttribution(t *testing.T) {
	srv := tontest.New(120)
	defer srv.Close()
	for _, page := range []int{1, 2, 1000} {
		src := newSrc(t, srv, page, nil)
		got := map[string]int64{}
		for n := int64(1); n <= 105; n++ {
			b, err := src.Block(context.Background(), n)
			if err != nil {
				t.Fatalf("page %d block %d: %v", page, n, err)
			}
			if b.Time != tontest.GenesisTime.Add(time.Duration(n)*time.Second) {
				t.Fatalf("block time %v", b.Time)
			}
			for _, tr := range b.Transfers {
				if _, dup := got[tr.Key()]; dup {
					t.Fatalf("duplicate %s", tr.Key())
				}
				got[tr.Key()] = tr.BlockNumber
				if tr.Chain != model.ChainTON || tr.Decimals != 6 {
					t.Fatalf("bad transfer %+v", tr)
				}
			}
		}
		exp := srv.Expected(1, 105)
		if len(got) != len(exp) {
			t.Fatalf("page %d: got %d transfers, want %d", page, len(got), len(exp))
		}
		for _, e := range exp {
			if got[e.Key] != e.Block {
				t.Errorf("page %d: %s in block %d, want %d", page, e.Key, got[e.Key], e.Block)
			}
		}
	}
}

func TestSourceMalformedAndInconsistent(t *testing.T) {
	calls := 0
	api := &stubAPI{
		blocks: []ton.BlockRef{
			{Workchain: -1, Shard: "8000000000000000", Seqno: 5, GenUtime: 100},
			{Workchain: 0, Shard: "8000000000000000", Seqno: 9, StartLt: 10, EndLt: 20},
		},
		items: []ton.JettonTransfer{
			{JettonMaster: ton.USDTMaster, SourceWallet: "0:" + zeros, Source: "bad", Destination: "0:" + zeros, Amount: "1", TransactionHash: hash, TransactionLt: 11},
			{JettonMaster: ton.USDTMaster, SourceWallet: "0:" + zeros, Source: "0:" + zeros, Destination: "0:" + zeros, Amount: "x", TransactionHash: hash, TransactionLt: 12},
			{JettonMaster: ton.USDTMaster, SourceWallet: "0:" + zeros, Source: "0:" + zeros, Destination: "0:" + zeros, Amount: "7", TransactionHash: hash, TransactionLt: 13},
			{JettonMaster: ton.USDTMaster, SourceWallet: "0:" + zeros, Source: "0:" + zeros, Destination: "0:" + zeros, Amount: "7", TransactionHash: hash, TransactionLt: 13}, // duplicate row
			{JettonMaster: ton.USDTMaster, SourceWallet: "0:" + zeros, Source: "0:" + zeros, Destination: "0:" + zeros, Amount: "7", TransactionHash: hash, TransactionLt: 99}, // outside lt range
		},
	}
	src := jetton.NewSource(api, usdt(t), nil, func() { calls++ })
	b, err := src.Block(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Transfers) != 1 || calls != 2 || b.Transfers[0].Amount.Int64() != 7 {
		t.Errorf("transfers=%d decodeErrors=%d", len(b.Transfers), calls)
	}
	// masterchain block missing → inconsistent
	api.blocks = api.blocks[1:]
	if _, err := src.Block(context.Background(), 5); !errors.Is(err, chain.ErrInconsistent) {
		t.Errorf("err = %v", err)
	}
	// only masterchain → empty block
	api.blocks = []ton.BlockRef{{Workchain: -1, Shard: "8000000000000000", Seqno: 5, GenUtime: 100}}
	b, err = src.Block(context.Background(), 5)
	if err != nil || len(b.Transfers) != 0 || b.Time.Unix() != 100 {
		t.Errorf("empty block %+v %v", b, err)
	}
	api.blocks = append(api.blocks, ton.BlockRef{Workchain: 0, Shard: "zz", Seqno: 1})
	if _, err := src.Block(context.Background(), 5); !errors.Is(err, chain.ErrInconsistent) {
		t.Errorf("bad shard err = %v", err)
	}
}

const (
	zeros = "0000000000000000000000000000000000000000000000000000000000000000"
	hash  = "VAo5uCmcxdAjnu+ivkJoRFomK4l6bNlw8fsqxOcK7xk="
)

type stubAPI struct {
	blocks []ton.BlockRef
	items  []ton.JettonTransfer
}

func (s *stubAPI) MasterchainInfo(context.Context) (ton.MasterchainInfo, error) {
	return ton.MasterchainInfo{}, nil
}
func (s *stubAPI) MasterchainBlockShards(context.Context, int64) ([]ton.BlockRef, error) {
	return s.blocks, nil
}
func (s *stubAPI) JettonTransfers(context.Context, string, uint64, uint64, int, int) ([]ton.JettonTransfer, error) {
	return s.items, nil
}
