package trc20

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/chain/tron"
)

type fakeAPI struct {
	infos   []tron.TransactionInfo
	block   tron.Block
	blkErr  error
	blkCall int
}

func (f *fakeAPI) NowBlock(context.Context) (tron.BlockHeader, error) {
	return tron.BlockHeader{Number: 100}, nil
}
func (f *fakeAPI) BlockByNum(context.Context, int64) (tron.Block, error) {
	f.blkCall++
	return f.block, f.blkErr
}
func (f *fakeAPI) TransactionInfoByBlockNum(context.Context, int64) ([]tron.TransactionInfo, error) {
	return f.infos, nil
}

func newSrc(api API) *Source {
	c, _ := tron.ParseAddress("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
	return NewSource(api, New(Token{Contract: c, Symbol: "USDT", Decimals: 6}), nil, nil)
}

func TestEmptyResponseIsVerified(t *testing.T) {
	ts := time.UnixMilli(1791324426000).UTC()
	// Truly empty block.
	api := &fakeAPI{block: tron.Block{BlockHeader: tron.BlockHeader{Number: 7, Timestamp: ts}}}
	b, err := newSrc(api).Block(context.Background(), 7)
	if err != nil || api.blkCall != 1 || !b.Time.Equal(ts) || len(b.Transfers) != 0 {
		t.Fatalf("empty block: %+v %v", b, err)
	}
	// Node returned [] but the block has transactions -> transient error, no gap.
	api = &fakeAPI{block: tron.Block{BlockHeader: tron.BlockHeader{Number: 7}, TxCount: 250}}
	if _, err := newSrc(api).Block(context.Background(), 7); !errors.Is(err, chain.ErrInconsistent) {
		t.Fatalf("want chain.ErrInconsistent, got %v", err)
	}
	// Node doesn't have the block yet.
	api = &fakeAPI{blkErr: tron.ErrBlockNotFound}
	if _, err := newSrc(api).Block(context.Background(), 7); !errors.Is(err, tron.ErrBlockNotFound) {
		t.Fatalf("want ErrBlockNotFound, got %v", err)
	}
}

func TestWrongBlockNumberRejected(t *testing.T) {
	api := &fakeAPI{infos: []tron.TransactionInfo{{ID: "aa", BlockNumber: 8}}}
	if _, err := newSrc(api).Block(context.Background(), 7); !errors.Is(err, chain.ErrInconsistent) {
		t.Fatalf("want chain.ErrInconsistent, got %v", err)
	}
}

func TestHeadAndDecodeErrorsAreCounted(t *testing.T) {
	malformed := tron.Log{Address: "a614f803b6fd780986a42c78ec9c7f77e6ded13c",
		Topics: []string{TransferTopic, "00", "00"}, Data: "00"}
	good := tron.Log{Address: "a614f803b6fd780986a42c78ec9c7f77e6ded13c", Topics: []string{TransferTopic,
		"000000000000000000000000f4a3aa3c52cdc41e3c1a7b8f7493ae1164ee7f07",
		"0000000000000000000000007452f02038a6039b730c7ec929a3380ff1b4a6e7"},
		Data: "0000000000000000000000000000000000000000000000000000000017d78400"}
	api := &fakeAPI{infos: []tron.TransactionInfo{{ID: "aa", BlockNumber: 7, BlockTimeStamp: 1000, Log: []tron.Log{malformed, good}}}}
	c, _ := tron.ParseAddress("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t")
	decErrs := 0
	src := NewSource(api, New(Token{Contract: c, Symbol: "USDT", Decimals: 6}), nil, func() { decErrs++ })

	if h, err := src.Head(context.Background()); err != nil || h != 100 {
		t.Fatalf("head = %d, %v", h, err)
	}
	b, err := src.Block(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if decErrs != 1 || len(b.Transfers) != 1 || b.Transfers[0].LogIndex != 1 || b.TxCount != 1 {
		t.Fatalf("decErrs=%d block=%+v", decErrs, b)
	}
}
