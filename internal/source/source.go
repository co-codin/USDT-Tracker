// Package source abstracts where blocks of transfers come from.
package source

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/decoder"
	"github.com/co-codin/USDT-Tracker/internal/model"
	"github.com/co-codin/USDT-Tracker/internal/tron"
)

// Block is a processed block with all decoded token transfers (unfiltered).
type Block struct {
	Number    int64
	Time      time.Time
	TxCount   int
	Transfers []model.Transfer
}

// Source provides the chain head and decoded blocks.
type Source interface {
	// Head returns the latest block number known to the node.
	Head(ctx context.Context) (int64, error)
	// Block returns all token transfers in block num.
	Block(ctx context.Context, num int64) (Block, error)
}

// TronAPI is the subset of tron.Client used by TronSource.
type TronAPI interface {
	NowBlock(ctx context.Context) (tron.BlockHeader, error)
	BlockByNum(ctx context.Context, num int64) (tron.Block, error)
	TransactionInfoByBlockNum(ctx context.Context, num int64) ([]tron.TransactionInfo, error)
}

// ErrInconsistent means the node returned data that does not match the block
// requested (e.g. a lagging backend behind a load balancer). It is transient.
var ErrInconsistent = errors.New("source: inconsistent node response")

// TronSource reads blocks via the TRON HTTP API and decodes token transfers.
type TronSource struct {
	api      TronAPI
	dec      *decoder.Decoder
	log      *slog.Logger
	onDecErr func()
}

// NewTronSource creates a source. onDecodeError (optional) is called for every
// malformed log so it can be counted.
func NewTronSource(api TronAPI, dec *decoder.Decoder, log *slog.Logger, onDecodeError func()) *TronSource {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &TronSource{api: api, dec: dec, log: log, onDecErr: onDecodeError}
}

// Head implements Source.
func (s *TronSource) Head(ctx context.Context) (int64, error) {
	h, err := s.api.NowBlock(ctx)
	if err != nil {
		return 0, err
	}
	return h.Number, nil
}

// Block implements Source. It fetches all transaction infos (one request per
// block). An empty response is ambiguous (empty block vs. node that doesn't
// have the block yet), so it is verified with getblockbynum.
func (s *TronSource) Block(ctx context.Context, num int64) (Block, error) {
	infos, err := s.api.TransactionInfoByBlockNum(ctx, num)
	if err != nil {
		return Block{}, err
	}
	if len(infos) == 0 {
		b, err := s.api.BlockByNum(ctx, num)
		if err != nil {
			return Block{}, err
		}
		if b.TxCount > 0 {
			return Block{}, fmt.Errorf("%w: block %d has %d txs but no tx infos", ErrInconsistent, num, b.TxCount)
		}
		return Block{Number: num, Time: b.Timestamp}, nil
	}
	for _, in := range infos {
		if in.BlockNumber != num {
			return Block{}, fmt.Errorf("%w: asked block %d, got tx %s in block %d", ErrInconsistent, num, in.ID, in.BlockNumber)
		}
	}
	transfers, errs := s.dec.DecodeBlock(infos)
	for _, e := range errs {
		s.log.Warn("skipping malformed log", "block", num, "err", e)
		if s.onDecErr != nil {
			s.onDecErr()
		}
	}
	return Block{Number: num, Time: infos[0].BlockTime(), TxCount: len(infos), Transfers: transfers}, nil
}
