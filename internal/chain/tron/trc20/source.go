package trc20

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/chain/tron"
)

// API is the subset of tron.Client used by Source.
type API interface {
	NowBlock(ctx context.Context) (tron.BlockHeader, error)
	BlockByNum(ctx context.Context, num int64) (tron.Block, error)
	TransactionInfoByBlockNum(ctx context.Context, num int64) ([]tron.TransactionInfo, error)
}

var _ chain.Source = (*Source)(nil)

// Source reads blocks via the TRON HTTP API and decodes TRC-20 transfers.
// It implements chain.Source.
type Source struct {
	api      API
	dec      *Decoder
	log      *slog.Logger
	onDecErr func()
}

// NewSource creates a source. onDecodeError (optional) is called for every
// malformed log so it can be counted.
func NewSource(api API, dec *Decoder, log *slog.Logger, onDecodeError func()) *Source {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Source{api: api, dec: dec, log: log, onDecErr: onDecodeError}
}

// Head implements chain.Source.
func (s *Source) Head(ctx context.Context) (int64, error) {
	h, err := s.api.NowBlock(ctx)
	if err != nil {
		return 0, err
	}
	return h.Number, nil
}

// Block implements chain.Source. It fetches all transaction infos (one request per
// block). An empty response is ambiguous (empty block vs. node that doesn't
// have the block yet), so it is verified with getblockbynum.
func (s *Source) Block(ctx context.Context, num int64) (chain.Block, error) {
	infos, err := s.api.TransactionInfoByBlockNum(ctx, num)
	if err != nil {
		return chain.Block{}, err
	}
	if len(infos) == 0 {
		b, err := s.api.BlockByNum(ctx, num)
		if err != nil {
			return chain.Block{}, err
		}
		if b.TxCount > 0 {
			return chain.Block{}, fmt.Errorf("%w: block %d has %d txs but no tx infos", chain.ErrInconsistent, num, b.TxCount)
		}
		return chain.Block{Number: num, Time: b.Timestamp}, nil
	}
	for _, in := range infos {
		if in.BlockNumber != num {
			return chain.Block{}, fmt.Errorf("%w: asked block %d, got tx %s in block %d", chain.ErrInconsistent, num, in.ID, in.BlockNumber)
		}
	}
	transfers, errs := s.dec.DecodeBlock(infos)
	for _, e := range errs {
		s.log.Warn("skipping malformed log", "block", num, "err", e)
		if s.onDecErr != nil {
			s.onDecErr()
		}
	}
	return chain.Block{Number: num, Time: infos[0].BlockTime(), TxCount: len(infos), Transfers: transfers}, nil
}
