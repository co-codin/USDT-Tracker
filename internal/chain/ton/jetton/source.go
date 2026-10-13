// Package jetton turns TON masterchain blocks into USD₮ (or any jetton)
// transfers using the toncenter v3 index. It implements chain.Source with
// masterchain seqno as the block number.
//
// Attribution: a masterchain block commits a set of shard blocks; every
// jetton transfer whose transaction runs on an account inside one of those
// shards with a logical time inside that shard block's [start_lt, end_lt]
// belongs to this masterchain block. Consecutive blocks of one shard have
// disjoint lt ranges, so every transfer is attributed to exactly one
// masterchain block and the seqno cursor never skips or double-counts.
package jetton

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sort"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/chain/ton"
	"github.com/co-codin/USDT-Tracker/internal/model"
)

// API is the subset of ton.Client used by Source.
type API interface {
	MasterchainInfo(ctx context.Context) (ton.MasterchainInfo, error)
	MasterchainBlockShards(ctx context.Context, seqno int64) ([]ton.BlockRef, error)
	JettonTransfers(ctx context.Context, master string, startLt, endLt uint64, limit, offset int) ([]ton.JettonTransfer, error)
}

// Token describes the jetton to follow.
type Token struct {
	Master   ton.Address // jetton master
	Symbol   string
	Decimals int
}

// warnPages logs a warning every this many pages of one masterchain block.
const warnPages = 100

var _ chain.Source = (*Source)(nil)

// Source implements chain.Source for TON.
type Source struct {
	api      API
	tok      Token
	master   string // user-friendly bounceable master for queries
	pageSize int
	log      *slog.Logger
	onDecErr func()
}

// NewSource creates a source. onDecodeError (optional) is called for every
// malformed transfer item so it can be counted.
func NewSource(api API, tok Token, log *slog.Logger, onDecodeError func()) *Source {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Source{api: api, tok: tok, master: tok.Master.Friendly(true, false), pageSize: ton.MaxPageSize, log: log, onDecErr: onDecodeError}
}

// SetPageSize overrides the page size (tests).
func (s *Source) SetPageSize(n int) {
	if n > 0 {
		s.pageSize = n
	}
}

// Head implements chain.Source: the last indexed masterchain seqno.
func (s *Source) Head(ctx context.Context) (int64, error) {
	info, err := s.api.MasterchainInfo(ctx)
	if err != nil {
		return 0, err
	}
	return info.Last.Seqno, nil
}

type shardRange struct {
	wc     int32
	shard  uint64
	lo, hi uint64
}

// Block implements chain.Source.
func (s *Source) Block(ctx context.Context, num int64) (chain.Block, error) {
	blocks, err := s.api.MasterchainBlockShards(ctx, num)
	if err != nil {
		return chain.Block{}, fmt.Errorf("%w: %w", chain.ErrInconsistent, err)
	}
	var (
		mcTime  time.Time
		haveMC  bool
		ranges  []shardRange
		txCount int
		lo, hi  uint64
	)
	for _, b := range blocks {
		if b.Workchain == -1 {
			if b.Seqno == num {
				mcTime, haveMC = b.Time(), true
				txCount += b.TxCount
			}
			continue // USD₮ wallets live in workchain 0
		}
		sh, err := ton.ParseShard(b.Shard)
		if err != nil {
			return chain.Block{}, fmt.Errorf("%w: %w", chain.ErrInconsistent, err)
		}
		r := shardRange{wc: b.Workchain, shard: sh, lo: uint64(b.StartLt), hi: uint64(b.EndLt)}
		if r.hi < r.lo {
			return chain.Block{}, fmt.Errorf("%w: shard block %d has end_lt < start_lt", chain.ErrInconsistent, b.Seqno)
		}
		if len(ranges) == 0 || r.lo < lo {
			lo = r.lo
		}
		if r.hi > hi {
			hi = r.hi
		}
		ranges = append(ranges, r)
		txCount += b.TxCount
	}
	if !haveMC {
		return chain.Block{}, fmt.Errorf("%w: masterchain block %d missing from response", chain.ErrInconsistent, num)
	}
	out := chain.Block{Number: num, Time: mcTime, TxCount: txCount}
	if len(ranges) == 0 {
		return out, nil
	}
	// Page by logical time (sort=asc): the next page starts at the last lt
	// seen (inclusive, duplicates are dropped via seen). Offsets are only
	// used while a whole page shares one lt, so there is no page cap and no
	// O(n²) deep offsets: a huge block (airdrop) is slow, never stuck.
	seen := map[string]bool{}
	start, offset := lo, 0
	for page := 1; ; page++ {
		if page%warnPages == 0 {
			s.log.Warn("very large ton block, still paging jetton transfers", "block", num, "pages", page, "lt", start)
		}
		items, err := s.api.JettonTransfers(ctx, s.master, start, hi, s.pageSize, offset)
		if err != nil {
			return chain.Block{}, err
		}
		for _, it := range items {
			t, ok, err := s.convert(it, num, mcTime, ranges)
			if err != nil {
				s.log.Warn("skipping malformed jetton transfer", "block", num, "tx", it.TransactionHash, "err", err)
				if s.onDecErr != nil {
					s.onDecErr()
				}
				continue
			}
			if !ok || seen[t.Key()] {
				continue
			}
			seen[t.Key()] = true
			out.Transfers = append(out.Transfers, t)
		}
		if len(items) < s.pageSize {
			break
		}
		last := uint64(items[len(items)-1].TransactionLt)
		switch {
		case last > start:
			start, offset = last, 0
		case last == start:
			offset += s.pageSize
		default:
			return chain.Block{}, fmt.Errorf("%w: jetton transfers not sorted by lt (got %d after %d)", chain.ErrInconsistent, last, start)
		}
	}
	sort.SliceStable(out.Transfers, func(i, j int) bool { return out.Transfers[i].TxID < out.Transfers[j].TxID })
	return out, nil
}

// convert maps an API item to a transfer. ok=false means it belongs to a
// different masterchain block, another jetton, or the transaction aborted.
func (s *Source) convert(it ton.JettonTransfer, num int64, bt time.Time, ranges []shardRange) (model.Transfer, bool, error) {
	master, err := ton.ParseAddress(it.JettonMaster)
	if err != nil {
		return model.Transfer{}, false, fmt.Errorf("jetton_master: %w", err)
	}
	if master != s.tok.Master {
		return model.Transfer{}, false, nil
	}
	wallet, err := ton.ParseAddress(it.SourceWallet)
	if err != nil {
		return model.Transfer{}, false, fmt.Errorf("source_wallet: %w", err)
	}
	lt := uint64(it.TransactionLt)
	in := false
	for _, r := range ranges {
		if r.wc == wallet.Workchain && lt >= r.lo && lt <= r.hi && ton.ShardContains(r.shard, wallet.Hash) {
			in = true
			break
		}
	}
	if !in {
		return model.Transfer{}, false, nil
	}
	if it.TransactionAborted {
		return model.Transfer{}, false, nil
	}
	from, err := ton.ParseAddress(it.Source)
	if err != nil {
		return model.Transfer{}, false, fmt.Errorf("source: %w", err)
	}
	to, err := ton.ParseAddress(it.Destination)
	if err != nil {
		return model.Transfer{}, false, fmt.Errorf("destination: %w", err)
	}
	amt, ok := new(big.Int).SetString(it.Amount, 10)
	if !ok || amt.Sign() < 0 {
		return model.Transfer{}, false, fmt.Errorf("bad amount %q", it.Amount)
	}
	h, err := ton.HashHex(it.TransactionHash)
	if err != nil {
		return model.Transfer{}, false, err
	}
	return model.Transfer{
		Chain: model.ChainTON, TxID: h, LogIndex: 0, BlockNumber: num, BlockTime: bt,
		Contract: s.tok.Master.Raw(), Symbol: s.tok.Symbol, Decimals: s.tok.Decimals,
		From: from.Raw(), To: to.Raw(), Amount: amt, Memo: it.Comment(),
	}, true, nil
}
