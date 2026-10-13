// Package erc20 turns Ethereum blocks into ERC-20 (USDT by default) Transfer
// events using eth_getLogs over block ranges. It implements chain.Source and
// chain.RangeSource. Read-only: no keys, no transactions.
package erc20

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/chain/eth"
	"github.com/co-codin/USDT-Tracker/internal/model"
)

// API is the subset of eth.Client used by Source.
type API interface {
	HeaderByTag(ctx context.Context, tag eth.HeadTag) (int64, error)
	GetLogs(ctx context.Context, f eth.LogFilter) ([]eth.Log, error)
	LogsWithHeaders(ctx context.Context, f eth.LogFilter) ([]eth.Log, []*eth.Header, error)
	HeaderByNumber(ctx context.Context, num int64) (*eth.Header, error)
}

// Options are optional Source settings.
type Options struct {
	// HeadTag picks the head block: latest (default), safe or finalized.
	HeadTag eth.HeadTag
	// OnDecodeError counts malformed logs.
	OnDecodeError func()
	// OnReorg is called when the parent hash of the first block of a range
	// does not match the last block delivered, i.e. a reorg deeper than the
	// configured confirmations happened and already delivered transfers may
	// no longer be canonical.
	OnReorg func()
}

// Token describes the ERC-20 token to follow.
type Token struct {
	Contract string // canonical lowercase 0x address
	Symbol   string
	Decimals int
}

var (
	_ chain.Source      = (*Source)(nil)
	_ chain.RangeSource = (*Source)(nil)
)

// Source implements chain.Source for Ethereum.
type Source struct {
	api      API
	tok      Token
	chunk    int64
	log      *slog.Logger
	opt      Options
	noBatch  atomic.Bool
	mu       sync.Mutex
	lastNum  int64
	lastHash string
}

// NewSource creates a source. chunk is the max number of blocks per
// eth_getLogs call (default 50); ranges the node rejects are halved
// automatically.
func NewSource(api API, tok Token, chunk int, log *slog.Logger, opt Options) *Source {
	if chunk <= 0 {
		chunk = 50
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	tok.Contract = strings.ToLower(tok.Contract)
	if opt.HeadTag == "" {
		opt.HeadTag = eth.HeadLatest
	}
	return &Source{api: api, tok: tok, chunk: int64(chunk), log: log, opt: opt, lastNum: -1}
}

// Head implements chain.Source.
func (s *Source) Head(ctx context.Context) (int64, error) {
	return s.api.HeaderByTag(ctx, s.opt.HeadTag)
}

// Block implements chain.Source.
func (s *Source) Block(ctx context.Context, num int64) (chain.Block, error) {
	bs, err := s.Blocks(ctx, num, num)
	if err != nil {
		return chain.Block{}, err
	}
	return bs[0], nil
}

// Blocks implements chain.RangeSource: one Block per number in [from, to].
//
// Logs and the headers of every block are fetched in one JSON-RPC batch, so a
// lagging backend cannot make blocks it does not have look empty: a missing
// header, a broken parent-hash chain or a log whose blockHash differs from
// the header yields chain.ErrInconsistent and the range is retried.
func (s *Source) Blocks(ctx context.Context, from, to int64) ([]chain.Block, error) {
	if to < from {
		return nil, fmt.Errorf("erc20: invalid range %d-%d", from, to)
	}
	blocks := make([]chain.Block, to-from+1)
	hashes := make([]string, len(blocks))
	for i := range blocks {
		blocks[i].Number = from + int64(i)
	}
	prev, firstParent := "", ""
	for start := from; start <= to; start += s.chunk {
		end := min(to, start+s.chunk-1)
		logs, hdrs, err := s.fetch(ctx, start, end)
		if err != nil {
			return nil, err
		}
		for i, h := range hdrs {
			num := start + int64(i)
			if h == nil {
				return nil, fmt.Errorf("block %d not available on node yet: %w", num, chain.ErrInconsistent)
			}
			if n, err := eth.ParseQuantity(h.Number); err != nil || n != num || h.Hash == "" {
				return nil, fmt.Errorf("bad header for block %d: %w", num, chain.ErrInconsistent)
			}
			if prev != "" && !strings.EqualFold(h.ParentHash, prev) {
				return nil, fmt.Errorf("block %d parent hash does not link to block %d (node switched forks mid-range): %w", num, num-1, chain.ErrInconsistent)
			}
			if num == from {
				firstParent = h.ParentHash
			}
			prev = h.Hash
			idx := num - from
			hashes[idx] = strings.ToLower(h.Hash)
			ts, err := eth.ParseQuantity(h.Timestamp)
			if err != nil {
				return nil, fmt.Errorf("bad timestamp for block %d: %w", num, chain.ErrInconsistent)
			}
			blocks[idx].Time = time.Unix(ts, 0).UTC()
		}
		for _, lg := range logs {
			if err := s.add(blocks, hashes, from, lg); err != nil {
				return nil, err
			}
		}
	}
	for i := range blocks {
		b := &blocks[i]
		txs := map[string]struct{}{}
		for j := range b.Transfers {
			b.Transfers[j].BlockTime = b.Time
			txs[b.Transfers[j].TxID] = struct{}{}
		}
		b.TxCount = len(txs)
	}
	s.checkReorg(from, to, firstParent, hashes[len(hashes)-1])
	return blocks, nil
}

// checkReorg compares the parent of the first block of this range with the
// last block delivered by the previous call.
func (s *Source) checkReorg(from, to int64, parent, last string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lastNum, lastHash := s.lastNum, s.lastHash
	s.lastNum, s.lastHash = to, last
	if lastNum != from-1 || lastHash == "" || parent == "" || strings.EqualFold(parent, lastHash) {
		return
	}
	s.log.Error("ethereum reorg deeper than the confirmation depth detected: transfers already delivered from the replaced blocks may not be canonical; consider ETH_HEAD_TAG=finalized",
		"block", from-1, "delivered_hash", lastHash, "canonical_hash", parent)
	if s.opt.OnReorg != nil {
		s.opt.OnReorg()
	}
}

// fetch returns logs and headers for [from, to], halving ranges the node
// refuses and falling back to separate calls when batches are unsupported.
func (s *Source) fetch(ctx context.Context, from, to int64) ([]eth.Log, []*eth.Header, error) {
	f := eth.LogFilter{FromBlock: from, ToBlock: to, Address: s.tok.Contract, Topics: []string{eth.TransferTopic}}
	var logs []eth.Log
	var hdrs []*eth.Header
	var err error
	if !s.noBatch.Load() {
		logs, hdrs, err = s.api.LogsWithHeaders(ctx, f)
		if errors.Is(err, eth.ErrBatchUnsupported) {
			s.log.Warn("RPC endpoint does not support JSON-RPC batches; falling back to separate calls (weaker lagging-node protection)")
			s.noBatch.Store(true)
		}
	}
	if s.noBatch.Load() {
		logs, err = s.api.GetLogs(ctx, f)
		if err == nil {
			hdrs = make([]*eth.Header, 0, to-from+1)
			for n := from; n <= to; n++ {
				h, herr := s.api.HeaderByNumber(ctx, n)
				if herr != nil {
					return nil, nil, herr
				}
				hdrs = append(hdrs, h)
			}
		}
	}
	if err != nil {
		if !errors.Is(err, eth.ErrRangeTooLarge) || from == to {
			return nil, nil, err
		}
		mid := from + (to-from)/2
		s.log.Debug("eth_getLogs range too large, splitting", "from", from, "to", to)
		l1, h1, err := s.fetch(ctx, from, mid)
		if err != nil {
			return nil, nil, err
		}
		l2, h2, err := s.fetch(ctx, mid+1, to)
		if err != nil {
			return nil, nil, err
		}
		logs, hdrs = append(l1, l2...), append(h1, h2...)
	}
	if len(hdrs) != int(to-from+1) {
		return nil, nil, fmt.Errorf("got %d headers for %d-%d: %w", len(hdrs), from, to, chain.ErrInconsistent)
	}
	return logs, hdrs, nil
}

func (s *Source) decodeErr(lg eth.Log, why string) {
	s.log.Warn("skipping malformed erc20 log", "tx", lg.TransactionHash, "log_index", lg.LogIndex, "reason", why)
	if s.opt.OnDecodeError != nil {
		s.opt.OnDecodeError()
	}
}

func (s *Source) add(blocks []chain.Block, hashes []string, from int64, lg eth.Log) error {
	if lg.Removed {
		return nil // reorged out; confirmations should make this rare
	}
	num, err := eth.ParseQuantity(lg.BlockNumber)
	if err != nil {
		s.decodeErr(lg, "block number")
		return nil
	}
	idx := num - from
	if idx < 0 || idx >= int64(len(blocks)) {
		return fmt.Errorf("log in block %d outside requested range: %w", num, chain.ErrInconsistent)
	}
	if strings.ToLower(lg.Address) != s.tok.Contract {
		return fmt.Errorf("log from contract %s, want %s: %w", lg.Address, s.tok.Contract, chain.ErrInconsistent)
	}
	if len(lg.Topics) != 3 || strings.ToLower(lg.Topics[0]) != eth.TransferTopic {
		s.decodeErr(lg, "not a Transfer(address,address,uint256) event")
		return nil
	}
	fromAddr, err1 := eth.TopicAddress(lg.Topics[1])
	toAddr, err2 := eth.TopicAddress(lg.Topics[2])
	data := strings.TrimPrefix(lg.Data, "0x")
	amt, ok := new(big.Int).SetString(data, 16)
	if err1 != nil || err2 != nil || len(data) != 64 || !ok {
		s.decodeErr(lg, "bad topics or data")
		return nil
	}
	li, err := eth.ParseQuantity(lg.LogIndex)
	if err != nil || !strings.HasPrefix(lg.TransactionHash, "0x") || len(lg.TransactionHash) != 66 {
		s.decodeErr(lg, "bad log index or tx hash")
		return nil
	}
	if !strings.EqualFold(lg.BlockHash, hashes[idx]) {
		return fmt.Errorf("log in block %d has hash %s, header says %s: %w", num, lg.BlockHash, hashes[idx], chain.ErrInconsistent)
	}
	b := &blocks[idx]
	b.Transfers = append(b.Transfers, model.Transfer{
		Chain: model.ChainETH, TxID: strings.ToLower(lg.TransactionHash), LogIndex: int(li),
		BlockNumber: num, BlockTime: b.Time, Contract: s.tok.Contract, Symbol: s.tok.Symbol,
		Decimals: s.tok.Decimals, From: fromAddr, To: toAddr, Amount: amt,
	})
	return nil
}
