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
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/chain/eth"
	"github.com/co-codin/USDT-Tracker/internal/model"
)

// API is the subset of eth.Client used by Source.
type API interface {
	BlockNumber(ctx context.Context) (int64, error)
	GetLogs(ctx context.Context, f eth.LogFilter) ([]eth.Log, error)
	BlockTime(ctx context.Context, num int64) (time.Time, error)
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
	onDecErr func()
}

// NewSource creates a source. chunk is the max number of blocks per
// eth_getLogs call (default 50); ranges the node rejects are halved
// automatically. onDecodeError (optional) counts malformed logs.
func NewSource(api API, tok Token, chunk int, log *slog.Logger, onDecodeError func()) *Source {
	if chunk <= 0 {
		chunk = 50
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	tok.Contract = strings.ToLower(tok.Contract)
	return &Source{api: api, tok: tok, chunk: int64(chunk), log: log, onDecErr: onDecodeError}
}

// Head implements chain.Source.
func (s *Source) Head(ctx context.Context) (int64, error) { return s.api.BlockNumber(ctx) }

// Block implements chain.Source.
func (s *Source) Block(ctx context.Context, num int64) (chain.Block, error) {
	bs, err := s.Blocks(ctx, num, num)
	if err != nil {
		return chain.Block{}, err
	}
	return bs[0], nil
}

// Blocks implements chain.RangeSource: one Block per number in [from, to].
func (s *Source) Blocks(ctx context.Context, from, to int64) ([]chain.Block, error) {
	if to < from {
		return nil, fmt.Errorf("erc20: invalid range %d-%d", from, to)
	}
	blocks := make([]chain.Block, to-from+1)
	for i := range blocks {
		blocks[i].Number = from + int64(i)
	}
	for start := from; start <= to; start += s.chunk {
		end := min(to, start+s.chunk-1)
		logs, err := s.getLogs(ctx, start, end)
		if err != nil {
			return nil, err
		}
		for _, lg := range logs {
			if err := s.add(blocks, from, lg); err != nil {
				return nil, err
			}
		}
	}
	for i := range blocks {
		b := &blocks[i]
		if len(b.Transfers) > 0 && b.Time.IsZero() {
			t, err := s.api.BlockTime(ctx, b.Number)
			if err != nil {
				return nil, err
			}
			b.Time = t
			for j := range b.Transfers {
				b.Transfers[j].BlockTime = t
			}
		}
	}
	return blocks, nil
}

// getLogs fetches [from, to], halving the range when the node refuses it.
func (s *Source) getLogs(ctx context.Context, from, to int64) ([]eth.Log, error) {
	logs, err := s.api.GetLogs(ctx, eth.LogFilter{FromBlock: from, ToBlock: to, Address: s.tok.Contract, Topics: []string{eth.TransferTopic}})
	if err == nil {
		return logs, nil
	}
	if !errors.Is(err, eth.ErrRangeTooLarge) || from == to {
		return nil, err
	}
	mid := from + (to-from)/2
	s.log.Debug("eth_getLogs range too large, splitting", "from", from, "to", to)
	left, err := s.getLogs(ctx, from, mid)
	if err != nil {
		return nil, err
	}
	right, err := s.getLogs(ctx, mid+1, to)
	if err != nil {
		return nil, err
	}
	return append(left, right...), nil
}

func (s *Source) decodeErr(lg eth.Log, why string) {
	s.log.Warn("skipping malformed erc20 log", "tx", lg.TransactionHash, "log_index", lg.LogIndex, "reason", why)
	if s.onDecErr != nil {
		s.onDecErr()
	}
}

func (s *Source) add(blocks []chain.Block, from int64, lg eth.Log) error {
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
	b := &blocks[idx]
	if lg.BlockTimestamp != "" {
		if ts, err := eth.ParseQuantity(lg.BlockTimestamp); err == nil {
			b.Time = time.Unix(ts, 0).UTC()
		}
	}
	b.TxCount++
	b.Transfers = append(b.Transfers, model.Transfer{
		Chain: model.ChainETH, TxID: strings.ToLower(lg.TransactionHash), LogIndex: int(li),
		BlockNumber: num, BlockTime: b.Time, Contract: s.tok.Contract, Symbol: s.tok.Symbol,
		Decimals: s.tok.Decimals, From: fromAddr, To: toAddr, Amount: amt,
	})
	return nil
}
