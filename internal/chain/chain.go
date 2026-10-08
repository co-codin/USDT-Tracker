// Package chain defines the chain-agnostic contract between a blockchain
// backend and the processing loop. Each chain lives in its own subpackage
// (internal/chain/tron today; e.g. internal/chain/eth later) and provides a
// Source that turns blocks into decoded token transfers.
package chain

import (
	"context"
	"errors"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/model"
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

// ErrInconsistent means the node returned data that does not match the block
// requested (e.g. a lagging backend behind a load balancer). It is transient.
var ErrInconsistent = errors.New("chain: inconsistent node response")
