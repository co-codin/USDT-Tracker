// Package sink defines output destinations for matched transfers and a
// dispatcher that fans batches out to all enabled sinks.
package sink

import (
	"context"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/model"
)

// Batch is the set of matched transfers from a single block.
type Batch struct {
	BlockNumber int64
	BlockTime   time.Time
	Transfers   []model.Transfer
}

// Sink receives batches of matched transfers. Send must be idempotent with
// respect to model.Transfer.Key (delivery is at-least-once).
type Sink interface {
	Name() string
	Send(ctx context.Context, b Batch) error
	Close() error
}
