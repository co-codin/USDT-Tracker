package chain_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain"
	"github.com/co-codin/USDT-Tracker/internal/chain/tron/trc20"
	"github.com/co-codin/USDT-Tracker/internal/model"
)

// Every chain backend must satisfy chain.Source.
var _ chain.Source = (*trc20.Source)(nil)

// staticSource is the minimal shape a future backend (eth, sol) has to provide.
type staticSource struct{ head int64 }

func (s staticSource) Head(context.Context) (int64, error) { return s.head, nil }

func (s staticSource) Block(_ context.Context, n int64) (chain.Block, error) {
	if n > s.head {
		return chain.Block{}, fmt.Errorf("%w: block %d beyond head %d", chain.ErrInconsistent, n, s.head)
	}
	return chain.Block{Number: n, Time: time.Unix(n, 0), Transfers: []model.Transfer{{BlockNumber: n}}}, nil
}

func TestSourceContract(t *testing.T) {
	var src chain.Source = staticSource{head: 10}
	ctx := context.Background()
	if h, _ := src.Head(ctx); h != 10 {
		t.Fatalf("head %d", h)
	}
	b, err := src.Block(ctx, 7)
	if err != nil || b.Number != 7 || len(b.Transfers) != 1 {
		t.Fatalf("block: %+v %v", b, err)
	}
	if _, err := src.Block(ctx, 11); !errors.Is(err, chain.ErrInconsistent) {
		t.Fatalf("wrapped ErrInconsistent expected, got %v", err)
	}
}
