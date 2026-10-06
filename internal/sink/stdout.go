package sink

import (
	"context"
	"io"
	"log/slog"
	"strings"
)

// Stdout writes one structured JSON line (slog) per transfer.
type Stdout struct{ log *slog.Logger }

// NewStdout creates a stdout sink writing JSON lines to w.
func NewStdout(w io.Writer) *Stdout {
	return &Stdout{log: slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo}))}
}

// Name implements Sink.
func (s *Stdout) Name() string { return "stdout" }

// Send implements Sink.
func (s *Stdout) Send(ctx context.Context, b Batch) error {
	for _, t := range b.Transfers {
		s.log.LogAttrs(ctx, slog.LevelInfo, "transfer",
			slog.String("id", t.Key()),
			slog.String("tx_id", t.TxID),
			slog.Int("log_index", t.LogIndex),
			slog.Int64("block", t.BlockNumber),
			slog.Time("block_time", t.BlockTime),
			slog.String("token", t.Symbol),
			slog.String("from", t.From),
			slog.String("to", t.To),
			slog.String("amount", t.AmountDecimal()),
			slog.String("amount_raw", t.Amount.String()),
			slog.String("reasons", strings.Join(t.Reasons, ",")),
		)
	}
	return nil
}

// Close implements Sink.
func (s *Stdout) Close() error { return nil }
