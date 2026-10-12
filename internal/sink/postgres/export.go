package postgres

import (
	"context"
	"math/big"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/export"
)

var _ export.Source = (*Store)(nil)

// StreamTransfers implements export.Source: the stored transfers touching
// q.Address on q.Chain, oldest first, streamed row by row (constant memory).
func (s *Store) StreamTransfers(ctx context.Context, q export.Query, fn func(export.Row) error) error {
	var from, to *time.Time
	if !q.From.IsZero() {
		from = &q.From
	}
	if !q.To.IsZero() {
		to = &q.To
	}
	rows, err := s.pool.Query(ctx, `SELECT block_time, chain, tx_id, log_index, from_address, to_address,
		amount_raw::text, amount::text, symbol, COALESCE(memo, '')
		FROM trc20_transfers
		WHERE chain = $1 AND (from_address = $2 OR to_address = $2)
		  AND ($3::timestamptz IS NULL OR block_time >= $3)
		  AND ($4::timestamptz IS NULL OR block_time < $4)
		ORDER BY block_time, block_number, log_index, id`, q.Chain, q.Address, from, to)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			r        export.Row
			raw, amt string
		)
		if err := rows.Scan(&r.Time, &r.Chain, &r.TxID, &r.LogIndex, &r.From, &r.To, &raw, &amt, &r.Symbol, &r.Memo); err != nil {
			return err
		}
		r.AmountRaw, _ = new(big.Int).SetString(raw, 10)
		r.Decimals = decimalsOf(raw, amt)
		if err := fn(r); err != nil {
			return err
		}
	}
	return rows.Err()
}

// decimalsOf infers token decimals from amount_raw and amount (the table
// does not store decimals): amount = raw / 10^d.
func decimalsOf(raw, amt string) int {
	r, ok1 := new(big.Rat).SetString(raw)
	a, ok2 := new(big.Rat).SetString(amt)
	if !ok1 || !ok2 || a.Sign() == 0 {
		return 6
	}
	q := new(big.Rat).Quo(r, a)
	ten := big.NewRat(10, 1)
	p := big.NewRat(1, 1)
	for d := 0; d <= 36; d++ {
		if p.Cmp(q) == 0 {
			return d
		}
		p.Mul(p, ten)
	}
	return 6
}
