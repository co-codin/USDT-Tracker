package postgres

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/co-codin/USDT-Tracker/internal/outbox"
)

// Outbox implements outbox.Store on the deliveries table (migration 0007).
type Outbox struct {
	pool  *pgxpool.Pool
	scope string
	// Retention is how long finalised rows are kept for dedupe (default 30 days).
	Retention time.Duration
	lastPrune atomic.Int64 // unix nanos
}

var _ outbox.Store = (*Outbox)(nil)

// Outbox returns the durable delivery queue scoped to this store's cursor name.
func (s *Store) Outbox() *Outbox {
	return &Outbox{pool: s.pool, scope: s.cursorName, Retention: 30 * 24 * time.Hour}
}

// Enqueue implements outbox.Store.
func (o *Outbox) Enqueue(ctx context.Context, kind string, items []outbox.Item) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}
	b := &pgx.Batch{}
	for _, it := range items {
		b.Queue(`INSERT INTO deliveries (scope, kind, key, chat_id, payload) VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (scope, kind, key) DO NOTHING`, o.scope, kind, it.Key, it.ChatID, it.Payload)
	}
	br := o.pool.SendBatch(ctx, b)
	added := 0
	for range items {
		tag, err := br.Exec()
		if err != nil {
			_ = br.Close()
			return added, fmt.Errorf("postgres: outbox enqueue: %w", err)
		}
		added += int(tag.RowsAffected())
	}
	if err := br.Close(); err != nil {
		return added, fmt.Errorf("postgres: outbox enqueue: %w", err)
	}
	return added, nil
}

// Pending implements outbox.Store.
func (o *Outbox) Pending(ctx context.Context, kind string, limit int) ([]outbox.Item, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := o.pool.Query(ctx, `SELECT key, chat_id, payload, attempts, created_at FROM deliveries
		WHERE scope = $1 AND kind = $2 AND status = 'pending' ORDER BY created_at, key LIMIT $3`, o.scope, kind, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: outbox pending: %w", err)
	}
	items, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (outbox.Item, error) {
		var it outbox.Item
		err := r.Scan(&it.Key, &it.ChatID, &it.Payload, &it.Attempts, &it.Created)
		return it, err
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: outbox pending: %w", err)
	}
	return items, nil
}

// Count implements outbox.Store.
func (o *Outbox) Count(ctx context.Context, kind string) (int, error) {
	var n int
	err := o.pool.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE scope = $1 AND kind = $2 AND status = 'pending'`, o.scope, kind).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("postgres: outbox count: %w", err)
	}
	return n, nil
}

// Done implements outbox.Store. It also prunes old finalised rows (at most hourly).
func (o *Outbox) Done(ctx context.Context, kind string, keys []string, status string) error {
	if len(keys) == 0 {
		return nil
	}
	if _, err := o.pool.Exec(ctx, `UPDATE deliveries SET status = $4, done_at = now()
		WHERE scope = $1 AND kind = $2 AND key = ANY($3) AND status = 'pending'`, o.scope, kind, keys, status); err != nil {
		return fmt.Errorf("postgres: outbox done: %w", err)
	}
	if last := o.lastPrune.Load(); o.Retention > 0 && time.Since(time.Unix(0, last)) > time.Hour && o.lastPrune.CompareAndSwap(last, time.Now().UnixNano()) {
		_, _ = o.pool.Exec(ctx, `DELETE FROM deliveries WHERE status <> 'pending' AND done_at < now() - $1::interval`,
			fmt.Sprintf("%d seconds", int64(o.Retention/time.Second)))
	}
	return nil
}

// Failed implements outbox.Store.
func (o *Outbox) Failed(ctx context.Context, kind string, keys []string) error {
	if _, err := o.pool.Exec(ctx, `UPDATE deliveries SET attempts = attempts + 1
		WHERE scope = $1 AND kind = $2 AND key = ANY($3) AND status = 'pending'`, o.scope, kind, keys); err != nil {
		return fmt.Errorf("postgres: outbox failed: %w", err)
	}
	return nil
}
