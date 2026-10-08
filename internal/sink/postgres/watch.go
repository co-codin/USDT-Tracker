package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/co-codin/USDT-Tracker/internal/watch"
)

// WatchStore implements watch.Store on the watch_addresses table
// (migration 0002). It shares the sink's connection pool.
type WatchStore struct {
	pool *pgxpool.Pool
}

var _ watch.Store = (*WatchStore)(nil)

// Watchlist returns the runtime watch-list store backed by this database.
func (s *Store) Watchlist() *WatchStore { return &WatchStore{pool: s.pool} }

const watchCols = `address, label, direction, enabled, created_at`

func scanWatches(rows pgx.Rows) ([]watch.Entry, error) {
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (watch.Entry, error) {
		var e watch.Entry
		err := r.Scan(&e.Address, &e.Label, &e.Direction, &e.Enabled, &e.CreatedAt)
		e.CreatedAt = e.CreatedAt.UTC()
		return e, err
	})
	if err != nil {
		return nil, fmt.Errorf("postgres: read watch_addresses: %w", err)
	}
	return out, nil
}

// ListWatches implements watch.Store.
func (w *WatchStore) ListWatches(ctx context.Context) ([]watch.Entry, error) {
	rows, err := w.pool.Query(ctx, `SELECT `+watchCols+` FROM watch_addresses ORDER BY created_at, address`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list watch_addresses: %w", err)
	}
	return scanWatches(rows)
}

// EnabledWatches implements watch.Source.
func (w *WatchStore) EnabledWatches(ctx context.Context) ([]watch.Entry, error) {
	rows, err := w.pool.Query(ctx, `SELECT `+watchCols+` FROM watch_addresses WHERE enabled ORDER BY created_at, address`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list enabled watch_addresses: %w", err)
	}
	return scanWatches(rows)
}

// AddWatch implements watch.Store. Direction defaults to "both".
func (w *WatchStore) AddWatch(ctx context.Context, e watch.Entry) (watch.Entry, error) {
	if e.Direction == "" {
		e.Direction = watch.DirectionBoth
	}
	rows, err := w.pool.Query(ctx, `INSERT INTO watch_addresses (address, label, direction, enabled)
		VALUES ($1, $2, $3, $4) ON CONFLICT (address) DO NOTHING RETURNING `+watchCols,
		e.Address, e.Label, e.Direction, e.Enabled)
	if err != nil {
		return watch.Entry{}, fmt.Errorf("postgres: insert watch address: %w", err)
	}
	out, err := scanWatches(rows)
	if err != nil {
		return watch.Entry{}, err
	}
	if len(out) == 0 {
		return watch.Entry{}, watch.ErrExists
	}
	return out[0], nil
}

// DeleteWatch implements watch.Store.
func (w *WatchStore) DeleteWatch(ctx context.Context, address string) error {
	tag, err := w.pool.Exec(ctx, `DELETE FROM watch_addresses WHERE address = $1`, address)
	if err != nil {
		return fmt.Errorf("postgres: delete watch address: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return watch.ErrNotFound
	}
	return nil
}
