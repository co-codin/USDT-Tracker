// Package postgres stores transfers in PostgreSQL (pgx) and doubles as the
// cursor store when enabled, so progress and data live in the same database.
package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/co-codin/USDT-Tracker/internal/cursor"
	"github.com/co-codin/USDT-Tracker/internal/sink"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrationLockID is an arbitrary constant for pg_advisory_lock.
const migrationLockID = 727_384_001

// Store implements sink.Sink and cursor.Store.
type Store struct {
	pool       *pgxpool.Pool
	cursorName string
	closeOnce  sync.Once
}

var (
	_ sink.Sink    = (*Store)(nil)
	_ cursor.Store = (*Store)(nil)
)

// Open connects, pings and applies pending migrations.
func Open(ctx context.Context, dsn, cursorName string) (*Store, error) {
	if dsn == "" {
		return nil, fmt.Errorf("postgres: dsn is required")
	}
	if cursorName == "" {
		cursorName = "default"
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	s := &Store{pool: pool, cursorName: cursorName}
	if err := s.Migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Migrate applies embedded SQL migrations exactly once, in lexical order,
// guarded by an advisory lock so concurrent instances don't race.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("postgres: acquire: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("postgres: advisory lock: %w", err)
	}
	defer conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrationLockID) //nolint:errcheck

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("postgres: schema_migrations: %w", err)
	}
	files, err := fs.Glob(migrationsFS, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, f := range files {
		version := strings.TrimSuffix(strings.TrimPrefix(f, "migrations/"), ".sql")
		var exists bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, version).Scan(&exists); err != nil {
			return fmt.Errorf("postgres: check migration %s: %w", version, err)
		}
		if exists {
			continue
		}
		sqlText, err := migrationsFS.ReadFile(f)
		if err != nil {
			return err
		}
		err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, string(sqlText)); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES ($1)`, version)
			return err
		})
		if err != nil {
			return fmt.Errorf("postgres: apply migration %s: %w", version, err)
		}
	}
	return nil
}

// Name implements sink.Sink.
func (s *Store) Name() string { return "postgres" }

const insertSQL = `INSERT INTO trc20_transfers
	(tx_id, log_index, block_number, block_time, contract, symbol, from_address, to_address, amount_raw, amount, reasons)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::numeric, $10::numeric, $11)
	ON CONFLICT (tx_id, log_index) DO NOTHING`

// Send inserts the batch in one transaction; duplicates are ignored.
func (s *Store) Send(ctx context.Context, b sink.Batch) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		batch := &pgx.Batch{}
		for _, t := range b.Transfers {
			reasons := t.Reasons
			if reasons == nil {
				reasons = []string{}
			}
			batch.Queue(insertSQL, t.TxID, t.LogIndex, t.BlockNumber, t.BlockTime, t.Contract, t.Symbol,
				t.From, t.To, t.Amount.String(), t.AmountDecimal(), reasons)
		}
		return tx.SendBatch(ctx, batch).Close()
	})
}

// Load implements cursor.Store.
func (s *Store) Load(ctx context.Context) (cursor.State, error) {
	var st cursor.State
	err := s.pool.QueryRow(ctx, `SELECT last_block, updated_at FROM listener_cursor WHERE name=$1`, s.cursorName).
		Scan(&st.LastBlock, &st.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return cursor.State{}, nil
	}
	if err != nil {
		return cursor.State{}, fmt.Errorf("postgres: load cursor: %w", err)
	}
	return st, nil
}

// Save implements cursor.Store.
func (s *Store) Save(ctx context.Context, st cursor.State) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO listener_cursor(name, last_block, updated_at) VALUES ($1, $2, $3)
		ON CONFLICT (name) DO UPDATE SET last_block = EXCLUDED.last_block, updated_at = EXCLUDED.updated_at`,
		s.cursorName, st.LastBlock, st.UpdatedAt)
	if err != nil {
		return fmt.Errorf("postgres: save cursor: %w", err)
	}
	return nil
}

// Ping checks connectivity (used by /healthz).
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Close implements sink.Sink; safe to call more than once.
func (s *Store) Close() error {
	s.closeOnce.Do(s.pool.Close)
	return nil
}
