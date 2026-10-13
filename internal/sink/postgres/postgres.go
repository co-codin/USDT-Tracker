// Package postgres stores transfers in PostgreSQL (pgx) and doubles as the
// cursor store when enabled, so progress and data live in the same database.
package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
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
	// Poll pg_try_advisory_lock instead of blocking in pg_advisory_lock: a
	// session blocked inside a statement holds a snapshot, and CREATE INDEX
	// CONCURRENTLY run by the lock holder waits for every older snapshot —
	// a deadlock when two instances migrate at once.
	for {
		var ok bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, migrationLockID).Scan(&ok); err != nil {
			return fmt.Errorf("postgres: advisory lock: %w", err)
		}
		if ok {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("postgres: advisory lock: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
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
		if strings.HasPrefix(string(sqlText), noTxMarker) {
			err = applyNoTx(ctx, conn, string(sqlText), version)
		} else {
			err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, string(sqlText)); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES ($1)`, version)
				return err
			})
		}
		if err != nil {
			return fmt.Errorf("postgres: apply migration %s: %w", version, err)
		}
	}
	return nil
}

// noTxMarker on the first line of a migration runs its statements one by one
// outside a transaction (needed for CREATE INDEX CONCURRENTLY). Every
// statement must be idempotent (IF NOT EXISTS) because a crash can leave the
// migration half applied.
const noTxMarker = "-- migrate:no-transaction"

var indexNameRE = regexp.MustCompile(`(?i)CREATE\s+(?:UNIQUE\s+)?INDEX\s+CONCURRENTLY\s+IF\s+NOT\s+EXISTS\s+(\w+)`)

func applyNoTx(ctx context.Context, conn *pgxpool.Conn, sqlText, version string) error {
	// An interrupted CONCURRENTLY build leaves an INVALID index that
	// IF NOT EXISTS would silently keep: drop those first.
	for _, m := range indexNameRE.FindAllStringSubmatch(sqlText, -1) {
		var invalid bool
		err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			WHERE c.relname = $1 AND c.relnamespace = current_schema()::regnamespace AND NOT i.indisvalid)`, m[1]).Scan(&invalid)
		if err != nil {
			return err
		}
		if invalid {
			if _, err := conn.Exec(ctx, `DROP INDEX CONCURRENTLY IF EXISTS `+pgx.Identifier{m[1]}.Sanitize()); err != nil {
				return err
			}
		}
	}
	for _, stmt := range splitStatements(sqlText) {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	_, err := conn.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES ($1)`, version)
	return err
}

// splitStatements splits a simple migration file on ';' at line ends,
// dropping comment-only chunks (no dollar quoting in no-tx migrations).
func splitStatements(sqlText string) []string {
	var out []string
	for _, chunk := range strings.Split(sqlText, ";\n") {
		var lines []string
		for _, l := range strings.Split(chunk, "\n") {
			if t := strings.TrimSpace(l); t != "" && !strings.HasPrefix(t, "--") {
				lines = append(lines, l)
			}
		}
		if st := strings.TrimSuffix(strings.TrimSpace(strings.Join(lines, "\n")), ";"); st != "" {
			out = append(out, st)
		}
	}
	return out
}

// Name implements sink.Sink.
func (s *Store) Name() string { return "postgres" }

const insertSQL = `INSERT INTO trc20_transfers
	(tx_id, log_index, block_number, block_time, contract, symbol, from_address, to_address, amount_raw, amount, reasons, chain, memo)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::numeric, $10::numeric, $11, $12, $13)
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
				t.From, t.To, t.Amount.String(), t.AmountDecimal(), reasons, t.ChainName(), nullable(t.Memo))
		}
		return tx.SendBatch(ctx, batch).Close()
	})
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// Cursor returns a cursor.Store for another named cursor in the same
// database (e.g. one per chain).
func (s *Store) Cursor(name string) cursor.Store { return namedCursor{s: s, name: name} }

type namedCursor struct {
	s    *Store
	name string
}

func (n namedCursor) Load(ctx context.Context) (cursor.State, error) { return n.s.load(ctx, n.name) }
func (n namedCursor) Save(ctx context.Context, st cursor.State) error {
	return n.s.save(ctx, n.name, st)
}

// Load implements cursor.Store.
func (s *Store) Load(ctx context.Context) (cursor.State, error) { return s.load(ctx, s.cursorName) }

// Save implements cursor.Store.
func (s *Store) Save(ctx context.Context, st cursor.State) error {
	return s.save(ctx, s.cursorName, st)
}

// CursorName returns the default cursor name.
func (s *Store) CursorName() string { return s.cursorName }

func (s *Store) load(ctx context.Context, name string) (cursor.State, error) {
	var st cursor.State
	err := s.pool.QueryRow(ctx, `SELECT last_block, updated_at FROM listener_cursor WHERE name=$1`, name).
		Scan(&st.LastBlock, &st.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return cursor.State{}, nil
	}
	if err != nil {
		return cursor.State{}, fmt.Errorf("postgres: load cursor: %w", err)
	}
	return st, nil
}

func (s *Store) save(ctx context.Context, name string, st cursor.State) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO listener_cursor(name, last_block, updated_at) VALUES ($1, $2, $3)
		ON CONFLICT (name) DO UPDATE SET last_block = EXCLUDED.last_block, updated_at = EXCLUDED.updated_at`,
		name, st.LastBlock, st.UpdatedAt)
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
