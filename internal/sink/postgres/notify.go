package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/co-codin/USDT-Tracker/internal/notify"
)

// NotifyStore implements notify.Store on notify_users / notify_subscriptions
// (migration 0006). It shares the sink's connection pool.
type NotifyStore struct {
	pool *pgxpool.Pool
	name string
}

var _ notify.Store = (*NotifyStore)(nil)

// Notify returns the notify-bot store backed by this database.
func (s *Store) Notify() *NotifyStore {
	return &NotifyStore{pool: s.pool, name: "notify:" + s.cursorName}
}

// EnsureUser implements notify.Store.
func (n *NotifyStore) EnsureUser(ctx context.Context, chatID int64, username string) (notify.User, error) {
	u := notify.User{ChatID: chatID}
	err := n.pool.QueryRow(ctx, `INSERT INTO notify_users (chat_id, username) VALUES ($1, NULLIF($2, ''))
		ON CONFLICT (chat_id) DO UPDATE SET blocked = FALSE, username = COALESCE(EXCLUDED.username, notify_users.username)
		RETURNING plan, blocked`, chatID, username).Scan(&u.Plan, &u.Blocked)
	if err != nil {
		return u, fmt.Errorf("postgres: notify user: %w", err)
	}
	return u, nil
}

// SetPlan changes a user's plan (operator / billing hook).
func (n *NotifyStore) SetPlan(ctx context.Context, chatID int64, plan string) error {
	_, err := n.pool.Exec(ctx, `UPDATE notify_users SET plan = $2 WHERE chat_id = $1`, chatID, plan)
	return err
}

// AddSub implements notify.Store. The user row is locked so concurrent
// /add commands cannot exceed the plan limit.
func (n *NotifyStore) AddSub(ctx context.Context, chatID int64, chain, address string, limit func(string) int) error {
	return pgx.BeginFunc(ctx, n.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO notify_users (chat_id) VALUES ($1) ON CONFLICT DO NOTHING`, chatID); err != nil {
			return err
		}
		var plan string
		var cnt int
		if err := tx.QueryRow(ctx, `SELECT plan FROM notify_users WHERE chat_id = $1 FOR UPDATE`, chatID).Scan(&plan); err != nil {
			return err
		}
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT count(*), bool_or(chain = $2 AND address = $3) IS TRUE FROM notify_subscriptions WHERE chat_id = $1`,
			chatID, chain, address).Scan(&cnt, &exists); err != nil {
			return err
		}
		if exists {
			return notify.ErrExists
		}
		if cnt >= limit(plan) {
			return notify.ErrLimit
		}
		_, err := tx.Exec(ctx, `INSERT INTO notify_subscriptions (chat_id, chain, address) VALUES ($1, $2, $3)`, chatID, chain, address)
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "23505" {
			return notify.ErrExists
		}
		return err
	})
}

func scanSubs(rows pgx.Rows) ([]notify.Sub, error) {
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (notify.Sub, error) {
		var s notify.Sub
		err := r.Scan(&s.ChatID, &s.Chain, &s.Address, &s.CreatedAt)
		s.CreatedAt = s.CreatedAt.UTC()
		return s, err
	})
}

// ListSubs implements notify.Store.
func (n *NotifyStore) ListSubs(ctx context.Context, chatID int64) ([]notify.Sub, error) {
	rows, err := n.pool.Query(ctx, `SELECT chat_id, chain, address, created_at FROM notify_subscriptions
		WHERE chat_id = $1 ORDER BY created_at, chain, address`, chatID)
	if err != nil {
		return nil, err
	}
	return scanSubs(rows)
}

// RemoveSub implements notify.Store.
func (n *NotifyStore) RemoveSub(ctx context.Context, chatID int64, chain, address string) error {
	tag, err := n.pool.Exec(ctx, `DELETE FROM notify_subscriptions WHERE chat_id = $1 AND chain = $2 AND address = $3`, chatID, chain, address)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return notify.ErrNotFound
	}
	return nil
}

// ActiveSubs implements notify.Store.
func (n *NotifyStore) ActiveSubs(ctx context.Context) ([]notify.Sub, error) {
	rows, err := n.pool.Query(ctx, `SELECT s.chat_id, s.chain, s.address, s.created_at
		FROM notify_subscriptions s JOIN notify_users u USING (chat_id) WHERE NOT u.blocked`)
	if err != nil {
		return nil, err
	}
	return scanSubs(rows)
}

// SetBlocked implements notify.Store.
func (n *NotifyStore) SetBlocked(ctx context.Context, chatID int64, blocked bool) error {
	_, err := n.pool.Exec(ctx, `UPDATE notify_users SET blocked = $2 WHERE chat_id = $1`, chatID, blocked)
	return err
}

// LoadOffset implements notify.Store.
func (n *NotifyStore) LoadOffset(ctx context.Context) (int64, error) {
	var off int64
	err := n.pool.QueryRow(ctx, `SELECT next_offset FROM notify_bot_state WHERE name = $1`, n.name).Scan(&off)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return off, err
}

// SaveOffset implements notify.Store.
func (n *NotifyStore) SaveOffset(ctx context.Context, next int64) error {
	_, err := n.pool.Exec(ctx, `INSERT INTO notify_bot_state (name, next_offset) VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET next_offset = EXCLUDED.next_offset`, n.name, next)
	return err
}
