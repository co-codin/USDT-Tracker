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

// GetUser implements notify.Store.
func (n *NotifyStore) GetUser(ctx context.Context, chatID int64) (notify.User, bool, error) {
	u := notify.User{ChatID: chatID, Plan: notify.PlanFree}
	err := n.pool.QueryRow(ctx, `SELECT plan, blocked FROM notify_users WHERE chat_id = $1`, chatID).Scan(&u.Plan, &u.Blocked)
	if errors.Is(err, pgx.ErrNoRows) {
		return u, false, nil
	}
	if err != nil {
		return u, false, fmt.Errorf("postgres: notify user: %w", err)
	}
	return u, true, nil
}

// SetPlan changes a user's plan (operator / billing hook).
func (n *NotifyStore) SetPlan(ctx context.Context, chatID int64, plan string) error {
	_, err := n.pool.Exec(ctx, `UPDATE notify_users SET plan = $2 WHERE chat_id = $1`, chatID, plan)
	return err
}

// AddSub implements notify.Store. The user row is locked so concurrent
// /add commands cannot exceed the plan limit; bot-wide caps are checked
// under a transaction-scoped advisory lock.
func (n *NotifyStore) AddSub(ctx context.Context, chatID int64, username, chain, address string, lim notify.Limits) error {
	return pgx.BeginFunc(ctx, n.pool, func(tx pgx.Tx) error {
		if lim.MaxUsers > 0 || lim.MaxTotalSubs > 0 {
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('usdt-tracker:notify-capacity'))`); err != nil {
				return err
			}
		}
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM notify_users WHERE chat_id = $1)`, chatID).Scan(&exists); err != nil {
			return err
		}
		if !exists && lim.MaxUsers > 0 {
			var users int
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM notify_users`).Scan(&users); err != nil {
				return err
			}
			if users >= lim.MaxUsers {
				return notify.ErrCapacity
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO notify_users (chat_id, username) VALUES ($1, NULLIF($2, ''))
			ON CONFLICT (chat_id) DO UPDATE SET blocked = FALSE, username = COALESCE(EXCLUDED.username, notify_users.username)`,
			chatID, username); err != nil {
			return err
		}
		var plan string
		var cnt int
		if err := tx.QueryRow(ctx, `SELECT plan FROM notify_users WHERE chat_id = $1 FOR UPDATE`, chatID).Scan(&plan); err != nil {
			return err
		}
		var dup bool
		if err := tx.QueryRow(ctx, `SELECT count(*), bool_or(chain = $2 AND address = $3) IS TRUE FROM notify_subscriptions WHERE chat_id = $1`,
			chatID, chain, address).Scan(&cnt, &dup); err != nil {
			return err
		}
		if dup {
			return notify.ErrExists
		}
		if cnt >= lim.PerPlan(plan) {
			return notify.ErrLimit
		}
		if lim.MaxTotalSubs > 0 {
			var total int
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM notify_subscriptions`).Scan(&total); err != nil {
				return err
			}
			if total >= lim.MaxTotalSubs {
				return notify.ErrCapacity
			}
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
