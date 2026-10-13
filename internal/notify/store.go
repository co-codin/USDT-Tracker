// Package notify implements the self-service Telegram notify bot: users
// /add their own TRON, TON or Ethereum addresses and get a private message
// whenever USDT arrives or leaves. It contains the subscription store
// contract (Postgres and in-memory), the command handler, the long-polling
// loop and the dispatcher that the listeners feed (independent of the
// watch list / filter).
package notify

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// Plans. PlanFree is the default; other plans (e.g. PlanPremium) are set by
// an operator or a future billing integration (UPDATE notify_users SET plan=…).
const (
	PlanFree    = "free"
	PlanPremium = "premium"
)

var (
	// ErrLimit means the user's plan does not allow another address.
	ErrLimit = errors.New("notify: address limit reached")
	// ErrExists means the address is already subscribed by this user.
	ErrExists = errors.New("notify: already subscribed")
	// ErrNotFound means the subscription does not exist.
	ErrNotFound = errors.New("notify: not subscribed")
	// ErrCapacity means the bot-wide user or subscription cap is reached.
	ErrCapacity = errors.New("notify: bot capacity reached")
)

// Limits bound one AddSub call.
type Limits struct {
	// PerPlan returns the max addresses for a plan.
	PerPlan func(plan string) int
	// MaxUsers caps the number of users with at least one /add (0 = no cap).
	MaxUsers int
	// MaxTotalSubs caps subscriptions across all users (0 = no cap).
	MaxTotalSubs int
}

// Sub is one subscribed address.
type Sub struct {
	ChatID    int64
	Chain     string
	Address   string
	CreatedAt time.Time
}

// User is a bot user.
type User struct {
	ChatID  int64
	Plan    string
	Blocked bool
}

// Store persists users and subscriptions.
type Store interface {
	// GetUser returns the user; ok=false when the chat never used /add
	// (read-only: commands other than /add never create rows).
	GetUser(ctx context.Context, chatID int64) (u User, ok bool, err error)
	// AddSub creates the user if needed (plan free, unblocked) and
	// subscribes. Returns ErrLimit / ErrExists / ErrCapacity.
	AddSub(ctx context.Context, chatID int64, username, chain, address string, lim Limits) error
	ListSubs(ctx context.Context, chatID int64) ([]Sub, error)
	RemoveSub(ctx context.Context, chatID int64, chain, address string) error
	// ActiveSubs returns every subscription of users that have not blocked the bot.
	ActiveSubs(ctx context.Context) ([]Sub, error)
	SetBlocked(ctx context.Context, chatID int64, blocked bool) error
	LoadOffset(ctx context.Context) (int64, error)
	SaveOffset(ctx context.Context, next int64) error
}

// MemoryStore is an in-memory Store (tests, demos).
type MemoryStore struct {
	mu     sync.Mutex
	users  map[int64]*User
	subs   []Sub
	offset int64
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore { return &MemoryStore{users: map[int64]*User{}} }

// SetPlan changes a user's plan (operator hook).
func (m *MemoryStore) SetPlan(chatID int64, plan string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if u, ok := m.users[chatID]; ok {
		u.Plan = plan
	}
}

// GetUser implements Store.
func (m *MemoryStore) GetUser(_ context.Context, chatID int64) (User, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if u, ok := m.users[chatID]; ok {
		return *u, true, nil
	}
	return User{ChatID: chatID, Plan: PlanFree}, false, nil
}

// AddSub implements Store.
func (m *MemoryStore) AddSub(_ context.Context, chatID int64, _, chain, address string, lim Limits) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.users[chatID]
	if !ok {
		if lim.MaxUsers > 0 && len(m.users) >= lim.MaxUsers {
			return ErrCapacity
		}
		u = &User{ChatID: chatID, Plan: PlanFree}
		m.users[chatID] = u
	}
	u.Blocked = false
	n := 0
	for _, s := range m.subs {
		if s.ChatID == chatID {
			if s.Chain == chain && s.Address == address {
				return ErrExists
			}
			n++
		}
	}
	if n >= lim.PerPlan(u.Plan) {
		return ErrLimit
	}
	if lim.MaxTotalSubs > 0 && len(m.subs) >= lim.MaxTotalSubs {
		return ErrCapacity
	}
	m.subs = append(m.subs, Sub{ChatID: chatID, Chain: chain, Address: address, CreatedAt: time.Now().UTC()})
	return nil
}

// ListSubs implements Store.
func (m *MemoryStore) ListSubs(_ context.Context, chatID int64) ([]Sub, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Sub
	for _, s := range m.subs {
		if s.ChatID == chatID {
			out = append(out, s)
		}
	}
	return out, nil
}

// RemoveSub implements Store.
func (m *MemoryStore) RemoveSub(_ context.Context, chatID int64, chain, address string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, s := range m.subs {
		if s.ChatID == chatID && s.Chain == chain && s.Address == address {
			m.subs = append(m.subs[:i], m.subs[i+1:]...)
			return nil
		}
	}
	return ErrNotFound
}

// ActiveSubs implements Store.
func (m *MemoryStore) ActiveSubs(context.Context) ([]Sub, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Sub
	for _, s := range m.subs {
		if u := m.users[s.ChatID]; u == nil || !u.Blocked {
			out = append(out, s)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ChatID < out[j].ChatID })
	return out, nil
}

// SetBlocked implements Store.
func (m *MemoryStore) SetBlocked(_ context.Context, chatID int64, blocked bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if u, ok := m.users[chatID]; ok {
		u.Blocked = blocked
	}
	return nil
}

// LoadOffset implements Store.
func (m *MemoryStore) LoadOffset(context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.offset, nil
}

// SaveOffset implements Store.
func (m *MemoryStore) SaveOffset(_ context.Context, next int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.offset = next
	return nil
}
