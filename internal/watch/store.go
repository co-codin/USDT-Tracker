// Package watch manages the runtime watch list: deposit addresses that
// payment gateways add and remove without a restart. It contains the entry
// type and validation, the Store/Source interfaces (implemented by
// sink/postgres and MemoryStore) and the admin HTTP API (/v1/addresses).
package watch

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/co-codin/USDT-Tracker/internal/chain/tron"
)

// Directions accepted for a watched address (same values as filter.direction).
const (
	DirectionBoth     = "both"
	DirectionIncoming = "incoming"
	DirectionOutgoing = "outgoing"
)

// MaxLabelLen is the maximum label length in characters.
const MaxLabelLen = 256

var (
	// ErrExists is returned by Store.Add when the address is already stored.
	ErrExists = errors.New("watch address already exists")
	// ErrNotFound is returned by Store.Delete when the address is not stored.
	ErrNotFound = errors.New("watch address not found")
)

// Entry is one runtime watch address.
type Entry struct {
	Address   string    `json:"address"` // canonical base58 ("T…")
	Label     *string   `json:"label"`
	Direction string    `json:"direction"` // both|incoming|outgoing
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
}

// Source supplies the enabled watch addresses; the filter polls it.
type Source interface {
	EnabledWatches(ctx context.Context) ([]Entry, error)
}

// Store is the read/write watch-list storage used by the admin API.
type Store interface {
	Source
	// ListWatches returns every stored address (enabled or not), oldest first.
	ListWatches(ctx context.Context) ([]Entry, error)
	// AddWatch inserts e (already validated) and returns the stored row.
	// It returns ErrExists if the address is already present.
	AddWatch(ctx context.Context, e Entry) (Entry, error)
	// DeleteWatch removes address; ErrNotFound if it is not stored.
	DeleteWatch(ctx context.Context, address string) error
}

// NormalizeAddress validates a base58check TRON address ("T…", 34 chars,
// correct checksum) and returns its canonical form. Hex addresses are
// rejected so the stored value is always the form users see on explorers.
func NormalizeAddress(s string) (string, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "T") || len(s) != 34 {
		return "", fmt.Errorf("%w: want a base58 address starting with T (34 chars)", tron.ErrInvalidAddress)
	}
	a, err := tron.AddressFromBase58(s)
	if err != nil {
		return "", err
	}
	if a.String() != s {
		return "", fmt.Errorf("%w: non-canonical base58", tron.ErrInvalidAddress)
	}
	return s, nil
}

// NormalizeDirection validates d; empty means DirectionBoth.
func NormalizeDirection(d string) (string, error) {
	switch d = strings.ToLower(strings.TrimSpace(d)); d {
	case "":
		return DirectionBoth, nil
	case DirectionBoth, DirectionIncoming, DirectionOutgoing:
		return d, nil
	default:
		return "", fmt.Errorf("unknown direction %q (want both|incoming|outgoing)", d)
	}
}

// NormalizeLabel trims l; an empty label becomes nil.
func NormalizeLabel(l *string) (*string, error) {
	if l == nil {
		return nil, nil
	}
	s := strings.TrimSpace(*l)
	if s == "" {
		return nil, nil
	}
	if n := len([]rune(s)); n > MaxLabelLen {
		return nil, fmt.Errorf("label is %d characters, max %d", n, MaxLabelLen)
	}
	return &s, nil
}

// MemoryStore is a goroutine-safe in-memory Store.
type MemoryStore struct {
	mu   sync.Mutex
	rows map[string]Entry
	now  func() time.Time
	// Err, when set, is returned by every method (fault injection in tests).
	Err error
}

var _ Store = (*MemoryStore)(nil)

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{rows: map[string]Entry{}, now: func() time.Time { return time.Now().UTC() }}
}

// EnabledWatches implements Source.
func (m *MemoryStore) EnabledWatches(ctx context.Context) ([]Entry, error) {
	all, err := m.ListWatches(ctx)
	if err != nil {
		return nil, err
	}
	out := all[:0]
	for _, e := range all {
		if e.Enabled {
			out = append(out, e)
		}
	}
	return out, nil
}

// ListWatches implements Store.
func (m *MemoryStore) ListWatches(context.Context) ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return nil, m.Err
	}
	out := make([]Entry, 0, len(m.rows))
	for _, e := range m.rows {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].Address < out[j].Address
	})
	return out, nil
}

// AddWatch implements Store.
func (m *MemoryStore) AddWatch(_ context.Context, e Entry) (Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return Entry{}, m.Err
	}
	if _, ok := m.rows[e.Address]; ok {
		return Entry{}, ErrExists
	}
	if e.Direction == "" {
		e.Direction = DirectionBoth
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = m.now()
	}
	m.rows[e.Address] = e
	return e, nil
}

// DeleteWatch implements Store.
func (m *MemoryStore) DeleteWatch(_ context.Context, address string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	if _, ok := m.rows[address]; !ok {
		return ErrNotFound
	}
	delete(m.rows, address)
	return nil
}

// SetEnabled toggles an entry (test helper; the HTTP API has no toggle yet).
func (m *MemoryStore) SetEnabled(address string, enabled bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.rows[address]
	if !ok {
		return ErrNotFound
	}
	e.Enabled = enabled
	m.rows[address] = e
	return nil
}
