// Package outbox is the durable delivery queue behind the listener taps
// (notify bot DMs, large-transfer channel alerts). A tap enqueues before the
// block's cursor is saved, keyed so that re-processing a block (retry or
// restart) never enqueues twice; senders mark items done only after the
// message went out. Together this gives at-least-once delivery with
// persistent dedupe: a restart neither drops nor duplicates queued
// messages. The one remaining duplicate window is a crash between a
// successful send and Done.
package outbox

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Kinds used by the application.
const (
	KindNotify = "notify"
	KindAlert  = "alert"
)

// Final statuses passed to Done.
const (
	StatusSent    = "sent"
	StatusFailed  = "failed"  // gave up after retries
	StatusDropped = "dropped" // e.g. user blocked the bot
)

// Item is one queued message (or message part).
type Item struct {
	Key      string // dedupe key, unique per kind
	ChatID   int64  // grouping hint (0 when unused)
	Payload  []byte // JSON
	Attempts int
	Created  time.Time
}

// Store is a durable queue. Implementations must be safe for concurrent use.
type Store interface {
	// Enqueue inserts items whose key is new for kind and returns how many
	// were added (existing keys, pending or done, are ignored).
	Enqueue(ctx context.Context, kind string, items []Item) (int, error)
	// Pending returns up to limit undelivered items, oldest first.
	Pending(ctx context.Context, kind string, limit int) ([]Item, error)
	// Count returns the number of undelivered items.
	Count(ctx context.Context, kind string) (int, error)
	// Done finalises items (status sent|failed|dropped).
	Done(ctx context.Context, kind string, keys []string, status string) error
	// Failed records a failed attempt (items stay pending).
	Failed(ctx context.Context, kind string, keys []string) error
}

// Memory is an in-process Store (no durability). MaxPending > 0 bounds the
// queue: the oldest pending items are dropped (best-effort mode).
type Memory struct {
	MaxPending int
	// MaxKeys bounds the remembered done keys (default 200k).
	MaxKeys int

	mu      sync.Mutex
	seq     int64
	pending map[string]map[string]*memItem
	done    map[string]map[string]struct{}
	order   map[string][]string // done keys FIFO for eviction
	dropped int64
}

type memItem struct {
	Item
	seq int64
}

// NewMemory returns an empty in-memory store.
func NewMemory(maxPending int) *Memory {
	return &Memory{MaxPending: maxPending, pending: map[string]map[string]*memItem{}, done: map[string]map[string]struct{}{}, order: map[string][]string{}}
}

// Dropped returns how many pending items were evicted by MaxPending.
func (m *Memory) Dropped() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dropped
}

// Enqueue implements Store.
func (m *Memory) Enqueue(_ context.Context, kind string, items []Item) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.pending[kind]
	if p == nil {
		p = map[string]*memItem{}
		m.pending[kind] = p
	}
	added := 0
	for _, it := range items {
		if _, ok := p[it.Key]; ok {
			continue
		}
		if _, ok := m.done[kind][it.Key]; ok {
			continue
		}
		m.seq++
		if it.Created.IsZero() {
			it.Created = time.Now()
		}
		p[it.Key] = &memItem{Item: it, seq: m.seq}
		added++
	}
	if m.MaxPending > 0 && len(p) > m.MaxPending {
		all := m.sorted(kind)
		for _, it := range all[:len(all)-m.MaxPending] {
			delete(p, it.Key)
			m.markDone(kind, it.Key)
			m.dropped++
		}
	}
	return added, nil
}

func (m *Memory) sorted(kind string) []*memItem {
	out := make([]*memItem, 0, len(m.pending[kind]))
	for _, it := range m.pending[kind] {
		out = append(out, it)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	return out
}

// Pending implements Store.
func (m *Memory) Pending(_ context.Context, kind string, limit int) ([]Item, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	all := m.sorted(kind)
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	out := make([]Item, len(all))
	for i, it := range all {
		out[i] = it.Item
	}
	return out, nil
}

// Count implements Store.
func (m *Memory) Count(_ context.Context, kind string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pending[kind]), nil
}

// Done implements Store.
func (m *Memory) Done(_ context.Context, kind string, keys []string, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		if _, ok := m.pending[kind][k]; ok {
			delete(m.pending[kind], k)
			m.markDone(kind, k)
		}
	}
	return nil
}

func (m *Memory) markDone(kind, key string) {
	d := m.done[kind]
	if d == nil {
		d = map[string]struct{}{}
		m.done[kind] = d
	}
	d[key] = struct{}{}
	m.order[kind] = append(m.order[kind], key)
	maxKeys := m.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 200_000
	}
	if over := len(m.order[kind]) - maxKeys; over > 0 {
		for _, k := range m.order[kind][:over] {
			delete(d, k)
		}
		m.order[kind] = append([]string(nil), m.order[kind][over:]...)
	}
}

// Failed implements Store.
func (m *Memory) Failed(_ context.Context, kind string, keys []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range keys {
		if it, ok := m.pending[kind][k]; ok {
			it.Attempts++
		}
	}
	return nil
}
