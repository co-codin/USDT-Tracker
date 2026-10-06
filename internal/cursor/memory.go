package cursor

import (
	"context"
	"sync"
)

// MemoryStore is an in-memory Store (used for backfills and tests).
type MemoryStore struct {
	mu sync.Mutex
	st State
}

// Load returns the current state.
func (m *MemoryStore) Load(context.Context) (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.st, nil
}

// Save stores the state.
func (m *MemoryStore) Save(_ context.Context, st State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.st = st
	return nil
}
