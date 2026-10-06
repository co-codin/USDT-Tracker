// Package cursor tracks processing progress and deduplicates events.
//
// The cursor stores the highest block that was fully processed. On restart the
// listener resumes at LastBlock+1, so no block is skipped. Dedupe is an
// in-memory set of recently emitted transfer keys (tx id + log index) that
// protects against reprocessing the same block (e.g. a crash between emitting
// and persisting the cursor, or a manual rewind).
package cursor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// State is the persisted progress.
type State struct {
	LastBlock int64     `json:"last_block"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Store persists and loads the cursor state.
type Store interface {
	Load(ctx context.Context) (State, error)
	Save(ctx context.Context, st State) error
}

// FileStore persists the cursor as a JSON file, written atomically.
type FileStore struct{ path string }

// NewFileStore returns a file-backed store.
func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

// Load reads the cursor. A missing file yields a zero state.
func (f *FileStore) Load(_ context.Context) (State, error) {
	b, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return State{}, nil
	}
	if err != nil {
		return State{}, fmt.Errorf("cursor: read %s: %w", f.path, err)
	}
	var st State
	if err := json.Unmarshal(b, &st); err != nil {
		return State{}, fmt.Errorf("cursor: parse %s: %w", f.path, err)
	}
	return st, nil
}

// Save writes the cursor atomically (temp file + rename).
func (f *FileStore) Save(_ context.Context, st State) error {
	if err := os.MkdirAll(filepath.Dir(f.path), 0o750); err != nil {
		return fmt.Errorf("cursor: mkdir: %w", err)
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp, err := os.CreateTemp(filepath.Dir(f.path), ".cursor-*.tmp")
	if err != nil {
		return fmt.Errorf("cursor: temp file: %w", err)
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("cursor: write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("cursor: sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		return fmt.Errorf("cursor: rename: %w", err)
	}
	ok = true
	return nil
}

// NextBlock decides where processing starts.
//
//   - If a cursor was persisted, resume right after it (no gaps).
//   - Else if startBlock > 0, start exactly there.
//   - Else if startBlock < 0, start |startBlock| blocks behind the safe head.
//   - Else (0) start at the safe head (only new blocks).
func NextBlock(st State, startBlock, safeHead int64) int64 {
	switch {
	case st.LastBlock > 0:
		return st.LastBlock + 1
	case startBlock > 0:
		return startBlock
	case startBlock < 0:
		n := safeHead + startBlock
		if n < 1 {
			n = 1
		}
		return n
	default:
		return safeHead
	}
}

// Dedupe remembers the most recent N keys (FIFO eviction, O(1) per op).
// Keys are "<tx_id>:<log_index>" (see model.Transfer.Key).
type Dedupe struct {
	mu   sync.Mutex
	ring []string
	pos  int
	seen map[string]struct{}
}

// NewDedupe creates a dedupe set that keeps the most recent maxSize keys.
func NewDedupe(maxSize int) *Dedupe {
	if maxSize <= 0 {
		maxSize = 100_000
	}
	return &Dedupe{ring: make([]string, maxSize), seen: make(map[string]struct{}, maxSize)}
}

// Seen reports whether key was already recorded.
func (d *Dedupe) Seen(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.seen[key]
	return ok
}

// Add records key and reports whether it was new. When the set is full the
// oldest key is evicted.
func (d *Dedupe) Add(key string) (fresh bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[key]; ok {
		return false
	}
	if old := d.ring[d.pos]; old != "" {
		delete(d.seen, old)
	}
	d.ring[d.pos] = key
	d.pos = (d.pos + 1) % len(d.ring)
	d.seen[key] = struct{}{}
	return true
}

// Len returns the number of remembered keys.
func (d *Dedupe) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}
