package cursor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "cursor.json")
	s := NewFileStore(path)

	st, err := s.Load(ctx)
	if err != nil || st.LastBlock != 0 {
		t.Fatalf("missing file should load as zero state, got %+v %v", st, err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	for _, b := range []int64{100, 101, 102} {
		if err := s.Save(ctx, State{LastBlock: b, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	st, err = NewFileStore(path).Load(ctx) // fresh instance = restart
	if err != nil {
		t.Fatal(err)
	}
	if st.LastBlock != 102 || !st.UpdatedAt.Equal(now) {
		t.Fatalf("got %+v", st)
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatalf("temp files left behind: %v", entries)
	}
}

func TestFileStoreCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor.json")
	_ = os.WriteFile(path, []byte("{not json"), 0o644)
	if _, err := NewFileStore(path).Load(context.Background()); err == nil {
		t.Fatal("expected error for corrupt cursor (must not silently restart from head)")
	}
}

func TestNextBlock(t *testing.T) {
	tests := []struct {
		name        string
		st          State
		start, safe int64
		want        int64
	}{
		{"resume after cursor", State{LastBlock: 500}, 0, 1000, 501},
		{"cursor wins over start_block", State{LastBlock: 500}, 10, 1000, 501},
		{"explicit start", State{}, 900, 1000, 900},
		{"relative start", State{}, -100, 1000, 900},
		{"relative start clamps", State{}, -5000, 1000, 1},
		{"default safe head", State{}, 0, 1000, 1000},
	}
	for _, tt := range tests {
		if got := NextBlock(tt.st, tt.start, tt.safe); got != tt.want {
			t.Errorf("%s: got %d, want %d", tt.name, got, tt.want)
		}
	}
}

func TestDedupe(t *testing.T) {
	d := NewDedupe(3)
	if !d.Add("tx1:0") || d.Add("tx1:0") {
		t.Fatal("first add must be fresh, second must be duplicate")
	}
	if !d.Add("tx1:1") {
		t.Fatal("same tx, different log index must be distinct")
	}
	d.Add("tx2:0")
	if !d.Seen("tx1:0") || d.Len() != 3 {
		t.Fatal("expected 3 keys remembered")
	}
	d.Add("tx3:0") // evicts oldest (tx1:0)
	if d.Seen("tx1:0") {
		t.Fatal("oldest key should be evicted")
	}
	if !d.Seen("tx1:1") || !d.Seen("tx3:0") || d.Len() != 3 {
		t.Fatal("recent keys must be kept")
	}
}

func TestDedupeConcurrent(t *testing.T) {
	d := NewDedupe(1000)
	done := make(chan int)
	for g := 0; g < 8; g++ {
		go func() {
			fresh := 0
			for i := 0; i < 500; i++ {
				if d.Add(fmt.Sprintf("tx%d:0", i)) {
					fresh++
				}
			}
			done <- fresh
		}()
	}
	total := 0
	for g := 0; g < 8; g++ {
		total += <-done
	}
	if total != 500 {
		t.Fatalf("each key must be fresh exactly once, got %d", total)
	}
}

func TestMemoryStore(t *testing.T) {
	var m MemoryStore
	ctx := context.Background()
	if st, _ := m.Load(ctx); st.LastBlock != 0 {
		t.Fatal("zero state expected")
	}
	_ = m.Save(ctx, State{LastBlock: 9})
	if st, _ := m.Load(ctx); st.LastBlock != 9 {
		t.Fatalf("got %d", st.LastBlock)
	}
}

func TestFileStoreSaveErrors(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "file")
	_ = os.WriteFile(blocker, []byte("x"), 0o644)
	// Parent "directory" is a regular file -> mkdir must fail.
	if err := NewFileStore(filepath.Join(blocker, "cursor.json")).Save(context.Background(), State{LastBlock: 1}); err == nil {
		t.Fatal("expected error")
	}
	// Target path is a directory -> rename must fail and leave no temp file.
	target := filepath.Join(dir, "asdir")
	_ = os.MkdirAll(filepath.Join(target, "child"), 0o755)
	if err := NewFileStore(target).Save(context.Background(), State{LastBlock: 1}); err == nil {
		t.Fatal("expected error when the cursor path is a directory")
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("temp file left behind: %s", e.Name())
		}
	}
	if _, err := NewFileStore(dir).Load(context.Background()); err == nil {
		t.Fatal("loading a directory should fail")
	}
}
