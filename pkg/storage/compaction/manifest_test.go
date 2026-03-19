package compaction

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestManifestStore_WriteAndLoad(t *testing.T) {
	dir := t.TempDir()
	store, err := NewManifestStore(dir)
	if err != nil {
		t.Fatalf("NewManifestStore: %v", err)
	}

	now := time.Date(2026, 3, 19, 12, 0, 0, 0, time.UTC)
	m := &Manifest{
		ID:          "compact-main-1-123",
		Index:       "main",
		InputIDs:    []string{"seg-1", "seg-2", "seg-3"},
		OutputLevel: L1,
		TrivialMove: false,
		StartedAt:   now,
	}

	if err := store.Write(m); err != nil {
		t.Fatalf("Write: %v", err)
	}

	loaded, err := store.LoadPending()
	if err != nil {
		t.Fatalf("LoadPending: %v", err)
	}

	if len(loaded) != 1 {
		t.Fatalf("LoadPending: got %d manifests, want 1", len(loaded))
	}

	got := loaded[0]
	if got.ID != m.ID {
		t.Errorf("ID: got %q, want %q", got.ID, m.ID)
	}

	if got.Index != m.Index {
		t.Errorf("Index: got %q, want %q", got.Index, m.Index)
	}

	if len(got.InputIDs) != len(m.InputIDs) {
		t.Fatalf("InputIDs len: got %d, want %d", len(got.InputIDs), len(m.InputIDs))
	}

	for i, id := range got.InputIDs {
		if id != m.InputIDs[i] {
			t.Errorf("InputIDs[%d]: got %q, want %q", i, id, m.InputIDs[i])
		}
	}

	if got.OutputLevel != m.OutputLevel {
		t.Errorf("OutputLevel: got %d, want %d", got.OutputLevel, m.OutputLevel)
	}

	if got.TrivialMove != m.TrivialMove {
		t.Errorf("TrivialMove: got %v, want %v", got.TrivialMove, m.TrivialMove)
	}

	if !got.StartedAt.Equal(m.StartedAt) {
		t.Errorf("StartedAt: got %v, want %v", got.StartedAt, m.StartedAt)
	}
}

func TestManifestStore_Remove(t *testing.T) {
	dir := t.TempDir()
	store, err := NewManifestStore(dir)
	if err != nil {
		t.Fatalf("NewManifestStore: %v", err)
	}

	m := &Manifest{
		ID:          "compact-main-1-456",
		Index:       "main",
		InputIDs:    []string{"seg-4", "seg-5"},
		OutputLevel: L1,
		StartedAt:   time.Now(),
	}

	if err := store.Write(m); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := store.Remove(m.ID); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	loaded, err := store.LoadPending()
	if err != nil {
		t.Fatalf("LoadPending: %v", err)
	}

	if len(loaded) != 0 {
		t.Errorf("LoadPending after Remove: got %d manifests, want 0", len(loaded))
	}
}

func TestManifestStore_LoadPending_SkipsCorrupt(t *testing.T) {
	dir := t.TempDir()
	store, err := NewManifestStore(dir)
	if err != nil {
		t.Fatalf("NewManifestStore: %v", err)
	}

	// Write a valid manifest.
	valid := &Manifest{
		ID:          "compact-valid",
		Index:       "main",
		InputIDs:    []string{"seg-1"},
		OutputLevel: L1,
		StartedAt:   time.Now(),
	}

	if err := store.Write(valid); err != nil {
		t.Fatalf("Write valid: %v", err)
	}

	// Write corrupt JSON directly to the pending directory.
	corruptPath := filepath.Join(store.dir, "compact-corrupt.json")
	if err := os.WriteFile(corruptPath, []byte("not valid json{{{"), 0o644); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}

	// Write a .tmp file that should be skipped (non-.json extension).
	tmpPath := filepath.Join(store.dir, "compact-partial.json.tmp")
	if err := os.WriteFile(tmpPath, []byte("partial write"), 0o644); err != nil {
		t.Fatalf("write tmp file: %v", err)
	}

	loaded, err := store.LoadPending()
	if err != nil {
		t.Fatalf("LoadPending: %v", err)
	}

	if len(loaded) != 1 {
		t.Fatalf("LoadPending: got %d manifests, want 1 (should skip corrupt and tmp)", len(loaded))
	}

	if loaded[0].ID != valid.ID {
		t.Errorf("ID: got %q, want %q", loaded[0].ID, valid.ID)
	}
}

func TestManifestStore_CleanupInterrupted(t *testing.T) {
	dir := t.TempDir()
	store, err := NewManifestStore(dir)
	if err != nil {
		t.Fatalf("NewManifestStore: %v", err)
	}

	m1 := &Manifest{
		ID:          "compact-cleanup-1",
		Index:       "main",
		InputIDs:    []string{"seg-10", "seg-11"},
		OutputLevel: L1,
		StartedAt:   time.Now(),
	}
	m2 := &Manifest{
		ID:          "compact-cleanup-2",
		Index:       "logs",
		InputIDs:    []string{"seg-20"},
		OutputLevel: L2,
		TrivialMove: true,
		StartedAt:   time.Now(),
	}

	if err := store.Write(m1); err != nil {
		t.Fatalf("Write m1: %v", err)
	}

	if err := store.Write(m2); err != nil {
		t.Fatalf("Write m2: %v", err)
	}

	// Verify both manifests exist.
	pending, err := store.LoadPending()
	if err != nil {
		t.Fatalf("LoadPending before cleanup: %v", err)
	}

	if len(pending) != 2 {
		t.Fatalf("pending before cleanup: got %d, want 2", len(pending))
	}

	// Run cleanup with a dummy existsFn (not used by current conservative implementation).
	existsFn := func(id string) bool { return false }
	cleaned := store.CleanupInterrupted(pending, existsFn)

	if len(cleaned) != 2 {
		t.Errorf("cleaned count: got %d, want 2", len(cleaned))
	}

	// Verify manifests are removed.
	after, err := store.LoadPending()
	if err != nil {
		t.Fatalf("LoadPending after cleanup: %v", err)
	}

	if len(after) != 0 {
		t.Errorf("pending after cleanup: got %d, want 0", len(after))
	}
}

func TestManifestStore_RemoveNonexistent(t *testing.T) {
	dir := t.TempDir()
	store, err := NewManifestStore(dir)
	if err != nil {
		t.Fatalf("NewManifestStore: %v", err)
	}

	// Removing a manifest that does not exist should not return an error.
	if err := store.Remove("does-not-exist"); err != nil {
		t.Errorf("Remove nonexistent: unexpected error: %v", err)
	}
}
