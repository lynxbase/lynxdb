package colstore

import (
	"path/filepath"
	"testing"
	"time"
)

// TestWrittenBatchesAreFilledAgain requires a store to keep the batches of
// the buffers it wrote for the buffers to fill again, one per buffer at
// most, and to give them up once nothing has been appended for a while.
func TestWrittenBatchesAreFilledAgain(t *testing.T) {
	s, err := OpenStore(filepath.Join(t.TempDir(), "store"), StoreOptions{MergeFanIn: -1, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	spare := func() int {
		s.spare.mu.Lock()
		defer s.spare.mu.Unlock()

		return len(s.spare.held)
	}
	for round := 0; round < 3*len(s.shards); round++ {
		if _, err := s.Append(storeDocs(round*100, 100)); err != nil {
			t.Fatal(err)
		}
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if n := spare(); n != 1 {
			t.Fatalf("round %d: %d spare batches after one buffer was written", round, n)
		}
	}
	s.dropSpare()
	if n := spare(); n != 1 {
		t.Fatalf("%d spare batches right after an append", n)
	}
	s.spare.mu.Lock()
	s.spare.used = time.Now().Add(-2 * spareIdle)
	s.spare.mu.Unlock()
	s.dropSpare()
	if n := spare(); n != 0 {
		t.Fatalf("%d spare batches in a store that has been idle", n)
	}
	if _, err := s.Append(storeDocs(100000, 100)); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	snap := s.Table().Snapshot()
	defer snap.Release()
	var rows uint64
	for _, h := range snap.Parts() {
		rows += h.Rows
	}
	if want := uint64(3*len(s.shards)*100 + 100); rows != want {
		t.Fatalf("%d rows, want %d", rows, want)
	}
}
