package colstore

import (
	"path/filepath"
	"sync"
	"testing"
)

// TestSyncWithNothingToWriteStartsNoFlush requires a Sync that finds every
// appended row written to return without a flush, and one that follows an
// Append to flush, whoever else calls at the same time.
func TestSyncWithNothingToWriteStartsNoFlush(t *testing.T) {
	s, err := OpenStore(filepath.Join(t.TempDir(), "store"), StoreOptions{MergeFanIn: -1, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	flushes := func() uint64 {
		s.syncMu.Lock()
		defer s.syncMu.Unlock()

		return s.syncBegun
	}
	rows := func() uint64 {
		snap := s.Table().Snapshot()
		defer snap.Release()
		var n uint64
		for _, h := range snap.Parts() {
			n += h.Rows
		}

		return n
	}
	if err := s.Sync(); err != nil || flushes() != 0 {
		t.Fatalf("a Sync of an empty store: %v after %d flushes", err, flushes())
	}
	for round := 1; round <= 3; round++ {
		if _, err := s.Append(storeDocs(round*1000, 50)); err != nil {
			t.Fatal(err)
		}
		before := flushes()
		var wg sync.WaitGroup
		for range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := s.Sync(); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		if got := rows(); got != uint64(50*round) {
			t.Fatalf("round %d: %d rows are written after Sync, want %d", round, got, 50*round)
		}
		if flushes() == before {
			t.Fatalf("round %d: Sync after an Append started no flush", round)
		}
		settled := flushes()
		for range 100 {
			if err := s.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if flushes() != settled {
			t.Fatalf("round %d: %d flushes for Syncs with nothing to write", round, flushes()-settled)
		}
	}
}
