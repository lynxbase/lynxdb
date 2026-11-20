package colstore

import (
	"path/filepath"
	"runtime"
	"testing"
)

// TestACompactionOfMoreRunsThanTheBudgetTakesMergesThemInTurns requires a
// partition of more runs than one merge may read under the block budget on
// every core to end as one run with all its rows, merged in sets of two up
// to the widest a merge reads until twice that many runs are left.
func TestACompactionOfMoreRunsThanTheBudgetTakesMergesThemInTurns(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(4))
	skipSyncs(t)
	s, err := OpenStore(filepath.Join(t.TempDir(), "store"), StoreOptions{SortKey: []string{"k"}, MergeFanIn: -1, MergeBlocks: 16, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	const rows, runs = 6000, 37
	writeInterleavedRuns(t, s, rows, runs)
	_, sum := rowsAndSumOf(t, s.Table())
	widest := s.widest()
	sets := narrowed([][]sortedRun{make([]sortedRun, runs)}, widest)
	if len(sets) < 2 {
		t.Fatalf("%d runs make %d sets of at most %d", runs, len(sets), widest)
	}
	for _, set := range sets {
		if len(set) > widest || len(set) < 2 {
			t.Fatalf("a set of %d runs, want 2 to %d", len(set), widest)
		}
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if n := runsOf(s); n != 1 {
		t.Fatalf("%d runs after the compaction, want 1", n)
	}
	if n, got := rowsAndSumOf(t, s.Table()); n != rows || got != sum {
		t.Fatalf("%d rows of sum %d after the compaction, want %d of %d", n, got, rows, sum)
	}
	// Each turn writes the runs one tier up.
	turns := 0
	for n := runs; n > 1; turns++ {
		if n <= 2*widest {
			break
		}
		n = (n + widest - 1) / widest
	}
	turns++
	snap := s.Table().Snapshot()
	if tier := snap.Parts()[0].Tier; tier != turns {
		t.Fatalf("the compacted run is of tier %d, want %d after merges of at most %d runs", tier, turns, widest)
	}
	snap.Release()
	within(t, "Close", func() { _ = s.Close() })
}
