package colstore

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// writeInterleavedRuns writes rows rows to s as runs runs whose keys
// interleave, row i going to run i%runs; s writes its buffers only on Sync.
func writeInterleavedRuns(t *testing.T, s *Store, rows, runs int) {
	t.Helper()
	for r := 0; r < runs; r++ {
		var buf bytes.Buffer
		for i := r; i < rows; i += runs {
			fmt.Fprintf(&buf, `{"k":"%08d","n":%d}`+"\n", i, i)
			if buf.Len() > 1<<20 {
				if _, err := s.Append(buf.Bytes()); err != nil {
					t.Fatal(err)
				}
				buf.Reset()
			}
		}
		if _, err := s.Append(buf.Bytes()); err != nil {
			t.Fatal(err)
		}
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCompactReturnsWhenARangeMergeCannotCreateItsPart requires a merge
// whose range merges cannot all create their parts to end with an error and
// leave its runs as they were, rather than have a range merge that is done
// wait for one that never began to give up the end of its range.
func TestCompactReturnsWhenARangeMergeCannotCreateItsPart(t *testing.T) {
	if testing.Short() {
		t.Skip("writes enough rows for range merges to take from each other")
	}
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(4))
	skipSyncs(t)
	path := filepath.Join(t.TempDir(), "store")
	s, err := OpenStore(path, StoreOptions{SortKey: []string{"k"}, MergeFanIn: 1 << 10, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	// Four ranges of more rows each than a range merge takes from another.
	const rows = 4 * (mergedPartRows + mergedPartRows/4)
	writeInterleavedRuns(t, s, rows, 2)
	// The second part the merge creates finds its name taken.
	taken := filepath.Join(path, fmt.Sprintf("%016x%s%s", s.dir.nextID.Load()+1, partSuffix, tmpSuffix))
	if err := os.WriteFile(taken, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	compacted := make(chan error, 1)
	go func() { compacted <- s.Compact() }()
	select {
	case err := <-compacted:
		if err == nil {
			t.Fatal("Compact succeeded although a range merge could not create its part")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Compact did not return after a range merge could not create its part")
	}
	if n, _ := rowsAndSumOf(t, s.Table()); n != rows {
		t.Fatalf("%d rows after the failed merge, want %d", n, rows)
	}
	within(t, "Close", func() { _ = s.Close() })
}
