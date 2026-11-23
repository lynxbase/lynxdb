package colstore

import (
	"bytes"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/blevesearch/mmap-go"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// splitAtLines cuts data into at most n pieces of similar size that end at
// line ends.
func splitAtLines(data []byte, n int) [][]byte {
	chunks := make([][]byte, 0, n)
	for rest := data; len(rest) > 0; {
		size := min(len(data)/n+1, len(rest))
		if size < len(rest) {
			if nl := bytes.IndexByte(rest[size:], '\n'); nl >= 0 {
				size += nl + 1
			} else {
				size = len(rest)
			}
		}
		chunks = append(chunks, rest[:size])
		rest = rest[size:]
	}

	return chunks
}

// writeOrderedRuns shreds data into runs ordered by key, working on workers
// cores: each run is sorted as a whole and written as a part to sink(i). It
// returns the number of runs and of lines that are not JSON objects.
func writeOrderedRuns(t testing.TB, data []byte, workers, runs, blockRows int, key []string, sink func(i int) io.Writer) (int, int64) {
	t.Helper()
	opts := part.Options{Ordered: true, SortKey: key}
	chunks := splitAtLines(data, runs)
	var rejected atomic.Int64
	var wg sync.WaitGroup
	slots := make(chan struct{}, workers)
	for i, chunk := range chunks {
		wg.Add(1)
		slots <- struct{}{}
		go func(i int, chunk []byte) {
			defer wg.Done()
			defer func() { <-slots }()
			all := shred.NewBatch(shred.Options{TimeField: "time_us"})
			errs, _ := all.AddNDJSON(chunk, 0, shred.Intake{}, nil)
			rejected.Add(int64(len(errs)))
			all.SortBy(key)
			w := part.NewWriter(sink(i), opts)
			block := shred.Get(shred.Options{})
			defer shred.Put(block)
			for from := 0; from < all.Rows; from += blockRows {
				block.Reset()
				all.CopyRows(block, from, min(from+blockRows, all.Rows))
				if err := w.WriteBatch(block); err != nil {
					t.Error(err)

					return
				}
			}
			if err := w.Close(); err != nil {
				t.Error(err)
			}
		}(i, chunk)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	return len(chunks), rejected.Load()
}

// runCount returns how many sorted runs to cut data into: at least want, and
// enough to keep each under LYNXDB_BENCH_RUN_MB megabytes of JSON (256 by
// default), which bounds what the workers hold while they sort.
func runCount(t testing.TB, data []byte, want int) int {
	t.Helper()
	limit := envInt(t, "LYNXDB_BENCH_RUN_MB", 256) << 20

	return max(want, (len(data)+limit-1)/limit)
}

// mapFile maps the file at path for reading, so that a large input is held
// by the page cache alone, and unmaps it when the test ends.
func mapFile(t testing.TB, path string) []byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = data.Unmap() })

	return data
}

// orderedRuns is writeOrderedRuns with the runs kept in memory.
func orderedRuns(t testing.TB, data []byte, workers, runs, blockRows int, key []string) ([]*part.Part, int64) {
	t.Helper()
	bufs := make([]bytes.Buffer, runs)
	n, rejected := writeOrderedRuns(t, data, workers, runs, blockRows, key, func(i int) io.Writer {
		bufs[i].Grow(len(data) / runs / 3)

		return &bufs[i]
	})
	out := make([]*part.Part, n)
	for i := range out {
		p, err := part.Open(bufs[i].Bytes())
		if err != nil {
			t.Fatal(err)
		}
		out[i] = p
	}

	return out, rejected
}

// separateRuns reports whether LYNXDB_BENCH_LAYOUT asks for an ordered table
// as it is right after ingest, every part a sorted run over the whole key
// space, instead of one sorted run cut into parts with disjoint key ranges.
func separateRuns(t testing.TB) bool {
	t.Helper()
	switch v := os.Getenv("LYNXDB_BENCH_LAYOUT"); v {
	case "", "merged":
		return false
	case "runs":
		return true
	default:
		t.Fatalf("LYNXDB_BENCH_LAYOUT=%q is neither merged nor runs", v)

		return false
	}
}

// mergeBlocksHeld caps the decoded blocks that concurrent merges hold, each
// several megabytes of column values.
const mergeBlocksHeld = 2048

// mergeRanges merges ordered runs into at most n parts with disjoint key
// ranges, one core per range, writing range i to sink(i). It returns the
// number of parts written.
func mergeRanges(t testing.TB, runs []*part.Part, n, blockRows int, key []string, sink func(i int) io.Writer) int {
	t.Helper()
	opts := part.Options{Ordered: true, SortKey: key}
	cuts := part.SplitKeys(runs, n)
	// A merge holds one decoded block per run, so the merges that run at
	// once are bounded by the blocks they hold together.
	slots := make(chan struct{}, max(1, min(runtime.GOMAXPROCS(0), mergeBlocksHeld/len(runs))))
	var wg sync.WaitGroup
	for i := 0; i <= len(cuts); i++ {
		wg.Add(1)
		slots <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-slots }()
			var lo, hi []byte
			if i > 0 {
				lo = cuts[i-1]
			}
			if i < len(cuts) {
				hi = cuts[i]
			}
			w := part.NewWriter(sink(i), opts)
			if err := part.MergeRange(w, runs, blockRows, lo, hi); err != nil {
				t.Error(err)

				return
			}
			if err := w.Close(); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	return len(cuts) + 1
}
