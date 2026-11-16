package colstore

import (
	"bytes"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"

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
			rejected.Add(int64(len(all.AddNDJSON(chunk, 0, nil))))
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

// mergeRanges merges ordered runs into at most n parts with disjoint key
// ranges, one core per range, writing range i to sink(i). It returns the
// number of parts written.
func mergeRanges(t testing.TB, runs []*part.Part, n, blockRows int, key []string, sink func(i int) io.Writer) int {
	t.Helper()
	opts := part.Options{Ordered: true, SortKey: key}
	cuts := part.SplitKeys(runs, n)
	var wg sync.WaitGroup
	for i := 0; i <= len(cuts); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
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
