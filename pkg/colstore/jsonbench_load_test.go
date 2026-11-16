package colstore

import (
	"bytes"
	"io"
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

// orderedRuns shreds data into runs ordered by key, working on workers cores:
// each run is sorted as a whole and kept as a part in memory. It returns the
// runs and the number of lines that are not JSON objects.
func orderedRuns(t testing.TB, data []byte, workers, runs, blockRows int, key []string) ([]*part.Part, int64) {
	t.Helper()
	opts := part.Options{Ordered: true, SortKey: key}
	chunks := splitAtLines(data, runs)
	out := make([]*part.Part, len(chunks))
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
			var buf bytes.Buffer
			buf.Grow(len(chunk) / 3)
			w := part.NewWriter(&buf, opts)
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

				return
			}
			p, err := part.Open(buf.Bytes())
			if err != nil {
				t.Error(err)

				return
			}
			out[i] = p
		}(i, chunk)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	return out, rejected.Load()
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
