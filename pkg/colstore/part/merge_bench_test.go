package part

import (
	"bytes"
	"io"
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

var blueskyKey = []string{"kind", "commit.operation", "commit.collection", "did"}

// benchEnv reads a positive integer setting of a benchmark.
func benchEnv(b *testing.B, name string, def int) int {
	b.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		b.Fatalf("%s=%q is not a positive integer", name, v)
	}

	return n
}

// blueskyRuns writes rows events of benchdata as runs parts, each sorted by
// blueskyKey in blocks of blockRows rows, as flushes write them.
func blueskyRuns(b *testing.B, rows, runs, blockRows int) []*Part {
	b.Helper()
	g := benchdata.New(1, max(rows/8, 1))
	opts := Options{Ordered: true, SortKey: blueskyKey}
	block := shred.NewBatch(shred.Options{})
	var lines []byte
	out := make([]*Part, runs)
	for i := range out {
		n := rows / runs
		if i == runs-1 {
			n = rows - i*n
		}
		lines = g.Append(lines[:0], n)
		all := shred.NewBatch(shred.Options{TimeField: "time_us"})
		if errs := all.AddNDJSON(lines, 0, nil); len(errs) > 0 {
			b.Fatal(errs[0])
		}
		all.SortBy(blueskyKey)
		var buf bytes.Buffer
		w := NewWriter(&buf, opts)
		for from := 0; from < all.Rows; from += blockRows {
			block.Reset()
			all.CopyRows(block, from, min(from+blockRows, all.Rows))
			if err := w.WriteBatch(block); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		p, err := Open(buf.Bytes())
		if err != nil {
			b.Fatal(err)
		}
		out[i] = p
	}

	return out
}

type countingSink struct{ n int64 }

func (s *countingSink) Write(p []byte) (int, error) {
	s.n += int64(len(p))

	return len(p), nil
}

// heapProbe collects garbage before every write to it and keeps the largest
// heap that was left.
type heapProbe struct{ peak uint64 }

func (p *heapProbe) Write(b []byte) (int, error) {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	p.peak = max(p.peak, ms.HeapAlloc)

	return len(b), nil
}

// BenchmarkMergeRange merges sorted runs of Bluesky-shaped events into one
// part on one goroutine, which is what each range merge of a store does on
// its core, and reports rows per second, the bytes written and the heap the
// merge holds per run it reads. LYNXDB_BENCH_MERGE_ROWS sets the events
// (512Ki by default) and LYNXDB_BENCH_MERGE_RUNS the runs (16 by default).
func BenchmarkMergeRange(b *testing.B) {
	const blockRows = 16384
	rows := benchEnv(b, "LYNXDB_BENCH_MERGE_ROWS", 1<<19)
	runs := benchEnv(b, "LYNXDB_BENCH_MERGE_RUNS", 16)
	srcs := blueskyRuns(b, rows, runs, blockRows)
	opts := Options{Ordered: true, SortKey: blueskyKey}
	merge := func(sink io.Writer) {
		w := NewWriter(sink, opts)
		if err := Merge(w, srcs, blockRows); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
	runtime.GC()
	runtime.GC()
	var floor runtime.MemStats
	runtime.ReadMemStats(&floor)
	probe := &heapProbe{}
	merge(probe)
	held := probe.peak - min(probe.peak, floor.HeapAlloc)

	var sink countingSink
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		merge(&sink)
	}
	b.StopTimer()
	n := float64(b.N)
	b.ReportMetric(float64(rows)*n/b.Elapsed().Seconds(), "rows/s")
	b.ReportMetric(float64(sink.n)/n/1e6, "MB-out")
	b.ReportMetric(float64(held)/float64(runs)/1e6, "MB-held/run")
}
