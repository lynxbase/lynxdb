package colstore

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// countingWriter keeps the size of what is written and nothing else.
type countingWriter struct{ n atomic.Int64 }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n.Add(int64(len(p)))

	return len(p), nil
}

// heapWatch samples the bytes of live and unswept heap objects until stopped
// and returns the highest sample.
func heapWatch() (stop func() uint64) {
	const name = "/memory/classes/heap/objects:bytes"
	read := func() uint64 {
		s := []metrics.Sample{{Name: name}}
		metrics.Read(s)

		return s[0].Value.Uint64()
	}
	peak := read()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				peak = max(peak, read())
			}
		}
	}()

	return func() uint64 {
		close(done)
		wg.Wait()

		return max(peak, read())
	}
}

type mergeCost struct {
	wall, cpu time.Duration
	mallocs   uint64
	allocated uint64
	peakHeap  uint64
	size      int64
	parts     int
}

// runMerge runs fn, which merges into the writers sink hands out, and
// returns what it cost; the peak heap counts from the heap in use at the
// start.
func runMerge(fn func(sink func(i int) io.Writer) int) mergeCost {
	outs := make([]countingWriter, 4096)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	stop := heapWatch()
	cpuBefore, begin := cpuTime(), time.Now()
	parts := fn(func(i int) io.Writer { return &outs[i] })
	c := mergeCost{wall: time.Since(begin), cpu: cpuTime() - cpuBefore, parts: parts}
	peak := stop()
	runtime.ReadMemStats(&after)
	c.mallocs, c.allocated = after.Mallocs-before.Mallocs, after.TotalAlloc-before.TotalAlloc
	c.peakHeap = peak - min(peak, before.HeapAlloc)
	for i := range outs {
		c.size += outs[i].n.Load()
	}

	return c
}

// liveProbe is a sink that collects garbage before every block written to
// it and keeps the largest heap that was left.
type liveProbe struct{ peak *atomic.Uint64 }

func (p liveProbe) Write(b []byte) (int, error) {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	for {
		old := p.peak.Load()
		if ms.HeapAlloc <= old || p.peak.CompareAndSwap(old, ms.HeapAlloc) {
			return len(b), nil
		}
	}
}

// measureMerge runs fn once collecting garbage at every block, which shows
// the memory merging holds, and then n times back to back, as a store that
// keeps merging runs it, of which the run using the least CPU counts.
func measureMerge(t testing.TB, n int, profile string, fn func(sink func(i int) io.Writer) int) (held uint64, best mergeCost) {
	t.Helper()
	// Two collections empty the pools.
	runtime.GC()
	runtime.GC()
	var floor runtime.MemStats
	runtime.ReadMemStats(&floor)
	var peak atomic.Uint64
	fn(func(int) io.Writer { return liveProbe{&peak} })
	held = peak.Load() - min(peak.Load(), floor.HeapAlloc)
	if profile != "" {
		f, err := os.Create(profile)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			t.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}
	for run := 0; run < n; run++ {
		c := runMerge(fn)
		if run == 0 || c.cpu < best.cpu {
			best = c
		}
	}

	return held, best
}

// contentDigest folds every value of every row of parts, in row order, into
// one number that does not depend on where blocks or parts are cut.
func contentDigest(t testing.TB, parts []*part.Part) (digest uint64, rows, blocks int) {
	t.Helper()
	var blk part.Block
	var col vec.Column
	var rowHash []uint64
	mix := func(h uint64) uint64 {
		h ^= h >> 33
		h *= 0xff51afd7ed558ccd
		h ^= h >> 33

		return h
	}
	for _, p := range parts {
		seeds := make([]uint64, len(p.Paths()))
		for i, path := range p.Paths() {
			seeds[i] = xxhash.Sum64String(path)
		}
		for b := 0; b < p.NumBlocks(); b++ {
			if err := p.OpenBlock(b, &blk); err != nil {
				t.Fatal(err)
			}
			blocks++
			rowHash = append(rowHash[:0], make([]uint64, blk.Rows)...)
			refs := blk.Columns()
			for j := range refs {
				ref := &refs[j]
				if err := blk.Decode(ref, &col); err != nil {
					t.Fatal(err)
				}
				seed := seeds[ref.PathID] + uint64(ref.Kind)*0x9e3779b97f4a7c15
				for r := 0; r < blk.Rows; r++ {
					if !col.IsValid(r) {
						continue
					}
					var v uint64
					switch ref.Kind {
					case vec.KindBool:
						if col.Bool(r) {
							v = 1
						}
					case vec.KindInt:
						v = uint64(col.Int(r))
					case vec.KindFloat:
						v = math.Float64bits(col.Float(r))
					case vec.KindString, vec.KindVariant:
						v = xxhash.Sum64(col.Bytes(r))
					}
					rowHash[r] += mix(seed ^ mix(v+1))
				}
			}
			for _, h := range rowHash {
				digest = (digest ^ h) * 0x100000001b3
			}
			rows += blk.Rows
		}
	}

	return digest, rows, blocks
}

// TestJSONBenchMerge times one merge pass over sorted runs built from the
// file named by LYNXDB_BENCH_NDJSON: as one merge on one goroutine, and as
// range merges on every core. LYNXDB_BENCH_MERGE_RUNS sets the number of
// source runs, LYNXDB_BENCH_RUNS the timed repetitions, LYNXDB_BENCH_SORT the
// sort key, LYNXDB_BENCH_MERGE_MODE keeps one of the modes whole, ranges
// and apart, the last of which merges the parts the range merges wrote, and
// LYNXDB_BENCH_PROFILE names a directory that receives one CPU profile per
// mode.
func TestJSONBenchMerge(t *testing.T) {
	path := os.Getenv("LYNXDB_BENCH_NDJSON")
	if path == "" {
		t.Skip("set LYNXDB_BENCH_NDJSON to a Bluesky NDJSON file")
	}
	data := mapFile(t, path)
	key := []string{"kind", "commit.operation", "commit.collection", "did"}
	if s := os.Getenv("LYNXDB_BENCH_SORT"); s != "" {
		key = strings.Split(s, ",")
	}
	const blockRows = 16384
	cores := runtime.GOMAXPROCS(0)
	nRuns := envInt(t, "LYNXDB_BENCH_MERGE_RUNS", 12)
	repeat := envInt(t, "LYNXDB_BENCH_RUNS", 5)
	profileDir := os.Getenv("LYNXDB_BENCH_PROFILE")
	profile := func(name string) string {
		if profileDir == "" {
			return ""
		}

		return filepath.Join(profileDir, name+".prof")
	}

	runs, _ := orderedRuns(t, data, cores, nRuns, blockRows, key)
	var rows uint64
	var srcSize, srcBlocks int
	for _, p := range runs {
		rows += p.Rows
		srcSize += p.Size()
		srcBlocks += p.NumBlocks()
	}
	var want uint64
	t.Logf("%d runs ordered by %v: %d rows in %d blocks, %.1f MB", len(runs), key, rows, srcBlocks, float64(srcSize)/1e6)

	opts := part.Options{Ordered: true, SortKey: key}
	whole := func(sink func(i int) io.Writer) int {
		w := part.NewWriter(sink(0), opts)
		if err := part.Merge(w, runs, blockRows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		return 1
	}
	ranges := func(sink func(i int) io.Writer) int {
		return mergeRanges(t, runs, cores, blockRows, key, sink)
	}

	// Both ways of merging hold the same rows in the same order.
	var apart []*part.Part
	for i, fn := range []func(func(int) io.Writer) int{whole, ranges} {
		bufs := make([]bytes.Buffer, cores)
		n := fn(func(i int) io.Writer { return &bufs[i] })
		merged := make([]*part.Part, n)
		for j := range merged {
			p, err := part.Open(bufs[j].Bytes())
			if err != nil {
				t.Fatal(err)
			}
			merged[j] = p
		}
		digest, got, blocks := contentDigest(t, merged)
		if uint64(got) != rows {
			t.Fatalf("merge holds %d rows, the runs %d", got, rows)
		}
		if i == 0 {
			want = digest
		} else if digest != want {
			t.Errorf("range merges hold other rows than the whole merge: digest %016x, want %016x", digest, want)
		}
		t.Logf("merged content digest %016x, %d parts, %d blocks", digest, n, blocks)
		apart = merged
	}
	// The parts of the range merges do not overlap, so merging them takes
	// their blocks over as they are stored.
	disjoint := func(sink func(i int) io.Writer) int {
		w := part.NewWriter(sink(0), opts)
		if err := part.Merge(w, apart, blockRows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		return 1
	}

	report := func(label, name string, fn func(func(int) io.Writer) int) {
		if only := os.Getenv("LYNXDB_BENCH_MERGE_MODE"); only != "" && only != name {
			return
		}
		held, c := measureMerge(t, repeat, profile("merge_"+name), fn)
		perMillion := float64(rows) / 1e6
		t.Logf("%-21s %5.0f ms wall, %5.0f krows/s, cpu %.2f s and %6.0f allocs of %4.0f MB per 1M rows, holds %.0f MB, peak heap +%.0f MB; %.2f MB in %d parts",
			label, ms(c.wall), float64(rows)/c.wall.Seconds()/1e3, c.cpu.Seconds()/perMillion,
			float64(c.mallocs)/perMillion, float64(c.allocated)/1e6/perMillion,
			float64(held)/1e6, float64(c.peakHeap)/1e6, float64(c.size)/1e6, c.parts)
	}
	report("whole, one goroutine", "whole", whole)
	report(fmt.Sprintf("ranges, %d cores", cores), "ranges", ranges)
	report("parts apart by key", "apart", disjoint)
}
