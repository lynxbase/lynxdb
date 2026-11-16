package colstore

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// TestJSONBenchIngest times writing LYNXDB_BENCH_NDJSON into a store in the
// empty directory LYNXDB_BENCH_DIR: until every row is durable, until the
// background merges have settled and, with LYNXDB_BENCH_COMPACT set, until
// everything is one sorted run. LYNXDB_BENCH_STREAMS writers (one per core by
// default) each take requests of LYNXDB_BENCH_REQUEST_DOCS lines (100000 by
// default); LYNXDB_BENCH_SORT orders the table ("default" for the benchmark's
// key), LYNXDB_BENCH_RUN_MB sets the input a writer buffers per run (128 by
// default), LYNXDB_BENCH_FANIN the runs merged at once (8 by default), and
// LYNXDB_BENCH_FLUSH_LEVEL and LYNXDB_BENCH_LEVEL the compression effort of
// flushed runs (fastest by default) and of merged parts (fast by default);
// compact is the third choice. LYNXDB_BENCH_PARTITION, a duration such as
// 1h, partitions the table by time.
func TestJSONBenchIngest(t *testing.T) {
	dir, path := os.Getenv("LYNXDB_BENCH_DIR"), os.Getenv("LYNXDB_BENCH_NDJSON")
	if dir == "" || path == "" || os.Getenv("LYNXDB_BENCH_INGEST") == "" {
		t.Skip("set LYNXDB_BENCH_INGEST, LYNXDB_BENCH_DIR and LYNXDB_BENCH_NDJSON")
	}
	opts := StoreOptions{
		TimeField:  "time_us",
		BlockRows:  envInt(t, "LYNXDB_BENCH_BLOCK_ROWS", 16384),
		FlushBytes: envInt(t, "LYNXDB_BENCH_RUN_MB", 128) << 20,
		MergeFanIn: envInt(t, "LYNXDB_BENCH_FANIN", 8),
	}
	if v := os.Getenv("LYNXDB_BENCH_PARTITION"); v != "" {
		width, err := time.ParseDuration(v)
		if err != nil || width <= 0 {
			t.Fatalf("LYNXDB_BENCH_PARTITION=%q is not a positive duration", v)
		}
		opts.Partition = width
	}
	opts.FlushLevel = benchLevel(t, "LYNXDB_BENCH_FLUSH_LEVEL", part.LevelFastest)
	opts.Part.Level = benchLevel(t, "LYNXDB_BENCH_LEVEL", part.LevelFast)
	switch s := os.Getenv("LYNXDB_BENCH_SORT"); s {
	case "":
	case "default":
		opts.SortKey = []string{"kind", "commit.operation", "commit.collection", "did"}
	default:
		opts.SortKey = strings.Split(s, ",")
	}
	streams := envInt(t, "LYNXDB_BENCH_STREAMS", runtime.GOMAXPROCS(0))
	requestDocs := envInt(t, "LYNXDB_BENCH_REQUEST_DOCS", 100000)
	data := mapFile(t, path)

	s, err := OpenStore(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if snap := s.Table().Snapshot(); len(snap.Parts()) != 0 {
		snap.Release()
		t.Fatal("the directory already holds a table")
	} else {
		snap.Release()
	}

	megabytes := float64(len(data)) / 1e6
	cpuStart, start := cpuTime(), time.Now()
	var rejected atomic.Int64
	var wg sync.WaitGroup
	for _, region := range splitAtLines(data, streams) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := s.NewWriter()
			for len(region) > 0 {
				var request []byte
				request, region = shred.SplitLines(region, requestDocs)
				if err := w.AddNDJSON(request); err != nil {
					t.Error(err)

					return
				}
			}
			if err := w.Close(); err != nil {
				t.Error(err)
			}
			rejected.Add(w.Rejected)
		}()
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	durable, durableCPU := time.Since(start), cpuTime()-cpuStart
	t.Logf("durable: %.1f MB of JSON on %d streams in %.2fs wall, %.2fs CPU (%.0f MB/s), %d lines rejected, key %v",
		megabytes, streams, durable.Seconds(), durableCPU.Seconds(), megabytes/durable.Seconds(), rejected.Load(), opts.SortKey)
	logStoreShape(t, s, "when durable")

	if err := s.Settle(); err != nil {
		t.Fatal(err)
	}
	settled, settledCPU := time.Since(start), cpuTime()-cpuStart
	t.Logf("settled: %.2fs wall, %.2fs CPU since the start (%.0f MB/s)", settled.Seconds(), settledCPU.Seconds(), megabytes/settled.Seconds())
	logStoreShape(t, s, "when settled")

	if os.Getenv("LYNXDB_BENCH_COMPACT") != "" {
		cpuBefore, begin := cpuTime(), time.Now()
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
		t.Logf("compacted: %.2fs wall, %.2fs CPU on top", time.Since(begin).Seconds(), (cpuTime() - cpuBefore).Seconds())
		logStoreShape(t, s, "when compacted")
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	t.Logf("the Go runtime obtained %.0f MB from the system", float64(mem.Sys)/1e6)
}

// benchLevel reads a compression effort setting: fastest, fast or compact.
func benchLevel(t testing.TB, name string, def part.Level) part.Level {
	t.Helper()
	switch v := os.Getenv(name); v {
	case "":
		return def
	case "fast":
		return part.LevelFast
	case "fastest":
		return part.LevelFastest
	case "compact":
		return part.LevelCompact
	default:
		t.Fatalf("%s=%q is not fastest, fast or compact", name, v)

		return 0
	}
}

// logStoreShape reports the rows, bytes and runs per tier of a store.
func logStoreShape(t testing.TB, s *Store, when string) {
	t.Helper()
	snap := s.Table().Snapshot()
	defer snap.Release()
	var rows uint64
	var size int64
	runs := map[int]map[uint64]struct{}{}
	partitions := map[int64]struct{}{}
	for _, h := range snap.Parts() {
		rows += h.Rows
		size += int64(h.Size())
		partitions[h.Partition] = struct{}{}
		if runs[h.Tier] == nil {
			runs[h.Tier] = map[uint64]struct{}{}
		}
		runs[h.Tier][h.Run] = struct{}{}
	}
	tiers := make([]int, 0, len(runs))
	for tier := range runs {
		tiers = append(tiers, tier)
	}
	sort.Ints(tiers)
	var shape strings.Builder
	for _, tier := range tiers {
		fmt.Fprintf(&shape, " tier %d: %d runs;", tier, len(runs[tier]))
	}
	t.Logf("%s: %d rows, %.1f MB in %d parts of %d partitions;%s", when, rows, float64(size)/1e6, len(snap.Parts()), len(partitions), shape.String())
}
