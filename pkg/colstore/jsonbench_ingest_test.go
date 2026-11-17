package colstore

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
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
// 1h, partitions the table by time, LYNXDB_BENCH_MERGE_BLOCKS bounds the
// decoded blocks that merges hold at once, LYNXDB_BENCH_MERGE_BACKLOG the
// runs due to be merged below which merges give way to the rows that wait
// to be written, LYNXDB_BENCH_NO_PACKING leaves
// identifier-like strings to the general codec, and LYNXDB_BENCH_VALUE_SLACK
// is the percentage by which long values may grow for a faster codec ("off"
// for none; the store's default is 50).
func TestJSONBenchIngest(t *testing.T) {
	dir, path := os.Getenv("LYNXDB_BENCH_DIR"), os.Getenv("LYNXDB_BENCH_NDJSON")
	if dir == "" || path == "" || os.Getenv("LYNXDB_BENCH_INGEST") == "" {
		t.Skip("set LYNXDB_BENCH_INGEST, LYNXDB_BENCH_DIR and LYNXDB_BENCH_NDJSON")
	}
	opts := ingestOptions(t)
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

	// With LYNXDB_BENCH_APPEND the streams share the store's buffers through
	// Append, as request handlers of a server do; otherwise each stream has
	// a writer of its own.
	shared := os.Getenv("LYNXDB_BENCH_APPEND") != ""
	// The heap that survives a collection is what the store really holds;
	// the rest of what the runtime obtains is garbage awaiting collection.
	var liveMax atomic.Uint64
	sampling := make(chan struct{})
	defer close(sampling)
	go func() {
		sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
		for {
			select {
			case <-sampling:
				return
			case <-time.After(10 * time.Millisecond):
				metrics.Read(sample)
				if v := sample[0].Value.Uint64(); v > liveMax.Load() {
					liveMax.Store(v)
				}
			}
		}
	}()
	// LYNXDB_BENCH_REPEAT feeds the input that many times over, to reach the
	// merge tiers of a larger load with a small file.
	repeat := envInt(t, "LYNXDB_BENCH_REPEAT", 1)
	megabytes := float64(len(data)) * float64(repeat) / 1e6
	cpuStart, start := cpuTime(), time.Now()
	var rejected atomic.Int64
	var wg sync.WaitGroup
	for _, piece := range splitAtLines(data, streams) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var w *Writer
			if !shared {
				w = s.NewWriter()
			}
			region, left := piece, repeat-1
			for len(region) > 0 || left > 0 {
				if len(region) == 0 {
					region, left = piece, left-1
				}
				var request []byte
				request, region = shred.SplitLines(region, requestDocs)
				if shared {
					n, err := s.Append(request)
					if err != nil {
						t.Error(err)

						return
					}
					rejected.Add(n)

					continue
				}
				if err := w.AddNDJSON(request); err != nil {
					t.Error(err)

					return
				}
			}
			if shared {
				return
			}
			if err := w.Close(); err != nil {
				t.Error(err)
			}
			rejected.Add(w.Rejected)
		}()
	}
	wg.Wait()
	if shared {
		if err := s.Sync(); err != nil {
			t.Error(err)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
	durable, durableCPU := time.Since(start), cpuTime()-cpuStart
	// LYNXDB_BENCH_HEAP_PROFILE names a file for a heap profile taken at
	// this point, while the write buffers are still held and merges run.
	if name := os.Getenv("LYNXDB_BENCH_HEAP_PROFILE"); name != "" {
		f, err := os.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
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
	t.Logf("the Go runtime obtained %.0f MB from the system; the live heap peaked at %.0f MB", float64(mem.Sys)/1e6, float64(liveMax.Load())/1e6)
}

// ingestOptions reads the store settings of the ingest benchmark from the
// environment.
func ingestOptions(t testing.TB) StoreOptions {
	t.Helper()
	opts := StoreOptions{
		TimeField:  "time_us",
		BlockRows:  envInt(t, "LYNXDB_BENCH_BLOCK_ROWS", 16384),
		FlushBytes: envInt(t, "LYNXDB_BENCH_RUN_MB", 128) << 20,
		MergeFanIn: envInt(t, "LYNXDB_BENCH_FANIN", 8),
	}
	if os.Getenv("LYNXDB_BENCH_MERGE_BACKLOG") != "" {
		opts.MergeBacklog = envInt(t, "LYNXDB_BENCH_MERGE_BACKLOG", 0)
	}
	if os.Getenv("LYNXDB_BENCH_MERGE_BLOCKS") != "" {
		opts.MergeBlocks = envInt(t, "LYNXDB_BENCH_MERGE_BLOCKS", 0)
	}
	if v := os.Getenv("LYNXDB_BENCH_PARTITION"); v != "" {
		width, err := time.ParseDuration(v)
		if err != nil || width <= 0 {
			t.Fatalf("LYNXDB_BENCH_PARTITION=%q is not a positive duration", v)
		}
		opts.Partition = width
	}
	opts.Part.NoPacking = os.Getenv("LYNXDB_BENCH_NO_PACKING") != ""
	switch v := os.Getenv("LYNXDB_BENCH_VALUE_SLACK"); v {
	case "":
	case "off":
		opts.Part.ValueSlack = -1
	default:
		opts.Part.ValueSlack = envInt(t, "LYNXDB_BENCH_VALUE_SLACK", 0)
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

	return opts
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
	if flushed, merged := s.dir.Written(); size > 0 {
		t.Logf("%s: %.1f MB written by flushes and %.1f MB by merges, %.2f times the size of the table",
			when, float64(flushed)/1e6, float64(merged)/1e6, float64(flushed+merged)/float64(size))
	}
}

// TestJSONBenchStoreLayouts requires the benchmark queries to give one
// answer over stores that hold LYNXDB_BENCH_NDJSON in arrival order, as
// sorted runs left by ingest, as one merged run, and partitioned by time.
func TestJSONBenchStoreLayouts(t *testing.T) {
	path := os.Getenv("LYNXDB_BENCH_NDJSON")
	if path == "" || os.Getenv("LYNXDB_BENCH_LAYOUTS") == "" {
		t.Skip("set LYNXDB_BENCH_LAYOUTS and LYNXDB_BENCH_NDJSON")
	}
	data := mapFile(t, path)
	key := []string{"kind", "commit.operation", "commit.collection", "did"}
	cores := runtime.GOMAXPROCS(0)
	type answers struct {
		q1, q2         []eventCount
		q3             []hourCount
		first, longest []userSpan
		searches       []int64
	}
	layouts := []struct {
		name    string
		opts    StoreOptions
		compact bool
	}{
		{"arrival order", StoreOptions{}, false},
		{"runs after ingest", StoreOptions{SortKey: key, FlushBytes: 16 << 20, MergeFanIn: 4}, false},
		{"one merged run", StoreOptions{SortKey: key, FlushBytes: 16 << 20, MergeFanIn: 4}, true},
		{"partitions of 5m, settled", StoreOptions{SortKey: key, FlushBytes: 16 << 20, MergeFanIn: 4, Partition: 5 * time.Minute}, false},
		{"partitions of 5m, merged", StoreOptions{SortKey: key, FlushBytes: 16 << 20, MergeFanIn: 4, Partition: 5 * time.Minute}, true},
	}
	var want answers
	for i, layout := range layouts {
		layout.opts.TimeField = "time_us"
		s, err := OpenStore(t.TempDir(), layout.opts)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for _, region := range splitAtLines(data, cores) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := s.Append(region); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if layout.compact {
			err = s.Compact()
		} else {
			err = s.Settle()
		}
		if err != nil {
			t.Fatal(err)
		}
		logStoreShape(t, s, layout.name)
		snap := s.Table().Snapshot()
		paths := []string{"kind", "commit.operation", "commit.collection"}
		created := snap.Narrow(KeyFilter{Paths: paths[:2], Values: [][]byte{[]byte("commit"), []byte("create")}})
		posts := snap.Narrow(KeyFilter{Paths: paths, Values: [][]byte{[]byte("commit"), []byte("create"), []byte("app.bsky.feed.post")}})
		var got answers
		if got.q1, err = queryTopEvents(snap, cores); err != nil {
			t.Fatal(err)
		}
		if got.q2, err = queryEventsWithUsers(created, cores, false); err != nil {
			t.Fatal(err)
		}
		if got.q3, err = queryEventsByHour(created, cores); err != nil {
			t.Fatal(err)
		}
		if got.first, got.longest, err = queryPostTimes(posts, cores, 3); err != nil {
			t.Fatal(err)
		}
		for _, q := range benchSearches {
			hits, err := q.run(snap, cores)
			if err != nil {
				t.Fatal(err)
			}
			got.searches = append(got.searches, hits)
		}
		posts.Release()
		created.Release()
		snap.Release()
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			want = got
			t.Logf("search hits: %v", got.searches)

			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s gives different answers than arrival order:\nQ1 equal %v, Q2 equal %v, Q3 equal %v, Q4 equal %v, Q5 equal %v, searches %v",
				layout.name, reflect.DeepEqual(got.q1, want.q1), reflect.DeepEqual(got.q2, want.q2), reflect.DeepEqual(got.q3, want.q3),
				reflect.DeepEqual(got.first, want.first), reflect.DeepEqual(got.longest, want.longest), got.searches)
		}
	}
}
