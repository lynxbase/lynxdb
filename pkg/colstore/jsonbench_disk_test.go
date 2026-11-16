package colstore

import (
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// loadToDir shreds data on every core into part files of d and commits them
// in one change: one arrival-ordered part per worker, or, with a key, parts
// with disjoint key ranges merged from sorted runs. It returns the number of
// lines that are not JSON objects.
func loadToDir(t testing.TB, d *Dir, data []byte, parts, blockRows int, key []string) int64 {
	t.Helper()
	files := make([]*PartFile, parts)
	create := func(i int) io.Writer {
		pf, err := d.Create()
		if err != nil {
			t.Error(err)

			return io.Discard
		}
		files[i] = pf

		return pf
	}
	var rejected int64
	if len(key) > 0 {
		cores := runtime.GOMAXPROCS(0)
		var runs []*part.Part
		runs, rejected = orderedRuns(t, data, cores, max(cores, parts), blockRows, key)
		files = files[:mergeRanges(t, runs, parts, blockRows, key, create)]
	} else {
		chunks := splitAtLines(data, parts)
		files = files[:len(chunks)]
		var count atomic.Int64
		var wg sync.WaitGroup
		for i, chunk := range chunks {
			wg.Add(1)
			out := part.NewWriter(create(i), part.Options{})
			go func(chunk []byte) {
				defer wg.Done()
				batch := shred.Get(shred.Options{TimeField: "time_us"})
				defer shred.Put(batch)
				var errs []shred.LineError
				for len(chunk) > 0 {
					var lines []byte
					lines, chunk = shred.SplitLines(chunk, blockRows)
					errs = batch.AddNDJSON(lines, 0, errs[:0])
					count.Add(int64(len(errs)))
					if err := out.WriteBatch(batch); err != nil {
						t.Error(err)
					}
					batch.Reset()
				}
				if err := out.Close(); err != nil {
					t.Error(err)
				}
			}(chunk)
		}
		wg.Wait()
		rejected = count.Load()
	}
	if t.Failed() {
		t.FailNow()
	}
	if _, err := d.Commit(files, nil); err != nil {
		t.Fatal(err)
	}

	return rejected
}

// TestJSONBenchDisk times loading to disk and the queries over the table in
// the directory named by LYNXDB_BENCH_DIR. An empty directory is loaded from
// LYNXDB_BENCH_NDJSON first, with every part fsynced before it is visible;
// a directory that already holds the table is only reopened, so dropping the
// page cache before the run makes each query's first run a cold one.
// LYNXDB_BENCH_SORT orders the parts by a comma-separated key ("default"
// for the benchmark's own), LYNXDB_BENCH_PARTS sets their number.
func TestJSONBenchDisk(t *testing.T) {
	dir := os.Getenv("LYNXDB_BENCH_DIR")
	if dir == "" {
		t.Skip("set LYNXDB_BENCH_DIR to a directory for the table")
	}
	cores := runtime.GOMAXPROCS(0)
	runs := envInt(t, "LYNXDB_BENCH_RUNS", 7)
	blockRows := envInt(t, "LYNXDB_BENCH_BLOCK_ROWS", 16384)
	var key []string
	switch s := os.Getenv("LYNXDB_BENCH_SORT"); s {
	case "":
	case "default":
		key = []string{"kind", "commit.operation", "commit.collection", "did"}
	default:
		key = strings.Split(s, ",")
	}

	begin := time.Now()
	d, err := OpenDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	opened := time.Since(begin)
	snap := d.Table().Snapshot()
	if len(snap.Parts()) == 0 {
		snap.Release()
		path := os.Getenv("LYNXDB_BENCH_NDJSON")
		if path == "" {
			t.Fatal("the directory is empty; set LYNXDB_BENCH_NDJSON to the file to load")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		cpuBefore, begin := cpuTime(), time.Now()
		rejected := loadToDir(t, d, data, envInt(t, "LYNXDB_BENCH_PARTS", cores), blockRows, key)
		took, cpu := time.Since(begin), cpuTime()-cpuBefore
		t.Logf("load to disk: %.1f MB of JSON in %.2fs wall, %.2fs CPU (%.0f MB/s), %d lines rejected, key %v",
			float64(len(data))/1e6, took.Seconds(), cpu.Seconds(), float64(len(data))/1e6/took.Seconds(), rejected, key)
		snap = d.Table().Snapshot()
	} else {
		t.Logf("reopened the table in %.1f ms", ms(opened))
	}
	defer snap.Release()

	var rows uint64
	var size int64
	blocks := 0
	for _, h := range snap.Parts() {
		rows += h.Rows
		size += int64(h.Size())
		blocks += h.NumBlocks()
	}
	t.Logf("table: %d rows, %.1f MB on disk in %d parts / %d blocks", rows, float64(size)/1e6, len(snap.Parts()), blocks)

	paths := []string{"kind", "commit.operation", "commit.collection"}
	created := snap.Narrow(KeyFilter{Paths: paths[:2], Values: [][]byte{[]byte("commit"), []byte("create")}})
	defer created.Release()
	posts := snap.Narrow(KeyFilter{Paths: paths, Values: [][]byte{[]byte("commit"), []byte("create"), []byte("app.bsky.feed.post")}})
	defer posts.Release()
	queries := []struct {
		name string
		run  func() error
	}{
		{"Q1", func() error { _, err := queryTopEvents(snap, cores); return err }},
		{"Q2", func() error { _, err := queryEventsWithUsers(created, cores, false); return err }},
		{"Q2 hash", func() error { _, err := queryEventsWithUsers(created, cores, true); return err }},
		{"Q3", func() error { _, err := queryEventsByHour(created, cores); return err }},
		{"Q4/Q5", func() error { _, _, err := queryPostTimes(posts, cores, 3); return err }},
	}
	for _, q := range queries {
		begin := time.Now()
		if err := q.run(); err != nil {
			t.Fatalf("%s: %v", q.name, err)
		}
		first := time.Since(begin)
		took, cpu, err := timeRuns(runs, "", q.run)
		if err != nil {
			t.Fatalf("%s: %v", q.name, err)
		}
		t.Logf("%-8s first run %9.2f ms, best of %d %9.2f ms on %d cores, CPU %9.2f ms, holds %.1f MB",
			q.name, ms(first), runs, ms(took[0]), cores, ms(cpu),
			float64(benchMemory.arenas.Load()+benchMemory.buffers.Load())/1e6)
	}
}
