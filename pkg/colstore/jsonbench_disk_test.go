package colstore

import (
	"bytes"
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

// loadToDir shreds data on every core into part files of d, one per worker,
// and commits them in one change; a non-empty key orders each part by it. It
// returns the number of lines that are not JSON objects.
func loadToDir(t testing.TB, d *Dir, data []byte, workers, blockRows int, key []string) int64 {
	t.Helper()
	opts := part.Options{Ordered: len(key) > 0, SortKey: key}
	chunks := make([][]byte, 0, workers)
	for rest := data; len(rest) > 0; {
		n := min(len(data)/workers+1, len(rest))
		if n < len(rest) {
			if nl := bytes.IndexByte(rest[n:], '\n'); nl >= 0 {
				n += nl + 1
			} else {
				n = len(rest)
			}
		}
		chunks = append(chunks, rest[:n])
		rest = rest[n:]
	}
	files := make([]*PartFile, len(chunks))
	var rejected atomic.Int64
	var wg sync.WaitGroup
	for i, chunk := range chunks {
		wg.Add(1)
		go func(i int, chunk []byte) {
			defer wg.Done()
			pf, err := d.Create()
			if err != nil {
				t.Error(err)

				return
			}
			files[i] = pf
			out := part.NewWriter(pf, opts)
			batch := shred.Get(shred.Options{TimeField: "time_us"})
			defer shred.Put(batch)
			var blocks []*part.Part
			var errs []shred.LineError
			for len(chunk) > 0 {
				var lines []byte
				lines, chunk = shred.SplitLines(chunk, blockRows)
				errs = batch.AddNDJSON(lines, 0, errs[:0])
				rejected.Add(int64(len(errs)))
				if !opts.Ordered {
					if err := out.WriteBatch(batch); err != nil {
						t.Error(err)
					}
					batch.Reset()

					continue
				}
				// An ordered part is the merge of its sorted blocks.
				batch.SortBy(key)
				var buf bytes.Buffer
				w := part.NewWriter(&buf, opts)
				if err := w.WriteBatch(batch); err != nil || w.Close() != nil {
					t.Error("block write failed")

					return
				}
				batch.Reset()
				p, err := part.Open(buf.Bytes())
				if err != nil {
					t.Error(err)

					return
				}
				blocks = append(blocks, p)
			}
			if opts.Ordered {
				if err := part.Merge(out, blocks, blockRows); err != nil {
					t.Error(err)
				}
			}
			if err := out.Close(); err != nil {
				t.Error(err)
			}
		}(i, chunk)
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	if _, err := d.Commit(files, nil); err != nil {
		t.Fatal(err)
	}

	return rejected.Load()
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
