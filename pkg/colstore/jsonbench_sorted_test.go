package colstore

import (
	"bytes"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// loadOrdered shreds data on every core into one ordered part per worker:
// every batch is sorted and written as a one-block part, and the worker
// then merges its blocks.
func loadOrdered(t testing.TB, data []byte, workers int, key []string) (*Table, int64) {
	t.Helper()
	opts := part.Options{Ordered: true, SortKey: key}
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
	handles := make([]*Handle, len(chunks))
	var rejected int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i, chunk := range chunks {
		wg.Add(1)
		go func(i int, chunk []byte) {
			defer wg.Done()
			batch := shred.Get(shred.Options{TimeField: "time_us"})
			defer shred.Put(batch)
			var blocks []*part.Part
			var errs []shred.LineError
			for len(chunk) > 0 {
				var lines []byte
				lines, chunk = shred.SplitLines(chunk, 16384)
				errs = batch.AddNDJSON(lines, 0, errs[:0])
				mu.Lock()
				rejected += int64(len(errs))
				mu.Unlock()
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
			var buf bytes.Buffer
			w := part.NewWriter(&buf, opts)
			if err := part.Merge(w, blocks, 16384); err != nil {
				t.Error(err)

				return
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
			handles[i] = NewHandle(p, nil)
		}(i, chunk)
	}
	wg.Wait()
	if rejected > 0 {
		t.Logf("%d lines rejected as invalid JSON", rejected)
	}
	tbl := NewTable()
	if err := tbl.Apply(handles, nil); err != nil {
		t.Fatal(err)
	}
	var size int64
	for _, h := range handles {
		size += int64(h.Size())
	}

	return tbl, size
}

// TestJSONBenchOrdered runs the benchmark queries over parts ordered by a
// sort key and over arrival-ordered parts, and requires equal answers.
func TestJSONBenchOrdered(t *testing.T) {
	path := os.Getenv("LYNXDB_BENCH_NDJSON")
	if path == "" {
		t.Skip("set LYNXDB_BENCH_NDJSON to a Bluesky NDJSON file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	key := []string{"kind", "commit.operation", "commit.collection", "did"}
	if s := os.Getenv("LYNXDB_BENCH_SORT"); s != "" {
		key = strings.Split(s, ",")
	}
	cores := runtime.GOMAXPROCS(0)

	start := time.Now()
	plain, plainSize, _ := loadNDJSON(t, data, cores, 16384, part.Options{})
	plainLoad := time.Since(start)
	defer plain.Close()
	parts := cores
	if s := os.Getenv("LYNXDB_BENCH_PARTS"); s != "" {
		if parts, err = strconv.Atoi(s); err != nil || parts < 1 {
			t.Fatalf("LYNXDB_BENCH_PARTS=%q", s)
		}
	}
	start = time.Now()
	ordered, orderedSize := loadOrdered(t, data, parts, key)
	orderedLoad := time.Since(start)
	defer ordered.Close()
	t.Logf("arrival order: %.1f MB, load %.2fs; ordered by %v in %d parts: %.1f MB, load %.2fs",
		float64(plainSize)/1e6, plainLoad.Seconds(), key, parts, float64(orderedSize)/1e6, orderedLoad.Seconds())

	type result struct {
		q1, q2, q2h    []eventCount
		q3             []hourCount
		first, longest []userSpan
	}
	run := func(tbl *Table, label string) result {
		snap := tbl.Snapshot()
		defer snap.Release()
		// The queries' own filters, stated as sort key prefixes so that
		// ordered parts skip the blocks outside them.
		paths := []string{"kind", "commit.operation", "commit.collection"}
		created := snap.Narrow(KeyFilter{Paths: paths[:2], Values: [][]byte{[]byte("commit"), []byte("create")}})
		defer created.Release()
		posts := snap.Narrow(KeyFilter{Paths: paths, Values: [][]byte{[]byte("commit"), []byte("create"), []byte("app.bsky.feed.post")}})
		defer posts.Release()
		var res result
		queries := []struct {
			name string
			run  func(workers int) error
		}{
			{"Q1", func(n int) (err error) { res.q1, err = queryTopEvents(snap, n); return }},
			{"Q2", func(n int) (err error) { res.q2, err = queryEventsWithUsers(created, n, false); return }},
			{"Q2 hash", func(n int) (err error) { res.q2h, err = queryEventsWithUsers(created, n, true); return }},
			{"Q3", func(n int) (err error) { res.q3, err = queryEventsByHour(created, n); return }},
			{"Q4/Q5", func(n int) (err error) { res.first, res.longest, err = queryPostTimes(posts, n, 3); return }},
		}
		for _, q := range queries {
			var best [2]time.Duration
			for i, workers := range []int{cores, 1} {
				for r := 0; r < 7; r++ {
					begin := time.Now()
					if err := q.run(workers); err != nil {
						t.Fatalf("%s: %v", q.name, err)
					}
					if took := time.Since(begin); r == 0 || took < best[i] {
						best[i] = took
					}
				}
			}
			t.Logf("%-14s %-6s %8.2f ms on %d cores, %8.2f ms on 1 core; %d blocks opened, %d skipped by key, %d pass the filters with %d rows; holds %.1f MB",
				label, q.name, float64(best[0].Microseconds())/1e3, cores, float64(best[1].Microseconds())/1e3,
				benchScan.opened.Load(), benchScan.skippedByKey.Load(), benchScan.blocks.Load(), benchScan.rows.Load(),
				float64(benchMemory.arenas.Load()+benchMemory.buffers.Load())/1e6)
		}

		return res
	}
	want := run(plain, "arrival order")
	got := run(ordered, "ordered")
	if !reflect.DeepEqual(got, want) {
		t.Error("ordered parts give different answers than arrival-ordered parts")
	}
}
