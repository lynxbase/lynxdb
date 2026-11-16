package colstore

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
)

// loadOrdered shreds data into sorted runs and merges them into at most
// parts parts with disjoint key ranges, so the table is one ordered run.
func loadOrdered(t testing.TB, data []byte, parts int, key []string) (*Table, int64) {
	t.Helper()
	cores := runtime.GOMAXPROCS(0)
	runs, rejected := orderedRuns(t, data, cores, max(cores, parts), 16384, key)
	if rejected > 0 {
		t.Logf("%d lines rejected as invalid JSON", rejected)
	}
	bufs := make([]bytes.Buffer, parts)
	n := mergeRanges(t, runs, parts, 16384, key, func(i int) io.Writer { return &bufs[i] })
	handles := make([]*Handle, 0, n)
	var size int64
	for i := 0; i < n; i++ {
		p, err := part.Open(bufs[i].Bytes())
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, NewHandle(p, nil))
		size += int64(p.Size())
	}
	tbl := NewTable()
	if err := tbl.Apply(handles, nil); err != nil {
		t.Fatal(err)
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
	// The arrival-ordered table doubles the memory the test needs.
	orderedOnly := os.Getenv("LYNXDB_BENCH_ORDERED_ONLY") != ""
	profileDir := os.Getenv("LYNXDB_BENCH_PROFILE")

	var plain *Table
	var plainSize int64
	start := time.Now()
	if !orderedOnly {
		plain, plainSize, _ = loadNDJSON(t, data, cores, 16384, part.Options{})
		defer plain.Close()
	}
	plainLoad := time.Since(start)
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
			id, name string
			run      func(workers int) error
		}{
			{"q1", "Q1", func(n int) (err error) { res.q1, err = queryTopEvents(snap, n); return }},
			{"q2", "Q2", func(n int) (err error) { res.q2, err = queryEventsWithUsers(created, n, false); return }},
			{"q2h", "Q2 hash", func(n int) (err error) { res.q2h, err = queryEventsWithUsers(created, n, true); return }},
			{"q3", "Q3", func(n int) (err error) { res.q3, err = queryEventsByHour(created, n); return }},
			{"q4", "Q4/Q5", func(n int) (err error) { res.first, res.longest, err = queryPostTimes(posts, n, 3); return }},
		}
		for _, q := range queries {
			var took [2][]time.Duration
			var cpu [2]time.Duration
			var held [2]float64
			for i, workers := range []int{cores, 1} {
				profile := ""
				if profileDir != "" {
					profile = filepath.Join(profileDir, fmt.Sprintf("%s_%s_%dw.prof", strings.Fields(label)[0], q.id, workers))
				}
				var err error
				took[i], cpu[i], err = timeRuns(7, profile, func() error { return q.run(workers) })
				if err != nil {
					t.Fatalf("%s: %v", q.name, err)
				}
				held[i] = float64(benchMemory.arenas.Load()+benchMemory.buffers.Load()) / 1e6
			}
			all, one := took[0], took[1]
			t.Logf("%-14s %-8s %8.2f ms on %d cores (median %.2f, cpu %.1f, holds %.1f MB), %8.2f ms on 1 core (median %.2f, cpu %.1f, holds %.1f MB); %d blocks opened, %d skipped by key, %d pass the filters with %d rows",
				label, q.name, ms(all[0]), cores, ms(all[3]), ms(cpu[0]), held[0], ms(one[0]), ms(one[3]), ms(cpu[1]), held[1],
				benchScan.opened.Load(), benchScan.skippedByKey.Load(), benchScan.blocks.Load(), benchScan.rows.Load())
		}

		return res
	}
	if orderedOnly {
		run(ordered, "ordered")

		return
	}
	want := run(plain, "arrival order")
	got := run(ordered, "ordered")
	if !reflect.DeepEqual(got, want) {
		t.Error("ordered parts give different answers than arrival-ordered parts")
	}
}
