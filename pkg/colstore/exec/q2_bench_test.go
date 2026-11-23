package exec_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
)

// blueskyDays writes rows events of benchdata spread over days days into a
// store in dir, partitioned by day and ordered as the server orders the
// Bluesky benchmark, and compacts it into one run per day.
func blueskyDays(b *testing.B, dir string, rows, days, users int) *colstore.Store {
	b.Helper()

	return blueskyStore(b, dir, rows, users, int64(days)*int64(24*time.Hour/time.Microsecond)/int64(max(rows, 1)))
}

// blueskyStore is blueskyDays for events whose times step by step
// microseconds on average.
func blueskyStore(b *testing.B, dir string, rows, users int, step int64) *colstore.Store {
	b.Helper()

	return blueskyPartitions(b, dir, rows, users, step, 24*time.Hour)
}

// blueskyPartitions is blueskyStore with partitions of the given length.
func blueskyPartitions(b testing.TB, dir string, rows, users int, step int64, partition time.Duration) *colstore.Store {
	b.Helper()

	return blueskyFrom(b, dir, rows, benchdata.New(1, users).Spread(step), partition)
}

// blueskyFrom is blueskyPartitions for the events of g.
func blueskyFrom(b testing.TB, dir string, rows int, g *benchdata.Generator, partition time.Duration) *colstore.Store {
	b.Helper()
	s, err := colstore.OpenStore(dir, colstore.StoreOptions{
		SortKey:    []string{"kind", "commit.operation", "commit.collection", "did"},
		TimeFields: []string{"time_us"},
		Partition:  partition,
		FlushLevel: part.LevelFastest,
	})
	if err != nil {
		b.Fatal(err)
	}
	const chunk = 100_000
	var lines []byte
	for left := rows; left > 0; left -= chunk {
		lines = g.Append(lines[:0], min(chunk, left))
		if _, err := s.Append(lines); err != nil {
			b.Fatal(err)
		}
	}
	if err := s.Sync(); err != nil {
		b.Fatal(err)
	}
	if err := s.Compact(); err != nil {
		b.Fatal(err)
	}

	return s
}

// envOr reads a positive integer setting of a benchmark.
func envOr(b *testing.B, name string, def int) int {
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

// q2 renders the rows of a result of the benchmark's second query.
func q2(res *exec.Result) string {
	var out []string
	for i := 0; i < res.Rows; i++ {
		row := res.Row(i, nil)
		out = append(out, fmt.Sprintf("%s=%d/%d", row[0].Bytes, row[1].Int, row[2].Int))
	}

	return strings.Join(out, " ")
}

// BenchmarkQ2Days times the benchmark's second query, events and distinct
// users by collection of the created commits, over a store of daily
// partitions, which a user active on several days spans; its answer must be
// that of a scan that hashes the groups. LYNXDB_BENCH_Q2_ROWS sets the events
// (2Mi by default), LYNXDB_BENCH_Q2_DAYS the days they cover (3),
// LYNXDB_BENCH_Q2_USERS the accounts that write them (one per 64 events) and
// LYNXDB_BENCH_Q2_WORKERS the goroutines of the query (one per CPU).
func BenchmarkQ2Days(b *testing.B) {
	rows := envOr(b, "LYNXDB_BENCH_Q2_ROWS", 1<<21)
	days := envOr(b, "LYNXDB_BENCH_Q2_DAYS", 3)
	opts := exec.Options{}
	if os.Getenv("LYNXDB_BENCH_Q2_WORKERS") != "" {
		opts.Workers = envOr(b, "LYNXDB_BENCH_Q2_WORKERS", 0)
	}
	s := blueskyDays(b, filepath.Join(b.TempDir(), "store"), rows, days, envOr(b, "LYNXDB_BENCH_Q2_USERS", rows/64))
	defer s.Close()
	snap := s.Table().Snapshot()
	defer snap.Release()
	created := exec.And(exec.Eq(str("kind"), exec.Str("commit")), exec.Eq(str("commit.operation"), exec.Str("create")))
	q := func(key *exec.Expr) exec.Node {
		return &exec.Sort{Input: &exec.Aggregate{
			Input: &exec.Scan{Filter: created},
			Keys:  []exec.Named{named("event", key)},
			Aggs:  []exec.Agg{{Name: "count", Func: exec.Count}, {Name: "users", Func: exec.CountDistinct, Arg: str("did")}},
		}, By: []exec.Order{{Column: "count", Desc: true}, {Column: "event"}}}
	}
	plan := q(str("commit.collection"))
	// Lower case leaves the collections as they are and keeps the groups from
	// the order of the rows.
	ref, err := exec.Run(context.Background(), snap, q(fn("lower", str("commit.collection"))), exec.Options{})
	if err != nil {
		b.Fatal(err)
	}
	res, err := exec.Run(context.Background(), snap, plan, opts)
	if err != nil {
		b.Fatal(err)
	}
	if !res.Stats.Ordered || ref.Stats.Ordered {
		b.Fatalf("the query ran in key order %v, the reference %v", res.Stats.Ordered, ref.Stats.Ordered)
	}
	if got, want := q2(res), q2(ref); got != want {
		b.Fatalf("answer\n %s\nwant\n %s", got, want)
	}
	partitions := map[int64]bool{}
	size := 0
	for _, h := range snap.Parts() {
		partitions[h.Partition] = true
		size += h.Size()
	}
	b.Logf("%d events, %.0f MB in %d parts of %d partitions; %d of %d blocks read for their distinct values: %s", rows,
		float64(size)/1e6, len(snap.Parts()), len(partitions), res.Stats.CountedByEntries, res.Stats.Blocks+res.Stats.CountedByEntries, q2(res))
	var cpu time.Duration
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		before := cpuTime()
		if _, err := exec.Run(context.Background(), snap, plan, opts); err != nil {
			b.Fatal(err)
		}
		cpu += cpuTime() - before
	}
	b.StopTimer()
	b.ReportMetric(cpu.Seconds()/float64(b.N)*1e3, "cpu-ms/op")
	b.ReportMetric(cpu.Seconds()/b.Elapsed().Seconds(), "cores")
}
