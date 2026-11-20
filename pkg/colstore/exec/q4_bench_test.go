package exec_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// postsScan is the scan of the created posts of the Bluesky benchmark.
func postsScan() *exec.Scan {
	return &exec.Scan{Filter: exec.And(exec.Eq(str("kind"), exec.Str("commit")), exec.Eq(str("commit.operation"), exec.Str("create")),
		exec.Eq(str("commit.collection"), exec.Str("app.bsky.feed.post")))}
}

// timeQuery is a query whose answer is checked against that of a reference
// plan, which the executor runs another way.
type timeQuery struct {
	name      string
	plan, ref exec.Node
	workers   int
}

// runTimeQueries times each query and its reference over snap, and requires
// the reference's answer of every run of the query.
func runTimeQueries(b *testing.B, snap *colstore.Snapshot, queries []timeQuery) {
	for _, tq := range queries {
		opts := exec.Options{Workers: tq.workers}
		ref, err := exec.Run(context.Background(), snap, tq.ref, opts)
		if err != nil {
			b.Fatal(err)
		}
		want := rendered(ref)
		b.Run(tq.name, func(b *testing.B) {
			b.Run("rewritten", func(b *testing.B) { timeQueryRuns(b, snap, tq.plan, opts, want) })
			b.Run("reference", func(b *testing.B) { timeQueryRuns(b, snap, tq.ref, opts, want) })
		})
	}
}

func timeQueryRuns(b *testing.B, snap *colstore.Snapshot, plan exec.Node, opts exec.Options, want string) {
	res, err := exec.Run(context.Background(), snap, plan, opts)
	if err != nil {
		b.Fatal(err)
	}
	head := want
	if fields := strings.Fields(want); len(fields) > 6 {
		head = strings.Join(fields[:6], " ") + " ..."
	}
	b.Logf("%d of %d blocks read, %d rows, in key order %v: %s",
		res.Stats.Blocks, res.Stats.Blocks+res.Stats.SkippedByTime, res.Stats.RowsMatched, res.Stats.Ordered, head)
	var cpu time.Duration
	rows := int64(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		before := cpuTime()
		res, err := exec.Run(context.Background(), snap, plan, opts)
		cpu += cpuTime() - before
		if err != nil {
			b.Fatal(err)
		}
		if got := rendered(res); got != want {
			b.Fatalf("answer\n %s\nwant\n %s", got, want)
		}
		rows += res.Stats.RowsMatched
	}
	b.StopTimer()
	b.ReportMetric(cpu.Seconds()/float64(b.N)*1e3, "cpu-ms/op")
	b.ReportMetric(cpu.Seconds()/b.Elapsed().Seconds(), "cores")
	b.ReportMetric(float64(rows)/cpu.Seconds()/1e6, "Mrows/cpu-s")
}

// earliestBy is the plan of the groups by did with the earliest, or the
// latest, value of arg, sorted by that first and kept to limit.
func earliestBy(latest bool, arg *exec.Expr, by []exec.Order, limit int) exec.Node {
	fn := exec.Min
	if latest {
		fn = exec.Max
	}

	return &exec.Sort{Input: &exec.Aggregate{
		Input: postsScan(),
		Keys:  []exec.Named{named("did", str("did"))},
		Aggs:  []exec.Agg{{Name: "at", Func: fn, Arg: arg}},
	}, By: by, Limit: limit}
}

// earliestQueries are the shapes of the benchmark's fourth query, each with
// a reference that computes the time and so leaves every block to be read.
func earliestQueries(name string, workers int) []timeQuery {
	plus0 := exec.Add(exec.Time(), exec.Int(0))
	var out []timeQuery
	for _, c := range []struct {
		name   string
		latest bool
		by     []exec.Order
		limit  int
	}{
		{"earliest", false, []exec.Order{{Column: "at"}, {Column: "did"}}, 3},
		{"earliest by time alone", false, []exec.Order{{Column: "at"}}, 3},
		{"earliest hundred", false, []exec.Order{{Column: "at"}}, 100},
		{"latest", true, []exec.Order{{Column: "at", Desc: true}}, 3},
	} {
		out = append(out, timeQuery{
			name:    name + " " + c.name,
			plan:    earliestBy(c.latest, exec.Time(), c.by, c.limit),
			ref:     earliestBy(c.latest, plus0, c.by, c.limit),
			workers: workers,
		})
	}

	return out
}

// BenchmarkQ4Days times the shape of the benchmark's fourth query, the
// accounts with the earliest posts, over a store of daily partitions and over
// one whose events share their times in runs, where groups tie at the limit.
// LYNXDB_BENCH_Q4_ROWS sets the events (2Mi by default), LYNXDB_BENCH_Q4_DAYS
// the days they cover (3) and LYNXDB_BENCH_Q4_WORKERS the goroutines of the
// query (one per CPU); the store of ties is also read by one goroutine, which
// takes its blocks one after another and so leaves some out.
func BenchmarkQ4Days(b *testing.B) {
	rows := envOr(b, "LYNXDB_BENCH_Q4_ROWS", 1<<21)
	days := envOr(b, "LYNXDB_BENCH_Q4_DAYS", 3)
	workers := 0
	if os.Getenv("LYNXDB_BENCH_Q4_WORKERS") != "" {
		workers = envOr(b, "LYNXDB_BENCH_Q4_WORKERS", 0)
	}
	for _, store := range []struct {
		name string
		step int64
	}{
		{"days", int64(days) * int64(24*time.Hour/time.Microsecond) / int64(max(rows, 1))},
		{"ties", 1},
	} {
		dir := filepath.Join(b.TempDir(), store.name)
		s := blueskyStore(b, dir, rows, rows/64, store.step)
		snap := s.Table().Snapshot()
		b.Logf("%s: %s", store.name, describeStore(snap))
		queries := earliestQueries(store.name, workers)
		if store.name == "ties" {
			queries = append(queries, earliestQueries("ties, one goroutine,", 1)...)
		}
		// What the writes left to collect and to return would count as the
		// time of the queries.
		debug.FreeOSMemory()
		runTimeQueries(b, snap, queries)
		snap.Release()
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}
}

// describeStore tells the size and the layout of the store of snap.
func describeStore(snap *colstore.Snapshot) string {
	partitions := map[int64]bool{}
	size, blocks := 0, 0
	for _, h := range snap.Parts() {
		partitions[h.Partition] = true
		size += h.Size()
		blocks += h.NumBlocks()
	}

	return fmt.Sprintf("%.0f MB in %d parts of %d partitions, %d blocks", float64(size)/1e6, len(snap.Parts()), len(partitions), blocks)
}
