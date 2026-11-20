package exec_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
)

// q3Text is the benchmark's third query.
const q3Text = `source=bluesky | where kind = 'commit' and commit.operation = 'create' and commit.collection in ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like') | eval hour_of_day = hour(_time) | stats count() as count by commit.collection, hour_of_day | sort hour_of_day, commit.collection`

// rowTimes returns plan with every row time that a key of an aggregate
// reads replaced by the same time plus zero, which the executor computes
// row by row.
func rowTimes(n exec.Node) exec.Node {
	var bare func(e *exec.Expr) *exec.Expr
	bare = func(e *exec.Expr) *exec.Expr {
		if e == nil {
			return nil
		}
		if e.Op == exec.OpColumn && e.Path == "_time" {
			return exec.Add(e, exec.Int(0))
		}
		c := *e
		c.Args = make([]*exec.Expr, len(e.Args))
		for i, a := range e.Args {
			c.Args[i] = bare(a)
		}

		return &c
	}
	switch n := n.(type) {
	case *exec.Sort:
		c := *n
		c.Input = rowTimes(n.Input)

		return &c
	case *exec.Project:
		c := *n
		c.Input = rowTimes(n.Input)

		return &c
	case *exec.Filter:
		c := *n
		c.Input = rowTimes(n.Input)

		return &c
	case *exec.Aggregate:
		c := *n
		c.Keys = make([]exec.Named, len(n.Keys))
		for i, k := range n.Keys {
			c.Keys[i] = exec.Named{Name: k.Name, Expr: bare(k.Expr)}
		}

		return &c
	}

	return n
}

// rendered renders the rows of a result.
func rendered(res *exec.Result) string {
	var out []string
	for i := 0; i < res.Rows; i++ {
		var cells []string
		for _, v := range res.Row(i, nil) {
			cells = append(cells, fmt.Sprintf("%d:%d:%g:%v:%s", v.Kind, v.Int, v.Float, v.Bool, v.Bytes))
		}
		out = append(out, strings.Join(cells, "/"))
	}

	return strings.Join(out, " ")
}

// BenchmarkQ3Days times the benchmark's third query, created posts, reposts
// and likes by collection and hour of the day, over a store of daily
// partitions compacted as the server compacts them; its answer must be that
// of the same query computed from the time of every row.
// LYNXDB_BENCH_Q3_ROWS sets the events (2Mi by default) and
// LYNXDB_BENCH_Q3_DAYS the days they cover (3).
func BenchmarkQ3Days(b *testing.B) {
	rows := envOr(b, "LYNXDB_BENCH_Q3_ROWS", 1<<21)
	days := envOr(b, "LYNXDB_BENCH_Q3_DAYS", 3)
	s := blueskyDays(b, filepath.Join(b.TempDir(), "store"), rows, days, envOr(b, "LYNXDB_BENCH_Q3_USERS", rows/64))
	defer s.Close()
	snap := s.Table().Snapshot()
	defer snap.Release()
	plan, err := pplPlan(q3Text, time.Now())
	if err != nil {
		b.Fatal(err)
	}
	ref, err := exec.Run(context.Background(), snap, rowTimes(plan), exec.Options{})
	if err != nil {
		b.Fatal(err)
	}
	res, err := exec.Run(context.Background(), snap, plan, exec.Options{})
	if err != nil {
		b.Fatal(err)
	}
	if got, want := rendered(res), rendered(ref); got != want {
		b.Fatalf("answer\n %s\nwant\n %s", got, want)
	}
	blocks := 0
	var u part.Usage
	widths := map[int64]int{}
	for _, h := range snap.Parts() {
		blocks += h.NumBlocks()
		if err := h.AddUsage(&u, nil); err != nil {
			b.Fatal(err)
		}
		for i := 0; i < h.NumBlocks(); i++ {
			// The narrowest buckets the block counts its rows in.
			for _, granule := range []int64{int64(time.Minute), int64(time.Hour), 24 * int64(time.Hour), 0} {
				if _, width := h.TimeCounts(i, granule, nil); width != 0 || granule == 0 {
					widths[width/int64(time.Second)]++

					break
				}
			}
		}
	}
	counts := u.Regions["footer.time_counts"]
	b.Logf("%d events in %d blocks of %d parts, %.1f MB; the rows by time take %d bytes, %.0f a block, %.3f a row, buckets of seconds:blocks %v; %+v",
		rows, blocks, len(snap.Parts()), float64(u.Bytes)/1e6, counts, float64(counts)/float64(blocks), float64(counts)/float64(rows), widths, res.Stats)
	var cpu time.Duration
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		before := cpuTime()
		if _, err := exec.Run(context.Background(), snap, plan, exec.Options{}); err != nil {
			b.Fatal(err)
		}
		cpu += cpuTime() - before
	}
	b.StopTimer()
	b.ReportMetric(cpu.Seconds()/float64(b.N)*1e3, "cpu-ms/op")
	b.ReportMetric(cpu.Seconds()/b.Elapsed().Seconds(), "cores")
}
