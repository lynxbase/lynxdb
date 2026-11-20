package exec_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// spanPlan is the benchmark's fifth query, the accounts whose posts span the
// longest time, with the accounts grouped by key.
func spanPlan(key *exec.Expr) exec.Node {
	return &exec.Sort{Input: &exec.Project{
		Input: &exec.Aggregate{
			Input: postsScan(),
			Keys:  []exec.Named{named("did", key)},
			Aggs:  []exec.Agg{{Name: "first", Func: exec.Min, Arg: exec.Time()}, {Name: "last", Func: exec.Max, Arg: exec.Time()}},
		},
		Exprs: []exec.Named{named("did", anyOf("did")), named("span_ms", exec.Sub(
			exec.Div(anyOf("last"), exec.Int(1_000_000)), exec.Div(anyOf("first"), exec.Int(1_000_000))))},
	}, By: []exec.Order{{Column: "span_ms", Desc: true}, {Column: "did"}}, Limit: 3}
}

// BenchmarkQ5 times the benchmark's fifth query over a store of one
// partition, as the server keeps the benchmark, against the same query with
// the accounts grouped by a computed key, which hashes them. The accounts
// write as the generator has them, and in the mix of the network, where a
// few accounts write so much that a block of posts holds the accounts as a
// dictionary. LYNXDB_BENCH_Q5_ROWS sets the events (2Mi by default), spread
// over a day, and LYNXDB_BENCH_Q5_WORKERS the goroutines of the query (one
// per CPU); the query is also timed on one goroutine.
func BenchmarkQ5(b *testing.B) {
	rows := envOr(b, "LYNXDB_BENCH_Q5_ROWS", 1<<21)
	workers := 0
	if os.Getenv("LYNXDB_BENCH_Q5_WORKERS") != "" {
		workers = envOr(b, "LYNXDB_BENCH_Q5_WORKERS", 0)
	}
	step := int64(24*time.Hour/time.Microsecond) / int64(max(rows, 1))
	for _, store := range []struct {
		name   string
		favour int
	}{{"generated", 2}, {"bots", 4}} {
		dir := filepath.Join(b.TempDir(), store.name)
		s := blueskyFrom(b, dir, rows, benchdata.New(1, rows/64).Spread(step).Favour(store.favour), 366*24*time.Hour)
		snap := s.Table().Snapshot()
		b.Logf("%s: %s", store.name, describeStore(snap))
		// What the writes left to collect would count as the time of the
		// queries.
		runtime.GC()
		plan, ref := spanPlan(str("did")), spanPlan(exec.Lower(str("did")))
		runTimeQueries(b, snap, []timeQuery{
			{name: store.name, plan: plan, ref: ref, workers: workers},
			{name: store.name + ", one goroutine", plan: plan, ref: ref, workers: 1},
		})
		snap.Release()
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}
}
