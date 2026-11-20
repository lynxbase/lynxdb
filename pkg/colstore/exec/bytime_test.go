package exec_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// TestFirstRowsByTimeLeaveBlocksOut requires that a plan which keeps the
// latest or the earliest rows gives the rows of the reference evaluator and
// does not open the blocks whose rows the kept ones rule out.
func TestFirstRowsByTimeLeaveBlocksOut(t *testing.T) {
	docs := synthDocs(6000, 13)
	rows := referenceRows(t, docs, "ts")
	id := named("id", anyOf("id"))
	at := named("at", exec.Time())
	for _, l := range []layout{{name: "blocks", blockRows: 64, parts: 1}, {name: "parts", blockRows: 100, parts: 3},
		{name: "ordered", blockRows: 128, parts: 2, sortKey: []string{"kind", "user"}}} {
		tbl := buildTable(t, docs, "ts", l)
		snap := tbl.Snapshot()
		for name, c := range map[string]struct {
			plan exec.Node
			// skips is set when most blocks hold only rows that the kept
			// ones rule out.
			skips bool
		}{
			"the latest": {&exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{at, id}},
				By: []exec.Order{{Column: "at", Desc: true}, {Column: "id"}}, Limit: 7}, true},
			"the earliest": {&exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{id, at}},
				By: []exec.Order{{Column: "at"}, {Column: "id", Desc: true}}, Limit: 5}, true},
			"the latest of a kind, as whole rows": {&exec.Sort{Input: &exec.Project{Input: &exec.Scan{Filter: exec.Eq(anyOf("kind"), exec.Str("commit"))},
				Exprs: []exec.Named{at, id}, Rest: true}, By: []exec.Order{{Column: "at", Desc: true}, {Column: "id"}}, Limit: 20}, true},
			"the latest of a few": {&exec.Sort{Input: &exec.Project{Input: &exec.Scan{Filter: exec.Eq(anyOf("user"), exec.Str("u0001"))},
				Exprs: []exec.Named{at, id}}, By: []exec.Order{{Column: "at", Desc: true}, {Column: "id"}}, Limit: 1000}, false},
			"by another column first": {&exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{at, id}},
				By: []exec.Order{{Column: "id", Desc: true}, {Column: "at"}}, Limit: 7}, false},
		} {
			for _, workers := range []int{1, 4} {
				t.Run(l.name+"/"+name, func(t *testing.T) {
					check(t, snap, rows, c.plan, true, workers)
					res, err := exec.Run(context.Background(), snap, c.plan, exec.Options{Workers: workers})
					if err != nil {
						t.Fatal(err)
					}
					blocks := 0
					for _, h := range snap.Parts() {
						blocks += h.NumBlocks()
					}
					left := res.Stats.SkippedByTime
					switch {
					case !c.skips && left != 0:
						t.Errorf("%d of %d blocks left out by time", left, blocks)
					case c.skips && l.sortKey == nil && left < int64(blocks)/2:
						t.Errorf("%d of %d blocks left out by time, want most", left, blocks)
					}
					// One part in the order of time keeps what the plan asks
					// for in its first blocks, however many goroutines scan.
					if c.skips && l.parts == 1 && res.Stats.Blocks > 2 {
						t.Errorf("%d blocks opened of a part in the order of time, want at most 2", res.Stats.Blocks)
					}
				})
			}
		}
		snap.Release()
	}
}

// TestEarliestGroupsLeaveBlocksOut requires that the groups with the
// earliest or the latest times, kept by a sort with a limit, are those of
// the reference evaluator however many groups share the time at the limit,
// and that most blocks are left out where blocks follow the order of time.
func TestEarliestGroupsLeaveBlocksOut(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	docs := make([][]byte, 4000)
	for i := range docs {
		// Runs of rows share a time, so that groups tie at every limit.
		docs[i] = fmt.Appendf(nil, `{"ts":%d,"kind":%q,"user":"u%03d"}`, 1_700_000_000_000_000+int64(i/40)*1_000_000,
			synthKinds[rng.Intn(len(synthKinds))], rng.Intn(300))
	}
	rows := referenceRows(t, docs, "ts")
	user := named("user", anyOf("user"))
	plus0 := exec.Add(exec.Time(), exec.Int(0))
	by := func(fn exec.AggFunc, arg *exec.Expr, filter *exec.Expr, limit int, order ...exec.Order) exec.Node {
		return &exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{Filter: filter}, Keys: []exec.Named{user},
			Aggs: []exec.Agg{{Name: "at", Func: fn, Arg: arg}}}, By: order, Limit: limit}
	}
	commits := exec.Eq(anyOf("kind"), exec.Str("commit"))
	for _, l := range []layout{{name: "blocks", blockRows: 64, parts: 1}, {name: "parts", blockRows: 100, parts: 3},
		{name: "ordered", blockRows: 128, parts: 2, sortKey: []string{"kind", "user"}}} {
		tbl := buildTable(t, docs, "ts", l)
		snap := tbl.Snapshot()
		for _, c := range []struct {
			name   string
			fn     exec.AggFunc
			filter *exec.Expr
			limit  int
			desc   bool
			nulls  bool
		}{
			{"the earliest", exec.Min, nil, 5, false, false},
			{"the earliest commits", exec.Min, commits, 3, false, false},
			{"the earliest, nulls first", exec.Min, nil, 4, false, true},
			{"the latest", exec.Max, nil, 7, true, false},
			{"the earliest of many", exec.Min, nil, 200, false, false},
		} {
			for _, workers := range []int{1, 4} {
				t.Run(fmt.Sprintf("%s/%s/%d", l.name, c.name, workers), func(t *testing.T) {
					at := exec.Order{Column: "at", Desc: c.desc, NullsFirst: c.nulls}
					plan := by(c.fn, exec.Time(), c.filter, c.limit, at, exec.Order{Column: "user"})
					res := check(t, snap, rows, plan, true, workers)
					if blocks := snap.Parts()[0].NumBlocks() * len(snap.Parts()); workers == 1 && l.sortKey == nil && c.limit < 10 &&
						res.Stats.SkippedByTime < int64(blocks)/2 {
						t.Errorf("%d of %d blocks left out by time, want most", res.Stats.SkippedByTime, blocks)
					}
					// Groups that a sort by time alone leaves equal are kept
					// in the order of their other columns either way.
					alone, err := exec.Run(context.Background(), snap, by(c.fn, exec.Time(), c.filter, c.limit, at), exec.Options{Workers: workers})
					if err != nil {
						t.Fatal(err)
					}
					ref, err := exec.Run(context.Background(), snap, by(c.fn, plus0, c.filter, c.limit, at), exec.Options{Workers: workers})
					if err != nil {
						t.Fatal(err)
					}
					if got, want := resultRows(alone), resultRows(ref); fmt.Sprint(got) != fmt.Sprint(want) {
						t.Errorf("by time alone\n got  %v\n want %v", got, want)
					}
				})
			}
		}
		snap.Release()
	}
}
