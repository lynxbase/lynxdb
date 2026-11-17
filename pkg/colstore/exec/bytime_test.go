package exec_test

import (
	"context"
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
				})
			}
		}
		snap.Release()
	}
}
