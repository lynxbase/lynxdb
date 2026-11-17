package exec_test

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// TestFirstRowsAsTheyComeOpenFewBlocks requires that a plan which takes its
// first rows in the order of the scan opens no more blocks than hold them,
// however many goroutines scan, and that a limit beyond the rows gives all.
func TestFirstRowsAsTheyComeOpenFewBlocks(t *testing.T) {
	tbl := buildTable(t, synthDocs(4000, 3), "ts", layouts[0])
	snap := tbl.Snapshot()
	defer snap.Release()
	commits := exec.Eq(anyOf("kind"), exec.Str("commit"))
	plan := func(filter *exec.Expr, limit int) exec.Node {
		return &exec.Sort{Input: &exec.Project{Input: &exec.Scan{Filter: filter}, Exprs: []exec.Named{named("id", num("id"))}}, Limit: limit}
	}
	for name, c := range map[string]struct {
		plan         exec.Node
		rows, blocks int
	}{
		"every row":               {plan(nil, 5), 5, 1},
		"rows of a kind":          {plan(commits, 5), 5, 1},
		"more than a block holds": {plan(nil, 300), 300, 3},
		"all there are":           {plan(nil, 1<<20), 4000, 18},
	} {
		t.Run(name, func(t *testing.T) {
			res, err := exec.Run(context.Background(), snap, c.plan, exec.Options{Workers: 4, Forgetful: true})
			if err != nil {
				t.Fatal(err)
			}
			if res.Rows != c.rows || res.Stats.Blocks > int64(c.blocks) {
				t.Fatalf("%d rows from %d blocks, want %d rows from at most %d", res.Rows, res.Stats.Blocks, c.rows, c.blocks)
			}
			handed := 0
			err = exec.Each(context.Background(), snap, c.plan, exec.Options{Workers: 4, Forgetful: true}, func(part *exec.Result) error {
				handed += part.Rows

				return nil
			})
			if err != nil || handed != c.rows {
				t.Fatalf("%d rows handed over, want %d: %v", handed, c.rows, err)
			}
		})
	}
}
