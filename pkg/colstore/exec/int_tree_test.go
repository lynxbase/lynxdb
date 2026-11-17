package exec_test

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// TestIntegerTreesKeepWhichRowsHaveNoValue requires, of expressions over
// integer columns that some rows lack, no value in the rows where a branch
// has none, whichever branch is computed first.
func TestIntegerTreesKeepWhichRowsHaveNoValue(t *testing.T) {
	a, b := num("n"), num("big")
	left := exec.Add(exec.Mul(a, exec.Int(2)), b)
	right := exec.Add(b, exec.Mul(a, exec.Int(2)))
	deep := exec.Sub(exec.Add(exec.Mul(a, exec.Int(3)), exec.Mod(b, exec.Int(7))), exec.Div(exec.Mul(b, exec.Int(2)), exec.Add(a, exec.Int(1))))
	hours := exec.Add(exec.HourOfDay(exec.Mul(a, exec.Int(3_600_000_000_000))), b)
	sum := func(e *exec.Expr) exec.Node {
		return &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("kind"),
			Aggs: []exec.Agg{{Name: "s", Func: exec.Sum, Arg: e}, {Name: "n", Func: exec.Count, Arg: e}}}
	}
	cases := map[string]exec.Node{
		"sum, a branch of a column first":  sum(left),
		"sum, a column first":              sum(right),
		"sum of branches of branches":      sum(deep),
		"sum over the hour of a product":   sum(hours),
		"rows":                             &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{named("id", num("id")), named("x", left), named("y", deep)}},
		"filter by a branch and a column":  &exec.Aggregate{Input: &exec.Scan{Filter: exec.Gt(exec.Mul(a, exec.Int(1_000_000_000_000_000)), b)}, Aggs: []exec.Agg{{Name: "n", Func: exec.Count}}},
		"filter by a column and a branch":  &exec.Aggregate{Input: &exec.Scan{Filter: exec.Le(b, exec.Add(exec.Mul(a, exec.Int(3)), exec.Int(-5)))}, Aggs: []exec.Agg{{Name: "n", Func: exec.Count}}},
		"filter by branches of both sides": &exec.Aggregate{Input: &exec.Scan{Filter: exec.Ne(exec.Mod(exec.Mul(a, exec.Int(2)), exec.Int(3)), exec.Mod(exec.Add(b, exec.Int(1)), exec.Int(3)))}, Aggs: []exec.Agg{{Name: "n", Func: exec.Count}}},
	}
	eachLayout(t, 3000, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
		for name, plan := range cases {
			t.Run(name, func(t *testing.T) {
				check(t, snap, rows, plan, false, workers)
			})
		}
	})
}
