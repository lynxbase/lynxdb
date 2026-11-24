package exec_test

import (
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// TestGroupsMergedWhileScanning runs aggregations whose workers have no
// budget to hold groups in, so that they merge them into the partitions
// they share while they scan: after every group, and after every few
// hundred bytes of them.
func TestGroupsMergedWhileScanning(t *testing.T) {
	aggs := []exec.Agg{
		{Name: "rows", Func: exec.Count},
		{Name: "sum_n", Func: exec.Sum, Arg: anyOf("n")},
		{Name: "first", Func: exec.Min, Arg: exec.Time()},
		{Name: "last", Func: exec.Max, Arg: exec.Time()},
		{Name: "colls", Func: exec.CountDistinct, Arg: str("coll")},
		{Name: "latest", Func: exec.Latest, Arg: anyOf("coll")},
		{Name: "users", Func: exec.CountDistinct, Arg: str("user")},
		{Name: "numbers", Func: exec.CountDistinct, Arg: anyOf("n")},
	}
	plans := map[string]exec.Node{
		"no keys":       &exec.Aggregate{Input: &exec.Scan{}, Aggs: aggs},
		"by coll":       &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("coll"), Aggs: aggs},
		"by flat":       &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("user"), Aggs: aggs},
		"by flat, rows": &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("user"), Aggs: aggs[:1]},
		"by dict":       &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("coll", "nested.x"), Aggs: aggs},
		"by mixed":      &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("n", "kind"), Aggs: aggs},
		"top": &exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("user"), Aggs: aggs[:2]},
			By: []exec.Order{{Column: "rows", Desc: true}, {Column: "user"}}, Limit: 7},
	}
	for name, plan := range statePlans(stateAggs()) {
		plans["states "+name] = plan
	}
	for _, c := range []struct{ budget, tableGroups int }{{1, 1 << 16}, {1, 4}, {2000, 1 << 16}, {2000, 4}} {
		restoreBudget, restoreLimits := exec.SetStageBudget(c.budget), exec.SetLimits(c.tableGroups, 0)
		eachLayout(t, 3000, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
			for name, plan := range plans {
				t.Run(fmt.Sprintf("%s/%d-%d", name, c.budget, c.tableGroups), func(t *testing.T) {
					_, sorted := plan.(*exec.Sort)
					check(t, snap, rows, plan, sorted, workers)
				})
			}
		})
		restoreLimits()
		restoreBudget()
	}
}
