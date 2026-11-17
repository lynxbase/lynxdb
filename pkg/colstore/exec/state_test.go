package exec_test

import (
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

func stateAggs() []exec.Agg {
	return []exec.Agg{
		{Name: "rows", Func: exec.Count},
		{Name: "min_msg", Func: exec.Min, Arg: str("msg")},
		{Name: "max_msg", Func: exec.Max, Arg: anyOf("msg")},
		{Name: "min_n", Func: exec.Min, Arg: anyOf("n")},
		{Name: "max_n", Func: exec.Max, Arg: anyOf("n")},
		{Name: "max_y", Func: exec.Max, Arg: str("nested.y")},
		{Name: "min_coll", Func: exec.Min, Arg: str("coll")},
		{Name: "max_lower", Func: exec.Max, Arg: exec.Lower(str("user"))},
		{Name: "min_nope", Func: exec.Min, Arg: anyOf("nope")},
	}
}

func statePlans(aggs []exec.Agg) map[string]exec.Node {
	created := exec.And(exec.Eq(str("kind"), exec.Str("commit")), exec.Eq(str("op"), exec.Str("create")))

	return map[string]exec.Node{
		"no keys":          &exec.Aggregate{Input: &exec.Scan{}, Aggs: aggs},
		"no keys, no rows": &exec.Aggregate{Input: &exec.Scan{Filter: exec.Eq(str("kind"), exec.Str("nope"))}, Aggs: aggs},
		"by dict":          &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("coll"), Aggs: aggs},
		"by two filtered":  &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll", "nested.y"), Aggs: aggs},
		"by flat":          &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("user"), Aggs: aggs},
		"by key order":     &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll", "user"), Aggs: aggs},
		"strings by key":   &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll", "user"), Aggs: aggs[:2]},
		"by mixed kinds":   &exec.Aggregate{Input: &exec.Scan{Filter: exec.Gt(num("nested.x"), exec.Int(2))}, Keys: keys("n"), Aggs: aggs},
		"by constant":      &exec.Aggregate{Input: &exec.Scan{Filter: exec.Eq(str("coll"), exec.Str("feed.post"))}, Keys: keys("coll"), Aggs: aggs},
	}
}

func TestStatesOfVariableSize(t *testing.T) {
	plans := statePlans(stateAggs())
	for _, limits := range [][2]int{{1 << 16, 4096}, {4, 0}, {1 << 16, 0}, {4, 1 << 20}} {
		restore := exec.SetLimits(limits[0], limits[1])
		eachLayout(t, 3000, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
			for name, plan := range plans {
				t.Run(fmt.Sprintf("%s/%d-%d", name, limits[0], limits[1]), func(t *testing.T) {
					check(t, snap, rows, plan, false, workers)
				})
			}
		})
		restore()
	}
}
