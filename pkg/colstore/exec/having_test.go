package exec_test

import (
	"context"
	"errors"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

func TestFilterAboveAggregate(t *testing.T) {
	eq := func(path, v string) *exec.Expr { return exec.Eq(str(path), exec.Str(v)) }
	aggs := []exec.Agg{
		{Name: "rows", Func: exec.Count},
		{Name: "users", Func: exec.CountDistinct, Arg: str("user")},
		{Name: "sum_n", Func: exec.Sum, Arg: num("n")},
		{Name: "avg_n", Func: exec.Avg, Arg: anyOf("n")},
		{Name: "first", Func: exec.Min, Arg: exec.Time()},
		{Name: "last", Func: exec.Max, Arg: exec.Time()},
	}
	byColl := &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("coll", "op"), Aggs: aggs}
	byUser := &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(eq("kind", "commit"), eq("op", "create"), eq("coll", "feed.post"))},
		Keys: keys("user"), Aggs: aggs[:1]}
	col := anyOf
	conds := map[string]*exec.Expr{
		"count":       exec.Gt(col("rows"), exec.Int(100)),
		"float":       exec.Le(col("avg_n"), exec.Float(48.5)),
		"null sum":    exec.Ge(col("sum_n"), exec.Int(0)),
		"key":         exec.Eq(col("op"), exec.Str("create")),
		"null key":    exec.Not(exec.Ne(col("coll"), exec.Str("feed.like"))),
		"two columns": exec.Lt(col("users"), col("rows")),
		"expression":  exec.Gt(exec.Sub(col("last"), col("first")), exec.Mul(exec.Int(1_000_000_000), exec.Int(100_000))),
		"logic": exec.Or(exec.And(exec.Gt(col("rows"), exec.Int(50)), exec.Lt(col("rows"), exec.Int(300))),
			exec.Eq(col("coll"), exec.Str("graph.block"))),
		"kinds differ": exec.Eq(col("coll"), exec.Int(1)),
		"nothing":      exec.Lt(col("rows"), exec.Int(0)),
	}
	eachLayout(t, 3000, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
		for name, cond := range conds {
			t.Run(name, func(t *testing.T) {
				filtered := &exec.Filter{Input: byColl, Cond: cond}
				check(t, snap, rows, filtered, false, workers)
				check(t, snap, rows, &exec.Sort{Input: &exec.Project{Input: filtered, Exprs: []exec.Named{
					named("coll", col("coll")), named("op", col("op")), named("twice", exec.Mul(col("rows"), exec.Int(2))),
				}}, By: []exec.Order{{Column: "twice", Desc: true}, {Column: "coll"}, {Column: "op"}}, Limit: 4}, true, workers)
			})
		}
		t.Run("ordered groups", func(t *testing.T) {
			plan := &exec.Sort{Input: &exec.Filter{Input: byUser, Cond: exec.Ge(col("rows"), exec.Int(2))},
				By: []exec.Order{{Column: "rows", Desc: true}, {Column: "user"}}, Limit: 10}
			check(t, snap, rows, plan, true, workers)
		})
		t.Run("one group", func(t *testing.T) {
			global := &exec.Aggregate{Input: &exec.Scan{}, Aggs: aggs}
			check(t, snap, rows, &exec.Filter{Input: global, Cond: exec.Gt(col("rows"), exec.Int(1))}, false, workers)
			check(t, snap, rows, &exec.Filter{Input: global, Cond: exec.Lt(col("rows"), exec.Int(1))}, false, workers)
		})
	})

	tbl := colstore.NewTable()
	defer tbl.Close()
	snap := tbl.Snapshot()
	defer snap.Release()
	for name, plan := range map[string]exec.Node{
		"above a scan":   &exec.Project{Input: &exec.Filter{Input: &exec.Scan{}, Cond: conds["count"]}, Exprs: []exec.Named{named("id", num("id"))}},
		"unknown column": &exec.Filter{Input: byColl, Cond: exec.Gt(col("nope"), exec.Int(1))},
		"not a filter":   &exec.Filter{Input: byColl, Cond: exec.Add(col("rows"), exec.Int(1))},
	} {
		if _, err := exec.Run(context.Background(), snap, plan, exec.Options{}); !errors.Is(err, exec.ErrUnsupported) {
			t.Errorf("%s: got %v, want ErrUnsupported", name, err)
		}
	}
}
