package exec_test

import (
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
)

func TestOrderedAggregation(t *testing.T) {
	docs := synthDocs(6000, 5)
	rows := referenceRows(t, docs, "ts")
	eq := func(path, v string) *exec.Expr { return exec.Eq(str(path), exec.Str(v)) }
	created := exec.And(eq("kind", "commit"), eq("op", "create"))
	posts := exec.And(eq("kind", "commit"), eq("op", "create"), eq("coll", "feed.post"))
	times := []exec.Agg{
		{Name: "rows", Func: exec.Count},
		{Name: "first", Func: exec.Min, Arg: exec.Time()},
		{Name: "last", Func: exec.Max, Arg: exec.Time()},
		{Name: "sum_n", Func: exec.Sum, Arg: num("n")},
		{Name: "count_msg", Func: exec.Count, Arg: anyOf("msg")},
		{Name: "max_expr", Func: exec.Max, Arg: exec.Mul(num("n"), num("nested.x"))},
	}
	users := exec.Agg{Name: "users", Func: exec.CountDistinct, Arg: str("user")}
	byUser := &exec.Aggregate{Input: &exec.Scan{Filter: posts}, Keys: keys("user"), Aggs: times}
	cases := []struct {
		name    string
		plan    exec.Node
		ordered bool
		sorted  bool
	}{
		{"distinct per group", &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll"), Aggs: []exec.Agg{times[0], users, times[1]}}, true, false},
		{"group by the last key", byUser, true, false},
		{"top of the last key", &exec.Sort{Input: byUser, By: []exec.Order{{Column: "first"}, {Column: "user"}}, Limit: 5}, true, true},
		{"projected", &exec.Sort{Input: &exec.Project{Input: byUser, Exprs: []exec.Named{named("user", anyOf("user")),
			named("span", exec.Sub(anyOf("last"), anyOf("first")))}}, By: []exec.Order{{Column: "span", Desc: true}, {Column: "user"}}, Limit: 7}, true, true},
		{"two keys", &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("user", "coll"), Aggs: times}, true, false},
		{"three keys", &exec.Aggregate{Input: &exec.Scan{Filter: eq("kind", "commit")}, Keys: keys("op", "coll", "user"), Aggs: times}, true, false},
		{"two keys and distinct", &exec.Aggregate{Input: &exec.Scan{Filter: eq("kind", "commit")}, Keys: keys("coll", "op"), Aggs: []exec.Agg{users, times[0]}}, true, false},
		{"distinct alone", &exec.Aggregate{Input: &exec.Scan{Filter: posts}, Aggs: []exec.Agg{users, times[0], times[2]}}, true, false},
		{"no rows", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(eq("kind", "commit"), eq("op", "create"), eq("coll", "nope"))}, Keys: keys("user"), Aggs: times}, true, false},
		{"distinct of no rows", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(eq("kind", "commit"), eq("op", "nope"), eq("coll", "x"))}, Aggs: []exec.Agg{users}}, true, false},
		{"missing group keys", &exec.Aggregate{Input: &exec.Scan{Filter: eq("kind", "identity")}, Keys: keys("op", "coll", "user"), Aggs: times}, false, false},
		{"not the whole key", &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll"), Aggs: times}, false, false},
		{"and a range", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(posts, exec.Gt(num("n"), exec.Int(5)))}, Keys: keys("user"), Aggs: times}, true, false},
		{"and a time window", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(created, exec.Ge(exec.Time(), exec.Int(1_700_030_000_000_000_000)),
			exec.Lt(exec.Time(), exec.Int(1_700_090_000_000_000_000)))}, Keys: keys("coll"), Aggs: []exec.Agg{times[0], users, times[2]}}, true, false},
		{"and logic", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(posts, exec.Or(exec.Not(exec.Col("flag", exec.KindAny)),
			exec.Lt(exec.Add(num("n"), num("nested.x")), exec.Int(40)), exec.Gt(exec.Col("score", exec.KindAny), exec.Float(900))))},
			Keys: keys("user"), Aggs: times}, true, false},
		{"and nothing", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(posts, exec.Lt(num("id"), exec.Int(0)))}, Keys: keys("user"), Aggs: times}, true, false},
		{"and a key gap", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(eq("kind", "commit"), exec.Ne(str("op"), exec.Str("delete")))},
			Keys: keys("op", "coll", "user"), Aggs: times}, true, false},
		{"and a text match", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(posts, exec.Match(str("msg"), tokenfilter.Word("love")))}, Keys: keys("user"), Aggs: times}, true, false},
		{"and a string equality", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(posts, eq("nested.y", "y1"))}, Keys: keys("user"), Aggs: times}, true, false},
		{"and a token of no block", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(posts, exec.Match(str("msg"), tokenfilter.Word("absent")))}, Keys: keys("user"), Aggs: times}, false, false},
		{"and a value of no block", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(created, eq("nested.y", "y9"))}, Keys: keys("coll"), Aggs: []exec.Agg{times[0], users}}, false, false},
		{"untyped numbers", &exec.Aggregate{Input: &exec.Scan{Filter: posts}, Keys: keys("user"), Aggs: []exec.Agg{{Name: "s", Func: exec.Sum, Arg: anyOf("n")}}}, false, false},
		{"untyped distinct", &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll"), Aggs: []exec.Agg{{Name: "u", Func: exec.CountDistinct, Arg: anyOf("user")}}}, false, false},
	}
	key := []string{"kind", "op", "coll", "user"}
	for _, l := range []layout{
		{name: "runs", blockRows: 64, parts: 3, sortKey: key},
		{name: "one run", blockRows: 200, parts: 1, sortKey: key},
		{name: "one block", blockRows: 1 << 16, parts: 2, sortKey: key},
	} {
		tbl := buildTable(t, docs, "ts", l)
		snap := tbl.Snapshot()
		for _, c := range cases {
			for _, workers := range []int{1, 5} {
				t.Run(fmt.Sprintf("%s/%s/%d", l.name, c.name, workers), func(t *testing.T) {
					res := check(t, snap, rows, c.plan, c.sorted, workers)
					if res.Stats.Ordered != c.ordered {
						t.Fatalf("ordered aggregation: %v, want %v", res.Stats.Ordered, c.ordered)
					}
				})
			}
		}
		snap.Release()
	}
}

func TestOrderedAggregationSkipsBlocksByToken(t *testing.T) {
	docs := synthDocs(6000, 5)
	rows := referenceRows(t, docs, "ts")
	plan := &exec.Aggregate{
		Input: &exec.Scan{Filter: exec.And(exec.Eq(str("kind"), exec.Str("commit")), exec.Eq(str("op"), exec.Str("create")),
			exec.Eq(str("n"), exec.Str("s3")))},
		Keys: keys("coll"),
		Aggs: []exec.Agg{{Name: "rows", Func: exec.Count}, {Name: "users", Func: exec.CountDistinct, Arg: str("user")}},
	}
	key := []string{"kind", "op", "coll", "user"}
	for _, l := range []layout{
		{name: "one stream", blockRows: 100, parts: 1, sortKey: key},
		{name: "streams", blockRows: 100, parts: 3, sortKey: key},
	} {
		tbl := buildTable(t, docs, "ts", l)
		snap := tbl.Snapshot()
		for _, workers := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s/%d", l.name, workers), func(t *testing.T) {
				res := check(t, snap, rows, plan, false, workers)
				if !res.Stats.Ordered || res.Stats.SkippedByToken == 0 {
					t.Fatalf("in key order: %v, with %d blocks skipped by token", res.Stats.Ordered, res.Stats.SkippedByToken)
				}
			})
		}
		// A scan that hashes takes over when the tokens leave few blocks.
		restore := exec.SetOrderedTokenShare(0.99)
		if res := check(t, snap, rows, plan, false, 2); res.Stats.Ordered {
			t.Fatalf("%s: aggregated in key order with most blocks ruled out", l.name)
		}
		restore()
		snap.Release()
	}
}
