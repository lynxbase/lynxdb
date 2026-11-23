package exec_test

import (
	"fmt"
	"strings"
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
		{Name: "spread", Func: exec.Stdev, Arg: num("n")},
		{Name: "var_time", Func: exec.Var, Arg: exec.ToFloat(exec.HourOfDay(exec.Time()))},
	}
	users := exec.Agg{Name: "users", Func: exec.CountDistinct, Arg: str("user")}
	stateful := []exec.Agg{
		{Name: "rows", Func: exec.Count},
		{Name: "texts", Func: exec.ApproxDistinct, Arg: anyOf("msg")},
		{Name: "ids", Func: exec.ApproxDistinct, Arg: num("id")},
		{Name: "p50_score", Func: exec.Percentile, Arg: float("score"), P: 50},
		{Name: "p99_score", Func: exec.Percentile, Arg: float("score"), P: 99},
		{Name: "p100_n", Func: exec.Percentile, Arg: anyOf("n"), P: 100},
		{Name: "sum_x", Func: exec.Sum, Arg: num("nested.x")},
	}
	picked := []exec.Agg{
		{Name: "rows", Func: exec.Count},
		{Name: "latest_msg", Func: exec.Latest, Arg: str("msg")},
		{Name: "latest_n", Func: exec.Latest, Arg: anyOf("n")},
		{Name: "earliest_id", Func: exec.Earliest, Arg: num("id")},
		{Name: "id_of_max_big", Func: exec.ArgMax, Arg: num("id"), By: num("big")},
		{Name: "msg_of_min_id", Func: exec.ArgMin, Arg: anyOf("msg"), By: exec.Mul(num("id"), exec.Int(3))},
	}
	// A string may hold a number, which makes the kind of an extreme depend
	// on every row of the group.
	extremes := []exec.Agg{
		{Name: "min_msg", Func: exec.Min, Arg: str("msg")},
		{Name: "max_y", Func: exec.Max, Arg: str("nested.y")},
	}
	byUser := &exec.Aggregate{Input: &exec.Scan{Filter: posts}, Keys: keys("user"), Aggs: times}
	// Counts and times alone come from the first and last row of each run.
	spans := []exec.Agg{times[0], times[1], times[2], {Name: "times", Func: exec.Count, Arg: exec.Time()}}
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
		{"times of runs", &exec.Aggregate{Input: &exec.Scan{Filter: posts}, Keys: keys("user"), Aggs: spans}, true, false},
		{"times of runs of two keys and a range", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(created, exec.Gt(num("n"), exec.Int(5)))},
			Keys: keys("coll", "user"), Aggs: spans[1:3]}, true, false},
		// The rows of a key before the last follow the keys after it, not time.
		{"times of a key before the last", &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll"), Aggs: spans}, false, false},
		{"the longest spans", &exec.Sort{Input: &exec.Project{Input: &exec.Aggregate{Input: &exec.Scan{Filter: posts}, Keys: keys("user"), Aggs: spans},
			Exprs: []exec.Named{named("user", anyOf("user")), named("span", exec.Sub(anyOf("last"), anyOf("first")))}},
			By: []exec.Order{{Column: "span", Desc: true}, {Column: "user"}}, Limit: 4}, true, true},
		{"two keys", &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("user", "coll"), Aggs: times}, true, false},
		{"states of variable size", &exec.Aggregate{Input: &exec.Scan{Filter: posts}, Keys: keys("user"), Aggs: stateful}, true, false},
		{"extremes of strings", &exec.Aggregate{Input: &exec.Scan{Filter: posts}, Keys: keys("user"), Aggs: extremes}, false, false},
		{"states of two keys", &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("user", "coll"), Aggs: stateful}, true, false},
		{"states of three keys", &exec.Aggregate{Input: &exec.Scan{Filter: eq("kind", "commit")}, Keys: keys("op", "coll", "user"), Aggs: stateful}, true, false},
		{"picked rows", &exec.Aggregate{Input: &exec.Scan{Filter: posts}, Keys: keys("user"), Aggs: picked}, true, false},
		{"picked rows of two keys", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(created, exec.Gt(num("n"), exec.Int(5)))}, Keys: keys("coll", "user"), Aggs: picked}, true, false},
		{"sorted by a picked value", &exec.Sort{Input: &exec.Filter{Input: &exec.Aggregate{Input: &exec.Scan{Filter: posts}, Keys: keys("user"), Aggs: picked},
			Cond: exec.Gt(anyOf("earliest_id"), exec.Int(100))}, By: []exec.Order{{Column: "earliest_id", Desc: true}}, Limit: 9}, true, true},
		{"top with picked values", &exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{Filter: posts}, Keys: keys("user"), Aggs: picked},
			By: []exec.Order{{Column: "rows", Desc: true}, {Column: "user"}}, Limit: 6}, true, true},
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
		{"and functions of rows", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(posts, exec.Or(exec.StartsWith(anyOf("msg"), exec.Str("good")),
			exec.IsNull(anyOf("n")), exec.Gt(exec.Mul(anyOf("n"), exec.Float(1.5)), exec.Float(120)), exec.IsMissing(anyOf("flag"))))},
			Keys: keys("user"), Aggs: times}, true, false},
		{"and a key that exists", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(created, exec.Exists(anyOf("coll")), exec.Not(exec.Exists(anyOf("tags"))))},
			Keys: keys("coll"), Aggs: []exec.Agg{times[0], users}}, true, false},
		{"functions as arguments", &exec.Aggregate{Input: &exec.Scan{Filter: posts}, Keys: keys("user"), Aggs: []exec.Agg{
			{Name: "chars", Func: exec.Sum, Arg: exec.Length(exec.Lower(anyOf("msg")))},
			{Name: "top", Func: exec.Max, Arg: exec.ToFloat(exec.Bin(num("n"), exec.Int(7)))},
			{Name: "typed", Func: exec.Count, Arg: exec.NullIf(exec.TypeOf(anyOf("n")), exec.Str("missing"))}}}, true, false},
		{"untyped numbers", &exec.Aggregate{Input: &exec.Scan{Filter: posts}, Keys: keys("user"), Aggs: []exec.Agg{{Name: "s", Func: exec.Sum, Arg: anyOf("n")}}}, false, false},
		{"untyped distinct", &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll"), Aggs: []exec.Agg{{Name: "u", Func: exec.CountDistinct, Arg: anyOf("user")}}}, true, false},
	}
	key := []string{"kind", "op", "coll", "user"}
	for _, l := range []layout{
		{name: "runs", blockRows: 64, parts: 3, sortKey: key},
		{name: "one run", blockRows: 200, parts: 1, sortKey: key},
		{name: "one block", blockRows: 1 << 16, parts: 2, sortKey: key},
		{name: "small chunks", blockRows: 200, parts: 1, sortKey: key},
		{name: "small chunks of runs", blockRows: 1 << 16, parts: 2, sortKey: key},
	} {
		if strings.HasPrefix(l.name, "small chunks") {
			t.Cleanup(exec.SetOrderedChunk(7))
		}
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
		snap.Release()
	}
}

// TestEstimatedDistinctCountIsExactInKeyOrder checks that an ApproxDistinct
// of the sort key column after the group keys gives the exact count, of
// groups with more values than a set of hashes holds too.
func TestEstimatedDistinctCountIsExactInKeyOrder(t *testing.T) {
	docs := synthDocs(9000, 6)
	rows := referenceRows(t, docs, "ts")
	created := exec.And(exec.Eq(str("kind"), exec.Str("commit")), exec.Eq(str("op"), exec.Str("create")))
	aggs := func(fn exec.AggFunc) []exec.Agg {
		return []exec.Agg{{Name: "rows", Func: exec.Count}, {Name: "users", Func: fn, Arg: str("user")}}
	}
	plan := func(fn exec.AggFunc, more ...exec.Agg) exec.Node {
		return &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll"), Aggs: append(aggs(fn), more...)}
	}
	texts := exec.Agg{Name: "texts", Func: exec.ApproxDistinct, Arg: str("msg")}
	for _, l := range []layout{
		{name: "runs", blockRows: 64, parts: 3, sortKey: []string{"kind", "op", "coll", "user"}},
		{name: "one stream", blockRows: 200, parts: 1, sortKey: []string{"kind", "op", "coll", "user"}},
	} {
		tbl := buildTable(t, docs, "ts", l)
		snap := tbl.Snapshot()
		for _, workers := range []int{1, 5} {
			res := checkAgainst(t, snap, rows, plan(exec.ApproxDistinct), plan(exec.CountDistinct), false, workers)
			most := int64(0)
			for i := 0; i < res.Rows; i++ {
				most = max(most, res.Value(i, 2).Int)
			}
			if !res.Stats.Ordered || most < 500 {
				t.Fatalf("%s: in key order: %v; the largest group has %d users, want more than a set holds", l.name, res.Stats.Ordered, most)
			}
			// Another estimate in the plan is still a sketch.
			with := checkAgainst(t, snap, rows, plan(exec.ApproxDistinct, texts), plan(exec.CountDistinct, texts), false, workers)
			if !with.Stats.Ordered {
				t.Fatalf("%s: with a second estimate the plan does not run in key order", l.name)
			}
		}
		snap.Release()
	}
}
