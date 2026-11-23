package exec_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

func str(path string) *exec.Expr   { return exec.Col(path, vec.KindString) }
func num(path string) *exec.Expr   { return exec.Col(path, vec.KindInt) }
func anyOf(path string) *exec.Expr { return exec.Col(path, exec.KindAny) }

func named(name string, e *exec.Expr) exec.Named { return exec.Named{Name: name, Expr: e} }

func keys(paths ...string) []exec.Named {
	out := make([]exec.Named, len(paths))
	for i, p := range paths {
		out[i] = named(p, anyOf(p))
	}

	return out
}

// eachLayout runs fn over the synthetic documents in every layout and with
// one and several workers.
func eachLayout(t *testing.T, n int, fn func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int)) {
	t.Helper()
	docs := synthDocs(n, 1)
	rows := referenceRows(t, docs, "ts")
	for _, l := range layouts {
		tbl := buildTable(t, docs, "ts", l)
		for _, workers := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s/%d", l.name, workers), func(t *testing.T) {
				snap := tbl.Snapshot()
				defer snap.Release()
				fn(t, snap, rows, workers)
			})
		}
	}
}

func TestFilters(t *testing.T) {
	second := int64(1_000_000_000)
	start := int64(1_700_000_000_000_000_000)
	filters := map[string]*exec.Expr{
		"none":             nil,
		"string eq":        exec.Eq(str("kind"), exec.Str("commit")),
		"string eq any":    exec.Eq(anyOf("kind"), exec.Str("identity")),
		"string eq absent": exec.Eq(str("kind"), exec.Str("nope")),
		"string ne":        exec.Ne(str("op"), exec.Str("create")),
		"string lt":        exec.Lt(str("coll"), exec.Str("feed.post")),
		"string ge flat":   exec.Ge(str("user"), exec.Str("u0100")),
		"literal first":    exec.Gt(exec.Str("u0100"), str("user")),
		"string eq flat":   exec.Eq(str("user"), exec.Str("u0007")),
		"wrong kind":       exec.Eq(num("kind"), exec.Int(1)),
		"string vs int":    exec.Eq(str("n"), exec.Str("s3")),
		"and": exec.And(exec.Eq(str("kind"), exec.Str("commit")), exec.Eq(str("op"), exec.Str("create")),
			exec.Eq(str("coll"), exec.Str("feed.post"))),
		"or": exec.Or(exec.Eq(str("coll"), exec.Str("feed.like")), exec.Eq(str("coll"), exec.Str("feed.post")),
			exec.Eq(str("kind"), exec.Str("account"))),
		"not":              exec.Not(exec.Eq(str("kind"), exec.Str("commit"))),
		"not missing":      exec.Not(exec.Eq(str("op"), exec.Str("create"))),
		"int eq":           exec.Eq(num("n"), exec.Int(7)),
		"int ne":           exec.Ne(num("n"), exec.Int(7)),
		"int range":        exec.And(exec.Ge(num("n"), exec.Int(10)), exec.Lt(num("n"), exec.Int(20))),
		"int no range":     exec.And(exec.Gt(num("n"), exec.Int(50)), exec.Lt(num("n"), exec.Int(10))),
		"int point":        exec.And(exec.Ge(num("n"), exec.Int(5)), exec.Eq(num("n"), exec.Int(7)), exec.Ne(num("n"), exec.Int(8))),
		"int any":          exec.Gt(anyOf("n"), exec.Int(50)),
		"int vs float":     exec.Le(num("n"), exec.Float(10.5)),
		"float lit any":    exec.Eq(anyOf("n"), exec.Float(3.5)),
		"float column":     exec.Lt(exec.Col("score", vec.KindFloat), exec.Float(100)),
		"float any":        exec.Ge(anyOf("score"), exec.Int(900)),
		"int all":          exec.Ge(num("id"), exec.Int(0)),
		"int none":         exec.Lt(num("id"), exec.Int(0)),
		"wide int":         exec.Gt(num("big"), exec.Int(0)),
		"nested":           exec.Eq(num("nested.x"), exec.Int(3)),
		"bool eq":          exec.Eq(exec.Col("flag", vec.KindBool), exec.Bool(true)),
		"bool ne":          exec.Ne(anyOf("flag"), exec.Bool(true)),
		"bool column":      exec.Col("flag", vec.KindBool),
		"bool of mixed":    exec.Eq(anyOf("n"), exec.Bool(false)),
		"any field word":   exec.MatchAny(tokenfilter.Word("love")),
		"any field value":  exec.MatchAny(tokenfilter.Word("commit")),
		"any field prefix": exec.MatchAny(tokenfilter.Prefix("graph")),
		"any field sub":    exec.MatchAny(tokenfilter.Substring("0042").IgnoreCase()),
		"any field absent": exec.MatchAny(tokenfilter.Word("nowhere")),
		"any field and":    exec.And(exec.MatchAny(tokenfilter.Word("y1")), exec.Not(exec.MatchAny(tokenfilter.Word("feed.like"))), exec.Gt(num("id"), exec.Int(200))),
		"any field or":     exec.Or(exec.MatchAny(tokenfilter.Word("identity")), exec.MatchAny(tokenfilter.Word("rare morning"))),
		"match word":       exec.Match(str("msg"), tokenfilter.Word("love")),
		"match phrase":     exec.Match(str("msg"), tokenfilter.Word("good morning")),
		"match prefix":     exec.Match(anyOf("msg"), tokenfilter.Prefix("crypto")),
		"match sub":        exec.Match(str("msg"), tokenfilter.Substring("ware")),
		"match dict":       exec.Match(str("coll"), tokenfilter.Prefix("graph")),
		"match const":      exec.Match(str("kind"), tokenfilter.Word("commit")),
		"match and eq":     exec.And(exec.Match(str("msg"), tokenfilter.Word("rare")), exec.Eq(str("op"), exec.Str("create"))),
		"match or match":   exec.Or(exec.Match(str("msg"), tokenfilter.Word("sun")), exec.Match(str("msg"), tokenfilter.Word("moon"))),
		"match two":        exec.And(exec.Match(str("msg"), tokenfilter.Word("sun")), exec.Match(str("msg"), tokenfilter.Word("moon"))),
		"not match":        exec.Not(exec.Match(str("msg"), tokenfilter.Word("the"))),
		"time range": exec.And(exec.Ge(exec.Time(), exec.Int(start+3000*second)),
			exec.Lt(exec.Time(), exec.Int(start+9000*second))),
		"time empty":   exec.Lt(exec.Time(), exec.Int(start)),
		"hour of day":  exec.Eq(exec.HourOfDay(exec.Time()), exec.Int(23)),
		"expressions":  exec.Gt(exec.Add(num("n"), num("nested.x")), exec.Mul(num("nested.x"), exec.Int(20))),
		"division":     exec.Eq(exec.Div(num("n"), num("nested.x")), exec.Int(4)),
		"literal true": exec.And(exec.Bool(true), exec.Eq(num("nested.x"), exec.Int(1))),
		"nested logic": exec.Or(exec.And(exec.Eq(str("kind"), exec.Str("commit")), exec.Not(exec.Or(exec.Eq(str("op"), exec.Str("create")),
			exec.Gt(num("n"), exec.Int(30))))), exec.And(exec.Match(str("msg"), tokenfilter.Word("love")), exec.Lt(num("n"), exec.Int(5)))),
	}
	eachLayout(t, 3000, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
		for name, filter := range filters {
			t.Run(name, func(t *testing.T) {
				rowsPlan := &exec.Sort{
					Input: &exec.Project{Input: &exec.Scan{Filter: filter}, Exprs: []exec.Named{named("id", num("id"))}},
					By:    []exec.Order{{Column: "id"}},
				}
				check(t, snap, rows, rowsPlan, true, workers)
				count := &exec.Aggregate{Input: &exec.Scan{Filter: filter}, Aggs: []exec.Agg{{Name: "count", Func: exec.Count}}}
				check(t, snap, rows, count, true, workers)
			})
		}
	})
}

func TestAggregates(t *testing.T) {
	all := []exec.Agg{
		{Name: "rows", Func: exec.Count},
		{Name: "count_n", Func: exec.Count, Arg: anyOf("n")},
		{Name: "count_n_int", Func: exec.Count, Arg: num("n")},
		{Name: "count_msg", Func: exec.Count, Arg: str("msg")},
		{Name: "count_tags", Func: exec.Count, Arg: anyOf("tags")},
		{Name: "sum_n", Func: exec.Sum, Arg: anyOf("n")},
		{Name: "sum_n_int", Func: exec.Sum, Arg: num("n")},
		{Name: "min_n", Func: exec.Min, Arg: anyOf("n")},
		{Name: "max_n", Func: exec.Max, Arg: anyOf("n")},
		{Name: "max_n_int", Func: exec.Max, Arg: num("n")},
		{Name: "avg_n", Func: exec.Avg, Arg: anyOf("n")},
		{Name: "avg_score", Func: exec.Avg, Arg: exec.Col("score", vec.KindFloat)},
		{Name: "min_score", Func: exec.Min, Arg: anyOf("score")},
		{Name: "min_time", Func: exec.Min, Arg: exec.Time()},
		{Name: "max_time", Func: exec.Max, Arg: exec.Time()},
		{Name: "sum_x", Func: exec.Sum, Arg: num("nested.x")},
		{Name: "sum_expr", Func: exec.Sum, Arg: exec.Mul(num("n"), num("nested.x"))},
		{Name: "max_hour", Func: exec.Max, Arg: exec.HourOfDay(exec.Time())},
		{Name: "users", Func: exec.CountDistinct, Arg: str("user")},
		{Name: "distinct_n", Func: exec.CountDistinct, Arg: anyOf("n")},
		{Name: "distinct_x", Func: exec.CountDistinct, Arg: num("nested.x")},
		{Name: "distinct_flag", Func: exec.CountDistinct, Arg: anyOf("flag")},
	}
	created := exec.And(exec.Eq(str("kind"), exec.Str("commit")), exec.Eq(str("op"), exec.Str("create")))
	plans := map[string]exec.Node{
		"no keys":           &exec.Aggregate{Input: &exec.Scan{}, Aggs: all},
		"no keys filtered":  &exec.Aggregate{Input: &exec.Scan{Filter: created}, Aggs: all},
		"no keys, no rows":  &exec.Aggregate{Input: &exec.Scan{Filter: exec.Eq(str("kind"), exec.Str("nope"))}, Aggs: all},
		"count by dict":     &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("coll"), Aggs: all[:1]},
		"count by dict sel": &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll"), Aggs: all[:1]},
		"by dict":           &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("coll"), Aggs: all},
		"by dict filtered":  &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll"), Aggs: all},
		"by two":            &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("kind", "op"), Aggs: all},
		"by three":          &exec.Aggregate{Input: &exec.Scan{Filter: exec.Gt(num("n"), exec.Int(20))}, Keys: keys("kind", "coll", "nested.y"), Aggs: all},
		"by hour": &exec.Aggregate{Input: &exec.Scan{Filter: created},
			Keys: []exec.Named{named("coll", str("coll")), named("hour", exec.HourOfDay(exec.Time()))}, Aggs: all[:1]},
		"by hour all": &exec.Aggregate{Input: &exec.Scan{},
			Keys: []exec.Named{named("hour", exec.HourOfDay(exec.Time())), named("coll", str("coll"))}, Aggs: all},
		"by small int":    &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("nested.x"), Aggs: all},
		"by mixed kinds":  &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("n"), Aggs: all},
		"by typed mixed":  &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{named("n", num("n"))}, Aggs: all},
		"by bool":         &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("flag", "kind"), Aggs: all},
		"by float":        &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("score"), Aggs: all[:6]},
		"by wide int":     &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("big"), Aggs: all[:6]},
		"by flat":         &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("user"), Aggs: all},
		"by flat sel":     &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: []exec.Named{named("user", str("user"))}, Aggs: all},
		"by flat and":     &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("coll", "user"), Aggs: all},
		"by missing":      &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("nope"), Aggs: all},
		"by expr":         &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{named("k", exec.Div(num("n"), exec.Int(10)))}, Aggs: all},
		"by wide expr":    &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{named("k", exec.Mul(num("id"), exec.Int(1000)))}, Aggs: all[:6]},
		"keys only":       &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("kind", "op")},
		"keyed, no rows":  &exec.Aggregate{Input: &exec.Scan{Filter: exec.Eq(str("kind"), exec.Str("nope"))}, Keys: keys("coll"), Aggs: all},
		"distinct only":   &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll"), Aggs: all[18:19]},
		"global distinct": &exec.Aggregate{Input: &exec.Scan{}, Aggs: all[18:]},
	}
	eachLayout(t, 3000, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
		for name, plan := range plans {
			t.Run(name, func(t *testing.T) { check(t, snap, rows, plan, false, workers) })
		}
	})
}

func TestSpilledAggregates(t *testing.T) {
	aggs := []exec.Agg{
		{Name: "rows", Func: exec.Count},
		{Name: "sum_n", Func: exec.Sum, Arg: anyOf("n")},
		{Name: "first", Func: exec.Min, Arg: exec.Time()},
		{Name: "last", Func: exec.Max, Arg: exec.Time()},
		{Name: "colls", Func: exec.CountDistinct, Arg: str("coll")},
	}
	plans := map[string]exec.Node{
		"by flat":  &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("user"), Aggs: aggs},
		"by dict":  &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("coll", "nested.x"), Aggs: aggs},
		"by mixed": &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("n", "kind"), Aggs: aggs},
	}
	for _, limits := range [][2]int{{4, 0}, {1 << 16, 0}, {4, 1 << 20}} {
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

func TestSortAndProject(t *testing.T) {
	created := exec.And(exec.Eq(str("kind"), exec.Str("commit")), exec.Eq(str("op"), exec.Str("create")))
	spans := &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("user"), Aggs: []exec.Agg{
		{Name: "first", Func: exec.Min, Arg: exec.Time()},
		{Name: "last", Func: exec.Max, Arg: exec.Time()},
		{Name: "rows", Func: exec.Count},
		{Name: "avg_n", Func: exec.Avg, Arg: anyOf("n")},
	}}
	span := &exec.Project{Input: spans, Exprs: []exec.Named{
		named("user", anyOf("user")),
		named("span", exec.Sub(exec.Div(anyOf("last"), exec.Int(1_000_000)), exec.Div(anyOf("first"), exec.Int(1_000_000)))),
		named("hour", exec.HourOfDay(anyOf("first"))),
		named("half", exec.Div(anyOf("avg_n"), exec.Int(2))),
		named("one", exec.Int(1)),
	}}
	byColl := &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("coll"), Aggs: []exec.Agg{{Name: "rows", Func: exec.Count}}}
	plans := map[string]exec.Node{
		"earliest":      &exec.Sort{Input: spans, By: []exec.Order{{Column: "first"}, {Column: "user"}}, Limit: 3},
		"longest":       &exec.Sort{Input: span, By: []exec.Order{{Column: "span", Desc: true}, {Column: "user"}}, Limit: 5},
		"all sorted":    &exec.Sort{Input: span, By: []exec.Order{{Column: "span", Desc: true}, {Column: "user"}}},
		"by float":      &exec.Sort{Input: span, By: []exec.Order{{Column: "half"}, {Column: "user", Desc: true}}, Limit: 40},
		"counts":        &exec.Sort{Input: byColl, By: []exec.Order{{Column: "rows", Desc: true}, {Column: "coll"}}},
		"top counts":    &exec.Sort{Input: byColl, By: []exec.Order{{Column: "rows", Desc: true}, {Column: "coll"}}, Limit: 4},
		"more than all": &exec.Sort{Input: byColl, By: []exec.Order{{Column: "coll", Desc: true}}, Limit: 1000},
		"rows top": &exec.Sort{Input: &exec.Project{Input: &exec.Scan{Filter: created}, Exprs: []exec.Named{
			named("n", anyOf("n")), named("id", num("id")), named("user", str("user")), named("score", anyOf("score")),
			named("twice", exec.Mul(num("id"), exec.Int(2))), named("lit", exec.Str("x")),
		}}, By: []exec.Order{{Column: "n", Desc: true}, {Column: "id"}}, Limit: 50},
		"rows sorted": &exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{
			named("n", anyOf("n")), named("id", num("id")), named("flag", anyOf("flag")),
		}}, By: []exec.Order{{Column: "n"}, {Column: "id", Desc: true}}},
	}
	for _, limits := range [][2]int{{1 << 16, 4096}, {8, 0}} {
		restore := exec.SetLimits(limits[0], limits[1])
		eachLayout(t, 3000, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
			for name, plan := range plans {
				t.Run(name, func(t *testing.T) { check(t, snap, rows, plan, true, workers) })
			}
		})
		restore()
	}
}

func TestLimitWithoutOrder(t *testing.T) {
	docs := synthDocs(2000, 3)
	tbl := buildTable(t, docs, "ts", layouts[0])
	snap := tbl.Snapshot()
	defer snap.Release()
	plan := &exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{named("id", num("id"))}}, Limit: 17}
	res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: 3})
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows != 17 || len(res.Columns[0].Values.Ints) != 17 {
		t.Fatalf("got %d rows and %d values, want 17", res.Rows, len(res.Columns[0].Values.Ints))
	}
}

func TestEmptyTable(t *testing.T) {
	tbl := colstore.NewTable()
	defer tbl.Close()
	snap := tbl.Snapshot()
	defer snap.Release()
	count := &exec.Aggregate{Input: &exec.Scan{}, Aggs: []exec.Agg{{Name: "count", Func: exec.Count}, {Name: "sum", Func: exec.Sum, Arg: num("n")}}}
	res, err := exec.Run(context.Background(), snap, count, exec.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows != 1 || res.Value(0, 0).Int != 0 || !res.Value(0, 1).IsNull() {
		t.Fatalf("count over nothing: %d rows, %v", res.Rows, res.Row(0, nil))
	}
	for name, plan := range map[string]exec.Node{
		"keyed": &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("kind"), Aggs: count.Aggs},
		"rows":  &exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{named("id", num("id"))}}, By: []exec.Order{{Column: "id"}}, Limit: 3},
	} {
		res, err := exec.Run(context.Background(), snap, plan, exec.Options{})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if res.Rows != 0 || len(res.Columns) == 0 {
			t.Fatalf("%s: %d rows, %d columns", name, res.Rows, len(res.Columns))
		}
	}
}

func TestUnsupportedPlans(t *testing.T) {
	tbl := colstore.NewTable()
	defer tbl.Close()
	snap := tbl.Snapshot()
	defer snap.Release()
	scan := &exec.Scan{}
	count := []exec.Agg{{Name: "count", Func: exec.Count}}
	for name, plan := range map[string]exec.Node{
		"bare scan":         scan,
		"nil":               nil,
		"nested aggregate":  &exec.Aggregate{Input: &exec.Aggregate{Input: scan, Aggs: count}, Aggs: count},
		"sum of nothing":    &exec.Aggregate{Input: scan, Aggs: []exec.Agg{{Name: "s", Func: exec.Sum}}},
		"empty aggregate":   &exec.Aggregate{Input: scan},
		"unknown sort":      &exec.Sort{Input: &exec.Aggregate{Input: scan, Aggs: count}, By: []exec.Order{{Column: "nope"}}},
		"unknown column":    &exec.Project{Input: &exec.Aggregate{Input: scan, Aggs: count}, Exprs: []exec.Named{named("x", anyOf("nope"))}},
		"bool ordering":     &exec.Aggregate{Input: &exec.Scan{Filter: exec.Lt(anyOf("a"), exec.Bool(true))}, Aggs: count},
		"too few arguments": &exec.Aggregate{Input: scan, Aggs: []exec.Agg{{Name: "s", Func: exec.Sum, Arg: exec.Coalesce()}}},
	} {
		if _, err := exec.Run(context.Background(), snap, plan, exec.Options{}); !errors.Is(err, exec.ErrUnsupported) {
			t.Errorf("%s: got %v, want ErrUnsupported", name, err)
		}
	}
}

func TestPushdown(t *testing.T) {
	docs := synthDocs(4000, 2)
	rows := referenceRows(t, docs, "ts")
	ordered := buildTable(t, docs, "ts", layout{blockRows: 64, parts: 2, sortKey: []string{"kind", "op", "coll", "user"}})
	snap := ordered.Snapshot()
	defer snap.Release()

	eq := func(path, v string) *exec.Expr { return exec.Eq(str(path), exec.Str(v)) }
	second := int64(1_000_000_000)
	start := int64(1_700_000_000_000_000_000)
	cases := []struct {
		name   string
		filter *exec.Expr
		key    []string
		// tokens counts the hashes pushed down, and unbound those of them
		// that no column is named for.
		tokens, unbound int
		minTime, maxTim int64
	}{
		{name: "prefix of two", filter: exec.And(eq("op", "create"), eq("kind", "commit")), key: []string{"commit", "create"}, tokens: 2},
		{name: "gap in the key", filter: exec.And(eq("kind", "commit"), eq("coll", "feed.post")), key: []string{"commit"}, tokens: 3},
		{name: "not leading", filter: eq("op", "create"), tokens: 1},
		{name: "nested and", filter: exec.And(eq("kind", "commit"), exec.And(eq("op", "create"), exec.Eq(exec.Str("feed.post"), anyOf("coll")))),
			key: []string{"commit", "create", "feed.post"}, tokens: 4},
		{name: "under or", filter: exec.Or(eq("kind", "commit"), eq("kind", "account"))},
		{name: "under not", filter: exec.Not(eq("kind", "commit"))},
		{name: "typed int", filter: exec.Eq(num("kind"), exec.Int(1))},
		{name: "match", filter: exec.And(exec.Match(str("msg"), tokenfilter.Word("good morning")), eq("kind", "commit")), key: []string{"commit"}, tokens: 3},
		{name: "prefix match", filter: exec.Match(str("msg"), tokenfilter.Prefix("crypto"))},
		{name: "match of every column", filter: exec.And(exec.MatchAny(tokenfilter.Word("good morning")), eq("kind", "commit")),
			key: []string{"commit"}, tokens: 3, unbound: 2},
		{name: "time", filter: exec.And(exec.Ge(exec.Time(), exec.Int(start+5*second)), exec.Lt(exec.Time(), exec.Int(start+9*second))),
			minTime: start + 5*second, maxTim: start + 9*second - 1},
		{name: "time from", filter: exec.Gt(exec.Int(start), exec.Time()), minTime: -1 << 63, maxTim: start - 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts := exec.Pushdown(snap, c.filter)
			if len(opts.Key.Values) != len(c.key) || len(opts.Key.Paths) != len(c.key) {
				t.Fatalf("key filter %v=%q, want values %q", opts.Key.Paths, opts.Key.Values, c.key)
			}
			for i, v := range c.key {
				if string(opts.Key.Values[i]) != v || opts.Key.Paths[i] != []string{"kind", "op", "coll"}[i] {
					t.Fatalf("key filter %v=%q, want values %q", opts.Key.Paths, opts.Key.Values, c.key)
				}
			}
			tokens, unbound := 0, 0
			for _, set := range opts.ColumnTokens {
				tokens += len(set.Hashes)
				if set.Path == "" {
					unbound += len(set.Hashes)
				}
			}
			if tokens != c.tokens || unbound != c.unbound || len(opts.Tokens) != 0 {
				t.Errorf("%d tokens, %d of them of no column, want %d and %d", tokens, unbound, c.tokens, c.unbound)
			}
			if opts.MinTime != c.minTime || opts.MaxTime != c.maxTim {
				t.Errorf("time %d..%d, want %d..%d", opts.MinTime, opts.MaxTime, c.minTime, c.maxTim)
			}
			plan := &exec.Aggregate{Input: &exec.Scan{Filter: c.filter}, Keys: keys("coll"), Aggs: []exec.Agg{{Name: "count", Func: exec.Count}}}
			check(t, snap, rows, plan, false, 2)
		})
	}

	count := func(filter *exec.Expr) exec.Stats {
		t.Helper()
		plan := &exec.Aggregate{Input: &exec.Scan{Filter: filter}, Aggs: []exec.Agg{{Name: "count", Func: exec.Count}}}

		return check(t, snap, rows, plan, true, 2).Stats
	}
	if s := count(exec.And(eq("kind", "commit"), eq("op", "delete"))); s.SkippedByKey == 0 {
		t.Errorf("an equality on the sort key skipped no block: %+v", s)
	}
	if s := count(exec.Match(str("msg"), tokenfilter.Word("absentword"))); s.SkippedByToken == 0 || s.Blocks != 0 {
		t.Errorf("a token that no block holds opened blocks: %+v", s)
	}
	if s := count(exec.Lt(exec.Time(), exec.Int(start))); s.SkippedByTime == 0 || s.Blocks != 0 {
		t.Errorf("a time range before the data opened blocks: %+v", s)
	}
	if s := count(exec.Lt(num("id"), exec.Int(-5))); s.SkippedByStats != s.Blocks || s.Blocks == 0 {
		t.Errorf("a range outside the column's bounds decoded blocks: %+v", s)
	}
	if s := count(exec.And(eq("kind", "commit"), eq("op", "create"))); s.RowsMatched == 0 || s.HeldBytes == 0 || s.BlocksMatched == 0 {
		t.Errorf("stats of a matching query: %+v", s)
	}
}

func TestMixedKinds(t *testing.T) {
	var docs [][]byte
	for i := 0; i < 600; i++ {
		var v string
		switch i % 6 {
		case 0:
			v = fmt.Sprintf(`"v":%d`, i%4)
		case 1:
			v = fmt.Sprintf(`"v":"%d"`, i%4)
		case 2:
			v = fmt.Sprintf(`"v":%d.5`, i%4)
		case 3:
			v = fmt.Sprintf(`"v":%v`, i%4 == 3)
		case 4:
			v = `"v":null`
		default:
			v = `"w":1`
		}
		docs = append(docs, []byte(fmt.Sprintf(`{"id":%d,"g":"g%d",%s}`, i, i%3, v)))
	}
	rows := referenceRows(t, docs, "")
	tbl := buildTable(t, docs, "", layout{blockRows: 100, parts: 2})
	snap := tbl.Snapshot()
	defer snap.Release()
	aggs := []exec.Agg{
		{Name: "rows", Func: exec.Count},
		{Name: "count", Func: exec.Count, Arg: anyOf("v")},
		{Name: "count_str", Func: exec.Count, Arg: str("v")},
		{Name: "sum", Func: exec.Sum, Arg: anyOf("v")},
		{Name: "sum_int", Func: exec.Sum, Arg: num("v")},
		{Name: "min", Func: exec.Min, Arg: anyOf("v")},
		{Name: "max", Func: exec.Max, Arg: anyOf("v")},
		{Name: "max_float", Func: exec.Max, Arg: exec.Col("v", vec.KindFloat)},
		{Name: "avg", Func: exec.Avg, Arg: anyOf("v")},
		{Name: "distinct", Func: exec.CountDistinct, Arg: anyOf("v")},
		{Name: "distinct_str", Func: exec.CountDistinct, Arg: str("v")},
		{Name: "var", Func: exec.Var, Arg: anyOf("v")},
		{Name: "stdev_int", Func: exec.Stdev, Arg: num("v")},
		{Name: "stdev_expr", Func: exec.Stdev, Arg: exec.Mul(anyOf("v"), exec.Float(2.5))},
	}
	for name, plan := range map[string]exec.Node{
		"global":       &exec.Aggregate{Input: &exec.Scan{}, Aggs: aggs},
		"by g":         &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("g"), Aggs: aggs},
		"by v":         &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("v"), Aggs: aggs},
		"by v and g":   &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("v", "g"), Aggs: aggs},
		"by typed v":   &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{named("v", str("v"))}, Aggs: aggs},
		"ints only":    &exec.Aggregate{Input: &exec.Scan{Filter: exec.Ge(num("v"), exec.Int(0))}, Keys: keys("g"), Aggs: aggs},
		"numbers":      &exec.Aggregate{Input: &exec.Scan{Filter: exec.Ge(anyOf("v"), exec.Int(2))}, Keys: keys("v"), Aggs: aggs},
		"strings":      &exec.Aggregate{Input: &exec.Scan{Filter: exec.Ge(anyOf("v"), exec.Str("2"))}, Keys: keys("v"), Aggs: aggs},
		"sorted mixed": &exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("v"), Aggs: aggs[:1]}, By: []exec.Order{{Column: "v", Desc: true}}},
		"rows": &exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{named("v", anyOf("v")), named("id", num("id"))}},
			By: []exec.Order{{Column: "v"}, {Column: "id"}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, ordered := plan.(*exec.Sort)
			res := check(t, snap, rows, plan, ordered, 2)
			if name != "by v" {
				return
			}
			col := res.Columns[0]
			if col.Kind != exec.KindAny || len(col.Kinds) != res.Rows {
				t.Fatalf("a key of mixed kinds reports kind %v and %d row kinds", col.Kind, len(col.Kinds))
			}
		})
	}
}

func TestTimeColumnIsTheRowTime(t *testing.T) {
	docs := [][]byte{[]byte(`{"ts":1700000000000000,"a":1}`), []byte(`{"ts":1700003600000000,"a":2}`)}
	tbl := buildTable(t, docs, "ts", layout{blockRows: 10, parts: 1})
	snap := tbl.Snapshot()
	defer snap.Release()
	plan := &exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{
		named("t", exec.Col(part.TimePath, exec.KindAny)), named("hour", exec.HourOfDay(exec.Time())),
	}}, By: []exec.Order{{Column: "t"}}}
	res, err := exec.Run(context.Background(), snap, plan, exec.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(resultRows(res)); got != "[1700000000000000000 | 22 1700003600000000000 | 23]" {
		t.Fatalf("got %s", got)
	}
}

func TestConstantKeyMissingInSomeRows(t *testing.T) {
	var docs [][]byte
	for i := 0; i < 500; i++ {
		c := ""
		if i%3 != 0 {
			c = `,"c":7`
		}
		docs = append(docs, []byte(fmt.Sprintf(`{"id":%d,"u":"user-%d"%s}`, i, i, c)))
	}
	rows := referenceRows(t, docs, "")
	plan := &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("u", "c"), Aggs: []exec.Agg{{Name: "rows", Func: exec.Count}}}
	for _, blockRows := range []int{64, 1 << 16} {
		tbl := buildTable(t, docs, "", layout{blockRows: blockRows, parts: 1})
		snap := tbl.Snapshot()
		check(t, snap, rows, plan, false, 2)
		snap.Release()
	}
}

func TestCountsOfValues(t *testing.T) {
	aggs := []exec.Agg{{Name: "rows", Func: exec.Count}}
	for _, path := range []string{"n", "big", "msg", "flag", "score", "op", "tags", "nested.x"} {
		for name, arg := range map[string]*exec.Expr{"any": anyOf(path), "int": num(path), "str": str(path),
			"float": exec.Col(path, vec.KindFloat), "bool": exec.Col(path, vec.KindBool)} {
			aggs = append(aggs, exec.Agg{Name: path + " " + name, Func: exec.Count, Arg: arg})
		}
	}
	filter := exec.Or(exec.Gt(num("id"), exec.Int(1500)), exec.Eq(str("kind"), exec.Str("identity")))
	plans := map[string]exec.Node{
		"all":               &exec.Aggregate{Input: &exec.Scan{}, Aggs: aggs},
		"filtered":          &exec.Aggregate{Input: &exec.Scan{Filter: filter}, Aggs: aggs},
		"by kind":           &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("kind"), Aggs: aggs},
		"by kind, filtered": &exec.Aggregate{Input: &exec.Scan{Filter: filter}, Keys: keys("kind", "op"), Aggs: aggs},
		"by user, filtered": &exec.Aggregate{Input: &exec.Scan{Filter: filter}, Keys: keys("user"), Aggs: aggs},
		"with the values": &exec.Aggregate{Input: &exec.Scan{Filter: filter}, Keys: keys("kind"),
			Aggs: append(aggs[:len(aggs):len(aggs)], exec.Agg{Name: "sum", Func: exec.Sum, Arg: num("n")},
				exec.Agg{Name: "distinct", Func: exec.CountDistinct, Arg: anyOf("big")})},
	}
	eachLayout(t, 3000, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
		for name, plan := range plans {
			t.Run(name, func(t *testing.T) { check(t, snap, rows, plan, false, workers) })
		}
	})
}

// TestPickedRows covers First, Last, Earliest and Latest over documents whose
// times are all different and do not follow the order of the documents.
func TestPickedRows(t *testing.T) {
	var docs [][]byte
	for i := 0; i < 3000; i++ {
		var v string
		switch i % 7 {
		case 0, 1:
			v = fmt.Sprintf(`,"v":"s%d"`, i)
		case 2:
			v = fmt.Sprintf(`,"v":%d`, i)
		case 3:
			v = fmt.Sprintf(`,"v":%d.5`, i)
		case 4:
			v = fmt.Sprintf(`,"v":%v`, i%2 == 0)
		case 5:
			v = `,"v":null`
		}
		ts := 1_700_000_000_000_000 + int64(i*7919%3000)*1_000_000
		docs = append(docs, []byte(fmt.Sprintf(`{"id":%d,"ts":%d,"k":"k%d","rare":%d,"w":%d.5%s}`, i, ts, i%11, i%400, i*31%1000, v)))
	}
	rows := referenceRows(t, docs, "ts")
	byTime := []exec.Agg{
		{Name: "rows", Func: exec.Count},
		{Name: "earliest", Func: exec.Earliest, Arg: anyOf("v")},
		{Name: "latest", Func: exec.Latest, Arg: anyOf("v")},
		{Name: "latest_text", Func: exec.Latest, Arg: str("v")},
		{Name: "latest_id", Func: exec.Latest, Arg: num("id")},
		{Name: "none", Func: exec.Earliest, Arg: anyOf("nope")},
		{Name: "v_of_max_id", Func: exec.ArgMax, Arg: anyOf("v"), By: num("id")},
		{Name: "id_of_min_time", Func: exec.ArgMin, Arg: num("id"), By: exec.Time()},
		{Name: "id_of_max_v", Func: exec.ArgMax, Arg: num("id"), By: anyOf("v")},
		{Name: "k_of_min_v", Func: exec.ArgMin, Arg: str("k"), By: anyOf("v")},
		{Name: "v_of_max_neg", Func: exec.ArgMax, Arg: anyOf("v"), By: exec.Sub(exec.Float(0.25), exec.ToFloat(num("id")))},
		{Name: "v_of_none", Func: exec.ArgMin, Arg: anyOf("v"), By: anyOf("k")},
	}
	byPlace := append(byTime[:len(byTime):len(byTime)], exec.Agg{Name: "first", Func: exec.First, Arg: anyOf("v")},
		exec.Agg{Name: "last", Func: exec.Last, Arg: anyOf("v")}, exec.Agg{Name: "last_id", Func: exec.Last, Arg: num("id")},
		exec.Agg{Name: "any", Func: exec.AnyValue, Arg: anyOf("v")},
		exec.Agg{Name: "row_v", Func: exec.FirstRow, Arg: anyOf("v")}, exec.Agg{Name: "row_id", Func: exec.FirstRow, Arg: num("id")},
		exec.Agg{Name: "id_of_min_w", Func: exec.ArgMin, Arg: num("id"), By: anyOf("w")},
		exec.Agg{Name: "id_of_max_rare", Func: exec.ArgMax, Arg: num("id"), By: num("rare")},
		exec.Agg{Name: "v_of_min_rare", Func: exec.ArgMin, Arg: anyOf("v"), By: num("rare")})
	filter := exec.Gt(num("id"), exec.Int(500))
	for _, l := range []layout{
		{name: "blocks", blockRows: 64, parts: 1},
		{name: "parts", blockRows: 100, parts: 3},
		{name: "one block", blockRows: 1 << 16, parts: 1},
		{name: "ordered", blockRows: 128, parts: 2, sortKey: []string{"k", "rare"}},
	} {
		aggs := byPlace
		if l.sortKey != nil {
			aggs = byTime
		}
		plans := map[string]exec.Node{
			"all":      &exec.Aggregate{Input: &exec.Scan{}, Aggs: aggs},
			"by key":   &exec.Aggregate{Input: &exec.Scan{Filter: filter}, Keys: keys("k"), Aggs: aggs},
			"by many":  &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("rare", "k"), Aggs: aggs},
			"filtered": &exec.Aggregate{Input: &exec.Scan{Filter: exec.Eq(str("k"), exec.Str("k3"))}, Keys: keys("rare"), Aggs: aggs},
			"top": &exec.Sort{Input: &exec.Project{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("rare"), Aggs: aggs},
				Exprs: []exec.Named{named("rare", anyOf("rare")), named("latest", anyOf("latest")), named("double", exec.Mul(anyOf("rows"), exec.Int(2)))}},
				By: []exec.Order{{Column: "rare", Desc: true}}, Limit: 9},
			"sorted by": &exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("rare"), Aggs: aggs},
				By: []exec.Order{{Column: "latest", Desc: true}, {Column: "rare"}}, Limit: 20},
			"sorted by all": &exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("rare"), Aggs: aggs},
				By: []exec.Order{{Column: "latest_id"}}},
			"filtered by": &exec.Filter{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("rare"), Aggs: aggs},
				Cond: exec.And(exec.Gt(anyOf("latest_id"), exec.Int(2000)), exec.Gt(anyOf("rows"), exec.Int(1)))},
			"a function of": &exec.Sort{Input: &exec.Project{Input: &exec.Filter{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("k"), Aggs: aggs},
				Cond: exec.Lt(anyOf("latest_id"), exec.Int(2990))},
				Exprs: []exec.Named{named("k", anyOf("k")), named("up", exec.Upper(anyOf("latest_text"))), named("next", exec.Add(anyOf("latest_id"), exec.Int(1))), named("e", anyOf("earliest"))}},
				By: []exec.Order{{Column: "next"}}},
			"no group passes": &exec.Filter{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("k"), Aggs: aggs},
				Cond: exec.Gt(anyOf("latest_id"), exec.Int(1<<40))},
		}
		tbl := buildTable(t, docs, "ts", l)
		snap := tbl.Snapshot()
		restore := exec.SetLimits(64, 1)
		for name, plan := range plans {
			for _, workers := range []int{1, 4} {
				t.Run(fmt.Sprintf("%s/%s/%d", l.name, name, workers), func(t *testing.T) {
					_, sorted := plan.(*exec.Sort)
					check(t, snap, rows, plan, sorted, workers)
				})
			}
		}
		restore()
		snap.Release()
	}
	scan := &exec.Scan{}
	for name, plan := range map[string]exec.Node{
		"of a function":   &exec.Aggregate{Input: scan, Aggs: []exec.Agg{{Name: "x", Func: exec.First, Arg: exec.Upper(anyOf("v"))}}},
		"row of function": &exec.Aggregate{Input: scan, Aggs: []exec.Agg{{Name: "x", Func: exec.FirstRow, Arg: exec.Upper(anyOf("v"))}}},
		"by a string":     &exec.Aggregate{Input: scan, Aggs: []exec.Agg{{Name: "x", Func: exec.ArgMax, Arg: anyOf("v"), By: str("k")}}},
	} {
		if _, err := exec.Run(context.Background(), colstore.NewTable().Snapshot(), plan, exec.Options{}); !errors.Is(err, exec.ErrUnsupported) {
			t.Errorf("%s: got %v, want ErrUnsupported", name, err)
		}
	}
}
