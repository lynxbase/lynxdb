package exec_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

func float(path string) *exec.Expr { return exec.Col(path, vec.KindFloat) }

// scalarExprs returns expressions of every scalar function over the
// synthetic documents, by name.
func scalarExprs() []exec.Named {
	n, x, big, score := anyOf("n"), num("nested.x"), anyOf("big"), anyOf("score")
	msg, kind, coll, user := anyOf("msg"), str("kind"), anyOf("coll"), str("user")

	return []exec.Named{
		named("any plus int", exec.Add(n, exec.Int(3))),
		named("any times float", exec.Mul(n, exec.Float(1.5))),
		named("float minus int", exec.Sub(score, x)),
		named("int over any", exec.Div(big, n)),
		named("float over float", exec.Div(score, exec.Sub(float("n"), exec.Float(10.5)))),
		named("mod", exec.Mod(num("big"), exec.Add(x, exec.Int(-3)))),
		named("nested ints", exec.Add(exec.ToFloat(exec.Mul(x, num("id"))), exec.Float(0.5))),
		named("joined", exec.Add(kind, coll)),
		named("abs", exec.Abs(exec.Sub(n, exec.Int(50)))),
		named("abs of float", exec.Abs(exec.Sub(score, exec.Float(500)))),
		named("round", exec.Round(score)),
		named("round to digits", exec.Round(exec.Div(score, exec.Int(7)), exec.Int(2))),
		named("round to tens", exec.Round(n, exec.Int(-1))),
		named("floor", exec.Floor(exec.Div(score, exec.Float(-3)))),
		named("ceil", exec.Ceil(exec.Div(score, exec.Int(3)))),
		named("floor of any", exec.Floor(n)),
		named("sqrt", exec.Sqrt(exec.Sub(n, exec.Int(40)))),
		named("ln", exec.Ln(exec.Sub(score, exec.Int(100)))),
		named("log", exec.Log(score)),
		named("log to base", exec.Log(n, exec.Int(2))),
		named("exp", exec.Exp(exec.Div(n, exec.Float(10)))),
		named("pow", exec.Pow(x, exec.Int(3))),
		named("pow of any", exec.Pow(exec.Sub(n, exec.Int(30)), exec.Float(0.5))),
		named("trim", exec.Trim(exec.Concat(exec.Str(" \t"), msg, exec.Str("  ")))),
		named("trim characters", exec.Trim(msg, exec.Str("gdtme "))),
		named("trim left", exec.TrimLeft(coll, exec.Str("fedg."))),
		named("trim right", exec.TrimRight(exec.Concat(coll, exec.Str("\n")))),
		named("index of", exec.IndexOf(msg, exec.Str("o"))),
		named("index of any", exec.IndexOf(exec.Add(exec.Str("né "), coll), n)),
		named("bin time", exec.Bin(exec.Time(), exec.Int(60_000_000_000))),
		named("bin float", exec.Bin(score, exec.Float(2.5))),
		named("bin any", exec.Bin(n, x)),
		named("to int", exec.ToInt(n)),
		named("to int of text", exec.ToInt(exec.Substr(user, exec.Int(1)))),
		named("to float", exec.ToFloat(n)),
		named("to string", exec.ToString(n)),
		named("to string of float", exec.ToString(score)),
		named("to bool", exec.ToBool(n)),
		named("if", exec.If(exec.Gt(n, exec.Int(50)), exec.Str("high"), exec.ToString(x))),
		named("if of flag", exec.If(anyOf("flag"), n, score)),
		named("case", exec.Case(exec.Lt(n, exec.Int(10)), exec.Str("low"), exec.Lt(n, exec.Float(60.5)), exec.Int(1), exec.Eq(coll, exec.Str("feed.post")), kind)),
		named("case with default", exec.Case(exec.StartsWith(msg, exec.Str("good")), exec.Int(1), exec.Int(0))),
		named("coalesce", exec.Coalesce(anyOf("op"), msg, exec.Str("none"))),
		named("coalesce numbers", exec.Coalesce(num("n"), float("n"), x)),
		named("nullif", exec.NullIf(kind, exec.Str("commit"))),
		named("nullif numbers", exec.NullIf(n, exec.Float(5))),
		named("exists", exec.Exists(n)),
		named("exists typed", exec.Exists(num("n"))),
		named("is null", exec.IsNull(n)),
		named("is missing", exec.IsMissing(n)),
		named("type", exec.TypeOf(n)),
		named("type of tags", exec.TypeOf(anyOf("tags"))),
		named("type of sum", exec.TypeOf(exec.Add(n, x))),
		named("length", exec.Length(msg)),
		named("length of kind", exec.Length(kind)),
		named("lower", exec.Lower(msg)),
		named("upper", exec.Upper(coll)),
		named("substr", exec.Substr(msg, exec.Int(2), exec.Int(5))),
		named("substr from end", exec.Substr(user, exec.Int(-2))),
		named("substr by column", exec.Substr(msg, x, x)),
		named("concat", exec.Concat(kind, exec.Str("/"), exec.Coalesce(coll, exec.Str("-")), exec.Str("/"), user)),
		named("concat with a number", exec.Concat(kind, exec.ToString(x))),
		named("starts with", exec.StartsWith(coll, exec.Str("feed."))),
		named("ends with", exec.EndsWith(msg, exec.Str("e"))),
		named("replace", exec.Replace(msg, exec.Str("o"), exec.Str("00"))),
		named("replace in keys", exec.Replace(exec.Upper(coll), exec.Str("."), exec.Str(""))),
		named("upper of lower", exec.Upper(exec.Substr(exec.Lower(kind), exec.Int(0), exec.Int(3)))),
		named("compare", exec.Le(exec.Mul(n, exec.Int(2)), score)),
		named("compare strings", exec.Lt(kind, user)),
		named("logic", exec.Or(exec.And(exec.Gt(n, exec.Int(20)), exec.Lt(score, exec.Float(500))), exec.Not(anyOf("flag")))),
		named("match", exec.Match(exec.Lower(msg), tokenfilter.Word("good"))),
		named("hour", exec.HourOfDay(exec.Time())),
	}
}

func TestScalarFunctions(t *testing.T) {
	exprs := scalarExprs()
	eachLayout(t, 2500, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
		for _, e := range exprs {
			t.Run(e.Name, func(t *testing.T) {
				filter := exec.Gt(num("id"), exec.Int(700))
				row := []exec.Named{named("id", num("id")), named("v", e.Expr)}
				count := []exec.Agg{{Name: "rows", Func: exec.Count}}
				aggs := []exec.Agg{count[0], {Name: "values", Func: exec.Count, Arg: e.Expr}, {Name: "distinct", Func: exec.CountDistinct, Arg: e.Expr},
					{Name: "sum", Func: exec.Sum, Arg: exec.Coalesce(exec.ToFloat(e.Expr), exec.ToFloat(exec.Length(exec.ToString(e.Expr))))}}
				plans := map[string]exec.Node{
					"rows":          &exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Exprs: row}, By: []exec.Order{{Column: "id"}}},
					"filtered rows": &exec.Sort{Input: &exec.Project{Input: &exec.Scan{Filter: filter}, Exprs: row}, By: []exec.Order{{Column: "id"}}},
					"key":           &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{named("v", e.Expr)}, Aggs: count},
					"filtered keys": &exec.Aggregate{Input: &exec.Scan{Filter: filter}, Keys: []exec.Named{named("kind", anyOf("kind")), named("v", e.Expr)}, Aggs: count},
					"argument":      &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("kind"), Aggs: aggs},
					"all arguments": &exec.Aggregate{Input: &exec.Scan{Filter: filter}, Aggs: aggs},
					"filter":        &exec.Aggregate{Input: &exec.Scan{Filter: e.Expr}, Keys: keys("kind"), Aggs: count},
					"negated":       &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(filter, exec.Not(e.Expr))}, Aggs: count},
					"compared":      &exec.Aggregate{Input: &exec.Scan{Filter: exec.Ge(exec.ToString(e.Expr), exec.Str("c"))}, Aggs: count},
				}
				for name, plan := range plans {
					_, sorted := plan.(*exec.Sort)
					t.Run(name, func(t *testing.T) { check(t, snap, rows, plan, sorted, workers) })
				}
			})
		}
	})
}

func TestPresenceFilters(t *testing.T) {
	count := []exec.Agg{{Name: "rows", Func: exec.Count}}
	eachLayout(t, 2500, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
		for _, path := range []string{"n", "kind", "op", "msg", "flag", "tags", "nope", "nested.x"} {
			for name, e := range map[string]*exec.Expr{
				"exists": exec.Exists(anyOf(path)), "null": exec.IsNull(anyOf(path)), "missing": exec.IsMissing(anyOf(path)),
				"exists as int": exec.Exists(num(path)), "missing as string": exec.IsMissing(str(path)), "null as int": exec.IsNull(num(path)),
			} {
				for filter, f := range map[string]*exec.Expr{"alone": e, "negated": exec.Not(e),
					"with others": exec.And(exec.Gt(num("id"), exec.Int(300)), exec.Or(e, exec.Eq(str("kind"), exec.Str("identity"))))} {
					t.Run(fmt.Sprintf("%s/%s/%s", path, name, filter), func(t *testing.T) {
						check(t, snap, rows, &exec.Aggregate{Input: &exec.Scan{Filter: f}, Keys: keys("kind"), Aggs: count}, false, workers)
					})
				}
			}
		}
	})
}

func TestScalarFunctionsOverAggregates(t *testing.T) {
	agg := &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("kind", "coll"), Aggs: []exec.Agg{
		{Name: "rows", Func: exec.Count}, {Name: "total", Func: exec.Sum, Arg: anyOf("n")}, {Name: "top", Func: exec.Max, Arg: anyOf("score")}}}
	plan := &exec.Sort{Input: &exec.Project{Input: agg, Exprs: []exec.Named{
		named("kind", anyOf("kind")),
		named("coll", anyOf("coll")),
		named("label", exec.Concat(exec.Upper(anyOf("kind")), exec.Str(":"), exec.Coalesce(anyOf("coll"), exec.Str("-")))),
		named("share", exec.Div(exec.ToFloat(anyOf("total")), exec.ToFloat(anyOf("rows")))),
		named("size", exec.Case(exec.Gt(anyOf("rows"), exec.Int(100)), exec.Str("big"), exec.Gt(anyOf("rows"), exec.Int(10)), exec.Str("mid"), exec.Str("small"))),
		named("bucket", exec.Bin(anyOf("top"), exec.Int(100))),
		named("typed", exec.TypeOf(anyOf("total"))),
		named("none", exec.IsMissing(anyOf("coll"))),
	}}, By: []exec.Order{{Column: "kind"}, {Column: "coll"}}}
	eachLayout(t, 2500, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
		check(t, snap, rows, plan, true, workers)
	})
}

func TestNullAndMissingKeys(t *testing.T) {
	var docs [][]byte
	for i := 0; i < 900; i++ {
		switch i % 9 {
		case 0, 1:
			docs = append(docs, []byte(fmt.Sprintf(`{"id":%d,"k":"a"}`, i)))
		case 2, 3, 4:
			docs = append(docs, []byte(fmt.Sprintf(`{"id":%d,"k":null}`, i)))
		default:
			docs = append(docs, []byte(fmt.Sprintf(`{"id":%d}`, i)))
		}
	}
	rows := referenceRows(t, docs, "")
	count := []exec.Agg{{Name: "rows", Func: exec.Count}}
	upper := exec.Upper(anyOf("k"))
	cases := []struct {
		name string
		plan exec.Node
		want string
	}{
		{"a path", &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("k"), Aggs: count}, `["a" | 200 null | 700]`},
		{"a path as strings", &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{named("k", str("k"))}, Aggs: count}, `["a" | 200 null | 700]`},
		{"a function", &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{named("k", upper)}, Aggs: count}, `["A" | 200 null | 700]`},
		{"null function", &exec.Aggregate{Input: &exec.Scan{Filter: exec.IsNull(upper)}, Aggs: count}, `[700]`},
		{"missing function", &exec.Aggregate{Input: &exec.Scan{Filter: exec.IsMissing(upper)}, Aggs: count}, `[0]`},
		{"present function", &exec.Aggregate{Input: &exec.Scan{Filter: exec.Exists(upper)}, Aggs: count}, `[900]`},
		{"null path", &exec.Aggregate{Input: &exec.Scan{Filter: exec.IsNull(anyOf("k"))}, Aggs: count}, `[300]`},
		{"missing path", &exec.Aggregate{Input: &exec.Scan{Filter: exec.IsMissing(anyOf("k"))}, Aggs: count}, `[400]`},
		{"kinds", &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{named("t", exec.TypeOf(anyOf("k"))), named("u", exec.TypeOf(upper))}, Aggs: count},
			`["missing" | "null" | 400 "null" | "null" | 300 "string" | "string" | 200]`},
	}
	for _, blockRows := range []int{64, 1 << 16} {
		tbl := buildTable(t, docs, "", layout{blockRows: blockRows, parts: 2})
		snap := tbl.Snapshot()
		for _, c := range cases {
			t.Run(fmt.Sprintf("%s/%d", c.name, blockRows), func(t *testing.T) {
				res := check(t, snap, rows, c.plan, false, 3)
				got := resultRows(res)
				sort.Strings(got)
				if fmt.Sprint(got) != c.want {
					t.Fatalf("got %v, want %s", got, c.want)
				}
			})
		}
		snap.Release()
	}
}

// TestTotalsOverGroups covers the share of each group in the total of a
// column, over the groups that pass the filter above the aggregate.
func TestTotalsOverGroups(t *testing.T) {
	aggs := []exec.Agg{
		{Name: "rows", Func: exec.Count},
		{Name: "sum_score", Func: exec.Sum, Arg: float("score")},
		{Name: "sum_n", Func: exec.Sum, Arg: num("n")},
		{Name: "max_msg", Func: exec.Max, Arg: str("msg")},
	}
	shares := []exec.Named{
		named("user", anyOf("user")),
		named("rows", anyOf("rows")),
		named("all", exec.Total(anyOf("rows"))),
		named("share", exec.Round(exec.Div(exec.Mul(anyOf("rows"), exec.Float(100)), exec.Total(anyOf("rows"))), exec.Int(3))),
		named("of score", exec.Div(anyOf("sum_score"), exec.Total(anyOf("sum_score")))),
		named("ints", exec.Sub(exec.Total(anyOf("sum_n")), anyOf("sum_n"))),
		named("no numbers", exec.Total(anyOf("max_msg"))),
	}
	posts := exec.And(exec.Eq(str("kind"), exec.Str("commit")), exec.Eq(str("op"), exec.Str("create")), exec.Eq(str("coll"), exec.Str("feed.post")))
	byUser := func(filter *exec.Expr) exec.Node {
		return &exec.Aggregate{Input: &exec.Scan{Filter: filter}, Keys: keys("user"), Aggs: aggs}
	}
	top := []exec.Order{{Column: "rows", Desc: true}, {Column: "user"}}
	plans := map[string]exec.Node{
		"top":           &exec.Sort{Input: &exec.Project{Input: byUser(nil), Exprs: shares}, By: top, Limit: 10},
		"in key order":  &exec.Sort{Input: &exec.Project{Input: byUser(posts), Exprs: shares}, By: top, Limit: 10},
		"of the passed": &exec.Sort{Input: &exec.Project{Input: &exec.Filter{Input: byUser(nil), Cond: exec.Gt(anyOf("rows"), exec.Int(3))}, Exprs: shares}, By: top},
		"of none":       &exec.Project{Input: &exec.Filter{Input: byUser(nil), Cond: exec.Gt(anyOf("rows"), exec.Int(1<<40))}, Exprs: shares},
		"of one group":  &exec.Project{Input: &exec.Aggregate{Input: &exec.Scan{}, Aggs: aggs}, Exprs: shares[1:]},
		"above a share": &exec.Sort{Input: &exec.Project{Input: &exec.Filter{Input: byUser(nil),
			Cond: exec.Gt(exec.Mul(anyOf("rows"), exec.Int(200)), exec.Total(anyOf("rows")))}, Exprs: shares}, By: top},
	}
	for _, limits := range [][2]int{{1 << 16, 4096}, {4, 0}} {
		restore := exec.SetLimits(limits[0], limits[1])
		eachLayout(t, 3000, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
			for name, plan := range plans {
				t.Run(fmt.Sprintf("%s/%d", name, limits[0]), func(t *testing.T) {
					_, sorted := plan.(*exec.Sort)
					check(t, snap, rows, plan, sorted, workers)
				})
			}
		})
		restore()
	}
	for name, plan := range map[string]exec.Node{
		"a total of rows":       &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{named("x", exec.Total(anyOf("n")))}},
		"a total of a function": &exec.Project{Input: byUser(nil), Exprs: []exec.Named{named("x", exec.Total(exec.Add(anyOf("rows"), exec.Int(1))))}},
	} {
		if _, err := exec.Run(context.Background(), colstore.NewTable().Snapshot(), plan, exec.Options{}); !errors.Is(err, exec.ErrUnsupported) {
			t.Errorf("%s: got %v, want ErrUnsupported", name, err)
		}
	}
}
