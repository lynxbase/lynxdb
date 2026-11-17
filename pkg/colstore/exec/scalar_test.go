package exec_test

import (
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
		{"a path", &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("k"), Aggs: count}, `["a" | 200 null | 300 null | 400]`},
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
