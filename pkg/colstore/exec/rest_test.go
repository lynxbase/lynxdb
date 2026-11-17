package exec_test

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// TestRestOfTheFields requires, of projections that end in every other
// field of their rows, the rows of the reference evaluator.
func TestRestOfTheFields(t *testing.T) {
	eq := func(path, v string) *exec.Expr { return exec.Eq(anyOf(path), exec.Str(v)) }
	id := named("id", anyOf("id"))
	cases := []struct {
		name    string
		plan    exec.Node
		ordered bool
	}{
		{"every field of every row", &exec.Project{Input: &exec.Scan{}, Rest: true}, false},
		{"every field of some rows", &exec.Project{Input: &exec.Scan{Filter: eq("user", "u0001")}, Rest: true}, false},
		{"the first rows of a sort", &exec.Sort{Input: &exec.Project{Input: &exec.Scan{Filter: eq("kind", "commit")}, Exprs: []exec.Named{id}, Rest: true},
			By: []exec.Order{{Column: "id", Desc: true}}, Limit: 7}, true},
		{"every row of a sort", &exec.Sort{Input: &exec.Project{Input: &exec.Scan{Filter: eq("kind", "identity")}, Exprs: []exec.Named{id}, Rest: true},
			By: []exec.Order{{Column: "id"}}}, true},
		{"computed and hidden fields", &exec.Sort{Input: &exec.Project{Input: &exec.Scan{},
			Exprs:  []exec.Named{named("at", exec.Time()), named("n", exec.Mul(anyOf("score"), exec.Int(2))), named("who", anyOf("user")), id},
			Rest:   true,
			Except: []string{"nested", "msg", "nope", "_time"}}, By: []exec.Order{{Column: "at", Desc: true}, {Column: "id"}}, Limit: 5}, true},
		{"columns read for the rows of the result", &exec.Sort{Input: &exec.Project{Input: &exec.Scan{},
			Exprs: []exec.Named{id, named("msg", anyOf("msg")), named("y", anyOf("nested.y")), named("twice", exec.Mul(anyOf("id"), exec.Int(2)))}},
			By: []exec.Order{{Column: "twice", Desc: true}}, Limit: 9}, true},
	}
	cases = append(cases, []struct {
		name    string
		plan    exec.Node
		ordered bool
	}{
		{"a sort by a column that is not shown", &exec.Sort{Input: &exec.Project{Input: &exec.Scan{},
			Exprs: []exec.Named{id, named("who", anyOf("user")), named("at", exec.Time())}},
			By: []exec.Order{{Column: "at", Desc: true}, {Column: "id"}}, Limit: 6, Hide: []string{"at", "id"}}, true},
		{"whole rows by a column that is not shown", &exec.Sort{Input: &exec.Project{Input: &exec.Scan{Filter: eq("kind", "commit")},
			Exprs: []exec.Named{id}, Rest: true, Except: []string{"big"}}, By: []exec.Order{{Column: "id", Desc: true}}, Limit: 4, Hide: []string{"id"}}, true},
		{"groups by a column that is not shown", &exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("coll"),
			Aggs: []exec.Agg{{Name: "n", Func: exec.Count}}}, By: []exec.Order{{Column: "n", Desc: true}, {Column: "coll"}}, Limit: 3, Hide: []string{"n"}}, true},
	}...)
	eachLayout(t, 3000, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				check(t, snap, rows, c.plan, c.ordered, workers)
			})
		}
		t.Run("a limit without a sort", func(t *testing.T) {
			plan := &exec.Sort{Input: &exec.Project{Input: &exec.Scan{Filter: eq("kind", "commit")}, Rest: true}, Limit: 3}
			res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: workers})
			if err != nil {
				t.Fatal(err)
			}
			kind := -1
			for i, c := range res.Columns {
				if c.Name == "kind" {
					kind = i
				}
			}
			if res.Rows != 3 || kind < 0 {
				t.Fatalf("%d rows, kind is column %d", res.Rows, kind)
			}
			for r := 0; r < res.Rows; r++ {
				if v := res.Value(r, kind); string(v.Bytes) != "commit" {
					t.Fatalf("row %d is of kind %q", r, v.Bytes)
				}
			}
		})
	})
}

// TestFieldsOfPickedRows requires, of an aggregate that picks the first row
// of every group, the fields of those rows; the table keeps its rows in the
// order they came in, which is the order a first row is told by.
func TestFieldsOfPickedRows(t *testing.T) {
	docs := synthDocs(3000, 11)
	rows := referenceRows(t, docs, "ts")
	first := exec.Agg{Name: "row", Func: exec.FirstRow, Arg: exec.Time()}
	row := named("row", anyOf("row"))
	for _, l := range []layout{{name: "blocks", blockRows: 64, parts: 1}, {name: "parts", blockRows: 100, parts: 3}} {
		tbl := buildTable(t, docs, "ts", l)
		snap := tbl.Snapshot()
		for name, plan := range map[string]exec.Node{
			"the first row of every user": &exec.Project{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("user"), Aggs: []exec.Agg{first}},
				Exprs: []exec.Named{row}, Rest: true},
			"with a count and without some fields": &exec.Sort{Input: &exec.Project{
				Input: &exec.Aggregate{Input: &exec.Scan{Filter: exec.Eq(anyOf("kind"), exec.Str("commit"))}, Keys: keys("coll", "op"),
					Aggs: []exec.Agg{{Name: "n", Func: exec.Count}, first, {Name: "one", Func: exec.FirstRow, Arg: anyOf("id")}}},
				Exprs: []exec.Named{named("events", anyOf("n")), named("id", anyOf("one")), row}, Rest: true, Except: []string{"nested", "msg", "ts"}},
				By: []exec.Order{{Column: "events", Desc: true}, {Column: "id"}}, Limit: 8},
			"filtered and sorted by what a row holds": &exec.Sort{Input: &exec.Project{
				Input: &exec.Filter{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("user"),
					Aggs: []exec.Agg{{Name: "n", Func: exec.Count}, first, {Name: "one", Func: exec.FirstRow, Arg: anyOf("id")}}},
					Cond: exec.Gt(anyOf("n"), exec.Int(3))},
				Exprs: []exec.Named{named("twice", exec.Mul(anyOf("one"), exec.Int(2))), row}, Rest: true},
				By: []exec.Order{{Column: "twice", Desc: true}}, Limit: 5, Hide: []string{"twice"}},
		} {
			for _, workers := range []int{1, 3} {
				t.Run(l.name+"/"+name, func(t *testing.T) {
					_, sorted := plan.(*exec.Sort)
					check(t, snap, rows, plan, sorted, workers)
				})
			}
		}
		snap.Release()
	}
}
