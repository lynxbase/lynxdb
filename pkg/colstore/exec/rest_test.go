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
