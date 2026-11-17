package exec_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// named renders the rows of a result with the names of their columns, since
// blocks handed over one by one differ in the fields their rows hold.
func namedRows(res *exec.Result) []string {
	out := make([]string, res.Rows)
	for i := range out {
		var fields []string
		for c, v := range res.Row(i, nil) {
			if v.Kind != exec.KindAny {
				fields = append(fields, res.Columns[c].Name+"="+render(v))
			}
		}
		sort.Strings(fields)
		out[i] = strings.Join(fields, " ")
	}

	return out
}

// TestEachHandsOverTheRowsOfRun requires that the rows handed over block by
// block are those that the plan gives when it runs to its end.
func TestEachHandsOverTheRowsOfRun(t *testing.T) {
	eq := func(path, v string) *exec.Expr { return exec.Eq(anyOf(path), exec.Str(v)) }
	cases := map[string]exec.Node{
		"columns":              &exec.Project{Input: &exec.Scan{Filter: eq("kind", "commit")}, Exprs: []exec.Named{named("id", num("id")), named("m", exec.Upper(anyOf("msg")))}},
		"whole rows":           &exec.Project{Input: &exec.Scan{Filter: exec.Lt(num("id"), exec.Int(700))}, Rest: true, Except: []string{"big"}},
		"columns and the rest": &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{named("who", anyOf("user"))}, Rest: true},
		"no row":               &exec.Project{Input: &exec.Scan{Filter: eq("kind", "nope")}, Rest: true},
	}
	eachLayout(t, 3000, func(t *testing.T, snap *colstore.Snapshot, _ []refRow, workers int) {
		for name, plan := range cases {
			t.Run(name, func(t *testing.T) {
				whole, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: workers})
				if err != nil {
					t.Fatal(err)
				}
				want := namedRows(whole)
				var got []string
				calls := 0
				err = exec.Each(context.Background(), snap, plan, exec.Options{Workers: workers}, func(res *exec.Result) error {
					calls++
					got = append(got, namedRows(res)...)

					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				sort.Strings(got)
				sort.Strings(want)
				if len(got) != len(want) {
					t.Fatalf("%d rows in %d calls, the plan gives %d", len(got), calls, len(want))
				}
				for i := range got {
					if got[i] != want[i] {
						t.Fatalf("row %d\n got  %s\n want %s", i, got[i], want[i])
					}
				}
			})
		}
		t.Run("a limit and an error end it", func(t *testing.T) {
			plan := &exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Rest: true}, Limit: 37}
			rows := 0
			if err := exec.Each(context.Background(), snap, plan, exec.Options{Workers: workers}, func(res *exec.Result) error {
				rows += res.Rows

				return nil
			}); err != nil || rows != 37 {
				t.Fatalf("%d rows of a limit of 37, %v", rows, err)
			}
			failed := errors.New("enough")
			calls := 0
			err := exec.Each(context.Background(), snap, &exec.Project{Input: &exec.Scan{}, Rest: true}, exec.Options{Workers: workers}, func(*exec.Result) error {
				calls++

				return failed
			})
			if !errors.Is(err, failed) || calls > workers {
				t.Fatalf("after an error of the caller: %v, %d calls", err, calls)
			}
		})
	})
	tbl := buildTable(t, synthDocs(10, 1), "ts", layout{blockRows: 64, parts: 1})
	snap := tbl.Snapshot()
	defer snap.Release()
	for name, plan := range map[string]exec.Node{
		"an aggregate": &exec.Aggregate{Input: &exec.Scan{}, Aggs: []exec.Agg{{Name: "n", Func: exec.Count}}},
		"a sort":       &exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{named("id", num("id"))}}, By: []exec.Order{{Column: "id"}}},
	} {
		if err := exec.Each(context.Background(), snap, plan, exec.Options{}, func(*exec.Result) error { return nil }); !errors.Is(err, exec.ErrUnsupported) {
			t.Errorf("%s: %v", name, err)
		}
		if err := exec.Supports(snap, plan); err != nil {
			t.Errorf("%s is a plan that runs: %v", name, err)
		}
	}
	if err := exec.Supports(snap, &exec.Scan{}); !errors.Is(err, exec.ErrUnsupported) {
		t.Errorf("a scan alone: %v", err)
	}
}
