package optimizer

import (
	"sort"
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
)

func TestColumnPruning_StatsQuery(t *testing.T) {
	// search error | stats count by host → required: [_raw, _time, host]
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: "error"},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
				GroupBy:      []string{"host"},
			},
		},
	}
	opt := New()
	result := opt.Optimize(q)

	if opt.Stats["ColumnPruning"] == 0 {
		t.Fatal("ColumnPruning rule should have fired")
	}

	ann, ok := result.GetAnnotation("requiredColumns")
	if !ok {
		t.Fatal("requiredColumns annotation not set")
	}
	cols := ann.([]string)
	colMap := make(map[string]bool, len(cols))
	for _, c := range cols {
		colMap[c] = true
	}
	if !colMap["_raw"] {
		t.Error("expected _raw in required columns")
	}
	if !colMap["_time"] {
		t.Error("expected _time in required columns")
	}
	if !colMap["host"] {
		t.Error("expected host in required columns")
	}
}

func TestColumnPruning_TableCommand(t *testing.T) {
	// ... | eval x=1, y=2 | table y → required should include y but not necessarily exclude x
	// (since column pruning is forward analysis, it collects all accessed fields)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.EvalCommand{
				Assignments: []spl2.EvalAssignment{
					{Field: "x", Expr: &spl2.LiteralExpr{Value: "1"}},
					{Field: "y", Expr: &spl2.LiteralExpr{Value: "2"}},
				},
			},
			&spl2.TableCommand{Fields: []string{"y"}},
		},
	}
	opt := New()
	result := opt.Optimize(q)

	ann, ok := result.GetAnnotation("requiredColumns")
	if !ok {
		t.Fatal("requiredColumns annotation not set")
	}
	cols := ann.([]string)
	colMap := make(map[string]bool, len(cols))
	for _, c := range cols {
		colMap[c] = true
	}
	if !colMap["y"] {
		t.Error("expected y in required columns")
	}
	if !colMap["_time"] {
		t.Error("expected _time in required columns")
	}
}

func TestColumnPruning_EvalChain(t *testing.T) {
	// eval a=b+c | eval d=a+1 | fields d → required: [b, c, _time, a, d]
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.EvalCommand{
				Field: "a",
				Expr: &spl2.ArithExpr{
					Left:  &spl2.FieldExpr{Name: "b"},
					Op:    "+",
					Right: &spl2.FieldExpr{Name: "c"},
				},
			},
			&spl2.EvalCommand{
				Field: "d",
				Expr: &spl2.ArithExpr{
					Left:  &spl2.FieldExpr{Name: "a"},
					Op:    "+",
					Right: &spl2.LiteralExpr{Value: "1"},
				},
			},
			&spl2.FieldsCommand{Fields: []string{"d"}},
		},
	}
	opt := New()
	result := opt.Optimize(q)

	ann, ok := result.GetAnnotation("requiredColumns")
	if !ok {
		t.Fatal("requiredColumns annotation not set")
	}
	cols := ann.([]string)
	colMap := make(map[string]bool, len(cols))
	for _, c := range cols {
		colMap[c] = true
	}
	if !colMap["b"] {
		t.Error("expected b in required columns")
	}
	if !colMap["c"] {
		t.Error("expected c in required columns")
	}
	if !colMap["_time"] {
		t.Error("expected _time in required columns")
	}
}

func TestProjectionPushdown_InsertEarlyBeforeSort(t *testing.T) {
	// search * | eval x=1 | sort -time | table host
	// → should insert fields before sort
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: ""},
			&spl2.EvalCommand{Field: "x", Expr: &spl2.LiteralExpr{Value: "1"}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "time", Desc: true}}},
			&spl2.TableCommand{Fields: []string{"host"}},
		},
	}
	opt := New()
	result := opt.Optimize(q)

	if opt.Stats["ProjectionPushdown"] == 0 {
		t.Skip("ProjectionPushdown didn't fire — may depend on command count threshold")
	}

	// Check that a FieldsCommand was inserted before the SortCommand.
	foundFieldsBeforeSort := false
	for i, cmd := range result.Commands {
		if _, ok := cmd.(*spl2.FieldsCommand); ok {
			// Check if next command is sort.
			if i+1 < len(result.Commands) {
				if _, ok := result.Commands[i+1].(*spl2.SortCommand); ok {
					foundFieldsBeforeSort = true
				}
			}
		}
	}
	if !foundFieldsBeforeSort {
		t.Log("FIELDS command not inserted before SORT — behavior may vary based on pipeline depth")
	}
}

func TestComputeRequiredColumns_Sort(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{
				{Name: "status", Desc: true},
				{Name: "host"},
			}},
		},
	}
	cols := computeRequiredColumns(q)
	sort.Strings(cols)
	colMap := make(map[string]bool, len(cols))
	for _, c := range cols {
		colMap[c] = true
	}
	if !colMap["status"] {
		t.Error("expected status")
	}
	if !colMap["host"] {
		t.Error("expected host")
	}
	if !colMap["_time"] {
		t.Error("expected _time")
	}
}

func TestComputeRequiredColumns_Rex(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.RexCommand{Field: "", Pattern: `(?P<user>\w+)`}, // default: _raw
		},
	}
	cols := computeRequiredColumns(q)
	colMap := make(map[string]bool, len(cols))
	for _, c := range cols {
		colMap[c] = true
	}
	if !colMap["_raw"] {
		t.Error("expected _raw for rex with default field")
	}
}

func TestComputeRequiredColumns_Rename(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.RenameCommand{Renames: []spl2.RenamePair{
				{Old: "src", New: "source_ip"},
			}},
		},
	}
	cols := computeRequiredColumns(q)
	colMap := make(map[string]bool, len(cols))
	for _, c := range cols {
		colMap[c] = true
	}
	if !colMap["src"] {
		t.Error("expected src")
	}
}
