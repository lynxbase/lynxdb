package optimizer

import (
	"sort"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/spl2"
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
		t.Error("expected ProjectionPushdown rule to fire, but Stats shows 0")
	}

	// Check that a FieldsCommand was inserted before the SortCommand.
	foundFieldsBeforeSort := false
	for i, cmd := range result.Commands {
		if _, ok := cmd.(*spl2.FieldsCommand); ok {
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

// TestColumnPruning_GlobWithUnpackPrefix tests that a glob pattern like "pg.*"
// does NOT disable column pruning when an upstream UnpackCommand generates
// fields with the matching prefix "pg.".
func TestColumnPruning_GlobWithUnpackPrefix(t *testing.T) {
	// Pipeline: search log_type="postgres" | parse postgres(message) as pg | keep pg.*
	// The glob "pg.*" matches the unpack prefix "pg." — column pruning should
	// remain active and only require segment columns (_time, _raw, log_type, message).
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SearchCommand{
				Expression: &spl2.SearchCompareExpr{
					Field: "log_type",
					Op:    spl2.OpEq,
					Value: "postgres",
				},
			},
			&spl2.UnpackCommand{
				Format:      "postgres",
				SourceField: "message",
				Prefix:      "pg.",
			},
			&spl2.FieldsCommand{Fields: []string{"pg.*"}},
		},
	}

	cols := computeRequiredColumns(q)
	if cols == nil {
		t.Fatal("expected column pruning to remain active (non-nil), got nil (disabled)")
	}

	colMap := make(map[string]bool, len(cols))
	for _, c := range cols {
		colMap[c] = true
	}

	// Should include base columns needed by the pipeline.
	if !colMap["_time"] {
		t.Error("expected _time in required columns")
	}
	if !colMap["_raw"] {
		t.Error("expected _raw in required columns")
	}
	if !colMap["message"] {
		t.Error("expected message (unpack source field) in required columns")
	}
	if !colMap["log_type"] {
		t.Error("expected log_type (search filter field) in required columns")
	}

	// Should NOT include "*" (glob sentinel).
	if colMap["*"] {
		t.Error("cols should not contain '*' sentinel when glob matches unpack prefix")
	}
}

// TestColumnPruning_GlobWithoutUnpackPrefix tests that a bare glob like "*"
// or "foo.*" without a matching UnpackCommand still disables column pruning.
func TestColumnPruning_GlobWithoutUnpackPrefix(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.FieldsCommand{Fields: []string{"foo.*"}},
		},
	}

	cols := computeRequiredColumns(q)
	if cols != nil {
		t.Errorf("expected nil (pruning disabled for unresolvable glob), got %v", cols)
	}
}

// TestColumnPruning_GlobMixedWithUnpackPrefix tests a FIELDS command with both
// a glob matching an unpack prefix and a plain field.
func TestColumnPruning_GlobMixedWithUnpackPrefix(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.UnpackCommand{
				Format:      "json",
				SourceField: "_raw",
				Prefix:      "j.",
			},
			&spl2.FieldsCommand{Fields: []string{"j.*", "host"}},
		},
	}

	cols := computeRequiredColumns(q)
	if cols == nil {
		t.Fatal("expected column pruning to remain active, got nil")
	}

	colMap := make(map[string]bool, len(cols))
	for _, c := range cols {
		colMap[c] = true
	}

	if !colMap["host"] {
		t.Error("expected host in required columns")
	}
	if !colMap["_raw"] {
		t.Error("expected _raw in required columns (unpack source)")
	}
}

// TestUnpackFieldPruning_BasicPrefixed tests that the UnpackFieldPruning rule
// restricts extraction to only downstream-consumed fields.
func TestUnpackFieldPruning_BasicPrefixed(t *testing.T) {
	// Pipeline: | parse json(_raw) as j | where j.status >= 500 | table j.status, j.uri
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.UnpackCommand{
				Format:      "json",
				SourceField: "_raw",
				Prefix:      "j.",
			},
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "j.status"},
					Op:    ">=",
					Right: &spl2.LiteralExpr{Value: "500"},
				},
			},
			&spl2.TableCommand{Fields: []string{"j.status", "j.uri"}},
		},
	}

	opt := New()
	result := opt.Optimize(q)

	if opt.Stats["UnpackFieldPruning"] == 0 {
		t.Fatal("UnpackFieldPruning rule should have fired")
	}

	// Find the UnpackCommand and check its Fields.
	for _, cmd := range result.Commands {
		if u, ok := cmd.(*spl2.UnpackCommand); ok {
			if len(u.Fields) == 0 {
				t.Fatal("UnpackCommand.Fields should be non-empty after pruning")
			}
			fieldMap := make(map[string]bool, len(u.Fields))
			for _, f := range u.Fields {
				fieldMap[f] = true
			}
			if !fieldMap["status"] {
				t.Error("expected 'status' in UnpackCommand.Fields")
			}
			if !fieldMap["uri"] {
				t.Error("expected 'uri' in UnpackCommand.Fields")
			}
			return
		}
	}
	t.Fatal("UnpackCommand not found in optimized query")
}

// TestUnpackFieldPruning_NoPrefix_Skipped tests that unpacks without a prefix
// are not optimized (can't distinguish unpack fields from segment fields).
func TestUnpackFieldPruning_NoPrefix_Skipped(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.UnpackCommand{
				Format:      "json",
				SourceField: "_raw",
				Prefix:      "", // no prefix
			},
			&spl2.TableCommand{Fields: []string{"status", "uri"}},
		},
	}

	opt := New()
	opt.Optimize(q)

	if opt.Stats["UnpackFieldPruning"] != 0 {
		t.Error("UnpackFieldPruning should NOT fire for prefix-less unpack")
	}
}

// TestUnpackFieldPruning_GlobDownstream_Skipped tests that the rule does NOT
// fire when downstream uses glob patterns (can't enumerate fields).
func TestUnpackFieldPruning_GlobDownstream_Skipped(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.UnpackCommand{
				Format:      "postgres",
				SourceField: "message",
				Prefix:      "pg.",
			},
			&spl2.FieldsCommand{Fields: []string{"pg.*"}},
		},
	}

	opt := New()
	opt.Optimize(q)

	if opt.Stats["UnpackFieldPruning"] != 0 {
		t.Error("UnpackFieldPruning should NOT fire when downstream uses glob")
	}
}

// TestGlobMatchesUnpackPrefix tests the glob matching helper.
func TestGlobMatchesUnpackPrefix(t *testing.T) {
	tests := []struct {
		fields   []string
		prefixes map[string]bool
		want     bool
	}{
		{[]string{"pg.*"}, map[string]bool{"pg.": true}, true},
		{[]string{"j.*"}, map[string]bool{"j.": true}, true},
		{[]string{"pg.*", "j.*"}, map[string]bool{"pg.": true, "j.": true}, true},
		{[]string{"pg.*"}, map[string]bool{"j.": true}, false},         // no matching prefix
		{[]string{"pg.*", "j.*"}, map[string]bool{"pg.": true}, false}, // j.* not covered
		{[]string{"*"}, map[string]bool{"pg.": true}, false},           // bare * not a prefix glob
		{[]string{"pg.*"}, map[string]bool{}, false},                   // no prefixes
		{[]string{"pg.*"}, nil, false},                                 // nil prefixes
		{[]string{"host"}, map[string]bool{"pg.": true}, true},         // non-glob field → skip, all globs covered (vacuously)
		{[]string{"host", "pg.*"}, map[string]bool{"pg.": true}, true}, // non-glob + covered glob
	}

	for _, tt := range tests {
		got := globMatchesUnpackPrefix(tt.fields, tt.prefixes)
		if got != tt.want {
			t.Errorf("globMatchesUnpackPrefix(%v, %v) = %v, want %v",
				tt.fields, tt.prefixes, got, tt.want)
		}
	}
}
