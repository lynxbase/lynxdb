package optimizer

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func TestBloomEnrichment_WhereSource(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "source"},
					Op:    "=",
					Right: &spl2.LiteralExpr{Value: "nginx"},
				},
			},
		},
	}
	rule := &bloomFilterPruningRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("bloom filter rule should have fired")
	}
	ann, ok := result.GetAnnotation("bloomTerms")
	if !ok {
		t.Fatal("bloomTerms annotation not set")
	}
	terms := ann.([]string)
	if len(terms) != 1 || terms[0] != "nginx" {
		t.Errorf("expected [nginx], got %v", terms)
	}
}

func TestBloomEnrichment_WhereHost(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "host"},
					Op:    "=",
					Right: &spl2.LiteralExpr{Value: "web01"},
				},
			},
		},
	}
	rule := &bloomFilterPruningRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("bloom filter rule should have fired")
	}
	ann, ok := result.GetAnnotation("bloomTerms")
	if !ok {
		t.Fatal("bloomTerms annotation not set")
	}
	terms := ann.([]string)
	if len(terms) != 1 || terms[0] != "web01" {
		t.Errorf("expected [web01], got %v", terms)
	}
}

func TestBloomEnrichment_NonBloomField(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "status"},
					Op:    "=",
					Right: &spl2.LiteralExpr{Value: "500"},
				},
			},
		},
	}
	rule := &bloomFilterPruningRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("bloom filter rule should NOT fire for non-bloom fields")
	}
}

func TestBloomEnrichment_NotEqual(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "host"},
					Op:    "!=",
					Right: &spl2.LiteralExpr{Value: "web01"},
				},
			},
		},
	}
	rule := &bloomFilterPruningRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("bloom filter rule should NOT fire for != comparisons")
	}
}

func TestTimeRangePruning_WhereTime(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "_time"},
						Op:    ">=",
						Right: &spl2.LiteralExpr{Value: "2024-01-01T00:00:00Z"},
					},
					Op: "and",
					Right: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "_time"},
						Op:    "<=",
						Right: &spl2.LiteralExpr{Value: "2024-01-31T23:59:59Z"},
					},
				},
			},
		},
	}
	rule := &timeRangePruningRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("time range rule should have fired")
	}
	ann, ok := result.GetAnnotation("timeAnnotation")
	if !ok {
		t.Fatal("timeAnnotation not set")
	}
	tb := ann.(map[string]string)
	if tb["earliest"] != "2024-01-01T00:00:00Z" {
		t.Errorf("earliest = %q, want 2024-01-01T00:00:00Z", tb["earliest"])
	}
	if tb["latest"] != "2024-01-31T23:59:59Z" {
		t.Errorf("latest = %q, want 2024-01-31T23:59:59Z", tb["latest"])
	}
}

func TestTimeRangePruning_NoTimeField(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "status"},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "400"},
				},
			},
		},
	}
	rule := &timeRangePruningRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("time range rule should NOT fire without _time predicates")
	}
}

func TestColumnStats_FieldPredicates(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "status"},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "500"},
				},
			},
		},
	}
	rule := &columnStatsPruningRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("column stats rule should have fired")
	}
	ann, ok := result.GetAnnotation("fieldPredicates")
	if !ok {
		t.Fatal("fieldPredicates annotation not set")
	}
	preds := ann.([]FieldPredInfo)
	if len(preds) != 1 {
		t.Fatalf("expected 1 predicate, got %d", len(preds))
	}
	if preds[0].Field != "status" || preds[0].Op != ">" || preds[0].Value != "500" {
		t.Errorf("unexpected predicate: %+v", preds[0])
	}
}

func TestColumnStats_TimeFieldExcluded(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "_time"},
					Op:    ">=",
					Right: &spl2.LiteralExpr{Value: "2024-01-01T00:00:00Z"},
				},
			},
		},
	}
	rule := &columnStatsPruningRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("_time should be excluded from field predicates")
	}
}

func TestEarlyLimit_PushThroughEval(t *testing.T) {
	// eval x=1 | head 10 → head 10 | eval x=1
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.EvalCommand{Field: "x", Expr: &spl2.LiteralExpr{Value: "1"}},
			&spl2.HeadCommand{Count: 10},
		},
	}
	rule := &earlyLimitRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("early limit should push head through eval")
	}
	if _, ok := result.Commands[0].(*spl2.HeadCommand); !ok {
		t.Errorf("expected HeadCommand first, got %T", result.Commands[0])
	}
	if _, ok := result.Commands[1].(*spl2.EvalCommand); !ok {
		t.Errorf("expected EvalCommand second, got %T", result.Commands[1])
	}
}

func TestEarlyLimit_SortHead_TopN(t *testing.T) {
	// sort -x | head 10 → TopNCommand
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "x", Desc: true}}},
			&spl2.HeadCommand{Count: 10},
		},
	}
	rule := &earlyLimitRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("early limit should create TopN from sort+head")
	}
	if len(result.Commands) != 1 {
		t.Fatalf("expected 1 command (TopN), got %d", len(result.Commands))
	}
	topn, ok := result.Commands[0].(*spl2.TopNCommand)
	if !ok {
		t.Fatalf("expected TopNCommand, got %T", result.Commands[0])
	}
	if topn.Limit != 10 {
		t.Errorf("expected limit 10, got %d", topn.Limit)
	}
	if len(topn.Fields) != 1 || topn.Fields[0].Name != "x" || !topn.Fields[0].Desc {
		t.Errorf("unexpected fields: %+v", topn.Fields)
	}
}

func TestEarlyLimit_NotPushThroughWhere(t *testing.T) {
	// where x>5 | head 10 → should NOT push through where
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "x"},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "5"},
				},
			},
			&spl2.HeadCommand{Count: 10},
		},
	}
	rule := &earlyLimitRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("head should NOT be pushed through where")
	}
}

func TestEarlyLimit_PushThroughRename(t *testing.T) {
	// rename a AS b | head 5 → head 5 | rename a AS b
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.RenameCommand{Renames: []spl2.RenamePair{{Old: "a", New: "b"}}},
			&spl2.HeadCommand{Count: 5},
		},
	}
	rule := &earlyLimitRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("head should push through rename")
	}
	if _, ok := result.Commands[0].(*spl2.HeadCommand); !ok {
		t.Errorf("expected HeadCommand first, got %T", result.Commands[0])
	}
}

func TestEarlyLimit_PushThroughFillnull(t *testing.T) {
	// fillnull value=0 | head 5 → head 5 | fillnull value=0
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.FillnullCommand{Value: "0"},
			&spl2.HeadCommand{Count: 5},
		},
	}
	rule := &earlyLimitRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("head should push through fillnull")
	}
	if _, ok := result.Commands[0].(*spl2.HeadCommand); !ok {
		t.Errorf("expected HeadCommand first, got %T", result.Commands[0])
	}
}

func TestPredicatePushdown_ThroughRename(t *testing.T) {
	// rename src AS source | WHERE source="nginx"
	// → WHERE src="nginx" | rename src AS source
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.RenameCommand{Renames: []spl2.RenamePair{{Old: "src", New: "source"}}},
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "source"},
					Op:    "=",
					Right: &spl2.LiteralExpr{Value: "nginx"},
				},
			},
		},
	}
	rule := &predicatePushdownRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("should push WHERE through RENAME")
	}
	// WHERE should be first, with "src" instead of "source"
	w, ok := result.Commands[0].(*spl2.WhereCommand)
	if !ok {
		t.Fatalf("expected WhereCommand first, got %T", result.Commands[0])
	}
	cmp := w.Expr.(*spl2.CompareExpr)
	field := cmp.Left.(*spl2.FieldExpr)
	if field.Name != "src" {
		t.Errorf("expected field 'src', got '%s'", field.Name)
	}
}

func TestPredicatePushdown_ThroughFillnull(t *testing.T) {
	// fillnull value=0 | WHERE status > 500
	// → WHERE status > 500 | fillnull value=0
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.FillnullCommand{Value: "0"},
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "status"},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "500"},
				},
			},
		},
	}
	rule := &predicatePushdownRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("should push WHERE through FILLNULL")
	}
	if _, ok := result.Commands[0].(*spl2.WhereCommand); !ok {
		t.Fatalf("expected WhereCommand first, got %T", result.Commands[0])
	}
	if _, ok := result.Commands[1].(*spl2.FillnullCommand); !ok {
		t.Fatalf("expected FillnullCommand second, got %T", result.Commands[1])
	}
}

func TestConstantFolding_StringConcat(t *testing.T) {
	// eval x = "abc" + "def" → eval x = "abcdef"
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.EvalCommand{
				Field: "x",
				Expr: &spl2.ArithExpr{
					Left:  &spl2.LiteralExpr{Value: "abc"},
					Op:    "+",
					Right: &spl2.LiteralExpr{Value: "def"},
				},
			},
		},
	}
	rule := &constantFoldingRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("constant folding should fold string concat")
	}
	e := result.Commands[0].(*spl2.EvalCommand)
	lit, ok := e.Expr.(*spl2.LiteralExpr)
	if !ok {
		t.Fatalf("expected LiteralExpr, got %T", e.Expr)
	}
	if lit.Value != "abcdef" {
		t.Errorf("expected 'abcdef', got '%s'", lit.Value)
	}
}

func TestConstantFolding_KnownFunctions(t *testing.T) {
	tests := []struct {
		name string
		fn   string
		arg  string
		want string
	}{
		{"len", "len", "hello", "5"},
		{"lower", "lower", "ABC", "abc"},
		{"upper", "upper", "abc", "ABC"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &spl2.Query{
				Commands: []spl2.Command{
					&spl2.EvalCommand{
						Field: "x",
						Expr: &spl2.FuncCallExpr{
							Name: tt.fn,
							Args: []spl2.Expr{&spl2.LiteralExpr{Value: tt.arg}},
						},
					},
				},
			}
			rule := &constantFoldingRule{}
			result, changed := rule.Apply(q)
			if !changed {
				t.Fatalf("constant folding should fold %s(%q)", tt.fn, tt.arg)
			}
			e := result.Commands[0].(*spl2.EvalCommand)
			lit, ok := e.Expr.(*spl2.LiteralExpr)
			if !ok {
				t.Fatalf("expected LiteralExpr, got %T", e.Expr)
			}
			if lit.Value != tt.want {
				t.Errorf("expected %q, got %q", tt.want, lit.Value)
			}
		})
	}
}

func TestCountStarOptimization(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
			},
		},
	}
	rule := &countStarOptimizationRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("count star optimization should have fired")
	}
	ann, ok := result.GetAnnotation("countStarOnly")
	if !ok {
		t.Fatal("countStarOnly annotation not set")
	}
	if ann != true {
		t.Errorf("expected true, got %v", ann)
	}
}

func TestCountStarOptimization_NotWithWhere(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "x"},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "5"},
				},
			},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
			},
		},
	}
	rule := &countStarOptimizationRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("count star should NOT fire with WHERE before stats")
	}
}

// Regression: countStarOnly must not fire when UnrollCommand precedes STATS,
// because unroll changes cardinality (1 row → N rows from array explosion).
// Fix: rules_agg.go checks for UnrollCommand in the pre-STATS command loop.
func TestCountStarOptimization_NotWithUnroll(t *testing.T) {
	// Pipeline: unpack_json | unroll field=items | STATS count
	// Unroll changes cardinality — the metadata count shortcut is invalid.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.UnpackCommand{Format: "json", SourceField: "_raw"},
			&spl2.UnrollCommand{Field: "items"},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
			},
		},
	}
	rule := &countStarOptimizationRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("countStarOnly should NOT fire when UnrollCommand (cardinality-changing) precedes STATS count; " +
			"unroll explodes arrays so metadata event count != actual row count")
	}
}

func TestCountStarOptimization_NotWithGroupBy(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
				GroupBy:      []string{"host"},
			},
		},
	}
	rule := &countStarOptimizationRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("count star should NOT fire with GROUP BY")
	}
}

func TestFilterPushdownIntoJoin(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.JoinCommand{
				Field:    "host",
				JoinType: "inner",
				Subquery: &spl2.Query{
					Commands: []spl2.Command{
						&spl2.SearchCommand{Term: "error"},
					},
				},
			},
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "status"},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "400"},
				},
			},
		},
	}
	rule := &filterPushdownIntoJoinRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("filter pushdown into join should have fired")
	}
	// Subquery should now have the WHERE appended.
	join := result.Commands[0].(*spl2.JoinCommand)
	if len(join.Subquery.Commands) != 2 {
		t.Fatalf("expected 2 commands in subquery, got %d", len(join.Subquery.Commands))
	}
	if _, ok := join.Subquery.Commands[1].(*spl2.WhereCommand); !ok {
		t.Error("expected WHERE pushed into subquery")
	}
}

func TestUnionFilterPushdown(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.AppendCommand{
				Subquery: &spl2.Query{
					Commands: []spl2.Command{
						&spl2.SearchCommand{Term: "info"},
					},
				},
			},
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "status"},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "400"},
				},
			},
		},
	}
	rule := &unionFilterPushdownRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("union filter pushdown should have fired")
	}
	appendCmd := result.Commands[0].(*spl2.AppendCommand)
	if len(appendCmd.Subquery.Commands) != 2 {
		t.Fatalf("expected 2 commands in subquery, got %d", len(appendCmd.Subquery.Commands))
	}
}

func TestAggregationPushdown_Eligible(t *testing.T) {
	// source=X | stats count by host → should annotate with partialAgg
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{
					{Func: "count", Alias: "count"},
				},
				GroupBy: []string{"host"},
			},
		},
	}
	rule := &aggregationPushdownRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("aggregation pushdown should have fired")
	}
	ann, ok := result.GetAnnotation("partialAgg")
	if !ok {
		t.Fatal("partialAgg annotation not set")
	}
	spec := ann.(*pipeline.PartialAggSpec)
	if len(spec.Funcs) != 1 || spec.Funcs[0].Name != "count" {
		t.Errorf("expected 1 count func, got %+v", spec.Funcs)
	}
	if len(spec.GroupBy) != 1 || spec.GroupBy[0] != "host" {
		t.Errorf("expected group by [host], got %v", spec.GroupBy)
	}
}

func TestAggregationPushdown_MultipleAggs(t *testing.T) {
	// source=X | stats count, sum(bytes), avg(latency), min(status), max(status) by host
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{
					{Func: "count"},
					{Func: "sum", Args: []spl2.Expr{&spl2.FieldExpr{Name: "bytes"}}},
					{Func: "avg", Args: []spl2.Expr{&spl2.FieldExpr{Name: "latency"}}},
					{Func: "min", Args: []spl2.Expr{&spl2.FieldExpr{Name: "status"}}},
					{Func: "max", Args: []spl2.Expr{&spl2.FieldExpr{Name: "status"}}},
				},
				GroupBy: []string{"host"},
			},
		},
	}
	rule := &aggregationPushdownRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("aggregation pushdown should have fired for all pushable aggs")
	}
	spec := result.Annotations["partialAgg"].(*pipeline.PartialAggSpec)
	if len(spec.Funcs) != 5 {
		t.Fatalf("expected 5 funcs, got %d", len(spec.Funcs))
	}
}

func TestAggregationPushdown_IneligibleSearchBefore(t *testing.T) {
	// source=X | search error | stats count → NOT annotated (search before stats
	// disqualifies pushdown; the standard pipeline handles the full filter→stats).
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: "error"},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count"}},
			},
		},
	}
	rule := &aggregationPushdownRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("should NOT fire when search precedes stats")
	}
}

func TestAggregationPushdown_EligibleDC(t *testing.T) {
	// source=X | stats dc(field) by Y → annotated (dc is now pushable)
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{
					{Func: "dc", Args: []spl2.Expr{&spl2.FieldExpr{Name: "user"}}},
				},
				GroupBy: []string{"host"},
			},
		},
	}
	rule := &aggregationPushdownRule{}
	_, changed := rule.Apply(q)
	if !changed {
		t.Error("should fire for dc (now pushable)")
	}
}

func TestAggregationPushdown_IneligibleNestedEval(t *testing.T) {
	// source=X | stats count(eval(len(msg))) by Y → not annotated (nested eval)
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{
					{
						Func: "count",
						Args: []spl2.Expr{
							&spl2.FuncCallExpr{
								Name: "len",
								Args: []spl2.Expr{&spl2.FieldExpr{Name: "msg"}},
							},
						},
					},
				},
				GroupBy: []string{"host"},
			},
		},
	}
	rule := &aggregationPushdownRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("should NOT fire with nested eval in agg args")
	}
}

func TestAggregationPushdown_IneligibleVariable(t *testing.T) {
	// $var | stats count → not annotated (CTE variable, no source)
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "myvar", IsVariable: true},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count"}},
			},
		},
	}
	rule := &aggregationPushdownRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("should NOT fire for CTE variable source")
	}
}

func TestAggregationPushdown_IneligibleNoSource(t *testing.T) {
	// stats count (no source) → not annotated
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count"}},
			},
		},
	}
	rule := &aggregationPushdownRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("should NOT fire without source clause")
	}
}

func TestAggregationPushdown_IneligibleWhereBefore(t *testing.T) {
	// source=X | where status>500 | stats count → NOT annotated (where before stats
	// disqualifies pushdown; the standard pipeline handles the full filter→stats).
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "status"},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "500"},
				},
			},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count"}},
			},
		},
	}
	rule := &aggregationPushdownRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("should NOT fire when WHERE precedes stats")
	}
}

func TestAggregationPushdown_AliasGeneration(t *testing.T) {
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "test"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{
					{Func: "count"}, // no alias → should be "count"
					{Func: "sum", Args: []spl2.Expr{&spl2.FieldExpr{Name: "bytes"}}}, // no alias → "sum(bytes)"
					{Func: "avg", Args: []spl2.Expr{&spl2.FieldExpr{Name: "lat"}}, Alias: "avg_lat"},
				},
			},
		},
	}
	rule := &aggregationPushdownRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("should fire")
	}
	spec := result.Annotations["partialAgg"].(*pipeline.PartialAggSpec)
	if spec.Funcs[0].Alias != "count" {
		t.Errorf("expected alias 'count', got %q", spec.Funcs[0].Alias)
	}
	if spec.Funcs[1].Alias != "sum(bytes)" {
		t.Errorf("expected alias 'sum(bytes)', got %q", spec.Funcs[1].Alias)
	}
	if spec.Funcs[2].Alias != "avg_lat" {
		t.Errorf("expected alias 'avg_lat', got %q", spec.Funcs[2].Alias)
	}
}

func TestEarlyLimit_SortTail_DescToAsc(t *testing.T) {
	// sort -x | tail 10 → TopN(asc x, 10)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "x", Desc: true}}},
			&spl2.TailCommand{Count: 10},
		},
	}
	rule := &earlyLimitRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("early limit should create TopN from sort+tail")
	}
	if len(result.Commands) != 1 {
		t.Fatalf("expected 1 command (TopN), got %d", len(result.Commands))
	}
	topn, ok := result.Commands[0].(*spl2.TopNCommand)
	if !ok {
		t.Fatalf("expected TopNCommand, got %T", result.Commands[0])
	}
	if topn.Limit != 10 {
		t.Errorf("expected limit 10, got %d", topn.Limit)
	}
	if len(topn.Fields) != 1 || topn.Fields[0].Name != "x" || topn.Fields[0].Desc {
		t.Errorf("expected ascending x, got %+v", topn.Fields)
	}
}

func TestEarlyLimit_SortTail_AscToDesc(t *testing.T) {
	// sort x | tail 5 → TopN(desc x, 5)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "x", Desc: false}}},
			&spl2.TailCommand{Count: 5},
		},
	}
	rule := &earlyLimitRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("early limit should create TopN from sort+tail")
	}
	if len(result.Commands) != 1 {
		t.Fatalf("expected 1 command (TopN), got %d", len(result.Commands))
	}
	topn, ok := result.Commands[0].(*spl2.TopNCommand)
	if !ok {
		t.Fatalf("expected TopNCommand, got %T", result.Commands[0])
	}
	if topn.Limit != 5 {
		t.Errorf("expected limit 5, got %d", topn.Limit)
	}
	if len(topn.Fields) != 1 || topn.Fields[0].Name != "x" || !topn.Fields[0].Desc {
		t.Errorf("expected descending x, got %+v", topn.Fields)
	}
}

func TestEarlyLimit_SortTail_MultiField(t *testing.T) {
	// sort -x y | tail 3 → TopN(asc x, desc y, 3)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{
				{Name: "x", Desc: true},
				{Name: "y", Desc: false},
			}},
			&spl2.TailCommand{Count: 3},
		},
	}
	rule := &earlyLimitRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("early limit should create TopN from sort+tail with multi-field")
	}
	if len(result.Commands) != 1 {
		t.Fatalf("expected 1 command (TopN), got %d", len(result.Commands))
	}
	topn, ok := result.Commands[0].(*spl2.TopNCommand)
	if !ok {
		t.Fatalf("expected TopNCommand, got %T", result.Commands[0])
	}
	if topn.Limit != 3 {
		t.Errorf("expected limit 3, got %d", topn.Limit)
	}
	if len(topn.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(topn.Fields))
	}
	// x was desc → now asc
	if topn.Fields[0].Name != "x" || topn.Fields[0].Desc {
		t.Errorf("expected field 0 ascending x, got %+v", topn.Fields[0])
	}
	// y was asc → now desc
	if topn.Fields[1].Name != "y" || !topn.Fields[1].Desc {
		t.Errorf("expected field 1 descending y, got %+v", topn.Fields[1])
	}
}

func TestEarlyLimit_TailWithoutSort_NoChange(t *testing.T) {
	// eval a=1 | tail 10 → no change (tail without preceding sort)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.EvalCommand{Field: "a", Expr: &spl2.LiteralExpr{Value: "1"}},
			&spl2.TailCommand{Count: 10},
		},
	}
	rule := &earlyLimitRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("tail without preceding sort should not be converted to TopN")
	}
}

func TestBloomEnrichment_InExpr(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.InExpr{
					Field: &spl2.FieldExpr{Name: "_source"},
					Values: []spl2.Expr{
						&spl2.LiteralExpr{Value: "nginx"},
						&spl2.LiteralExpr{Value: "redis"},
					},
				},
			},
		},
	}
	rule := &bloomFilterPruningRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("bloom filter rule should have fired for IN on bloom-eligible field")
	}
	ann, ok := result.GetAnnotation("bloomTerms")
	if !ok {
		t.Fatal("bloomTerms annotation not set")
	}
	terms := ann.([]string)
	if len(terms) != 2 {
		t.Fatalf("expected 2 bloom terms, got %d: %v", len(terms), terms)
	}
	termSet := map[string]bool{}
	for _, term := range terms {
		termSet[term] = true
	}
	if !termSet["nginx"] || !termSet["redis"] {
		t.Errorf("expected terms [nginx, redis], got %v", terms)
	}
}

func TestBloomEnrichment_InExpr_NotIn(t *testing.T) {
	// NOT IN should not produce bloom terms.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.InExpr{
					Field:   &spl2.FieldExpr{Name: "_source"},
					Negated: true,
					Values: []spl2.Expr{
						&spl2.LiteralExpr{Value: "nginx"},
					},
				},
			},
		},
	}
	rule := &bloomFilterPruningRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("bloom filter rule should NOT fire for NOT IN")
	}
}

func TestBloomEnrichment_InExpr_NonBloomField(t *testing.T) {
	// IN on a non-bloom field should not produce terms.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.InExpr{
					Field: &spl2.FieldExpr{Name: "status"},
					Values: []spl2.Expr{
						&spl2.LiteralExpr{Value: "200"},
						&spl2.LiteralExpr{Value: "500"},
					},
				},
			},
		},
	}
	rule := &bloomFilterPruningRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("bloom filter rule should NOT fire for non-bloom field IN")
	}
}

func TestBloomEnrichment_LikePrefix(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "_raw"},
					Op:    "like",
					Right: &spl2.LiteralExpr{Value: "/api/users/%"},
				},
			},
		},
	}
	rule := &bloomFilterPruningRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("bloom filter rule should have fired for LIKE with long prefix")
	}
	ann, ok := result.GetAnnotation("bloomTerms")
	if !ok {
		t.Fatal("bloomTerms annotation not set")
	}
	terms := ann.([]string)
	// Tokenizer splits "/api/users/" into ["api", "users"] (>= 3 chars each).
	termSet := map[string]bool{}
	for _, term := range terms {
		termSet[term] = true
	}
	if !termSet["api"] || !termSet["users"] {
		t.Errorf("expected bloom terms [api users], got %v", terms)
	}
}

func TestBloomEnrichment_LikePrefixTooShort(t *testing.T) {
	// Prefix "ab" is < 3 chars → should not produce a bloom term.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "_raw"},
					Op:    "like",
					Right: &spl2.LiteralExpr{Value: "ab%"},
				},
			},
		},
	}
	rule := &bloomFilterPruningRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("bloom filter rule should NOT fire for LIKE prefix shorter than 3 chars")
	}
}

func TestBloomEnrichment_LikeLeadingWildcard(t *testing.T) {
	// Leading wildcard → empty prefix → no bloom term.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "_raw"},
					Op:    "like",
					Right: &spl2.LiteralExpr{Value: "%error%"},
				},
			},
		},
	}
	rule := &bloomFilterPruningRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("bloom filter rule should NOT fire for LIKE with leading wildcard")
	}
}

func TestBloomEnrichment_LikeNonBloomField(t *testing.T) {
	// LIKE on a non-bloom field should not produce terms.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "uri"},
					Op:    "like",
					Right: &spl2.LiteralExpr{Value: "/api/%"},
				},
			},
		},
	}
	rule := &bloomFilterPruningRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("bloom filter rule should NOT fire for LIKE on non-bloom field")
	}
}

func TestExtractLikePrefix(t *testing.T) {
	tests := []struct {
		pattern string
		want    string
	}{
		{"/api/users/%", "/api/users/"},
		{"%error%", ""},
		{"exact_match", "exact"}, // '_' is LIKE single-char wildcard
		{"ab%", "ab"},
		{"_starts", ""},
		{"/api/%/users", "/api/"},
		{"", ""},
	}
	for _, tt := range tests {
		got := extractLikePrefix(tt.pattern)
		if got != tt.want {
			t.Errorf("extractLikePrefix(%q) = %q, want %q", tt.pattern, got, tt.want)
		}
	}
}

func TestConstantPropagation_MultiAssignment(t *testing.T) {
	// eval x=5, y=10 | WHERE x > 3
	// → eval x=5, y=10 | WHERE 5 > 3
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.EvalCommand{
				Assignments: []spl2.EvalAssignment{
					{Field: "x", Expr: &spl2.LiteralExpr{Value: "5"}},
					{Field: "y", Expr: &spl2.LiteralExpr{Value: "10"}},
				},
			},
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "x"},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "3"},
				},
			},
		},
	}
	rule := &constantPropagationRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("constant propagation should fire for multi-assignment eval")
	}
	w := result.Commands[1].(*spl2.WhereCommand)
	cmp := w.Expr.(*spl2.CompareExpr)
	lit, ok := cmp.Left.(*spl2.LiteralExpr)
	if !ok {
		t.Fatalf("expected LiteralExpr, got %T", cmp.Left)
	}
	if lit.Value != "5" {
		t.Errorf("expected '5', got '%s'", lit.Value)
	}
}

func TestContradictionDetection_SameFieldDifferentValues(t *testing.T) {
	// WHERE status = 200 AND status = 500 → false (contradiction)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "status"},
						Op:    "=",
						Right: &spl2.LiteralExpr{Value: "200"},
					},
					Op: "and",
					Right: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "status"},
						Op:    "=",
						Right: &spl2.LiteralExpr{Value: "500"},
					},
				},
			},
		},
	}
	rule := &predicateSimplificationRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("contradiction detection should fire")
	}
	w := result.Commands[0].(*spl2.WhereCommand)
	if !isLiteralFalse(w.Expr) {
		t.Errorf("expected false literal, got %v", w.Expr)
	}
}

func TestContradictionDetection_SameFieldSameValue(t *testing.T) {
	// WHERE status = 200 AND status = 200 → status = 200 (not a contradiction)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "status"},
						Op:    "=",
						Right: &spl2.LiteralExpr{Value: "200"},
					},
					Op: "and",
					Right: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "status"},
						Op:    "=",
						Right: &spl2.LiteralExpr{Value: "200"},
					},
				},
			},
		},
	}
	rule := &predicateSimplificationRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("x AND x → x simplification should fire")
	}
	w := result.Commands[0].(*spl2.WhereCommand)
	// Should simplify to just status = 200, not false.
	if isLiteralFalse(w.Expr) {
		t.Error("same value should not be detected as contradiction")
	}
}

func TestContradictionDetection_DifferentFields(t *testing.T) {
	// WHERE status = 200 AND level = "ERROR" → no contradiction (different fields)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "status"},
						Op:    "=",
						Right: &spl2.LiteralExpr{Value: "200"},
					},
					Op: "and",
					Right: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "level"},
						Op:    "=",
						Right: &spl2.LiteralExpr{Value: "ERROR"},
					},
				},
			},
		},
	}
	rule := &predicateSimplificationRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("different fields should not trigger contradiction detection")
	}
}

func TestContradictionDetection_NonEquality(t *testing.T) {
	// WHERE status > 200 AND status > 500 → not a contradiction (both can be true)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "status"},
						Op:    ">",
						Right: &spl2.LiteralExpr{Value: "200"},
					},
					Op: "and",
					Right: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "status"},
						Op:    ">",
						Right: &spl2.LiteralExpr{Value: "500"},
					},
				},
			},
		},
	}
	rule := &predicateSimplificationRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("non-equality comparisons should not trigger contradiction detection")
	}
}

func TestColumnStats_InExpr(t *testing.T) {
	// WHERE status IN (400, 404, 500) → should produce >=400 and <=500 predicates
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.InExpr{
					Field: &spl2.FieldExpr{Name: "status"},
					Values: []spl2.Expr{
						&spl2.LiteralExpr{Value: "400"},
						&spl2.LiteralExpr{Value: "404"},
						&spl2.LiteralExpr{Value: "500"},
					},
				},
			},
		},
	}
	rule := &columnStatsPruningRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("column stats rule should have fired for IN expression")
	}
	ann, ok := result.GetAnnotation("fieldPredicates")
	if !ok {
		t.Fatal("fieldPredicates annotation not set")
	}
	preds := ann.([]FieldPredInfo)
	if len(preds) != 2 {
		t.Fatalf("expected 2 predicates (min/max from IN), got %d: %+v", len(preds), preds)
	}

	// Should have >= min and <= max.
	hasGTE := false
	hasLTE := false
	for _, p := range preds {
		if p.Field != "status" {
			t.Errorf("expected field 'status', got %q", p.Field)
		}
		if p.Op == ">=" {
			hasGTE = true
			if p.Value != "400" {
				t.Errorf("expected min value '400', got %q", p.Value)
			}
		}
		if p.Op == "<=" {
			hasLTE = true
			if p.Value != "500" {
				t.Errorf("expected max value '500', got %q", p.Value)
			}
		}
	}
	if !hasGTE || !hasLTE {
		t.Errorf("expected both >= and <= predicates, got %+v", preds)
	}
}

func TestColumnStats_InExpr_NotIn(t *testing.T) {
	// WHERE status NOT IN (200, 404) → should NOT produce range predicates
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.InExpr{
					Field:   &spl2.FieldExpr{Name: "status"},
					Negated: true,
					Values: []spl2.Expr{
						&spl2.LiteralExpr{Value: "200"},
						&spl2.LiteralExpr{Value: "404"},
					},
				},
			},
		},
	}
	rule := &columnStatsPruningRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("column stats rule should NOT fire for NOT IN")
	}
}

func TestColumnStats_UnpackPrefixExcluded(t *testing.T) {
	// Predicates on unpack-generated fields (prefix "pg.") must be excluded
	// from segment-level pushdown. Without the fix, pg.duration_ms > 0 would
	// be pushed to the segment reader which returns 0 results because the
	// field doesn't exist in segment data.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.UnpackCommand{
				Format:      "postgres",
				SourceField: "message",
				Prefix:      "pg.",
			},
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "pg.duration_ms"},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "0"},
				},
			},
		},
	}
	rule := &columnStatsPruningRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("column stats rule should NOT fire for unpack-prefixed field pg.duration_ms")
	}
}

func TestColumnStats_UnpackPrefixMixedWithSegmentField(t *testing.T) {
	// Mix of unpack-generated and segment fields: only the segment field
	// predicate should survive.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.UnpackCommand{
				Format:      "json",
				SourceField: "message",
				Prefix:      "j.",
			},
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "j.status"},
						Op:    ">=",
						Right: &spl2.LiteralExpr{Value: "500"},
					},
					Op: "and",
					Right: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "level"},
						Op:    "=",
						Right: &spl2.LiteralExpr{Value: "ERROR"},
					},
				},
			},
		},
	}
	rule := &columnStatsPruningRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("column stats rule should have fired for segment field 'level'")
	}
	ann, ok := result.GetAnnotation("fieldPredicates")
	if !ok {
		t.Fatal("fieldPredicates annotation not set")
	}
	preds := ann.([]FieldPredInfo)
	if len(preds) != 1 {
		t.Fatalf("expected 1 predicate (only 'level'), got %d: %+v", len(preds), preds)
	}
	if preds[0].Field != "level" {
		t.Errorf("expected field 'level', got %q", preds[0].Field)
	}
}

func TestColumnStats_UnpackWithExplicitFields(t *testing.T) {
	// UnpackCommand with explicit Fields — those specific names should be excluded.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.UnpackCommand{
				Format:      "json",
				SourceField: "message",
				Prefix:      "j.",
				Fields:      []string{"status", "host"},
			},
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "j.status"},
					Op:    ">=",
					Right: &spl2.LiteralExpr{Value: "500"},
				},
			},
		},
	}
	rule := &columnStatsPruningRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("column stats rule should NOT fire for explicitly listed unpack field j.status")
	}
}

func TestBloomEnrichment_RegexMatch(t *testing.T) {
	// WHERE _raw =~ "connection refused"
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "_raw"},
					Op:    "=~",
					Right: &spl2.LiteralExpr{Value: `connection refused to (?P<host>\S+)`},
				},
			},
		},
	}
	rule := &bloomFilterPruningRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("bloom filter rule should have fired for =~ with literal")
	}
	ann, ok := result.GetAnnotation("bloomTerms")
	if !ok {
		t.Fatal("bloomTerms annotation not set")
	}
	terms := ann.([]string)
	// Tokenizer splits "connection refused to " into ["connection", "refused"].
	// "to" is < 3 chars and filtered out.
	termSet := map[string]bool{}
	for _, term := range terms {
		termSet[term] = true
	}
	if !termSet["connection"] {
		t.Errorf("expected bloom term 'connection', got %v", terms)
	}
	if !termSet["refused"] {
		t.Errorf("expected bloom term 'refused', got %v", terms)
	}
}

func TestBloomEnrichment_MatchFunc(t *testing.T) {
	// WHERE match(_raw, "error.*timeout")
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.FuncCallExpr{
					Name: "match",
					Args: []spl2.Expr{
						&spl2.FieldExpr{Name: "_raw"},
						&spl2.LiteralExpr{Value: `error_handler.*timeout`},
					},
				},
			},
		},
	}
	rule := &bloomFilterPruningRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("bloom filter rule should have fired for match() with literals")
	}
	ann, ok := result.GetAnnotation("bloomTerms")
	if !ok {
		t.Fatal("bloomTerms annotation not set")
	}
	terms := ann.([]string)
	// Tokenizer splits "error_handler" into ["error", "handler"] (underscore is a breaker)
	// and "timeout" stays as ["timeout"]. All >= 3 chars.
	termSet := map[string]bool{}
	for _, term := range terms {
		termSet[term] = true
	}
	if !termSet["error"] {
		t.Errorf("expected bloom term 'error', got %v", terms)
	}
	if !termSet["handler"] {
		t.Errorf("expected bloom term 'handler', got %v", terms)
	}
	if !termSet["timeout"] {
		t.Errorf("expected bloom term 'timeout', got %v", terms)
	}
}
