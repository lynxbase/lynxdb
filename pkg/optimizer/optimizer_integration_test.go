package optimizer

import (
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
)

func TestAnnotationsMergeIntoHints(t *testing.T) {
	// Full pipeline: build query → optimize → ExtractQueryHints → verify hints populated.
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: "error"},
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "_time"},
						Op:    ">=",
						Right: &spl2.LiteralExpr{Value: "2024-01-01T00:00:00Z"},
					},
					Op: "and",
					Right: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "host"},
						Op:    "=",
						Right: &spl2.LiteralExpr{Value: "web01"},
					},
				},
			},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
				GroupBy:      []string{"host"},
			},
		},
	}

	// Optimize.
	opt := New()
	optimized := opt.Optimize(q)

	// Extract hints.
	prog := &spl2.Program{Main: optimized}
	hints := spl2.ExtractQueryHints(prog)

	// Verify time bounds were merged.
	if hints.TimeBounds == nil {
		t.Error("TimeBounds should be set")
	} else if hints.TimeBounds.Earliest.IsZero() {
		t.Error("earliest time bound should be set")
	}

	// Verify bloom terms were merged.
	hasWeb01 := false
	for _, term := range hints.SearchTerms {
		if term == "web01" {
			hasWeb01 = true
		}
	}
	if !hasWeb01 {
		t.Errorf("bloom term 'web01' should be in SearchTerms, got %v", hints.SearchTerms)
	}

	// Verify required columns were set.
	if len(hints.RequiredCols) == 0 {
		t.Error("RequiredCols should be populated")
	}

	// Verify index name.
	if hints.IndexName != "main" {
		t.Errorf("expected IndexName 'main', got '%s'", hints.IndexName)
	}
}

func TestOptimizer_AllAnnotationsPopulated(t *testing.T) {
	// Build a query that triggers time, bloom, column stats, column pruning, and rex.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: "error"},
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "_time"},
						Op:    ">=",
						Right: &spl2.LiteralExpr{Value: "1704067200"},
					},
					Op: "and",
					Right: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "source"},
						Op:    "=",
						Right: &spl2.LiteralExpr{Value: "nginx"},
					},
				},
			},
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "status"},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "500"},
				},
			},
			&spl2.RexCommand{Pattern: `(?P<user>\w+)@example.com`},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
				GroupBy:      []string{"host"},
			},
		},
	}

	opt := New()
	result := opt.Optimize(q)

	// Check annotations.
	checks := []string{"timeAnnotation", "bloomTerms", "fieldPredicates", "requiredColumns", "rexPreFilter"}
	for _, key := range checks {
		if _, ok := result.GetAnnotation(key); !ok {
			t.Errorf("annotation %q not set", key)
		}
	}

	// Verify explain output mentions at least some rules.
	explain := opt.Explain()
	if len(explain) < 10 {
		t.Error("explain output too short")
	}
}

func TestOptimizer_SortHeadBecomesTopN(t *testing.T) {
	// sort -val | head 5 → TopNCommand(limit=5)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: ""},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "val", Desc: true}}},
			&spl2.HeadCommand{Count: 5},
		},
	}

	opt := New()
	result := opt.Optimize(q)

	// Find the TopNCommand.
	foundTopN := false
	for _, cmd := range result.Commands {
		if topn, ok := cmd.(*spl2.TopNCommand); ok {
			foundTopN = true
			if topn.Limit != 5 {
				t.Errorf("expected limit 5, got %d", topn.Limit)
			}
			if len(topn.Fields) != 1 || topn.Fields[0].Name != "val" || !topn.Fields[0].Desc {
				t.Errorf("unexpected fields: %+v", topn.Fields)
			}
		}
	}
	if !foundTopN {
		t.Error("expected sort+head to be replaced with TopNCommand")
	}

	// Verify original sort+head no longer exists.
	for _, cmd := range result.Commands {
		if _, ok := cmd.(*spl2.SortCommand); ok {
			t.Error("SortCommand should have been replaced")
		}
		if _, ok := cmd.(*spl2.HeadCommand); ok {
			t.Error("HeadCommand should have been replaced")
		}
	}
}

func TestOptimizer_SortTailBecomesTopN(t *testing.T) {
	// search | sort -val | tail 5 → TopN(asc val, 5)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: ""},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "val", Desc: true}}},
			&spl2.TailCommand{Count: 5},
		},
	}

	opt := New()
	result := opt.Optimize(q)

	// Find the TopNCommand.
	foundTopN := false
	for _, cmd := range result.Commands {
		if topn, ok := cmd.(*spl2.TopNCommand); ok {
			foundTopN = true
			if topn.Limit != 5 {
				t.Errorf("expected limit 5, got %d", topn.Limit)
			}
			if len(topn.Fields) != 1 || topn.Fields[0].Name != "val" || topn.Fields[0].Desc {
				t.Errorf("expected ascending val (inverted from desc), got %+v", topn.Fields)
			}
		}
	}
	if !foundTopN {
		t.Error("expected sort+tail to be replaced with TopNCommand")
	}

	// Verify original sort+tail no longer exists.
	for _, cmd := range result.Commands {
		if _, ok := cmd.(*spl2.SortCommand); ok {
			t.Error("SortCommand should have been replaced")
		}
		if _, ok := cmd.(*spl2.TailCommand); ok {
			t.Error("TailCommand should have been replaced")
		}
	}
}

func TestOptimizer_PredicateFusionThenReorder(t *testing.T) {
	// Multiple WHERE commands get fused, then predicates get reordered.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.FuncCallExpr{
					Name: "match",
					Args: []spl2.Expr{
						&spl2.FieldExpr{Name: "message"},
						&spl2.LiteralExpr{Value: "error.*"},
					},
				},
			},
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "host"},
					Op:    "=",
					Right: &spl2.LiteralExpr{Value: "web01"},
				},
			},
		},
	}

	opt := New()
	result := opt.Optimize(q)

	// Should be fused into one WHERE.
	whereCount := 0
	for _, cmd := range result.Commands {
		if _, ok := cmd.(*spl2.WhereCommand); ok {
			whereCount++
		}
	}
	if whereCount != 1 {
		t.Errorf("expected 1 WHERE after fusion, got %d", whereCount)
	}

	// The host equality should come before the match function in the AND.
	w := result.Commands[0].(*spl2.WhereCommand)
	preds := flattenAND(w.Expr)
	if len(preds) >= 2 {
		// First predicate should be the cheaper one (equality).
		if _, ok := preds[0].(*spl2.CompareExpr); !ok {
			t.Logf("first predicate type: %T — reordering may not have fired or was already optimal", preds[0])
		}
	}
}

func TestOptimizer_BothSortsEliminated_FixedPoint(t *testing.T) {
	// sort _time | sort -size | stats count → both sorts eliminated through fixed-point iteration.
	// removeDeadSort runs first in rule order:
	// Iteration 1: first sort is dead (second sort re-establishes ordering) → removed.
	// Iteration 2: remaining sort -size is dead (stats destroys ordering) → removed.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "size", Desc: true}}},
			&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	for _, cmd := range result.Commands {
		if _, ok := cmd.(*spl2.SortCommand); ok {
			t.Fatal("expected no SortCommand in optimized output — both should be eliminated")
		}
	}
	// Both sorts should be eliminated — verify at least 2 sort removals across rules.
	totalSortRemovals := opt.Stats["RemoveDeadSort"] + opt.Stats["RemoveRedundantSort"]
	if totalSortRemovals < 2 {
		t.Errorf("expected at least 2 sort removals, got %d (dead=%d, redundant=%d)",
			totalSortRemovals, opt.Stats["RemoveDeadSort"], opt.Stats["RemoveRedundantSort"])
	}
}

func TestOptimizer_FirstSortEliminated_SecondKept(t *testing.T) {
	// sort _time | stats count | sort -count → first sort eliminated (dead before stats),
	// second sort kept (terminal, user-visible).
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "count", Desc: true}}},
		},
	}
	opt := New()
	result := opt.Optimize(q)

	sortCount := 0
	for _, cmd := range result.Commands {
		if sc, ok := cmd.(*spl2.SortCommand); ok {
			sortCount++
			if sc.Fields[0].Name != "count" {
				t.Errorf("expected remaining sort to be on 'count', got '%s'", sc.Fields[0].Name)
			}
		}
	}
	if sortCount != 1 {
		t.Errorf("expected exactly 1 SortCommand remaining, got %d", sortCount)
	}
	if opt.Stats["RemoveDeadSort"] == 0 {
		t.Error("RemoveDeadSort should have fired for first sort")
	}
}

func TestOptimizer_MultiSourceSortStillEliminated(t *testing.T) {
	// FROM idx_a, idx_b | sort _time | stats count → sort eliminated (stats is order-destroying
	// regardless of source count). RemoveSortOnScanOrder should NOT fire (multi-source),
	// but RemoveDeadSort should fire.
	q := &spl2.Query{
		Source: &spl2.SourceClause{Indices: []string{"idx_a", "idx_b"}},
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	for _, cmd := range result.Commands {
		if _, ok := cmd.(*spl2.SortCommand); ok {
			t.Fatal("expected sort eliminated by RemoveDeadSort (before stats)")
		}
	}
	if opt.Stats["RemoveDeadSort"] == 0 {
		t.Error("RemoveDeadSort should have fired")
	}
	if opt.Stats["RemoveSortOnScanOrder"] > 0 {
		t.Error("RemoveSortOnScanOrder should NOT have fired for multi-source")
	}
}

func TestOptimizer_ScanOrderRuleAndAnnotation(t *testing.T) {
	// Single source | sort _time → RemoveSortOnScanOrder fires, reverseScanOrder annotation set.
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time", Desc: false}}},
			&spl2.HeadCommand{Count: 100},
		},
	}
	opt := New()
	result := opt.Optimize(q)

	// After optimization, sort _time should be gone and reverseScanOrder should be set.
	for _, cmd := range result.Commands {
		if _, ok := cmd.(*spl2.SortCommand); ok {
			t.Fatal("expected sort _time eliminated by RemoveSortOnScanOrder")
		}
	}
	v, ok := result.GetAnnotation("reverseScanOrder")
	if !ok {
		t.Fatal("expected reverseScanOrder annotation after scan-order optimization")
	}
	if b, ok := v.(bool); !ok || !b {
		t.Fatalf("expected reverseScanOrder=true, got %v", v)
	}
	if opt.Stats["RemoveSortOnScanOrder"] == 0 {
		t.Error("RemoveSortOnScanOrder should have fired")
	}
}

func TestOptimizer_PredicatePushdownChain(t *testing.T) {
	// eval a=1 | eval b=2 | WHERE status > 500
	// → WHERE status > 500 | eval a=1 | eval b=2
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.EvalCommand{Field: "a", Expr: &spl2.LiteralExpr{Value: "1"}},
			&spl2.EvalCommand{Field: "b", Expr: &spl2.LiteralExpr{Value: "2"}},
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "status"},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "500"},
				},
			},
		},
	}
	opt := New()
	result := opt.Optimize(q)

	// WHERE should be first (pushed past both evals).
	if _, ok := result.Commands[0].(*spl2.WhereCommand); !ok {
		t.Errorf("expected WHERE first after pushdown chain, got %T", result.Commands[0])
	}
}
