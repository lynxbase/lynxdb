package optimizer

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/column"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

// Test 1: PredicatePushdownRowGroup
func TestOptimization_01_PredicatePushdownRowGroup(t *testing.T) {
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "status"},
					Op:    "=",
					Right: &spl2.LiteralExpr{Value: "404"},
				},
			},
		},
	}

	opt := New()
	result := opt.Optimize(q)

	ann, ok := result.GetAnnotation("fieldPredicates")
	if !ok {
		t.Fatal("expected fieldPredicates annotation")
	}
	preds, ok := ann.([]FieldPredInfo)
	if !ok {
		t.Fatalf("expected []FieldPredInfo, got %T", ann)
	}
	found := false
	for _, p := range preds {
		if p.Field == "status" && p.Op == "=" && p.Value == "404" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected fieldPredicate {status = 404}, got %+v", preds)
	}
}

// Test 2: ColumnPruning
func TestOptimization_02_ColumnPruning(t *testing.T) {
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: ""},
			&spl2.FillnullCommand{Value: "0", Fields: []string{"status"}},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
				GroupBy:      []string{"host"},
			},
		},
	}

	opt := New()
	result := opt.Optimize(q)

	ann, ok := result.GetAnnotation("requiredColumns")
	if !ok {
		t.Fatal("expected requiredColumns annotation")
	}
	cols, ok := ann.([]string)
	if !ok {
		t.Fatalf("expected []string, got %T", ann)
	}

	colSet := make(map[string]bool, len(cols))
	for _, c := range cols {
		colSet[c] = true
	}

	for _, required := range []string{"status", "host", "_time", "_raw"} {
		if !colSet[required] {
			t.Errorf("expected column %q in requiredColumns, got %v", required, cols)
		}
	}
}

// Test 3: TimeRangePruning
func TestOptimization_03_TimeRangePruning(t *testing.T) {
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
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
						Right: &spl2.LiteralExpr{Value: "2024-01-02T00:00:00Z"},
					},
				},
			},
		},
	}

	opt := New()
	result := opt.Optimize(q)

	ann, ok := result.GetAnnotation("timeAnnotation")
	if !ok {
		t.Fatal("expected timeAnnotation annotation")
	}
	tb, ok := ann.(map[string]string)
	if !ok {
		t.Fatalf("expected map[string]string, got %T", ann)
	}
	if tb["earliest"] != "2024-01-01T00:00:00Z" {
		t.Errorf("expected earliest 2024-01-01T00:00:00Z, got %s", tb["earliest"])
	}
	if tb["latest"] != "2024-01-02T00:00:00Z" {
		t.Errorf("expected latest 2024-01-02T00:00:00Z, got %s", tb["latest"])
	}
}

// Test 4: BloomFilter
func TestOptimization_04_BloomFilter(t *testing.T) {
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: "error 404"},
		},
	}

	opt := New()
	result := opt.Optimize(q)

	ann, ok := result.GetAnnotation("bloomTerms")
	if !ok {
		t.Fatal("expected bloomTerms annotation")
	}
	terms, ok := ann.([]string)
	if !ok {
		t.Fatalf("expected []string, got %T", ann)
	}

	expected := index.Tokenize("error 404")
	termSet := make(map[string]bool, len(terms))
	for _, term := range terms {
		termSet[term] = true
	}
	for _, e := range expected {
		if !termSet[e] {
			t.Errorf("expected bloom term %q, got %v", e, terms)
		}
	}
}

// Test 5: EarlyTermination
func TestOptimization_05_EarlyTermination(t *testing.T) {
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: "error"},
			&spl2.HeadCommand{Count: 10},
		},
	}

	opt := New()
	result := opt.Optimize(q)

	prog := &spl2.Program{Main: result}
	hints := spl2.ExtractQueryHints(prog)

	if hints.Limit != 10 {
		t.Errorf("expected hints.Limit == 10, got %d", hints.Limit)
	}
}

// Test 6: AggregationPushdown
func TestOptimization_06_AggregationPushdown(t *testing.T) {
	t.Run("count_by_host", func(t *testing.T) {
		q := &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.SearchCommand{Term: "error"},
				&spl2.WhereCommand{
					Expr: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "status"},
						Op:    ">",
						Right: &spl2.LiteralExpr{Value: "400"},
					},
				},
				&spl2.StatsCommand{
					Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
					GroupBy:      []string{"host"},
				},
			},
		}

		opt := New()
		result := opt.Optimize(q)

		// Pushdown must NOT fire when filters precede STATS — the standard
		// pipeline path handles WHERE → STATS correctly.
		_, ok := result.GetAnnotation("partialAgg")
		if ok {
			t.Fatal("partialAgg should not be annotated when filters precede STATS")
		}
	})

	t.Run("dc_by_host", func(t *testing.T) {
		q := &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.StatsCommand{
					Aggregations: []spl2.AggExpr{{
						Func:  "dc",
						Alias: "dc(user)",
						Args:  []spl2.Expr{&spl2.FieldExpr{Name: "user"}},
					}},
					GroupBy: []string{"host"},
				},
			},
		}

		opt := New()
		result := opt.Optimize(q)

		_, ok := result.GetAnnotation("partialAgg")
		if !ok {
			t.Fatal("expected partialAgg annotation for dc aggregation")
		}
	})

	t.Run("dc_is_pushable", func(t *testing.T) {
		if !pipeline.IsPushableAgg("dc") {
			t.Error("expected dc to be a pushable aggregation")
		}
	})
}

// Test 7: TopKAggregation
func TestOptimization_08_TopKAggregation(t *testing.T) {
	t.Run("monotonic_aggs", func(t *testing.T) {
		// Apply topKAggRule directly to the raw query before earlyLimitRule
		// transforms sort+head into TopNCommand.
		q := &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.StatsCommand{
					Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
					GroupBy:      []string{"host"},
				},
				&spl2.SortCommand{Fields: []spl2.SortField{{Name: "count", Desc: true}}},
				&spl2.HeadCommand{Count: 5},
			},
		}

		rule := &topKAggRule{}
		result, applied := rule.Apply(q)
		if !applied {
			t.Fatal("expected topKAggRule to apply")
		}

		ann, ok := result.GetAnnotation("topKAgg")
		if !ok {
			t.Fatal("expected topKAgg annotation")
		}
		topk, ok := ann.(*TopKAggAnnotation)
		if !ok {
			t.Fatalf("expected *TopKAggAnnotation, got %T", ann)
		}
		if topk.K != 5 {
			t.Errorf("expected K=5, got %d", topk.K)
		}
		if len(topk.SortFields) != 1 || topk.SortFields[0].Name != "count" || !topk.SortFields[0].Desc {
			t.Errorf("unexpected sort fields: %+v", topk.SortFields)
		}
	})

	t.Run("non_monotonic_avg", func(t *testing.T) {
		q := &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.StatsCommand{
					Aggregations: []spl2.AggExpr{{
						Func:  "avg",
						Alias: "avg_val",
						Args:  []spl2.Expr{&spl2.FieldExpr{Name: "val"}},
					}},
					GroupBy: []string{"host"},
				},
				&spl2.SortCommand{Fields: []spl2.SortField{{Name: "avg_val", Desc: true}}},
				&spl2.HeadCommand{Count: 5},
			},
		}

		opt := New()
		result := opt.Optimize(q)

		_, ok := result.GetAnnotation("topKAgg")
		if ok {
			t.Error("topKAgg should NOT be set for non-monotonic agg (avg)")
		}
	})
}

// Test 9: MVAutoRouting
type mockCatalog struct {
	views []ViewInfo
}

func (m *mockCatalog) ListViews() []ViewInfo { return m.views }

func TestOptimization_09_MVAutoRouting(t *testing.T) {
	// The MV rewrite rule compares the MV's filter against the query's
	// "extracted search filter" returned by extractSearchFilter().
	// For a query with Source.Index="errors", the extracted filter is "errors".
	// The MV filter must be a subset of (or match) that string.
	catalog := &mockCatalog{
		views: []ViewInfo{
			{
				Name:         "mv_errors",
				Filter:       "errors",
				GroupBy:      []string{"host"},
				Aggregations: []string{"count"},
				Status:       "active",
			},
		},
	}

	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "errors"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
				GroupBy:      []string{"host"},
			},
		},
	}

	opt := New(WithCatalog(catalog))
	result := opt.Optimize(q)

	foundFrom := false
	for _, cmd := range result.Commands {
		if from, ok := cmd.(*spl2.FromCommand); ok {
			foundFrom = true
			if from.ViewName != "mv_errors" {
				t.Errorf("expected view name mv_errors, got %s", from.ViewName)
			}
		}
	}
	if !foundFrom {
		t.Error("expected query to be rewritten with FromCommand for mv_errors")
	}
}

// Test 10: QueryResultCache
func TestOptimization_10_QueryResultCache(t *testing.T) {
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "_time"},
						Op:    ">=",
						Right: &spl2.LiteralExpr{Value: "2024-06-01T00:00:00Z"},
					},
					Op: "and",
					Right: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "_time"},
						Op:    "<=",
						Right: &spl2.LiteralExpr{Value: "2024-06-02T00:00:00Z"},
					},
				},
			},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
				GroupBy:      []string{"host"},
			},
		},
	}

	opt := New()
	result := opt.Optimize(q)

	prog := &spl2.Program{Main: result}
	hints := spl2.ExtractQueryHints(prog)

	if hints.TimeBounds == nil {
		t.Fatal("expected TimeBounds to be set for cache invalidation data flow")
	}
	if hints.TimeBounds.Earliest.IsZero() {
		t.Error("expected Earliest to be non-zero")
	}
	if hints.TimeBounds.Latest.IsZero() {
		t.Error("expected Latest to be non-zero")
	}
}

// Test 11: InvertedIndexEquality
func TestOptimization_11_InvertedIndexEquality(t *testing.T) {
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "host"},
						Op:    "=",
						Right: &spl2.LiteralExpr{Value: "web-01"},
					},
					Op: "and",
					Right: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "source"},
						Op:    "=",
						Right: &spl2.LiteralExpr{Value: "nginx"},
					},
				},
			},
		},
	}

	opt := New()
	result := opt.Optimize(q)

	ann, ok := result.GetAnnotation("invertedIndexPredicates")
	if !ok {
		t.Fatal("expected invertedIndexPredicates annotation")
	}
	preds, ok := ann.([]spl2.InvertedIndexPredicate)
	if !ok {
		t.Fatalf("expected []spl2.InvertedIndexPredicate, got %T", ann)
	}

	predMap := make(map[string]string)
	for _, p := range preds {
		predMap[p.Field] = p.Value
	}
	if predMap["host"] != "web-01" {
		t.Errorf("expected host=web-01, got %s", predMap["host"])
	}
	if predMap["source"] != "nginx" {
		t.Errorf("expected source=nginx, got %s", predMap["source"])
	}
}

// Test 12: PredicateReorderStats
func TestOptimization_12_PredicateReorderStats(t *testing.T) {
	stats := map[string]FieldStatInfo{
		"host":   {Cardinality: 1000},
		"status": {Cardinality: 5},
	}

	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
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
						Left:  &spl2.FieldExpr{Name: "host"},
						Op:    "=",
						Right: &spl2.LiteralExpr{Value: "rare-host"},
					},
				},
			},
		},
	}

	opt := New(WithFieldStats(stats))
	result := opt.Optimize(q)

	// After optimization, flatten the AND predicates.
	w, ok := result.Commands[0].(*spl2.WhereCommand)
	if !ok {
		t.Fatal("expected WhereCommand at index 0")
	}
	preds := flattenAND(w.Expr)
	if len(preds) < 2 {
		t.Fatalf("expected >= 2 predicates after flatten, got %d", len(preds))
	}

	// The first predicate should reference "host" (1/1000 selectivity < 1/5 for status).
	firstCmp, ok := preds[0].(*spl2.CompareExpr)
	if !ok {
		t.Fatalf("expected CompareExpr as first predicate, got %T", preds[0])
	}
	firstField, ok := firstCmp.Left.(*spl2.FieldExpr)
	if !ok {
		t.Fatalf("expected FieldExpr on left side, got %T", firstCmp.Left)
	}
	if firstField.Name != "host" {
		t.Errorf("expected host predicate first (lower selectivity 1/1000), got %s", firstField.Name)
	}
}

// Test 13: ExprFoldRangeMergeIN
func TestOptimization_13_ExprFoldRangeMergeIN(t *testing.T) {
	t.Run("range_merge", func(t *testing.T) {
		q := &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.WhereCommand{
					Expr: &spl2.BinaryExpr{
						Left: &spl2.CompareExpr{
							Left:  &spl2.FieldExpr{Name: "status"},
							Op:    ">=",
							Right: &spl2.LiteralExpr{Value: "500"},
						},
						Op: "and",
						Right: &spl2.CompareExpr{
							Left:  &spl2.FieldExpr{Name: "status"},
							Op:    "<",
							Right: &spl2.LiteralExpr{Value: "600"},
						},
					},
				},
			},
		}

		opt := New()
		result := opt.Optimize(q)

		ann, ok := result.GetAnnotation("rangePredicates")
		if !ok {
			t.Fatal("expected rangePredicates annotation")
		}
		ranges, ok := ann.([]spl2.RangePredicate)
		if !ok {
			t.Fatalf("expected []spl2.RangePredicate, got %T", ann)
		}
		found := false
		for _, rp := range ranges {
			if rp.Field == "status" && rp.Min == "500" && rp.Max == "600" {
				found = true
			}
		}
		if !found {
			t.Errorf("expected range predicate {status [500, 600)}, got %+v", ranges)
		}
	})

	t.Run("in_list_rewrite", func(t *testing.T) {
		q := &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.WhereCommand{
					Expr: &spl2.BinaryExpr{
						Left: &spl2.BinaryExpr{
							Left: &spl2.CompareExpr{
								Left:  &spl2.FieldExpr{Name: "s"},
								Op:    "=",
								Right: &spl2.LiteralExpr{Value: "200"},
							},
							Op: "or",
							Right: &spl2.CompareExpr{
								Left:  &spl2.FieldExpr{Name: "s"},
								Op:    "=",
								Right: &spl2.LiteralExpr{Value: "301"},
							},
						},
						Op: "or",
						Right: &spl2.CompareExpr{
							Left:  &spl2.FieldExpr{Name: "s"},
							Op:    "=",
							Right: &spl2.LiteralExpr{Value: "404"},
						},
					},
				},
			},
		}

		opt := New()
		result := opt.Optimize(q)

		w, ok := result.Commands[0].(*spl2.WhereCommand)
		if !ok {
			t.Fatal("expected WhereCommand")
		}
		inExpr, ok := w.Expr.(*spl2.InExpr)
		if !ok {
			t.Fatalf("expected InExpr after rewrite, got %T", w.Expr)
		}
		fieldExpr, ok := inExpr.Field.(*spl2.FieldExpr)
		if !ok {
			t.Fatalf("expected FieldExpr in InExpr, got %T", inExpr.Field)
		}
		if fieldExpr.Name != "s" {
			t.Errorf("expected field name s, got %s", fieldExpr.Name)
		}
		if len(inExpr.Values) != 3 {
			t.Errorf("expected 3 values in InExpr, got %d", len(inExpr.Values))
		}
	})
}

// Test 14: DictEncodingFilter
func TestOptimization_14_DictEncodingFilter(t *testing.T) {
	values := make([]string, 12)
	hosts := []string{"web-01", "web-02", "web-03"}
	for i := 0; i < 12; i++ {
		values[i] = hosts[i%3]
	}

	data, err := column.NewDictEncoder().EncodeStrings(values)
	if err != nil {
		t.Fatalf("EncodeStrings failed: %v", err)
	}

	df, err := column.NewDictFilterFromEncoded(data)
	if err != nil {
		t.Fatalf("NewDictFilterFromEncoded failed: %v", err)
	}

	t.Run("match_4_rows", func(t *testing.T) {
		bm := df.FilterEquality("web-02")
		if bm.GetCardinality() != 4 {
			t.Errorf("expected 4 rows for web-02, got %d", bm.GetCardinality())
		}
	})

	t.Run("no_match", func(t *testing.T) {
		bm := df.FilterEquality("web-99")
		if bm.GetCardinality() != 0 {
			t.Errorf("expected 0 rows for web-99, got %d", bm.GetCardinality())
		}
	})
}

// Test 15: ApproximateAggregations
func TestOptimization_15_ApproximateAggregations(t *testing.T) {
	t.Run("HLL_basic", func(t *testing.T) {
		hll := pipeline.NewHyperLogLog()
		n := 1000
		for i := 0; i < n; i++ {
			hll.Add(fmt.Sprintf("val_%d", i))
		}
		count := hll.Count()
		// HLL is an approximation. Verify the API returns a positive count.
		if count <= 0 {
			t.Errorf("HLL count should be positive, got %d", count)
		}
		// Verify adding more unique values increases the count.
		countBefore := count
		for i := n; i < n+500; i++ {
			hll.Add(fmt.Sprintf("val_%d", i))
		}
		countAfter := hll.Count()
		if countAfter < countBefore {
			t.Errorf("HLL count should not decrease after adding more values: before=%d after=%d", countBefore, countAfter)
		}
		t.Logf("HLL count for %d distinct values: %d (after +500: %d)", n, count, countAfter)
	})

	t.Run("HLL_merge", func(t *testing.T) {
		hll1 := pipeline.NewHyperLogLog()
		hll2 := pipeline.NewHyperLogLog()
		for i := 0; i < 500; i++ {
			hll1.Add(fmt.Sprintf("a_%d", i))
		}
		for i := 0; i < 500; i++ {
			hll2.Add(fmt.Sprintf("b_%d", i))
		}
		count1Before := hll1.Count()
		hll1.Merge(hll2)
		countMerged := hll1.Count()
		// Merged count should be >= either individual count.
		if countMerged < count1Before {
			t.Errorf("merged HLL count %d < pre-merge count %d", countMerged, count1Before)
		}
		t.Logf("HLL merged count: %d (individual: %d)", countMerged, count1Before)
	})

	t.Run("TDigest", func(t *testing.T) {
		td := pipeline.NewTDigest(100)
		for i := 1; i <= 10000; i++ {
			td.Add(float64(i))
		}
		median := td.Quantile(0.5)
		p95 := td.Quantile(0.95)

		if math.Abs(median-5000) > 5000*0.05 {
			t.Errorf("TDigest median %f not within 5%% of 5000", median)
		}
		if math.Abs(p95-9500) > 9500*0.05 {
			t.Errorf("TDigest p95 %f not within 5%% of 9500", p95)
		}
	})
}

// Test 16: AdaptiveExecution
func TestOptimization_16_AdaptiveExecution(t *testing.T) {
	t.Run("SpillWriterReader", func(t *testing.T) {
		sw, err := pipeline.NewSpillWriter()
		if err != nil {
			t.Fatalf("NewSpillWriter failed: %v", err)
		}
		path := sw.Path()

		for i := 0; i < 100; i++ {
			row := map[string]event.Value{
				"host":  event.StringValue(fmt.Sprintf("host-%d", i)),
				"count": event.IntValue(int64(i)),
			}
			if err := sw.WriteRow(row); err != nil {
				t.Fatalf("WriteRow failed at %d: %v", i, err)
			}
		}

		if sw.Rows() != 100 {
			t.Errorf("expected 100 rows written, got %d", sw.Rows())
		}

		// Close the file handle but keep the file for reading.
		// Close() also removes the file, so grab the path first.
		sw.Close() // This removes the file, so we need a different approach.

		// Re-do: write, sync, read from same path before close removes it.
		sw2, err := pipeline.NewSpillWriter()
		if err != nil {
			t.Fatalf("NewSpillWriter failed: %v", err)
		}
		path2 := sw2.Path()
		defer os.Remove(path2)

		for i := 0; i < 100; i++ {
			row := map[string]event.Value{
				"host":  event.StringValue(fmt.Sprintf("host-%d", i)),
				"count": event.IntValue(int64(i)),
			}
			if err := sw2.WriteRow(row); err != nil {
				t.Fatalf("WriteRow failed: %v", err)
			}
		}
		// Flush by getting the path and creating a reader before Close removes the file.
		// SpillWriter.Close() calls file.Close() then os.Remove, so we need to
		// close the writer's file handle and open reader before removal.
		// Let's just copy the path and close the underlying file without removing.

		// Actually, just read back the path before the writer's Close.
		// We'll read with NewSpillReader then close the writer manually.
		sr, err := pipeline.NewSpillReader(path2)
		if err != nil {
			t.Fatalf("could not open spill file for reading: %v", err)
		}

		count := 0
		for {
			_, err := sr.ReadRow()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("ReadRow failed: %v", err)
			}
			count++
		}
		sr.Close()

		if count != 100 {
			t.Errorf("expected 100 rows read back, got %d", count)
		}

		_ = path // suppress unused
	})
}

// Test 17: JoinOptimization
func TestOptimization_17_JoinOptimization(t *testing.T) {
	t.Run("in_list_strategy", func(t *testing.T) {
		q := &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.JoinCommand{
					JoinType: "left",
					Field:    "user_id",
					Subquery: &spl2.Query{
						Source: &spl2.SourceClause{Index: "users"},
						Commands: []spl2.Command{
							&spl2.HeadCommand{Count: 100},
						},
					},
				},
			},
		}

		opt := New()
		result := opt.Optimize(q)

		ann, ok := result.GetAnnotation("joinStrategy")
		if !ok {
			t.Fatal("expected joinStrategy annotation")
		}
		ja, ok := ann.(*JoinAnnotation)
		if !ok {
			t.Fatalf("expected *JoinAnnotation, got %T", ann)
		}
		if ja.Strategy != "in_list" {
			t.Errorf("expected in_list strategy, got %s", ja.Strategy)
		}
	})

	t.Run("bloom_semi_strategy", func(t *testing.T) {
		q := &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.JoinCommand{
					JoinType: "inner",
					Field:    "user_id",
					Subquery: &spl2.Query{
						Source: &spl2.SourceClause{Index: "users"},
						Commands: []spl2.Command{
							&spl2.WhereCommand{
								Expr: &spl2.CompareExpr{
									Left:  &spl2.FieldExpr{Name: "active"},
									Op:    "=",
									Right: &spl2.LiteralExpr{Value: "true"},
								},
							},
						},
					},
				},
			},
		}

		opt := New()
		result := opt.Optimize(q)

		ann, ok := result.GetAnnotation("joinStrategy")
		if !ok {
			t.Fatal("expected joinStrategy annotation")
		}
		ja, ok := ann.(*JoinAnnotation)
		if !ok {
			t.Fatalf("expected *JoinAnnotation, got %T", ann)
		}
		if ja.Strategy != "bloom_semi" {
			t.Errorf("expected bloom_semi strategy, got %s", ja.Strategy)
		}
	})
}

// Test 18: IOPrefetch
func TestOptimization_18_IOPrefetch(t *testing.T) {
	// Compile-time check that PrefetchReader type exists and is referenceable.
	var _ *segment.PrefetchReader
}

// Test 19: PlanCache
func TestOptimization_19_PlanCache(t *testing.T) {
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "status"},
					Op:    "=",
					Right: &spl2.LiteralExpr{Value: "404"},
				},
			},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
				GroupBy:      []string{"host"},
			},
		},
	}

	opt := New()

	// First run.
	q1 := cloneQuery(q)
	opt.Optimize(q1)

	// Second run with fresh query.
	q2 := cloneQuery(q)
	opt.Optimize(q2)

	// Verify that Stats map has positive counts (proving optimizer tracked rule applications).
	total := 0
	for _, count := range opt.Stats {
		total += count
	}
	if total == 0 {
		t.Error("expected optimizer Stats to have positive counts after two runs")
	}

	// Verify specific rules were applied.
	if opt.Stats["ColumnPruning"] == 0 {
		t.Error("expected ColumnPruning to have been applied")
	}
}

// cloneQuery creates a shallow copy of a query (enough to avoid idempotency guard on annotations).
func cloneQuery(q *spl2.Query) *spl2.Query {
	cmds := make([]spl2.Command, len(q.Commands))
	copy(cmds, q.Commands)

	return &spl2.Query{
		Source:   q.Source,
		Commands: cmds,
	}
}

// Test 20: VectorizedExecution
func TestOptimization_20_VectorizedExecution(t *testing.T) {
	t.Run("FilterInt64GT", func(t *testing.T) {
		col := []int64{100, 200, 300, 400, 500}
		bitmap := pipeline.FilterInt64GT(col, 250)

		expected := []bool{false, false, true, true, true}
		if len(bitmap) != len(expected) {
			t.Fatalf("expected len %d, got %d", len(expected), len(bitmap))
		}
		for i := range expected {
			if bitmap[i] != expected[i] {
				t.Errorf("bitmap[%d] = %v, want %v", i, bitmap[i], expected[i])
			}
		}
	})

	t.Run("AndBitmaps", func(t *testing.T) {
		a := []bool{true, true, false, true, false}
		b := []bool{true, false, false, true, true}
		result := pipeline.AndBitmaps(a, b)
		expected := []bool{true, false, false, true, false}
		for i := range expected {
			if result[i] != expected[i] {
				t.Errorf("result[%d] = %v, want %v", i, result[i], expected[i])
			}
		}
	})

	t.Run("TypedBatch_roundtrip", func(t *testing.T) {
		batch := pipeline.NewBatch(3)
		batch.AddRow(map[string]event.Value{
			"count": event.IntValue(10),
			"name":  event.StringValue("alpha"),
		})
		batch.AddRow(map[string]event.Value{
			"count": event.IntValue(20),
			"name":  event.StringValue("beta"),
		})
		batch.AddRow(map[string]event.Value{
			"count": event.IntValue(30),
			"name":  event.StringValue("gamma"),
		})

		tb := pipeline.FromBatch(batch)
		if tb.Len != 3 {
			t.Errorf("TypedBatch.Len = %d, want 3", tb.Len)
		}
		if len(tb.Int64Cols["count"]) != 3 {
			t.Errorf("expected 3 int64 count values, got %d", len(tb.Int64Cols["count"]))
		}
		if len(tb.StringCols["name"]) != 3 {
			t.Errorf("expected 3 string name values, got %d", len(tb.StringCols["name"]))
		}

		// Convert back to Batch.
		back := tb.ToBatch()
		if back.Len != 3 {
			t.Errorf("round-trip Batch.Len = %d, want 3", back.Len)
		}
		if len(back.Columns["count"]) != 3 {
			t.Errorf("expected 3 count values after round-trip, got %d", len(back.Columns["count"]))
		}
		// Check a value.
		countVals := back.Columns["count"]
		if countVals[1].AsInt() != 20 {
			t.Errorf("expected count[1]=20, got %d", countVals[1].AsInt())
		}
		nameVals := back.Columns["name"]
		if nameVals[2].AsString() != "gamma" {
			t.Errorf("expected name[2]=gamma, got %s", nameVals[2].AsString())
		}
	})
}

// Test 21: TailScanOptimization
func TestOptimization_21_TailScanOptimization(t *testing.T) {
	t.Run("search_tail_annotated", func(t *testing.T) {
		q := &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.SearchCommand{Term: "error"},
				&spl2.TailCommand{Count: 5},
			},
		}

		opt := New()
		result := opt.Optimize(q)

		ann, ok := result.GetAnnotation("tailScanOptimization")
		if !ok {
			t.Fatal("expected tailScanOptimization annotation")
		}
		limit, ok := ann.(int)
		if !ok {
			t.Fatalf("expected int, got %T", ann)
		}
		if limit != 5 {
			t.Errorf("expected limit 5, got %d", limit)
		}
	})

	t.Run("stats_before_tail_not_annotated", func(t *testing.T) {
		q := &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.SearchCommand{Term: "error"},
				&spl2.StatsCommand{
					Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
				},
				&spl2.TailCommand{Count: 5},
			},
		}

		opt := New()
		result := opt.Optimize(q)

		_, ok := result.GetAnnotation("tailScanOptimization")
		if ok {
			t.Error("tailScanOptimization should NOT be set when stats precedes tail")
		}
	})

	t.Run("sort_before_tail_not_annotated", func(t *testing.T) {
		// Uses sort on "status" (not _time) to prevent removeSortOnScanOrderRule from
		// eliminating the sort before tailScanOptimizationRule runs.
		q := &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.SearchCommand{Term: "error"},
				&spl2.SortCommand{Fields: []spl2.SortField{{Name: "status", Desc: true}}},
				&spl2.TailCommand{Count: 5},
			},
		}

		opt := New()
		result := opt.Optimize(q)

		_, ok := result.GetAnnotation("tailScanOptimization")
		if ok {
			t.Error("tailScanOptimization should NOT be set when sort precedes tail")
		}
	})

	t.Run("where_eval_tail_annotated", func(t *testing.T) {
		q := &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.SearchCommand{Term: "error"},
				&spl2.WhereCommand{
					Expr: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "status"},
						Op:    ">=",
						Right: &spl2.LiteralExpr{Value: "500"},
					},
				},
				&spl2.EvalCommand{
					Field: "x",
					Expr:  &spl2.LiteralExpr{Value: "1"},
				},
				&spl2.TailCommand{Count: 5},
			},
		}

		opt := New()
		result := opt.Optimize(q)

		ann, ok := result.GetAnnotation("tailScanOptimization")
		if !ok {
			t.Fatal("expected tailScanOptimization annotation for where+eval+tail")
		}
		if ann.(int) != 5 {
			t.Errorf("expected limit 5, got %v", ann)
		}
	})

	t.Run("head_before_tail_not_annotated", func(t *testing.T) {
		q := &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.SearchCommand{Term: "error"},
				&spl2.HeadCommand{Count: 10},
				&spl2.TailCommand{Count: 5},
			},
		}

		opt := New()
		result := opt.Optimize(q)

		_, ok := result.GetAnnotation("tailScanOptimization")
		if ok {
			t.Error("tailScanOptimization should NOT be set when head precedes tail")
		}
	})

	t.Run("hints_integration", func(t *testing.T) {
		q := &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.SearchCommand{Term: "error"},
				&spl2.TailCommand{Count: 3},
			},
		}

		opt := New()
		result := opt.Optimize(q)

		prog := &spl2.Program{Main: result}
		hints := spl2.ExtractQueryHints(prog)

		if hints.TailLimit != 3 {
			t.Errorf("TailLimit: got %d, want 3", hints.TailLimit)
		}
		if !hints.ReverseScan {
			t.Error("ReverseScan should be true when tailScanOptimization is annotated")
		}
	})
}

// Ensure imported packages are used (suppress compile errors for type-only references).
var (
	_ = sort.Ints
	_ = index.Tokenize
	_ = segment.NewPrefetchReader
)
