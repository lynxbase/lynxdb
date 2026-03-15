package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// TestSearchExprIterator_VectorizedFieldEq tests the vectorized fast path
// for simple field=value search expressions (SearchCompareExpr with OpEq).
func TestSearchExprIterator_VectorizedFieldEq(t *testing.T) {
	// Create events with a "log_type" field — some postgres, some nginx.
	events := make([]*event.Event, 100)
	for i := range events {
		ev := event.NewEvent(time.Now(), fmt.Sprintf("log line %d", i))
		ev.Source = "test"
		ev.Fields = map[string]event.Value{}
		if i%5 == 0 {
			ev.Fields["log_type"] = event.StringValue("postgres")
		} else {
			ev.Fields["log_type"] = event.StringValue("nginx")
		}
		events[i] = ev
	}

	// search log_type = "postgres"
	expr := &spl2.SearchCompareExpr{
		Field: "log_type",
		Op:    spl2.OpEq,
		Value: "postgres",
	}
	eval := spl2.NewSearchEvaluator(expr)

	scan := NewScanIterator(events, 1024)
	search := NewSearchExprIteratorWithExpr(scan, eval, expr)

	ctx := context.Background()
	_ = search.Init(ctx)
	rows, err := CollectAll(ctx, search)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	// 20% match rate: 100/5 = 20
	if len(rows) != 20 {
		t.Errorf("expected 20 matching rows, got %d", len(rows))
	}

	// Verify all matched rows have log_type=postgres.
	for i, row := range rows {
		lt := row["log_type"]
		if lt.String() != "postgres" {
			t.Errorf("row %d: log_type=%q, want postgres", i, lt.String())
		}
	}

	// Verify vectorized path was detected.
	if !search.vecReady {
		t.Error("expected vecReady=true for field=value search")
	}
}

// TestSearchExprIterator_VectorizedFieldEq_CaseInsensitive tests that the
// vectorized path is case-insensitive by default (matching search semantics).
func TestSearchExprIterator_VectorizedFieldEq_CaseInsensitive(t *testing.T) {
	events := make([]*event.Event, 10)
	for i := range events {
		ev := event.NewEvent(time.Now(), fmt.Sprintf("log line %d", i))
		ev.Fields = map[string]event.Value{
			"level": event.StringValue("ERROR"),
		}
		events[i] = ev
	}

	// search level = "error" (lowercase) — should match "ERROR" (uppercase)
	expr := &spl2.SearchCompareExpr{
		Field:         "level",
		Op:            spl2.OpEq,
		Value:         "error",
		CaseSensitive: false,
	}
	eval := spl2.NewSearchEvaluator(expr)

	scan := NewScanIterator(events, 1024)
	search := NewSearchExprIteratorWithExpr(scan, eval, expr)

	ctx := context.Background()
	_ = search.Init(ctx)
	rows, err := CollectAll(ctx, search)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 10 {
		t.Errorf("expected 10 rows (case-insensitive match), got %d", len(rows))
	}
}

// TestSearchExprIterator_VectorizedFieldNotEq tests the vectorized path for !=.
func TestSearchExprIterator_VectorizedFieldNotEq(t *testing.T) {
	events := make([]*event.Event, 10)
	for i := range events {
		ev := event.NewEvent(time.Now(), fmt.Sprintf("log line %d", i))
		ev.Fields = map[string]event.Value{}
		if i < 3 {
			ev.Fields["status"] = event.StringValue("200")
		} else {
			ev.Fields["status"] = event.StringValue("500")
		}
		events[i] = ev
	}

	expr := &spl2.SearchCompareExpr{
		Field: "status",
		Op:    spl2.OpNotEq,
		Value: "200",
	}
	eval := spl2.NewSearchEvaluator(expr)

	scan := NewScanIterator(events, 1024)
	search := NewSearchExprIteratorWithExpr(scan, eval, expr)

	ctx := context.Background()
	_ = search.Init(ctx)
	rows, err := CollectAll(ctx, search)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 7 {
		t.Errorf("expected 7 rows (status!=200), got %d", len(rows))
	}
}

// TestSearchExprIterator_VectorizedFieldGt tests numeric comparison.
func TestSearchExprIterator_VectorizedFieldGt(t *testing.T) {
	events := make([]*event.Event, 10)
	for i := range events {
		ev := event.NewEvent(time.Now(), fmt.Sprintf("log line %d", i))
		ev.Fields = map[string]event.Value{
			"status": event.IntValue(int64(200 + i*50)),
		}
		events[i] = ev
	}

	// search status > 400
	expr := &spl2.SearchCompareExpr{
		Field: "status",
		Op:    spl2.OpGt,
		Value: "400",
	}
	eval := spl2.NewSearchEvaluator(expr)

	scan := NewScanIterator(events, 1024)
	search := NewSearchExprIteratorWithExpr(scan, eval, expr)

	ctx := context.Background()
	_ = search.Init(ctx)
	rows, err := CollectAll(ctx, search)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	// status values: 200,250,300,350,400,450,500,550,600,650
	// > 400: 450,500,550,600,650 = 5
	if len(rows) != 5 {
		t.Errorf("expected 5 rows (status>400), got %d", len(rows))
	}
}

// TestSearchExprIterator_VectorizedFieldExistence tests field=* (exists check).
func TestSearchExprIterator_VectorizedFieldExistence(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {
				event.StringValue("line1"),
				event.StringValue("line2"),
				event.StringValue("line3"),
			},
			"optional": {
				event.StringValue("present"),
				event.NullValue(),
				event.StringValue("also present"),
			},
		},
		Len: 3,
	}

	expr := &spl2.SearchCompareExpr{
		Field: "optional",
		Op:    spl2.OpEq,
		Value: "*",
	}
	eval := spl2.NewSearchEvaluator(expr)

	scan := &staticIterator{batches: []*Batch{batch}}
	search := NewSearchExprIteratorWithExpr(scan, eval, expr)

	ctx := context.Background()
	_ = search.Init(ctx)
	rows, err := CollectAll(ctx, search)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	// 2 non-null values
	if len(rows) != 2 {
		t.Errorf("expected 2 rows (field exists), got %d", len(rows))
	}
}

// TestSearchExprIterator_VectorizedSourceAlias tests that "source" is resolved to "_source".
func TestSearchExprIterator_VectorizedSourceAlias(t *testing.T) {
	events := make([]*event.Event, 10)
	for i := range events {
		ev := event.NewEvent(time.Now(), fmt.Sprintf("log line %d", i))
		ev.Fields = map[string]event.Value{}
		if i < 4 {
			ev.Source = "nginx"
		} else {
			ev.Source = "api-gw"
		}
		events[i] = ev
	}

	// search source=nginx — "source" alias should resolve to "_source" column.
	expr := &spl2.SearchCompareExpr{
		Field: "source",
		Op:    spl2.OpEq,
		Value: "nginx",
	}
	eval := spl2.NewSearchEvaluator(expr)

	scan := NewScanIterator(events, 1024)
	search := NewSearchExprIteratorWithExpr(scan, eval, expr)

	ctx := context.Background()
	_ = search.Init(ctx)
	rows, err := CollectAll(ctx, search)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 4 {
		t.Errorf("expected 4 rows (source=nginx), got %d", len(rows))
	}

	// Verify vectorized path resolved "source" to "_source".
	if search.vecField != "_source" {
		t.Errorf("vecField=%q, want _source", search.vecField)
	}
}

// TestSearchExprIterator_VectorizedFallbackMissingColumn tests that the
// vectorized path falls back to per-row evaluation when the field column
// is missing (needs JSON extraction from _raw).
func TestSearchExprIterator_VectorizedFallbackMissingColumn(t *testing.T) {
	// Events with JSON in _raw but no explicit "level" field column.
	events := []*event.Event{
		{Time: time.Now(), Raw: `{"level":"error","msg":"fail"}`, Fields: map[string]event.Value{}},
		{Time: time.Now(), Raw: `{"level":"info","msg":"ok"}`, Fields: map[string]event.Value{}},
		{Time: time.Now(), Raw: `{"level":"error","msg":"fail2"}`, Fields: map[string]event.Value{}},
	}

	expr := &spl2.SearchCompareExpr{
		Field: "level",
		Op:    spl2.OpEq,
		Value: "error",
	}
	eval := spl2.NewSearchEvaluator(expr)

	scan := NewScanIterator(events, 1024)
	search := NewSearchExprIteratorWithExpr(scan, eval, expr)

	ctx := context.Background()
	_ = search.Init(ctx)
	rows, err := CollectAll(ctx, search)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	// JSON fallback should extract level from _raw.
	if len(rows) != 2 {
		t.Errorf("expected 2 rows (level=error via JSON fallback), got %d", len(rows))
	}
}

// TestSearchExprIterator_VectorizedWildcardNotVectorized tests that
// SearchCompareExpr with HasWildcard=true does NOT use vectorized path.
func TestSearchExprIterator_VectorizedWildcardNotVectorized(t *testing.T) {
	expr := &spl2.SearchCompareExpr{
		Field:       "host",
		Op:          spl2.OpEq,
		Value:       "web-*",
		HasWildcard: true,
	}
	eval := spl2.NewSearchEvaluator(expr)

	scan := &staticIterator{batches: nil}
	search := NewSearchExprIteratorWithExpr(scan, eval, expr)

	if search.vecReady {
		t.Error("vecReady should be false for wildcard SearchCompareExpr")
	}
}

// BenchmarkSearchExprIterator_FieldEq_Vectorized benchmarks the vectorized
// field=value search path against 1024 events.
func BenchmarkSearchExprIterator_FieldEq_Vectorized(b *testing.B) {
	events := make([]*event.Event, 1024)
	for i := range events {
		ev := event.NewEvent(time.Now(), fmt.Sprintf("log line %d", i))
		ev.Fields = map[string]event.Value{}
		if i%20 == 0 { // 5% match rate
			ev.Fields["log_type"] = event.StringValue("postgres")
		} else {
			ev.Fields["log_type"] = event.StringValue("nginx")
		}
		events[i] = ev
	}

	expr := &spl2.SearchCompareExpr{
		Field: "log_type",
		Op:    spl2.OpEq,
		Value: "postgres",
	}
	eval := spl2.NewSearchEvaluator(expr)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan := NewScanIterator(events, 1024)
		search := NewSearchExprIteratorWithExpr(scan, eval, expr)
		_ = search.Init(ctx)
		batch, _ := search.Next(ctx)
		if batch == nil || batch.Len == 0 {
			b.Fatal("expected matches")
		}
	}
}
