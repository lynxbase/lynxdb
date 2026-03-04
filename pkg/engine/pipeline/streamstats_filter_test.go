package pipeline

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// TestStreamStatsFilterIntegration tests that WHERE can filter on
// STREAMSTATS-produced columns. This is a targeted reproduction for
// the E2E failure where STREAMSTATS count AS row_num | WHERE row_num = 1
// returns 0 rows.
func TestStreamStatsFilterIntegration(t *testing.T) {
	events := makeEvents(10)
	scan := NewScanIterator(events, 1024)

	// STREAMSTATS count AS row_num (current=true, unlimited window)
	aggs := []AggFunc{{Name: "count", Field: "", Alias: "row_num"}}
	ss := NewStreamStatsIterator(scan, aggs, nil, 0, true)

	// Compile WHERE row_num = 1
	expr := &spl2.CompareExpr{
		Left:  &spl2.FieldExpr{Name: "row_num"},
		Op:    "=",
		Right: &spl2.LiteralExpr{Value: "1"},
	}
	prog, err := vm.CompilePredicate(expr)
	if err != nil {
		t.Fatalf("compile predicate: %v", err)
	}

	filter := NewFilterIteratorWithExpr(ss, prog, expr)

	ctx := context.Background()
	if err := filter.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}

	rows, err := CollectAll(ctx, filter)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}

	t.Logf("got %d rows", len(rows))
	for i, row := range rows {
		t.Logf("  row %d: row_num=%v (type=%v)", i, row["row_num"], row["row_num"].Type())
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row where row_num=1, got %d", len(rows))
	}

	v := rows[0]["row_num"]
	if v.AsInt() != 1 {
		t.Errorf("expected row_num=1, got %v", v)
	}
}

// TestStreamStatsFilterLTE tests WHERE row_num <= 5 on STREAMSTATS output.
func TestStreamStatsFilterLTE(t *testing.T) {
	events := makeEvents(10)
	scan := NewScanIterator(events, 1024)

	aggs := []AggFunc{{Name: "count", Field: "", Alias: "row_num"}}
	ss := NewStreamStatsIterator(scan, aggs, nil, 0, true)

	// Compile WHERE row_num <= 5
	expr := &spl2.CompareExpr{
		Left:  &spl2.FieldExpr{Name: "row_num"},
		Op:    "<=",
		Right: &spl2.LiteralExpr{Value: "5"},
	}
	prog, err := vm.CompilePredicate(expr)
	if err != nil {
		t.Fatalf("compile predicate: %v", err)
	}

	filter := NewFilterIteratorWithExpr(ss, prog, expr)

	ctx := context.Background()
	if err := filter.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}

	rows, err := CollectAll(ctx, filter)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}

	t.Logf("got %d rows", len(rows))
	for i, row := range rows {
		t.Logf("  row %d: row_num=%v (type=%v)", i, row["row_num"], row["row_num"].Type())
	}

	if len(rows) != 5 {
		t.Fatalf("expected 5 rows where row_num<=5, got %d", len(rows))
	}
}

// TestStreamStatsColumnVisibility verifies the STREAMSTATS column is actually
// present in the batch that goes to the filter.
func TestStreamStatsColumnVisibility(t *testing.T) {
	events := makeEvents(5)
	scan := NewScanIterator(events, 1024)

	aggs := []AggFunc{{Name: "count", Field: "", Alias: "row_num"}}
	ss := NewStreamStatsIterator(scan, aggs, nil, 0, true)

	ctx := context.Background()
	if err := ss.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}

	batch, err := ss.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if batch == nil {
		t.Fatal("nil batch")
	}

	t.Logf("batch.Len=%d, columns=%d", batch.Len, len(batch.Columns))
	for k, col := range batch.Columns {
		t.Logf("  column %q: len=%d", k, len(col))
		for i, v := range col {
			if i < 5 {
				t.Logf("    [%d] type=%v value=%v", i, v.Type(), v)
			}
		}
	}

	col, ok := batch.Columns["row_num"]
	if !ok {
		t.Fatal("row_num column not found in batch")
	}
	if len(col) != batch.Len {
		t.Errorf("row_num column len=%d, batch.Len=%d", len(col), batch.Len)
	}

	// Verify values
	for i := 0; i < batch.Len && i < 5; i++ {
		v := col[i]
		expected := int64(i + 1)
		if v.AsInt() != expected {
			t.Errorf("row_num[%d]=%d, want %d (type=%v)", i, v.AsInt(), expected, v.Type())
		}
	}

	// Now manually test filter logic
	t.Log("\n=== Manual filter test ===")
	row := make(map[string]event.Value, len(batch.Columns))
	for i := 0; i < batch.Len; i++ {
		for k, c := range batch.Columns {
			if i < len(c) {
				row[k] = c[i]
			}
		}
		rn := row["row_num"]
		cmp := vm.CompareValues(rn, event.IntValue(1))
		t.Logf("  row %d: row_num=%v (type=%v), CompareValues(row_num, 1)=%d",
			i, rn, rn.Type(), cmp)
	}
}
