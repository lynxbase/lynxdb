package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// TestSortColumnarPath_Basic verifies that the columnar sort fast path
// produces the same result as the legacy row-based sort.
func TestSortColumnarPath_Basic(t *testing.T) {
	const n = 200
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		// Reverse order so sort actually has work.
		rows[i] = map[string]event.Value{
			"key":  event.IntValue(int64(n - 1 - i)),
			"data": event.StringValue(fmt.Sprintf("row-%d", n-1-i)),
		}
	}

	child := NewRowScanIterator(rows, DefaultBatchSize)
	iter := NewSortIterator(child, []SortField{{Name: "key", Desc: false}}, DefaultBatchSize)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != n {
		t.Fatalf("expected %d rows, got %d", n, len(result))
	}

	// Verify the columnar path was used (no spill, no row fallback).
	if !iter.useColumnar {
		t.Error("expected columnar fast path to be used")
	}

	// Verify ascending order.
	for i := 0; i < n; i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
		wantData := fmt.Sprintf("row-%d", i)
		if result[i]["data"].String() != wantData {
			t.Fatalf("row %d: expected data=%q, got %q", i, wantData, result[i]["data"].String())
		}
	}
}

// TestSortColumnarPath_Descending verifies descending columnar sort.
func TestSortColumnarPath_Descending(t *testing.T) {
	const n = 100
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"key": event.IntValue(int64(i)),
		}
	}

	child := NewRowScanIterator(rows, DefaultBatchSize)
	iter := NewSortIterator(child, []SortField{{Name: "key", Desc: true}}, DefaultBatchSize)

	ctx := context.Background()
	_ = iter.Init(ctx)
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != n {
		t.Fatalf("expected %d rows, got %d", n, len(result))
	}

	// Verify descending order.
	for i := 0; i < n; i++ {
		want := int64(n - 1 - i)
		got := result[i]["key"].AsInt()
		if got != want {
			t.Fatalf("row %d: expected key=%d, got %d", i, want, got)
		}
	}
}

// TestSortColumnarPath_MultiField verifies sort by multiple fields.
func TestSortColumnarPath_MultiField(t *testing.T) {
	rows := []map[string]event.Value{
		{"group": event.StringValue("b"), "val": event.IntValue(2)},
		{"group": event.StringValue("a"), "val": event.IntValue(3)},
		{"group": event.StringValue("a"), "val": event.IntValue(1)},
		{"group": event.StringValue("b"), "val": event.IntValue(1)},
	}

	child := NewRowScanIterator(rows, DefaultBatchSize)
	iter := NewSortIterator(child, []SortField{
		{Name: "group", Desc: false},
		{Name: "val", Desc: false},
	}, DefaultBatchSize)

	ctx := context.Background()
	_ = iter.Init(ctx)
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result))
	}

	// Expected order: (a,1), (a,3), (b,1), (b,2)
	expected := []struct {
		group string
		val   int64
	}{
		{"a", 1}, {"a", 3}, {"b", 1}, {"b", 2},
	}
	for i, e := range expected {
		g := result[i]["group"].String()
		v := result[i]["val"].AsInt()
		if g != e.group || v != e.val {
			t.Errorf("row %d: got (%q, %d), want (%q, %d)", i, g, v, e.group, e.val)
		}
	}
}

// TestSortColumnarPath_NullHandling verifies that null values are handled
// correctly in the columnar sort path.
func TestSortColumnarPath_NullHandling(t *testing.T) {
	rows := []map[string]event.Value{
		{"key": event.IntValue(3)},
		{"key": event.Value{}}, // null
		{"key": event.IntValue(1)},
		{"key": event.Value{}}, // null
		{"key": event.IntValue(2)},
	}

	child := NewRowScanIterator(rows, DefaultBatchSize)
	iter := NewSortIterator(child, []SortField{{Name: "key", Desc: false}}, DefaultBatchSize)

	ctx := context.Background()
	_ = iter.Init(ctx)
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(result))
	}

	// Nulls should sort consistently (CompareValues puts nulls first).
	// Verify non-null values are in order.
	var nonNulls []int64
	for _, r := range result {
		if !r["key"].IsNull() {
			nonNulls = append(nonNulls, r["key"].AsInt())
		}
	}
	if len(nonNulls) != 3 {
		t.Fatalf("expected 3 non-null values, got %d", len(nonNulls))
	}
	for i := 1; i < len(nonNulls); i++ {
		if nonNulls[i] < nonNulls[i-1] {
			t.Errorf("non-null values not sorted: %v", nonNulls)
			break
		}
	}
}

// TestSortColumnarPath_EmptyBatch verifies the edge case of empty input.
func TestSortColumnarPath_EmptyBatch(t *testing.T) {
	child := NewRowScanIterator(nil, DefaultBatchSize)
	iter := NewSortIterator(child, []SortField{{Name: "key", Desc: false}}, DefaultBatchSize)

	ctx := context.Background()
	_ = iter.Init(ctx)
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result))
	}
}

// TestSortColumnarPath_SingleRow verifies the edge case of a single row.
func TestSortColumnarPath_SingleRow(t *testing.T) {
	rows := []map[string]event.Value{
		{"key": event.IntValue(42)},
	}

	child := NewRowScanIterator(rows, DefaultBatchSize)
	iter := NewSortIterator(child, []SortField{{Name: "key", Desc: false}}, DefaultBatchSize)

	ctx := context.Background()
	_ = iter.Init(ctx)
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if result[0]["key"].AsInt() != 42 {
		t.Errorf("key: got %d, want 42", result[0]["key"].AsInt())
	}
}

// TestSortColumnarPath_MultipleBatches verifies that columnar sort correctly
// merges data arriving in multiple child batches.
func TestSortColumnarPath_MultipleBatches(t *testing.T) {
	const n = 500
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"key": event.IntValue(int64(n - 1 - i)),
		}
	}

	// Use small batch size to force multiple batches from child.
	child := NewRowScanIterator(rows, 64)
	iter := NewSortIterator(child, []SortField{{Name: "key", Desc: false}}, 64)

	ctx := context.Background()
	_ = iter.Init(ctx)
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != n {
		t.Fatalf("expected %d rows, got %d", n, len(result))
	}

	if !iter.useColumnar {
		t.Error("expected columnar fast path to be used")
	}

	for i := 0; i < n; i++ {
		if result[i]["key"].AsInt() != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, result[i]["key"].AsInt())
		}
	}
}

// TestSortColumnarPath_SparseColumns verifies that columnar sort handles
// batches with different column sets correctly.
func TestSortColumnarPath_SparseColumns(t *testing.T) {
	// First batch has columns "key" and "a".
	b1 := &Batch{
		Columns: map[string][]event.Value{
			"key": {event.IntValue(3), event.IntValue(1)},
			"a":   {event.StringValue("x"), event.StringValue("y")},
		},
		Len: 2,
	}
	// Second batch has columns "key" and "b".
	b2 := &Batch{
		Columns: map[string][]event.Value{
			"key": {event.IntValue(2)},
			"b":   {event.StringValue("z")},
		},
		Len: 1,
	}

	child := &staticIterator{batches: []*Batch{b1, b2}}
	iter := NewSortIterator(child, []SortField{{Name: "key", Desc: false}}, DefaultBatchSize)

	ctx := context.Background()
	_ = iter.Init(ctx)
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}

	// Expected order by key: 1, 2, 3.
	expectedKeys := []int64{1, 2, 3}
	for i, want := range expectedKeys {
		if result[i]["key"].AsInt() != want {
			t.Errorf("row %d: key=%d, want %d", i, result[i]["key"].AsInt(), want)
		}
	}

	// Row with key=1 should have "a"="y", null "b".
	if result[0]["a"].String() != "y" {
		t.Errorf("row 0: a=%q, want y", result[0]["a"].String())
	}
	if !result[0]["b"].IsNull() {
		t.Errorf("row 0: b should be null, got %v", result[0]["b"])
	}

	// Row with key=2 should have null "a", "b"="z".
	if !result[1]["a"].IsNull() {
		t.Errorf("row 1: a should be null, got %v", result[1]["a"])
	}
	if result[1]["b"].String() != "z" {
		t.Errorf("row 1: b=%q, want z", result[1]["b"].String())
	}
}

// TestSortColumnarPath_StableSort verifies that equal elements maintain
// their relative order (stability).
func TestSortColumnarPath_StableSort(t *testing.T) {
	rows := []map[string]event.Value{
		{"key": event.IntValue(1), "order": event.IntValue(0)},
		{"key": event.IntValue(2), "order": event.IntValue(1)},
		{"key": event.IntValue(1), "order": event.IntValue(2)},
		{"key": event.IntValue(2), "order": event.IntValue(3)},
		{"key": event.IntValue(1), "order": event.IntValue(4)},
	}

	child := NewRowScanIterator(rows, DefaultBatchSize)
	iter := NewSortIterator(child, []SortField{{Name: "key", Desc: false}}, DefaultBatchSize)

	ctx := context.Background()
	_ = iter.Init(ctx)
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	// Within key=1, order should be 0, 2, 4 (original order preserved).
	// Within key=2, order should be 1, 3.
	expectedOrders := []int64{0, 2, 4, 1, 3}
	for i, want := range expectedOrders {
		got := result[i]["order"].AsInt()
		if got != want {
			t.Errorf("row %d: order=%d, want %d (stable sort violated)", i, got, want)
		}
	}
}

// BenchmarkSortColumnar benchmarks the columnar sort path.
func BenchmarkSortColumnar(b *testing.B) {
	for _, n := range []int{1000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			// Pre-build rows in reverse order.
			rows := make([]map[string]event.Value, n)
			for i := 0; i < n; i++ {
				rows[i] = map[string]event.Value{
					"key":  event.IntValue(int64(n - 1 - i)),
					"data": event.StringValue(fmt.Sprintf("row-%06d", n-1-i)),
				}
			}

			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				child := NewRowScanIterator(rows, DefaultBatchSize)
				iter := NewSortIterator(child, []SortField{{Name: "key", Desc: false}}, DefaultBatchSize)
				_ = iter.Init(ctx)

				for {
					batch, err := iter.Next(ctx)
					if err != nil {
						b.Fatal(err)
					}
					if batch == nil {
						break
					}
				}
				_ = iter.Close()
			}
		})
	}
}
