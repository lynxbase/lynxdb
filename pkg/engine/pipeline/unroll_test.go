package pipeline

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestUnrollIterator_ArrayOfObjects(t *testing.T) {
	rows := []map[string]event.Value{
		{
			"order": event.StringValue("ORD-1"),
			"items": event.StringValue(`[{"sku":"A1","qty":2},{"sku":"B3","qty":1}]`),
		},
	}

	child := NewRowScanIterator(rows, 1024)
	iter := NewUnrollIterator(child, []string{"items"}, 1024)
	results, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(results))
	}

	// Both rows should have order=ORD-1
	for i, r := range results {
		if r["order"].String() != "ORD-1" {
			t.Errorf("row %d: order = %q, want %q", i, r["order"].String(), "ORD-1")
		}
	}

	// First row: items.sku=A1, items.qty=2
	if results[0]["items.sku"].String() != "A1" {
		t.Errorf("row 0: items.sku = %q, want %q", results[0]["items.sku"].String(), "A1")
	}
	if results[0]["items.qty"].AsInt() != 2 {
		t.Errorf("row 0: items.qty = %v, want 2", results[0]["items.qty"])
	}

	// Second row: items.sku=B3, items.qty=1
	if results[1]["items.sku"].String() != "B3" {
		t.Errorf("row 1: items.sku = %q, want %q", results[1]["items.sku"].String(), "B3")
	}
	if results[1]["items.qty"].AsInt() != 1 {
		t.Errorf("row 1: items.qty = %v, want 1", results[1]["items.qty"])
	}
}

func TestUnrollIterator_ArrayOfScalars(t *testing.T) {
	rows := []map[string]event.Value{
		{
			"name": event.StringValue("alice"),
			"tags": event.StringValue(`["admin","user","dev"]`),
		},
	}

	child := NewRowScanIterator(rows, 1024)
	iter := NewUnrollIterator(child, []string{"tags"}, 1024)
	results, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(results))
	}

	expected := []string{"admin", "user", "dev"}
	for i, r := range results {
		if r["tags"].String() != expected[i] {
			t.Errorf("row %d: tags = %q, want %q", i, r["tags"].String(), expected[i])
		}
		if r["name"].String() != "alice" {
			t.Errorf("row %d: name = %q, want %q", i, r["name"].String(), "alice")
		}
	}
}

func TestUnrollIterator_NonArrayField(t *testing.T) {
	rows := []map[string]event.Value{
		{
			"msg":  event.StringValue("hello"),
			"data": event.StringValue("not-an-array"),
		},
	}

	child := NewRowScanIterator(rows, 1024)
	iter := NewUnrollIterator(child, []string{"data"}, 1024)
	results, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 row, got %d", len(results))
	}
	if results[0]["data"].String() != "not-an-array" {
		t.Errorf("data = %q, want %q", results[0]["data"].String(), "not-an-array")
	}
}

func TestUnrollIterator_NullField(t *testing.T) {
	rows := []map[string]event.Value{
		{
			"msg": event.StringValue("hello"),
		},
	}

	child := NewRowScanIterator(rows, 1024)
	iter := NewUnrollIterator(child, []string{"items"}, 1024)
	results, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 row, got %d", len(results))
	}
}

func TestUnrollIterator_EmptyArray(t *testing.T) {
	rows := []map[string]event.Value{
		{
			"items": event.StringValue(`[]`),
		},
	}

	child := NewRowScanIterator(rows, 1024)
	iter := NewUnrollIterator(child, []string{"items"}, 1024)
	results, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	// Empty array: row passes through unchanged.
	if len(results) != 1 {
		t.Fatalf("expected 1 row, got %d", len(results))
	}
}

func TestUnrollIterator_ZipExpansion(t *testing.T) {
	rows := []map[string]event.Value{
		{
			"order":   event.StringValue("ORD-1"),
			"product": event.StringValue(`["Widget","Gadget"]`),
			"price":   event.StringValue(`[999.99,29.99]`),
		},
	}

	child := NewRowScanIterator(rows, 1024)
	iter := NewUnrollIterator(child, []string{"product", "price"}, 1024)
	results, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(results))
	}

	// Row 0: product=Widget, price=999.99
	if results[0]["product"].String() != "Widget" {
		t.Errorf("row 0: product = %q, want %q", results[0]["product"].String(), "Widget")
	}
	if results[0]["price"].AsFloat() != 999.99 {
		t.Errorf("row 0: price = %v, want 999.99", results[0]["price"])
	}
	if results[0]["order"].String() != "ORD-1" {
		t.Errorf("row 0: order = %q, want %q", results[0]["order"].String(), "ORD-1")
	}

	// Row 1: product=Gadget, price=29.99
	if results[1]["product"].String() != "Gadget" {
		t.Errorf("row 1: product = %q, want %q", results[1]["product"].String(), "Gadget")
	}
	if results[1]["price"].AsFloat() != 29.99 {
		t.Errorf("row 1: price = %v, want 29.99", results[1]["price"])
	}
}

func TestUnrollIterator_ZipMismatchedLengths(t *testing.T) {
	rows := []map[string]event.Value{
		{
			"a": event.StringValue(`[1,2,3]`),
			"b": event.StringValue(`["x","y"]`),
		},
	}

	child := NewRowScanIterator(rows, 1024)
	iter := NewUnrollIterator(child, []string{"a", "b"}, 1024)
	results, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	// Mismatched lengths — pass through unchanged.
	if len(results) != 1 {
		t.Fatalf("expected 1 row (pass-through), got %d", len(results))
	}
	if results[0]["a"].String() != `[1,2,3]` {
		t.Errorf("a = %q, want %q", results[0]["a"].String(), `[1,2,3]`)
	}
}

func TestUnrollIterator_ZipOneNotArray(t *testing.T) {
	rows := []map[string]event.Value{
		{
			"a": event.StringValue(`["x","y"]`),
			"b": event.StringValue("scalar-value"),
		},
	}

	child := NewRowScanIterator(rows, 1024)
	iter := NewUnrollIterator(child, []string{"a", "b"}, 1024)
	results, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	// One field is not an array — pass through unchanged.
	if len(results) != 1 {
		t.Fatalf("expected 1 row (pass-through), got %d", len(results))
	}
}

func TestUnrollIterator_SingleFieldBackwardCompat(t *testing.T) {
	// Verify single-field mode produces identical results to the original behavior.
	rows := []map[string]event.Value{
		{
			"id":   event.StringValue("1"),
			"tags": event.StringValue(`["a","b","c"]`),
		},
	}

	child := NewRowScanIterator(rows, 1024)
	iter := NewUnrollIterator(child, []string{"tags"}, 1024)
	results, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(results))
	}

	expected := []string{"a", "b", "c"}
	for i, r := range results {
		if r["tags"].String() != expected[i] {
			t.Errorf("row %d: tags = %q, want %q", i, r["tags"].String(), expected[i])
		}
		if r["id"].String() != "1" {
			t.Errorf("row %d: id = %q, want %q", i, r["id"].String(), "1")
		}
	}
}
