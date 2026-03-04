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
	iter := NewUnrollIterator(child, "items", 1024)
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
	iter := NewUnrollIterator(child, "tags", 1024)
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
	iter := NewUnrollIterator(child, "data", 1024)
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
	iter := NewUnrollIterator(child, "items", 1024)
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
	iter := NewUnrollIterator(child, "items", 1024)
	results, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	// Empty array: row passes through unchanged.
	if len(results) != 1 {
		t.Fatalf("expected 1 row, got %d", len(results))
	}
}
