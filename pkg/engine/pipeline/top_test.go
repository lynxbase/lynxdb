package pipeline

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestTopIterator_SortStability(t *testing.T) {
	// Create rows where multiple values have the same count.
	// "banana", "apple", "cherry" each appear twice.
	// With deterministic sort, equal-count entries should be ordered alphabetically.
	rows := []map[string]event.Value{
		{"fruit": event.StringValue("banana")},
		{"fruit": event.StringValue("cherry")},
		{"fruit": event.StringValue("apple")},
		{"fruit": event.StringValue("banana")},
		{"fruit": event.StringValue("cherry")},
		{"fruit": event.StringValue("apple")},
	}

	child := NewRowScanIterator(rows, 64)
	top := NewTopIterator(child, "fruit", "", 3, false, 64)

	ctx := context.Background()
	if err := top.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, top)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}

	// All have count=2, so should be sorted alphabetically: apple, banana, cherry.
	expected := []string{"apple", "banana", "cherry"}
	for i, row := range result {
		got := row["fruit"].String()
		if got != expected[i] {
			t.Errorf("row %d: expected %q, got %q", i, expected[i], got)
		}
	}
}

func TestTopIterator_SortStabilityRare(t *testing.T) {
	// Same as above but with ascending=true (rare command).
	rows := []map[string]event.Value{
		{"fruit": event.StringValue("banana")},
		{"fruit": event.StringValue("cherry")},
		{"fruit": event.StringValue("apple")},
		{"fruit": event.StringValue("banana")},
		{"fruit": event.StringValue("cherry")},
		{"fruit": event.StringValue("apple")},
	}

	child := NewRowScanIterator(rows, 64)
	top := NewTopIterator(child, "fruit", "", 3, true, 64)

	ctx := context.Background()
	if err := top.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, top)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}

	// All have count=2, ascending order — still alphabetical for ties.
	expected := []string{"apple", "banana", "cherry"}
	for i, row := range result {
		got := row["fruit"].String()
		if got != expected[i] {
			t.Errorf("row %d: expected %q, got %q", i, expected[i], got)
		}
	}
}

func TestTopIterator_SchemaWithoutByField(t *testing.T) {
	child := NewRowScanIterator(nil, 64)
	top := NewTopIterator(child, "status", "", 10, false, 64)

	schema := top.Schema()
	if len(schema) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(schema))
	}

	expected := []string{"status", "count", "percent"}
	for i, f := range schema {
		if f.Name != expected[i] {
			t.Errorf("schema[%d] = %q, want %q", i, f.Name, expected[i])
		}
	}
}

func TestTopIterator_SchemaWithByField(t *testing.T) {
	child := NewRowScanIterator(nil, 64)
	top := NewTopIterator(child, "status", "host", 10, false, 64)

	schema := top.Schema()
	if len(schema) != 4 {
		t.Fatalf("expected 4 fields (including byField), got %d", len(schema))
	}

	expected := []string{"status", "count", "percent", "host"}
	for i, f := range schema {
		if f.Name != expected[i] {
			t.Errorf("schema[%d] = %q, want %q", i, f.Name, expected[i])
		}
	}
}

func TestTopIterator_DifferentCounts(t *testing.T) {
	// Verify primary sort by count still works with secondary alphabetical.
	rows := []map[string]event.Value{
		{"method": event.StringValue("GET")},
		{"method": event.StringValue("GET")},
		{"method": event.StringValue("GET")},
		{"method": event.StringValue("POST")},
		{"method": event.StringValue("POST")},
		{"method": event.StringValue("DELETE")},
	}

	child := NewRowScanIterator(rows, 64)
	top := NewTopIterator(child, "method", "", 3, false, 64)

	ctx := context.Background()
	if err := top.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, top)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}

	// GET=3, POST=2, DELETE=1 — sorted by count desc.
	expected := []string{"GET", "POST", "DELETE"}
	for i, row := range result {
		got := row["method"].String()
		if got != expected[i] {
			t.Errorf("row %d: expected %q, got %q", i, expected[i], got)
		}
	}
}
