package pipeline

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestTraceIterator_BasicTrace(t *testing.T) {
	rows := []map[string]event.Value{
		{"_time": event.IntValue(1), "trace_id": event.StringValue("t1"), "span_id": event.StringValue("s1"), "parent_span_id": event.StringValue(""), "service": event.StringValue("api"), "operation": event.StringValue("GET /orders"), "duration_ms": event.IntValue(234)},
		{"_time": event.IntValue(2), "trace_id": event.StringValue("t1"), "span_id": event.StringValue("s2"), "parent_span_id": event.StringValue("s1"), "service": event.StringValue("auth"), "operation": event.StringValue("validate"), "duration_ms": event.IntValue(45)},
		{"_time": event.IntValue(3), "trace_id": event.StringValue("t1"), "span_id": event.StringValue("s3"), "parent_span_id": event.StringValue("s1"), "service": event.StringValue("db"), "operation": event.StringValue("SELECT"), "duration_ms": event.IntValue(60)},
	}

	iter := NewTraceIterator(NewRowScanIterator(rows, 1024), "trace_id", "span_id", "parent_span_id")
	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil {
		t.Fatal("expected batch, got nil")
	}
	if batch.Len != 3 {
		t.Fatalf("expected 3 rows, got %d", batch.Len)
	}

	// Root span should have depth 0.
	root := batch.Row(0)
	if root["_span_depth"].AsInt() != 0 {
		t.Errorf("root depth: got %d, want 0", root["_span_depth"].AsInt())
	}

	// Children should have depth 1.
	child1 := batch.Row(1)
	if child1["_span_depth"].AsInt() != 1 {
		t.Errorf("child1 depth: got %d, want 1", child1["_span_depth"].AsInt())
	}

	child2 := batch.Row(2)
	if child2["_span_depth"].AsInt() != 1 {
		t.Errorf("child2 depth: got %d, want 1", child2["_span_depth"].AsInt())
	}
}

func TestTraceIterator_MultipleTraces(t *testing.T) {
	rows := []map[string]event.Value{
		{"_time": event.IntValue(1), "trace_id": event.StringValue("t1"), "span_id": event.StringValue("s1"), "parent_span_id": event.StringValue(""), "service": event.StringValue("api")},
		{"_time": event.IntValue(2), "trace_id": event.StringValue("t2"), "span_id": event.StringValue("s2"), "parent_span_id": event.StringValue(""), "service": event.StringValue("web")},
		{"_time": event.IntValue(3), "trace_id": event.StringValue("t1"), "span_id": event.StringValue("s3"), "parent_span_id": event.StringValue("s1"), "service": event.StringValue("db")},
	}

	iter := NewTraceIterator(NewRowScanIterator(rows, 1024), "trace_id", "span_id", "parent_span_id")
	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil {
		t.Fatal("expected batch, got nil")
	}
	if batch.Len != 3 {
		t.Fatalf("expected 3 rows, got %d", batch.Len)
	}
}

func TestTraceIterator_NoParent(t *testing.T) {
	// All spans are roots (no parent_span_id).
	rows := []map[string]event.Value{
		{"_time": event.IntValue(1), "trace_id": event.StringValue("t1"), "span_id": event.StringValue("s1"), "parent_span_id": event.StringValue(""), "service": event.StringValue("api")},
		{"_time": event.IntValue(2), "trace_id": event.StringValue("t1"), "span_id": event.StringValue("s2"), "parent_span_id": event.StringValue(""), "service": event.StringValue("web")},
	}

	iter := NewTraceIterator(NewRowScanIterator(rows, 1024), "trace_id", "span_id", "parent_span_id")
	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil {
		t.Fatal("expected batch, got nil")
	}

	// Both should be roots with depth 0.
	for i := 0; i < batch.Len; i++ {
		row := batch.Row(i)
		if row["_span_depth"].AsInt() != 0 {
			t.Errorf("row %d depth: got %d, want 0", i, row["_span_depth"].AsInt())
		}
	}
}

func TestTraceIterator_DepthCalculation(t *testing.T) {
	rows := []map[string]event.Value{
		{"_time": event.IntValue(1), "trace_id": event.StringValue("t1"), "span_id": event.StringValue("s1"), "parent_span_id": event.StringValue(""), "service": event.StringValue("api"), "operation": event.StringValue("root")},
		{"_time": event.IntValue(2), "trace_id": event.StringValue("t1"), "span_id": event.StringValue("s2"), "parent_span_id": event.StringValue("s1"), "service": event.StringValue("svc"), "operation": event.StringValue("child1")},
		{"_time": event.IntValue(3), "trace_id": event.StringValue("t1"), "span_id": event.StringValue("s3"), "parent_span_id": event.StringValue("s2"), "service": event.StringValue("db"), "operation": event.StringValue("grandchild")},
	}

	iter := NewTraceIterator(NewRowScanIterator(rows, 1024), "trace_id", "span_id", "parent_span_id")
	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil {
		t.Fatal("expected batch, got nil")
	}

	// Verify depth: 0, 1, 2.
	expected := []int64{0, 1, 2}
	for i, exp := range expected {
		row := batch.Row(i)
		if row["_span_depth"].AsInt() != exp {
			t.Errorf("row %d depth: got %d, want %d", i, row["_span_depth"].AsInt(), exp)
		}
	}
}

func TestTraceIterator_EmptyInput(t *testing.T) {
	rows := []map[string]event.Value{}

	iter := NewTraceIterator(NewRowScanIterator(rows, 1024), "trace_id", "span_id", "parent_span_id")
	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch != nil && batch.Len > 0 {
		t.Errorf("expected empty batch, got %d rows", batch.Len)
	}
}
