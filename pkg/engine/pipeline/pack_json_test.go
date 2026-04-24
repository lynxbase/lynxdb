package pipeline

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestPackJson_SpecificFields(t *testing.T) {
	batch := NewBatch(0)
	batch.Len = 2
	batch.Columns["level"] = []event.Value{
		event.StringValue("error"),
		event.StringValue("info"),
	}
	batch.Columns["service"] = []event.Value{
		event.StringValue("api"),
		event.StringValue("web"),
	}
	batch.Columns["status"] = []event.Value{
		event.IntValue(500),
		event.IntValue(200),
	}

	child := &staticIterator{batches: []*Batch{batch}}
	iter := NewPackJsonIterator(child, []string{"level", "service"}, "output")

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	result, err := iter.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if result == nil {
		t.Fatal("expected batch, got nil")
	}

	outCol := result.Columns["output"]
	if outCol == nil || len(outCol) < 2 {
		t.Fatal("missing output column")
	}

	// Row 0: level=error, service=api.
	var obj0 map[string]interface{}
	if err := json.Unmarshal([]byte(outCol[0].String()), &obj0); err != nil {
		t.Fatalf("unmarshal row 0: %v", err)
	}
	if obj0["level"] != "error" || obj0["service"] != "api" {
		t.Errorf("row 0: got %v", obj0)
	}
	// Should NOT include status.
	if _, exists := obj0["status"]; exists {
		t.Errorf("row 0: should not contain 'status'")
	}

	// Row 1: level=info, service=web.
	var obj1 map[string]interface{}
	if err := json.Unmarshal([]byte(outCol[1].String()), &obj1); err != nil {
		t.Fatalf("unmarshal row 1: %v", err)
	}
	if obj1["level"] != "info" || obj1["service"] != "web" {
		t.Errorf("row 1: got %v", obj1)
	}
}

func TestPackJson_AllFields(t *testing.T) {
	batch := NewBatch(0)
	batch.Len = 1
	batch.Columns["level"] = []event.Value{event.StringValue("error")}
	batch.Columns["service"] = []event.Value{event.StringValue("api")}
	batch.Columns["_time"] = []event.Value{event.StringValue("2026-01-01T00:00:00Z")}
	batch.Columns["_raw"] = []event.Value{event.StringValue("raw log line")}

	child := &staticIterator{batches: []*Batch{batch}}
	iter := NewPackJsonIterator(child, nil, "output")

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	result, err := iter.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}

	outCol := result.Columns["output"]
	if outCol == nil || len(outCol) < 1 {
		t.Fatal("missing output column")
	}

	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(outCol[0].String()), &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Should include non-internal fields.
	if obj["level"] != "error" || obj["service"] != "api" {
		t.Errorf("all fields: got %v", obj)
	}
	// Should NOT include internal fields.
	if _, exists := obj["_time"]; exists {
		t.Error("should not contain _time")
	}
	if _, exists := obj["_raw"]; exists {
		t.Error("should not contain _raw")
	}
}

func TestPackJson_NullFields(t *testing.T) {
	batch := NewBatch(0)
	batch.Len = 1
	batch.Columns["level"] = []event.Value{event.StringValue("error")}
	batch.Columns["service"] = []event.Value{event.NullValue()}

	child := &staticIterator{batches: []*Batch{batch}}
	iter := NewPackJsonIterator(child, []string{"level", "service"}, "output")

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	result, err := iter.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}

	outCol := result.Columns["output"]
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(outCol[0].String()), &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Null fields should be excluded.
	if _, exists := obj["service"]; exists {
		t.Error("null field 'service' should be excluded")
	}
	if obj["level"] != "error" {
		t.Errorf("level: got %v, want error", obj["level"])
	}
}

func TestPackJson_TypePreservation(t *testing.T) {
	batch := NewBatch(0)
	batch.Len = 1
	batch.Columns["count"] = []event.Value{event.IntValue(42)}
	batch.Columns["ratio"] = []event.Value{event.FloatValue(3.14)}
	batch.Columns["active"] = []event.Value{event.BoolValue(true)}
	batch.Columns["name"] = []event.Value{event.StringValue("test")}

	child := &staticIterator{batches: []*Batch{batch}}
	iter := NewPackJsonIterator(child, []string{"count", "ratio", "active", "name"}, "output")

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	result, err := iter.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}

	outCol := result.Columns["output"]
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(outCol[0].String()), &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// int should be preserved as number.
	if count, ok := obj["count"].(float64); !ok || count != 42 {
		t.Errorf("count: got %v (%T), want 42", obj["count"], obj["count"])
	}
	// float should be preserved.
	if ratio, ok := obj["ratio"].(float64); !ok || ratio < 3.13 || ratio > 3.15 {
		t.Errorf("ratio: got %v", obj["ratio"])
	}
	// bool should be preserved.
	if active, ok := obj["active"].(bool); !ok || !active {
		t.Errorf("active: got %v", obj["active"])
	}
	// string should be preserved.
	if name, ok := obj["name"].(string); !ok || name != "test" {
		t.Errorf("name: got %v", obj["name"])
	}
}

// staticIterator is defined in rex_test.go and shared across pipeline tests.
