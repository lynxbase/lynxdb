package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestGlimpse_BasicFields(t *testing.T) {
	events := makeGlimpseEvents([]map[string]interface{}{
		{"level": "error", "msg": "fail", "count": 10},
		{"level": "info", "msg": "ok", "count": 5},
		{"level": "warn", "msg": "slow", "count": 8},
	})

	op := NewGlimpseIterator(NewScanIterator(events, 1024))
	ctx := context.Background()
	if err := op.Init(ctx); err != nil {
		t.Fatal(err)
	}

	batch, err := op.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil {
		t.Fatal("expected batch, got nil")
	}
	if batch.Len != 1 {
		t.Fatalf("expected 1 row, got %d", batch.Len)
	}

	raw := batch.Columns["_raw"][0].AsString()
	if !strings.Contains(raw, "level") {
		t.Error("expected 'level' in glimpse output")
	}
	if !strings.Contains(raw, "msg") {
		t.Error("expected 'msg' in glimpse output")
	}
	if !strings.Contains(raw, "count") {
		t.Error("expected 'count' in glimpse output")
	}
	if !strings.Contains(raw, "3 events sampled") {
		t.Errorf("expected '3 events sampled' in output, got: %s", raw)
	}
}

func TestGlimpse_NumericStats(t *testing.T) {
	events := makeGlimpseEvents([]map[string]interface{}{
		{"duration": 10},
		{"duration": 20},
		{"duration": 30},
		{"duration": 40},
		{"duration": 50},
		{"duration": 100},
		{"duration": 200},
		{"duration": 500},
		{"duration": 1000},
	})

	op := NewGlimpseIterator(NewScanIterator(events, 1024))
	ctx := context.Background()
	if err := op.Init(ctx); err != nil {
		t.Fatal(err)
	}

	batch, err := op.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	raw := batch.Columns["_raw"][0].AsString()
	if !strings.Contains(raw, "min=") {
		t.Errorf("expected min= in numeric stats, got: %s", raw)
	}
	if !strings.Contains(raw, "max=") {
		t.Errorf("expected max= in numeric stats, got: %s", raw)
	}
	if !strings.Contains(raw, "p50=") {
		t.Errorf("expected p50= in numeric stats, got: %s", raw)
	}
	if !strings.Contains(raw, "p99=") {
		t.Errorf("expected p99= in numeric stats, got: %s", raw)
	}
}

func TestGlimpse_NullCoverage(t *testing.T) {
	events := makeGlimpseEvents([]map[string]interface{}{
		{"level": "error", "trace_id": "abc"},
		{"level": "info"},
		{"level": "warn", "trace_id": "def"},
	})

	op := NewGlimpseIterator(NewScanIterator(events, 1024))
	ctx := context.Background()
	if err := op.Init(ctx); err != nil {
		t.Fatal(err)
	}

	batch, err := op.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	raw := batch.Columns["_raw"][0].AsString()
	if !strings.Contains(raw, "trace_id") {
		t.Error("expected 'trace_id' in output")
	}
}

func TestGlimpse_EmptyInput(t *testing.T) {
	events := makeGlimpseEvents([]map[string]interface{}{})

	op := NewGlimpseIterator(NewScanIterator(events, 1024))
	ctx := context.Background()
	if err := op.Init(ctx); err != nil {
		t.Fatal(err)
	}

	batch, err := op.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil {
		t.Fatal("expected batch even for empty input")
	}
	raw := batch.Columns["_raw"][0].AsString()
	if !strings.Contains(raw, "0 events sampled") {
		t.Errorf("expected '0 events sampled' for empty input, got: %s", raw)
	}
}

func TestGlimpse_FormatHint(t *testing.T) {
	events := makeGlimpseEvents([]map[string]interface{}{
		{"level": "error"},
	})

	op := NewGlimpseIterator(NewScanIterator(events, 1024))
	op.formatHint = "JSON"
	ctx := context.Background()
	if err := op.Init(ctx); err != nil {
		t.Fatal(err)
	}

	batch, err := op.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	raw := batch.Columns["_raw"][0].AsString()
	if !strings.Contains(raw, "Format: JSON") {
		t.Errorf("expected 'Format: JSON' in footer, got: %s", raw)
	}
}

func TestGlimpse_ConstantNumericField(t *testing.T) {
	events := makeGlimpseEvents([]map[string]interface{}{
		{"status": 200},
		{"status": 200},
		{"status": 200},
	})

	op := NewGlimpseIterator(NewScanIterator(events, 1024))
	ctx := context.Background()
	if err := op.Init(ctx); err != nil {
		t.Fatal(err)
	}

	batch, err := op.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	raw := batch.Columns["_raw"][0].AsString()
	if !strings.Contains(raw, "const 200") {
		t.Errorf("expected 'const 200' for constant field, got: %s", raw)
	}
}

func TestGlimpsePercentile(t *testing.T) {
	vals := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	p50 := glimpsePercentile(vals, 0.50)
	if p50 < 5 || p50 > 6 {
		t.Errorf("expected p50 around 5.5, got %g", p50)
	}
	p99 := glimpsePercentile(vals, 0.99)
	if p99 < 9 || p99 > 11 {
		t.Errorf("expected p99 around 10, got %g", p99)
	}
}

// makeGlimpseEvents creates events from maps for glimpse testing.
func makeGlimpseEvents(data []map[string]interface{}) []*event.Event {
	events := make([]*event.Event, len(data))
	now := time.Now()
	for i, row := range data {
		fields := make(map[string]event.Value)
		for k, v := range row {
			switch val := v.(type) {
			case string:
				fields[k] = event.StringValue(val)
			case int:
				fields[k] = event.IntValue(int64(val))
			case float64:
				fields[k] = event.FloatValue(val)
			case bool:
				fields[k] = event.BoolValue(val)
			}
		}
		events[i] = &event.Event{
			Time:   now,
			Fields: fields,
			Raw:    "",
			Source: "test",
		}
	}
	return events
}
