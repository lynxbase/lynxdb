package storage

import (
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment"
)

func TestFieldCatalog_SegmentStats(t *testing.T) {
	fc := NewFieldCatalog()

	stats := []segment.ColumnStats{
		{Name: "status", MinValue: "200", MaxValue: "500", Count: 90, NullCount: 10},
		{Name: "method", MinValue: "GET", MaxValue: "POST", Count: 100, NullCount: 0},
		{Name: "_time", MinValue: "1700000000", MaxValue: "1700003600", Count: 100, NullCount: 0},
	}
	fc.AddSegmentStats(stats, 100)

	fields := fc.Build()
	if len(fields) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(fields))
	}

	// Check coverage: status = 90% (90/100), method = 100% (100/100)
	seen := make(map[string]bool)
	for _, f := range fields {
		seen[f.Name] = true

		switch f.Name {
		case "status":
			if f.Coverage != 90 {
				t.Errorf("status coverage: expected 90, got %.1f", f.Coverage)
			}
			if f.Type != "int" {
				t.Errorf("status type: expected int, got %s", f.Type)
			}
		case "method":
			if f.Coverage != 100 {
				t.Errorf("method coverage: expected 100, got %.1f", f.Coverage)
			}
			if f.Type != "string" {
				t.Errorf("method type: expected string, got %s", f.Type)
			}
		case "_time":
			if f.Type != "datetime" {
				t.Errorf("_time type: expected datetime, got %s", f.Type)
			}
		default:
			t.Errorf("unexpected field: %q", f.Name)
		}
	}

	for _, expected := range []string{"status", "method", "_time"} {
		if !seen[expected] {
			t.Errorf("expected field %q not found in results", expected)
		}
	}
}

func TestFieldCatalog_Events(t *testing.T) {
	fc := NewFieldCatalog()

	events := []*event.Event{
		{Fields: map[string]event.Value{
			"status": event.IntValue(200),
			"source": event.StringValue("nginx"),
		}},
		{Fields: map[string]event.Value{
			"status": event.IntValue(404),
			"source": event.StringValue("nginx"),
		}},
		{Fields: map[string]event.Value{
			"status": event.IntValue(200),
			"source": event.StringValue("api"),
		}},
	}

	fc.AddEvents(events)
	fields := fc.Build()

	// 3 fields: "status" and "source" from Fields map, plus "index" builtin
	// (always present, defaults to "main" when event.Index is empty).
	if len(fields) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(fields))
	}

	for _, f := range fields {
		if f.Coverage != 100 {
			t.Errorf("%s coverage: expected 100, got %.1f", f.Name, f.Coverage)
		}
	}

	// Check top values.
	for _, f := range fields {
		if f.Name == "status" && len(f.TopValues) == 0 {
			t.Error("expected top values for status")
		}
		if f.Name == "status" && len(f.TopValues) > 0 {
			if f.TopValues[0].Value != "200" {
				t.Errorf("expected top value 200, got %s", f.TopValues[0].Value)
			}
			if f.TopValues[0].Count != 2 {
				t.Errorf("expected count 2, got %d", f.TopValues[0].Count)
			}
		}
	}
}

func TestFieldCatalog_Mixed(t *testing.T) {
	fc := NewFieldCatalog()

	// Add segment stats first.
	stats := []segment.ColumnStats{
		{Name: "status", MinValue: "200", MaxValue: "500", Count: 50, NullCount: 0},
	}
	fc.AddSegmentStats(stats, 50)

	// Add events.
	events := []*event.Event{
		{Fields: map[string]event.Value{"status": event.IntValue(200)}},
		{Fields: map[string]event.Value{"status": event.IntValue(500)}},
	}
	fc.AddEvents(events)

	fields := fc.Build()

	// 2 fields: "status" from segment+events, plus "index" builtin
	// (always present, defaults to "main" when event.Index is empty).
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(fields))
	}

	fieldMap := make(map[string]FieldInfo)
	for _, f := range fields {
		fieldMap[f.Name] = f
	}

	f, ok := fieldMap["status"]
	if !ok {
		t.Fatal("expected 'status' field in catalog")
	}
	if f.TotalCount != 52 { // 50 from segment + 2 from events
		t.Errorf("expected count 52, got %d", f.TotalCount)
	}
	// Coverage: 52/52 = 100% (52 events total, all have status).
	if f.Coverage != 100 {
		t.Errorf("expected 100%% coverage, got %.1f", f.Coverage)
	}

	// "index" builtin appears from the 2 AddEvents events (defaults to "main").
	idxF, ok := fieldMap["index"]
	if !ok {
		t.Fatal("expected 'index' field in catalog")
	}
	if idxF.TotalCount != 2 {
		t.Errorf("index total_count: expected 2, got %d", idxF.TotalCount)
	}
}

func TestFieldCatalog_BuiltinFields(t *testing.T) {
	fc := NewFieldCatalog()

	now := time.Now()
	events := []*event.Event{
		{
			Time:   now,
			Raw:    `level=info msg="request handled"`,
			Source: "nginx",
			Fields: map[string]event.Value{
				"level": event.StringValue("info"),
			},
		},
		{
			Time:   now.Add(time.Second),
			Raw:    `level=error msg="connection refused"`,
			Source: "nginx",
			Fields: map[string]event.Value{
				"level": event.StringValue("error"),
			},
		},
		{
			Time:   now.Add(2 * time.Second),
			Raw:    `level=info msg="ok"`,
			Source: "api",
			Fields: map[string]event.Value{
				"level": event.StringValue("info"),
			},
		},
	}

	fc.AddEvents(events)
	fields := fc.Build()

	// Should have: _time, _raw, _source, index, level (5 fields).
	// "index" is the physical partition key (defaults to "main" when unset).
	// _sourcetype, host are empty → excluded.
	fieldMap := make(map[string]FieldInfo)
	for _, f := range fields {
		fieldMap[f.Name] = f
	}

	for _, want := range []struct {
		name     string
		typ      string
		coverage float64
	}{
		{"_time", "datetime", 100},
		{"_raw", "string", 100},
		{"_source", "string", 100},
		{"index", "string", 100}, // physical partition key, defaults to "main"
		{"level", "string", 100},
	} {
		f, ok := fieldMap[want.name]
		if !ok {
			t.Errorf("expected field %q not found", want.name)

			continue
		}
		if f.Type != want.typ {
			t.Errorf("%s: type = %q, want %q", want.name, f.Type, want.typ)
		}
		if f.Coverage != want.coverage {
			t.Errorf("%s: coverage = %.1f, want %.1f", want.name, f.Coverage, want.coverage)
		}
	}

	// _source should have top values: nginx(2), api(1).
	src, ok := fieldMap["_source"]
	if !ok {
		t.Fatal("_source not found")
	}
	if len(src.TopValues) < 2 {
		t.Fatalf("_source: expected >=2 top values, got %d", len(src.TopValues))
	}
	if src.TopValues[0].Value != "nginx" || src.TopValues[0].Count != 2 {
		t.Errorf("_source top[0]: got %s(%d), want nginx(2)", src.TopValues[0].Value, src.TopValues[0].Count)
	}

	// Verify empty built-in fields are excluded.
	// "index" is always present (defaults to "main"); _sourcetype and host are empty.
	for _, absent := range []string{"_sourcetype", "host"} {
		if _, found := fieldMap[absent]; found {
			t.Errorf("field %q should not be present (empty on all events)", absent)
		}
	}
}

func TestFieldCatalog_SegmentStatsBuiltinTypes(t *testing.T) {
	fc := NewFieldCatalog()

	// Simulate stats from a real segment with all built-in columns.
	stats := []segment.ColumnStats{
		{Name: "_time", MinValue: "1700000000000000000", MaxValue: "1700003600000000000", Count: 500, NullCount: 0},
		{Name: "_raw", MinValue: "", MaxValue: "", Count: 500, NullCount: 0},
		{Name: "_source", MinValue: "api", MaxValue: "nginx", Count: 500, NullCount: 0},
		{Name: "status", MinValue: "200", MaxValue: "500", Count: 450, NullCount: 50},
	}
	fc.AddSegmentStats(stats, 500)

	fields := fc.Build()
	fieldMap := make(map[string]FieldInfo)
	for _, f := range fields {
		fieldMap[f.Name] = f
	}

	// _time should be "datetime", not "int".
	if f, ok := fieldMap["_time"]; ok {
		if f.Type != "datetime" {
			t.Errorf("_time type: got %q, want %q", f.Type, "datetime")
		}
	} else {
		t.Error("_time not found in catalog")
	}

	// _raw should be "string" (from override), even with empty min/max.
	if f, ok := fieldMap["_raw"]; ok {
		if f.Type != "string" {
			t.Errorf("_raw type: got %q, want %q", f.Type, "string")
		}
	} else {
		t.Error("_raw not found in catalog")
	}

	// _source should be "string".
	if f, ok := fieldMap["_source"]; ok {
		if f.Type != "string" {
			t.Errorf("_source type: got %q, want %q", f.Type, "string")
		}
	} else {
		t.Error("_source not found in catalog")
	}

	// status (user-defined) should still use inference → "int".
	if f, ok := fieldMap["status"]; ok {
		if f.Type != "int" {
			t.Errorf("status type: got %q, want %q", f.Type, "int")
		}
		if f.Coverage != 90 {
			t.Errorf("status coverage: got %.1f, want 90", f.Coverage)
		}
	} else {
		t.Error("status not found in catalog")
	}
}

func TestFieldCatalog_IndexUsesPhysicalPartition(t *testing.T) {
	// Verify that the "index" built-in field uses e.Index (the physical
	// partition key), NOT e.Source. Segments are stored under
	// segments/hot/<INDEX_NAME>/, so "index" must reflect that.
	fc := NewFieldCatalog()

	events := []*event.Event{
		{
			Time:   time.Now(),
			Raw:    "test event",
			Source: "nginx",
			Index:  "web-logs", // physical partition
			Fields: map[string]event.Value{},
		},
		{
			Time:   time.Now(),
			Raw:    "test event 2",
			Source: "postgres",
			Index:  "db-logs", // different partition
			Fields: map[string]event.Value{},
		},
		{
			Time:   time.Now(),
			Raw:    "test event 3",
			Source: "redis",
			// Index empty → defaults to "main"
			Fields: map[string]event.Value{},
		},
	}

	fc.AddEvents(events)
	fields := fc.Build()

	fieldMap := make(map[string]FieldInfo)
	for _, f := range fields {
		fieldMap[f.Name] = f
	}

	// "index" field should be present with 100% coverage.
	idxField, ok := fieldMap["index"]
	if !ok {
		t.Fatal("expected 'index' field in catalog")
	}
	if idxField.Coverage != 100 {
		t.Errorf("index coverage: expected 100, got %.1f", idxField.Coverage)
	}
	if idxField.TotalCount != 3 {
		t.Errorf("index total_count: expected 3, got %d", idxField.TotalCount)
	}

	// Top values should reflect Index values (physical partition names),
	// NOT Source values.
	if len(idxField.TopValues) < 3 {
		t.Fatalf("expected >=3 top values for index, got %d", len(idxField.TopValues))
	}
	topVals := make(map[string]bool)
	for _, tv := range idxField.TopValues {
		topVals[tv.Value] = true
	}
	if !topVals["web-logs"] {
		t.Error("expected 'web-logs' in index top values")
	}
	if !topVals["db-logs"] {
		t.Error("expected 'db-logs' in index top values")
	}
	if !topVals["main"] {
		t.Error("expected 'main' in index top values (default for empty Index)")
	}

	// _source should have Source values (nginx, postgres, redis), NOT Index values.
	srcField, ok := fieldMap["_source"]
	if !ok {
		t.Fatal("expected '_source' field in catalog")
	}
	if len(srcField.TopValues) < 3 {
		t.Fatalf("_source: expected >=3 top values, got %d", len(srcField.TopValues))
	}
	srcVals := make(map[string]bool)
	for _, tv := range srcField.TopValues {
		srcVals[tv.Value] = true
	}
	if !srcVals["nginx"] {
		t.Error("expected 'nginx' in _source top values")
	}
	if !srcVals["postgres"] {
		t.Error("expected 'postgres' in _source top values")
	}
	if !srcVals["redis"] {
		t.Error("expected 'redis' in _source top values")
	}
}

func TestFieldCatalog_Empty(t *testing.T) {
	fc := NewFieldCatalog()
	fields := fc.Build()
	if len(fields) != 0 {
		t.Errorf("expected 0 fields, got %d", len(fields))
	}
}
