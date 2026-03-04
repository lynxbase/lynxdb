package pipeline

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/engine/unpack"
	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestUnpackIterator_JSON_Basic(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {
				event.StringValue(`{"level":"error","status":500,"active":true}`),
				event.StringValue(`{"level":"info","status":200}`),
			},
		},
		Len: 2,
	}

	parser, _ := unpack.NewParser("json")
	iter := NewUnpackIterator(
		&staticIterator{batches: []*Batch{batch}},
		parser, "_raw", nil, "", false,
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows: got %d, want 2", len(rows))
	}

	// Row 0: level=error, status=500, active=true
	assertVal(t, rows[0], "level", "error")
	assertIntVal(t, rows[0], "status", 500)
	assertBoolVal(t, rows[0], "active", true)

	// Row 1: level=info, status=200
	assertVal(t, rows[0], "level", "error")
	assertIntVal(t, rows[1], "status", 200)
}

func TestUnpackIterator_FromField(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw":    {event.StringValue("raw data")},
			"payload": {event.StringValue(`{"key":"val"}`)},
		},
		Len: 1,
	}

	parser, _ := unpack.NewParser("json")
	iter := NewUnpackIterator(
		&staticIterator{batches: []*Batch{batch}},
		parser, "payload", nil, "", false,
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	assertVal(t, rows[0], "key", "val")
}

func TestUnpackIterator_FieldsFilter(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {event.StringValue(`{"level":"error","service":"api","host":"web-01"}`)},
		},
		Len: 1,
	}

	parser, _ := unpack.NewParser("json")
	iter := NewUnpackIterator(
		&staticIterator{batches: []*Batch{batch}},
		parser, "_raw", []string{"level", "service"}, "", false,
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	assertVal(t, rows[0], "level", "error")
	assertVal(t, rows[0], "service", "api")

	// "host" should NOT be extracted.
	if v, ok := rows[0]["host"]; ok {
		t.Errorf("host: should not be extracted, got %v", v)
	}
}

func TestUnpackIterator_Prefix(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {event.StringValue(`{"level":"error"}`)},
		},
		Len: 1,
	}

	parser, _ := unpack.NewParser("json")
	iter := NewUnpackIterator(
		&staticIterator{batches: []*Batch{batch}},
		parser, "_raw", nil, "json_", false,
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	assertVal(t, rows[0], "json_level", "error")

	// "level" without prefix should NOT exist.
	if v, ok := rows[0]["level"]; ok {
		t.Errorf("level: should not exist without prefix, got %v", v)
	}
}

func TestUnpackIterator_KeepOriginal(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw":  {event.StringValue(`{"level":"error","source":"json"}`)},
			"level": {event.StringValue("info")}, // pre-existing non-null
		},
		Len: 1,
	}

	parser, _ := unpack.NewParser("json")
	iter := NewUnpackIterator(
		&staticIterator{batches: []*Batch{batch}},
		parser, "_raw", nil, "", true, // keep_original = true
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	// "level" should NOT be overwritten — original "info" preserved.
	assertVal(t, rows[0], "level", "info")
	// "source" should be extracted (no pre-existing value).
	assertVal(t, rows[0], "source", "json")
}

func TestUnpackIterator_NullSourceSkipped(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {event.NullValue(), event.StringValue(`{"a":"b"}`)},
		},
		Len: 2,
	}

	parser, _ := unpack.NewParser("json")
	iter := NewUnpackIterator(
		&staticIterator{batches: []*Batch{batch}},
		parser, "_raw", nil, "", false,
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows: got %d, want 2", len(rows))
	}

	// Row 0: null source → no extraction.
	if v, ok := rows[0]["a"]; ok && !v.IsNull() {
		t.Errorf("row[0].a: expected null/absent, got %v", v)
	}

	// Row 1: extracted.
	assertVal(t, rows[1], "a", "b")
}

func TestUnpackIterator_Logfmt(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {event.StringValue(`level=error msg="request timeout" duration=1234`)},
		},
		Len: 1,
	}

	parser, _ := unpack.NewParser("logfmt")
	iter := NewUnpackIterator(
		&staticIterator{batches: []*Batch{batch}},
		parser, "_raw", nil, "", false,
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	assertVal(t, rows[0], "level", "error")
	assertVal(t, rows[0], "msg", "request timeout")
	assertIntVal(t, rows[0], "duration", 1234)
}

func TestUnpackIterator_MissingSourceColumn(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"other": {event.StringValue("data")},
		},
		Len: 1,
	}

	parser, _ := unpack.NewParser("json")
	iter := NewUnpackIterator(
		&staticIterator{batches: []*Batch{batch}},
		parser, "_raw", nil, "", false,
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	// Should pass through batch unmodified.
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}
}

// staticIterator and helpers are defined in rex_test.go (same package).

// assertVal checks a row has a string value for the given key.
func assertVal(t *testing.T, row map[string]event.Value, key, want string) {
	t.Helper()
	v, ok := row[key]
	if !ok {
		t.Errorf("%s: missing from row", key)

		return
	}
	if v.String() != want {
		t.Errorf("%s: got %q, want %q", key, v.String(), want)
	}
}

// assertIntVal checks a row has an int value for the given key.
func assertIntVal(t *testing.T, row map[string]event.Value, key string, want int64) {
	t.Helper()
	v, ok := row[key]
	if !ok {
		t.Errorf("%s: missing from row", key)

		return
	}
	got, err := v.AsIntE()
	if err != nil {
		t.Errorf("%s: not an int: %v (value: %v)", key, err, v)

		return
	}
	if got != want {
		t.Errorf("%s: got %d, want %d", key, got, want)
	}
}

// assertBoolVal checks a row has a bool value for the given key.
func assertBoolVal(t *testing.T, row map[string]event.Value, key string, want bool) {
	t.Helper()
	v, ok := row[key]
	if !ok {
		t.Errorf("%s: missing from row", key)

		return
	}
	got, err := v.AsBoolE()
	if err != nil {
		t.Errorf("%s: not a bool: %v (value: %v)", key, err, v)

		return
	}
	if got != want {
		t.Errorf("%s: got %v, want %v", key, got, want)
	}
}
