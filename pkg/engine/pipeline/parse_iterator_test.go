package pipeline

import (
	"context"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/engine/unpack"
	"github.com/lynxbase/lynxdb/pkg/event"
)

// Helpers

// mixedFormatBatch returns a batch with JSON, logfmt, and garbage lines.
func mixedFormatBatch() *Batch {
	return &Batch{
		Columns: map[string][]event.Value{
			"_raw": {
				event.StringValue(`{"level":"error","status":500}`),        // row 0: valid JSON
				event.StringValue(`level=info msg="all good" duration=42`), // row 1: valid logfmt
				event.StringValue(`this is total garbage !@#$%`),           // row 2: garbage
				event.StringValue(`{"service":"api","active":true}`),       // row 3: valid JSON
				event.StringValue(`not json { broken`),                     // row 4: broken JSON-ish
			},
		},
		Len: 5,
	}
}

func collectParse(t *testing.T, iter Iterator) []map[string]event.Value {
	t.Helper()
	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	return rows
}

func mustParser(t *testing.T, name string) unpack.FormatParser {
	t.Helper()
	p, err := unpack.NewParser(name)
	if err != nil {
		t.Fatalf("NewParser(%q): %v", name, err)
	}
	return p
}

// Test: on_error=propagate (default)

func TestParseIterator_Propagate_JSONMixed(t *testing.T) {
	batch := mixedFormatBatch()
	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"_raw", nil, "", OnErrorPropagate,
	)

	rows := collectParse(t, iter)
	if len(rows) != 5 {
		t.Fatalf("rows: got %d, want 5", len(rows))
	}

	// Row 0: valid JSON → fields extracted, no _error.
	assertVal(t, rows[0], "level", "error")
	assertIntVal(t, rows[0], "status", 500)
	if v, ok := rows[0]["_error"]; ok && !v.IsNull() {
		t.Errorf("row 0: unexpected non-null _error: %v", v)
	}

	// Row 1: logfmt line → JSON parse fails → _error set.
	if _, ok := rows[1]["_error"]; !ok {
		t.Errorf("row 1: expected _error for logfmt line parsed as JSON")
	}

	// Row 2: garbage → _error set.
	errVal, ok := rows[2]["_error"]
	if !ok {
		t.Errorf("row 2: expected _error for garbage line")
	} else {
		errStr := errVal.String()
		if !strings.HasPrefix(errStr, "parse:json:") {
			t.Errorf("row 2: _error should start with 'parse:json:', got %q", errStr)
		}
	}

	// Row 2: _error_detail should be an object.
	detailVal, ok := rows[2]["_error_detail"]
	if !ok {
		t.Errorf("row 2: expected _error_detail")
	} else if detailVal.Type() != event.FieldTypeObject {
		t.Errorf("row 2: _error_detail type: got %s, want object", detailVal.Type())
	}

	// Row 3: valid JSON → fields extracted.
	assertVal(t, rows[3], "service", "api")
	assertBoolVal(t, rows[3], "active", true)

	// Row 4: broken JSON → _error set.
	if _, ok := rows[4]["_error"]; !ok {
		t.Errorf("row 4: expected _error for broken JSON-ish line")
	}
}

// Test: on_error=null

func TestParseIterator_Null_JSONMixed(t *testing.T) {
	batch := mixedFormatBatch()
	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"_raw", nil, "", OnErrorNull,
	)

	rows := collectParse(t, iter)
	if len(rows) != 5 {
		t.Fatalf("rows: got %d, want 5", len(rows))
	}

	// Row 0: valid JSON → fields extracted.
	assertVal(t, rows[0], "level", "error")

	// Row 2: garbage → no _error, no extracted fields.
	if _, ok := rows[2]["_error"]; ok {
		t.Errorf("row 2: on_error=null should NOT set _error")
	}
	if _, ok := rows[2]["_error_detail"]; ok {
		t.Errorf("row 2: on_error=null should NOT set _error_detail")
	}

	// Extracted fields from row 0 should NOT leak to row 2.
	if v, ok := rows[2]["level"]; ok && !v.IsNull() {
		t.Errorf("row 2: on_error=null should NOT populate extracted fields, got level=%v", v)
	}
}

// Test: on_error=drop

func TestParseIterator_Drop_JSONMixed(t *testing.T) {
	batch := mixedFormatBatch()
	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"_raw", nil, "", OnErrorDrop,
	)

	rows := collectParse(t, iter)
	// Only rows 0 and 3 are valid JSON.
	if len(rows) != 2 {
		t.Fatalf("rows: got %d, want 2 (only valid JSON rows)", len(rows))
	}

	assertVal(t, rows[0], "level", "error")
	assertVal(t, rows[1], "service", "api")
}

// Test: on_error=strict

func TestParseIterator_Strict_JSONMixed(t *testing.T) {
	batch := mixedFormatBatch()
	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"_raw", nil, "", OnErrorStrict,
	)

	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}

	_, err := iter.Next(context.Background())
	if err == nil {
		t.Fatal("expected error from strict mode, got nil")
	}

	errStr := err.Error()
	if !strings.Contains(errStr, "parse:json:") {
		t.Errorf("strict error should mention parse:json:, got %q", errStr)
	}
	// Should include a truncated raw sample.
	if !strings.Contains(errStr, "row") {
		t.Errorf("strict error should mention row number, got %q", errStr)
	}
}

// Test: first_of chain

func TestParseIterator_FirstOf_JSONLogfmt(t *testing.T) {
	batch := mixedFormatBatch()
	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json"), mustParser(t, "logfmt")},
		"_raw", nil, "", OnErrorPropagate,
	)

	rows := collectParse(t, iter)
	if len(rows) != 5 {
		t.Fatalf("rows: got %d, want 5", len(rows))
	}

	// Row 0: JSON succeeds first.
	assertVal(t, rows[0], "level", "error")
	assertIntVal(t, rows[0], "status", 500)

	// Row 1: JSON fails, logfmt succeeds.
	assertVal(t, rows[1], "level", "info")
	assertVal(t, rows[1], "msg", "all good")
	assertIntVal(t, rows[1], "duration", 42)
	if v, ok := rows[1]["_error"]; ok && !v.IsNull() {
		t.Errorf("row 1: logfmt should succeed, no non-null _error expected, got %v", v)
	}

	// Row 2: both fail → _error with stages detail.
	errVal, ok := rows[2]["_error"]
	if !ok {
		t.Fatalf("row 2: expected _error when both formats fail")
	}
	errStr := errVal.String()
	if !strings.Contains(errStr, "first_of") {
		t.Errorf("row 2: _error should mention first_of, got %q", errStr)
	}

	// Row 2: _error_detail should have stages array.
	detail, ok := rows[2]["_error_detail"]
	if !ok {
		t.Fatalf("row 2: expected _error_detail")
	}
	if detail.Type() != event.FieldTypeObject {
		t.Fatalf("row 2: _error_detail type: got %s, want object", detail.Type())
	}
	detailObj, _ := detail.TryAsObject()
	stagesVal, ok := detailObj["stages"]
	if !ok {
		t.Fatalf("row 2: _error_detail should have 'stages' key")
	}
	stagesArr, ok := stagesVal.TryAsArray()
	if !ok {
		t.Fatalf("row 2: stages should be an array")
	}
	if len(stagesArr) != 2 {
		t.Errorf("row 2: stages length: got %d, want 2", len(stagesArr))
	}

	// Row 3: JSON succeeds first.
	assertVal(t, rows[3], "service", "api")
}

// Test: typed captures

func TestParseIterator_TypedCaptures(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {
				event.StringValue(`{"status":"200","dur":"1.5","active":"true","name":"test"}`),
				event.StringValue(`{"status":"abc","dur":"xyz","active":"maybe","name":"ok"}`),
			},
		},
		Len: 2,
	}

	captures := []CaptureSpec{
		{Name: "status", Type: "int"},
		{Name: "dur", Type: "float"},
		{Name: "active", Type: "bool"},
		{Name: "name", Type: "string"},
	}

	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"_raw", captures, "", OnErrorPropagate,
	)

	rows := collectParse(t, iter)
	if len(rows) != 2 {
		t.Fatalf("rows: got %d, want 2", len(rows))
	}

	// Row 0: successful coercion.
	assertIntVal(t, rows[0], "status", 200)
	v, ok := rows[0]["dur"]
	if !ok {
		t.Error("row 0: dur missing")
	} else {
		f, fok := v.TryAsFloat()
		if !fok || f != 1.5 {
			t.Errorf("row 0: dur: got %v, want 1.5", v)
		}
	}
	assertBoolVal(t, rows[0], "active", true)
	assertVal(t, rows[0], "name", "test")

	// Row 1: coercion failures for status, dur, active.
	// status should be null (coercion failed).
	sv, ok := rows[1]["status"]
	if ok && !sv.IsNull() {
		t.Errorf("row 1: status should be null after failed coercion, got %v", sv)
	}

	// _error should be set (propagate mode).
	if _, ok := rows[1]["_error"]; !ok {
		t.Error("row 1: expected _error for coercion failure")
	}

	// name should still succeed.
	assertVal(t, rows[1], "name", "ok")
}

// Test: typed captures with on_error=strict

func TestParseIterator_TypedCaptures_Strict(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {
				event.StringValue(`{"status":"abc"}`),
			},
		},
		Len: 1,
	}

	captures := []CaptureSpec{
		{Name: "status", Type: "int"},
	}

	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"_raw", captures, "", OnErrorStrict,
	)

	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}

	_, err := iter.Next(context.Background())
	if err == nil {
		t.Fatal("expected strict error from coercion failure")
	}
	if !strings.Contains(err.Error(), "coerce") {
		t.Errorf("strict error should mention coerce, got %q", err.Error())
	}
}

// Test: captures act as projection for dynamic formats

func TestParseIterator_CapturesProjection(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {
				event.StringValue(`{"level":"error","status":500,"host":"web-01","extra":"stuff"}`),
			},
		},
		Len: 1,
	}

	// Only capture level and status — host and extra should be filtered out.
	captures := []CaptureSpec{
		{Name: "level", Type: ""},
		{Name: "status", Type: "int"},
	}

	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"_raw", captures, "", OnErrorPropagate,
	)

	rows := collectParse(t, iter)
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}

	assertVal(t, rows[0], "level", "error")
	assertIntVal(t, rows[0], "status", 500)

	// host and extra should NOT be extracted (captures act as projection).
	if v, ok := rows[0]["host"]; ok && !v.IsNull() {
		t.Errorf("host should not be extracted when not in captures, got %v", v)
	}
	if v, ok := rows[0]["extra"]; ok && !v.IsNull() {
		t.Errorf("extra should not be extracted when not in captures, got %v", v)
	}
}

// Test: no-overwrite merge

func TestParseIterator_NoOverwriteMerge(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw":  {event.StringValue(`{"level":"error","source":"json_source"}`)},
			"level": {event.StringValue("info")}, // pre-existing non-null
		},
		Len: 1,
	}

	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"_raw", nil, "", OnErrorPropagate,
	)

	rows := collectParse(t, iter)
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}

	// "level" should NOT be overwritten — original "info" preserved.
	assertVal(t, rows[0], "level", "info")

	// "source" should be extracted (no pre-existing value).
	assertVal(t, rows[0], "source", "json_source")

	// Counter should have incremented.
	if iter.NoOverwriteCount() == 0 {
		t.Error("NoOverwriteCount should be > 0 after field collision")
	}
}

// Test: prefix

func TestParseIterator_Prefix(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {event.StringValue(`{"level":"error","status":500}`)},
		},
		Len: 1,
	}

	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"_raw", nil, "json_", OnErrorPropagate,
	)

	rows := collectParse(t, iter)
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}

	assertVal(t, rows[0], "json_level", "error")
	assertIntVal(t, rows[0], "json_status", 500)

	// Unprefixed should not exist.
	if v, ok := rows[0]["level"]; ok && !v.IsNull() {
		t.Errorf("level without prefix should not exist, got %v", v)
	}
}

// Test: from field (not _raw)

func TestParseIterator_FromField(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw":    {event.StringValue("raw data")},
			"message": {event.StringValue(`{"key":"val"}`)},
		},
		Len: 1,
	}

	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"message", nil, "", OnErrorPropagate,
	)

	rows := collectParse(t, iter)
	assertVal(t, rows[0], "key", "val")
}

// Test: null source rows are passed through

func TestParseIterator_NullSource(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {event.NullValue(), event.StringValue(`{"a":"b"}`)},
		},
		Len: 2,
	}

	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"_raw", nil, "", OnErrorPropagate,
	)

	rows := collectParse(t, iter)
	if len(rows) != 2 {
		t.Fatalf("rows: got %d, want 2", len(rows))
	}

	// Row 0: null source → no extraction, no error.
	if v, ok := rows[0]["a"]; ok && !v.IsNull() {
		t.Errorf("row 0: expected null/absent for 'a', got %v", v)
	}
	if _, ok := rows[0]["_error"]; ok {
		t.Error("row 0: null source should NOT set _error")
	}

	// Row 1: extracted.
	assertVal(t, rows[1], "a", "b")
}

// Test: missing source column passes batch through

func TestParseIterator_MissingSourceColumn(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"other": {event.StringValue("data")},
		},
		Len: 1,
	}

	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"_raw", nil, "", OnErrorPropagate,
	)

	rows := collectParse(t, iter)
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}
	assertVal(t, rows[0], "other", "data")
}

// Test: on_error mode matrix (4 modes x passing/failing rows)

func TestParseIterator_OnErrorMatrix(t *testing.T) {
	modes := []struct {
		name    string
		mode    OnErrorMode
		wantLen int  // expected rows after processing
		hasErr  bool // whether _error is set on failing rows
	}{
		{"propagate", OnErrorPropagate, 2, true},
		{"null", OnErrorNull, 2, false},
		{"drop", OnErrorDrop, 1, false},
		// strict tested separately (returns error)
	}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			batch := &Batch{
				Columns: map[string][]event.Value{
					"_raw": {
						event.StringValue(`{"status":200}`), // valid
						event.StringValue(`garbage`),        // invalid
					},
				},
				Len: 2,
			}

			iter := NewParseIterator(
				&staticIterator{batches: []*Batch{batch}},
				[]unpack.FormatParser{mustParser(t, "json")},
				"_raw", nil, "", m.mode,
			)

			rows := collectParse(t, iter)
			if len(rows) != m.wantLen {
				t.Fatalf("rows: got %d, want %d", len(rows), m.wantLen)
			}

			// First row (valid) should always have status.
			assertIntVal(t, rows[0], "status", 200)

			if m.mode != OnErrorDrop && m.wantLen > 1 {
				_, hasError := rows[1]["_error"]
				if hasError != m.hasErr {
					t.Errorf("_error present: got %v, want %v", hasError, m.hasErr)
				}
			}
		})
	}

	// Strict mode.
	t.Run("strict", func(t *testing.T) {
		batch := &Batch{
			Columns: map[string][]event.Value{
				"_raw": {
					event.StringValue(`{"status":200}`),
					event.StringValue(`garbage`),
				},
			},
			Len: 2,
		}

		iter := NewParseIterator(
			&staticIterator{batches: []*Batch{batch}},
			[]unpack.FormatParser{mustParser(t, "json")},
			"_raw", nil, "", OnErrorStrict,
		)

		if err := iter.Init(context.Background()); err != nil {
			t.Fatalf("Init: %v", err)
		}

		_, err := iter.Next(context.Background())
		if err == nil {
			t.Fatal("expected error from strict mode")
		}
	})
}

// Test: first_of with drop mode

func TestParseIterator_FirstOf_Drop(t *testing.T) {
	batch := mixedFormatBatch() // 5 rows: json, logfmt, garbage, json, broken
	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json"), mustParser(t, "logfmt")},
		"_raw", nil, "", OnErrorDrop,
	)

	rows := collectParse(t, iter)
	// Rows 0, 1, 3 should survive (json or logfmt parse succeeds).
	// Rows 2 and 4: garbage and broken JSON — both formats fail, drop.
	if len(rows) != 3 {
		t.Fatalf("rows: got %d, want 3", len(rows))
	}
}

// Test: docker format through ParseIterator

func TestParseIterator_DockerFormat(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {
				event.StringValue(`{"log":"hello world\n","stream":"stderr","time":"2026-01-01T00:00:00.000000000Z"}`),
			},
		},
		Len: 1,
	}

	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "docker")},
		"_raw", nil, "", OnErrorPropagate,
	)

	rows := collectParse(t, iter)
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}

	assertVal(t, rows[0], "log", "hello world")
	assertVal(t, rows[0], "stream", "stderr")
}

// Test: logfmt format (sanity)

func TestParseIterator_Logfmt(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {event.StringValue(`level=error msg="request timeout" duration=1234`)},
		},
		Len: 1,
	}

	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "logfmt")},
		"_raw", nil, "", OnErrorPropagate,
	)

	rows := collectParse(t, iter)
	assertVal(t, rows[0], "level", "error")
	assertVal(t, rows[0], "msg", "request timeout")
	assertIntVal(t, rows[0], "duration", 1234)
}

// Rows that fail to parse keep _error next to _raw for debugging.

func TestParseIterator_RFC002_Ex15_ErrorDebugging(t *testing.T) {
	// Build a batch that mixes valid JSON with garbage.
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {
				event.StringValue(`{"level":"info","status":200}`),
				event.StringValue(`this is garbage line 1`),
				event.StringValue(`{"level":"error","status":500}`),
				event.StringValue(`another garbage line`),
				event.StringValue(`not json at all {{{`),
			},
		},
		Len: 5,
	}

	parseIter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"_raw", nil, "", OnErrorPropagate,
	)

	rows := collectParse(t, parseIter)
	if len(rows) != 5 {
		t.Fatalf("rows: got %d, want 5", len(rows))
	}

	// Simulate "where exists(_error)" — filter to rows with _error.
	var errorRows []map[string]event.Value
	for _, row := range rows {
		if v, ok := row["_error"]; ok && !v.IsNull() {
			errorRows = append(errorRows, row)
		}
	}

	// Should be exactly the garbage rows (1, 3, 4).
	if len(errorRows) != 3 {
		t.Fatalf("error rows: got %d, want 3", len(errorRows))
	}

	// Each error row should have _error, _raw, and _error_detail.
	for i, row := range errorRows {
		if _, ok := row["_error"]; !ok {
			t.Errorf("error row %d: missing _error", i)
		}
		if _, ok := row["_raw"]; !ok {
			t.Errorf("error row %d: missing _raw", i)
		}
		if _, ok := row["_error_detail"]; !ok {
			t.Errorf("error row %d: missing _error_detail", i)
		}
	}
}

// Test: all-valid batch (no errors)

func TestParseIterator_AllValid(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {
				event.StringValue(`{"a":1}`),
				event.StringValue(`{"a":2}`),
				event.StringValue(`{"a":3}`),
			},
		},
		Len: 3,
	}

	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"_raw", nil, "", OnErrorPropagate,
	)

	rows := collectParse(t, iter)
	if len(rows) != 3 {
		t.Fatalf("rows: got %d, want 3", len(rows))
	}

	for i, row := range rows {
		if _, ok := row["_error"]; ok {
			t.Errorf("row %d: unexpected _error on valid JSON", i)
		}
	}
}

// Test: empty batch

func TestParseIterator_EmptyBatch(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{},
		Len:     0,
	}

	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"_raw", nil, "", OnErrorPropagate,
	)

	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// Empty batch should produce no rows.
	b, err := iter.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if b != nil && b.Len > 0 {
		t.Errorf("expected empty or nil batch, got Len=%d", b.Len)
	}
}

// Test: prefix + no-overwrite interaction

func TestParseIterator_Prefix_NoOverwrite(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw":    {event.StringValue(`{"level":"error"}`)},
			"p_level": {event.StringValue("existing")}, // pre-existing with prefix
		},
		Len: 1,
	}

	iter := NewParseIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]unpack.FormatParser{mustParser(t, "json")},
		"_raw", nil, "p_", OnErrorPropagate,
	)

	rows := collectParse(t, iter)
	// p_level should keep existing value.
	assertVal(t, rows[0], "p_level", "existing")

	if iter.NoOverwriteCount() == 0 {
		t.Error("NoOverwriteCount should be > 0")
	}
}
