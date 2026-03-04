package pipeline

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func TestJsonCmdIterator_Default(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {
				event.StringValue(`{"level":"error","status":500,"nested":{"key":"val"}}`),
			},
		},
		Len: 1,
	}

	iter := NewJsonCmdIterator(
		&staticIterator{batches: []*Batch{batch}},
		"_raw", nil,
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}

	assertVal(t, rows[0], "level", "error")
	assertIntVal(t, rows[0], "status", 500)
	assertVal(t, rows[0], "nested.key", "val")
}

func TestJsonCmdIterator_CustomField(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw":    {event.StringValue("not json")},
			"payload": {event.StringValue(`{"a":"b"}`)},
		},
		Len: 1,
	}

	iter := NewJsonCmdIterator(
		&staticIterator{batches: []*Batch{batch}},
		"payload", nil,
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	assertVal(t, rows[0], "a", "b")
}

func TestJsonCmdIterator_Paths(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {event.StringValue(`{"user":{"id":42,"name":"alice"},"action":"login"}`)},
		},
		Len: 1,
	}

	// Only extract user.id — other fields should not appear.
	iter := NewJsonCmdIterator(
		&staticIterator{batches: []*Batch{batch}},
		"_raw", []spl2.JsonPath{{Path: "user.id"}},
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	assertIntVal(t, rows[0], "user.id", 42)

	// "user.name" and "action" should NOT be extracted.
	if v, ok := rows[0]["user.name"]; ok && !v.IsNull() {
		t.Errorf("user.name: should not be extracted, got %v", v)
	}
	if v, ok := rows[0]["action"]; ok && !v.IsNull() {
		t.Errorf("action: should not be extracted, got %v", v)
	}
}

func TestJsonCmdIterator_NullSkipped(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {event.NullValue(), event.StringValue(`{"a":"b"}`)},
		},
		Len: 2,
	}

	iter := NewJsonCmdIterator(
		&staticIterator{batches: []*Batch{batch}},
		"_raw", nil,
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	// Row 0: null → no extraction.
	if v, ok := rows[0]["a"]; ok && !v.IsNull() {
		t.Errorf("row[0].a: expected null/absent, got %v", v)
	}

	// Row 1: extracted.
	assertVal(t, rows[1], "a", "b")
}

func TestJsonCmdIterator_PathsWithAlias(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {event.StringValue(`{"user":{"id":42},"action":"login"}`)},
		},
		Len: 1,
	}

	iter := NewJsonCmdIterator(
		&staticIterator{batches: []*Batch{batch}},
		"_raw", []spl2.JsonPath{
			{Path: "user.id", Alias: "uid"},
			{Path: "action"},
		},
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}

	// "uid" should be the output column, not "user.id".
	assertIntVal(t, rows[0], "uid", 42)
	// "user.id" should NOT appear as a column name.
	if v, ok := rows[0]["user.id"]; ok && !v.IsNull() {
		t.Errorf("user.id should not be extracted when aliased to uid, got %v", v)
	}
	// "action" has no alias, so it uses the path as column name.
	assertVal(t, rows[0], "action", "login")
}

func TestJsonCmdIterator_MalformedJSON(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {
				event.StringValue(`not json at all`),
				event.StringValue(`{"valid":"yes"}`),
			},
		},
		Len: 2,
	}

	iter := NewJsonCmdIterator(
		&staticIterator{batches: []*Batch{batch}},
		"_raw", nil,
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows: got %d, want 2", len(rows))
	}

	// Row 0: malformed → no extraction (pass-through).
	// Row 1: valid → extracted.
	assertVal(t, rows[1], "valid", "yes")
}

func TestJsonCmdIterator_BracketIndex(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {event.StringValue(`{"items":[{"name":"alpha"},{"name":"beta"},{"name":"gamma"}]}`)},
		},
		Len: 1,
	}

	iter := NewJsonCmdIterator(
		&staticIterator{batches: []*Batch{batch}},
		"_raw", []spl2.JsonPath{{Path: "items[0].name", Alias: "first"}},
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}

	assertVal(t, rows[0], "first", "alpha")
}

func TestJsonCmdIterator_NegativeIndex(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {event.StringValue(`{"items":[10,20,30]}`)},
		},
		Len: 1,
	}

	iter := NewJsonCmdIterator(
		&staticIterator{batches: []*Batch{batch}},
		"_raw", []spl2.JsonPath{{Path: "items[-1]", Alias: "last"}},
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}

	assertIntVal(t, rows[0], "last", 30)
}

func TestJsonCmdIterator_Wildcard(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {event.StringValue(`{"users":[{"name":"alice"},{"name":"bob"}]}`)},
		},
		Len: 1,
	}

	iter := NewJsonCmdIterator(
		&staticIterator{batches: []*Batch{batch}},
		"_raw", []spl2.JsonPath{{Path: "users[*].name", Alias: "names"}},
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}

	assertVal(t, rows[0], "names", `["alice","bob"]`)
}

func TestJsonCmdIterator_MixedDotAndBracket(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {event.StringValue(`{"items":[{"name":"alpha"}],"action":"login"}`)},
		},
		Len: 1,
	}

	// Mix dot-only and bracket paths in the same command.
	iter := NewJsonCmdIterator(
		&staticIterator{batches: []*Batch{batch}},
		"_raw", []spl2.JsonPath{
			{Path: "action"},                        // dot-only → fast path
			{Path: "items[0].name", Alias: "first"}, // bracket → jsonpath
		},
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}

	assertVal(t, rows[0], "action", "login")
	assertVal(t, rows[0], "first", "alpha")
}

func TestJsonCmdIterator_BracketOnlyPaths(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"_raw": {event.StringValue(`{"data":[100,200,300]}`)},
		},
		Len: 1,
	}

	// All paths are bracket paths — the dot parser should not run in extract-all mode.
	iter := NewJsonCmdIterator(
		&staticIterator{batches: []*Batch{batch}},
		"_raw", []spl2.JsonPath{
			{Path: "data[0]", Alias: "first"},
			{Path: "data[2]", Alias: "third"},
		},
	)

	rows, err := CollectAll(context.Background(), iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}

	assertIntVal(t, rows[0], "first", 100)
	assertIntVal(t, rows[0], "third", 300)

	// No other fields should be extracted (not running in all-fields mode).
	for key, vals := range rows[0] {
		if key == "first" || key == "third" || key == "_raw" {
			continue
		}
		if !vals.IsNull() {
			t.Errorf("unexpected field %q extracted: %v", key, vals)
		}
	}
}
