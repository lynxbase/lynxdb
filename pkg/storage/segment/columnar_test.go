package segment

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

// TestReadColumnar_Correctness verifies that the columnar read path produces
// identical results to the existing row-oriented ReadEvents path.
func TestReadColumnar_Correctness(t *testing.T) {
	events := generateTestEvents(500)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// Reference: existing row-oriented path.
	refEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}

	// New: columnar path (all columns).
	cr, err := r.ReadColumnar(nil, nil)
	if err != nil {
		t.Fatalf("ReadColumnar: %v", err)
	}

	if cr.Count != len(refEvents) {
		t.Fatalf("Count: got %d, want %d", cr.Count, len(refEvents))
	}

	// Compare timestamps.
	for i := range refEvents {
		expected := refEvents[i].Time.UnixNano()
		if cr.Timestamps[i] != expected {
			t.Errorf("Timestamps[%d]: got %d, want %d", i, cr.Timestamps[i], expected)
		}
	}

	// Compare _raw.
	if cr.Raws == nil {
		t.Fatal("expected Raws to be populated")
	}
	for i := range refEvents {
		if cr.Raws[i] != refEvents[i].Raw {
			t.Errorf("Raws[%d]: got %q, want %q", i, cr.Raws[i], refEvents[i].Raw)
		}
	}

	// Compare builtins: _source, _sourcetype, host, index.
	compareBuiltinStrings(t, cr.Builtins["_source"], refEvents, func(e *event.Event) string { return e.Source })
	compareBuiltinStrings(t, cr.Builtins["_sourcetype"], refEvents, func(e *event.Event) string { return e.SourceType })
	compareBuiltinStrings(t, cr.Builtins["host"], refEvents, func(e *event.Event) string { return e.Host })
	compareBuiltinStrings(t, cr.Builtins["index"], refEvents, func(e *event.Event) string { return e.Index })

	// Compare user fields: level (string), status (int), latency (float).
	compareStringField(t, cr.Fields["level"], refEvents, "level")
	compareIntField(t, cr.Fields["status"], refEvents, "status")
	compareFloatField(t, cr.Fields["latency"], refEvents, "latency")
}

// TestReadColumnar_WithBitmap verifies that ReadColumnar with a bitmap produces
// the same rows as ReadEventsByBitmap for the same bitmap.
func TestReadColumnar_WithBitmap(t *testing.T) {
	events := generateTestEvents(1000)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// Bitmap covering rows 100-199.
	bitmap := roaring.New()
	bitmap.AddRange(100, 200)

	// Reference: row-oriented path.
	refEvents, err := r.ReadEventsByBitmap(bitmap, nil)
	if err != nil {
		t.Fatalf("ReadEventsByBitmap: %v", err)
	}

	// Columnar path.
	cr, err := r.ReadColumnar(nil, bitmap)
	if err != nil {
		t.Fatalf("ReadColumnar with bitmap: %v", err)
	}

	if cr.Count != len(refEvents) {
		t.Fatalf("Count: got %d, want %d", cr.Count, len(refEvents))
	}

	// Compare timestamps.
	for i := range refEvents {
		expected := refEvents[i].Time.UnixNano()
		if cr.Timestamps[i] != expected {
			t.Errorf("Timestamps[%d]: got %d, want %d", i, cr.Timestamps[i], expected)
		}
	}

	// Compare _raw.
	for i := range refEvents {
		if cr.Raws[i] != refEvents[i].Raw {
			t.Errorf("Raws[%d]: got %q, want %q", i, cr.Raws[i], refEvents[i].Raw)
		}
	}
}

// TestReadColumnar_WithBitmapSparse tests a sparse bitmap (non-contiguous positions).
func TestReadColumnar_WithBitmapSparse(t *testing.T) {
	events := generateTestEvents(500)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// Sparse bitmap: rows 0, 10, 20, ..., 490.
	bitmap := roaring.New()
	for i := uint32(0); i < 500; i += 10 {
		bitmap.Add(i)
	}

	refEvents, err := r.ReadEventsByBitmap(bitmap, nil)
	if err != nil {
		t.Fatalf("ReadEventsByBitmap: %v", err)
	}

	cr, err := r.ReadColumnar(nil, bitmap)
	if err != nil {
		t.Fatalf("ReadColumnar: %v", err)
	}

	if cr.Count != len(refEvents) {
		t.Fatalf("Count: got %d, want %d (bitmap card %d)", cr.Count, len(refEvents), bitmap.GetCardinality())
	}

	for i := range refEvents {
		if cr.Timestamps[i] != refEvents[i].Time.UnixNano() {
			t.Errorf("Timestamps[%d]: mismatch", i)
		}
		if cr.Raws[i] != refEvents[i].Raw {
			t.Errorf("Raws[%d]: mismatch", i)
		}
	}
}

// TestReadColumnar_ColumnProjection verifies that requesting specific columns
// returns only those columns.
func TestReadColumnar_ColumnProjection(t *testing.T) {
	events := generateTestEvents(100)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// Read only _time and _raw.
	cr, err := r.ReadColumnar([]string{"_time", "_raw"}, nil)
	if err != nil {
		t.Fatalf("ReadColumnar: %v", err)
	}

	if cr.Count != 100 {
		t.Fatalf("Count: got %d, want 100", cr.Count)
	}
	if len(cr.Timestamps) != 100 {
		t.Errorf("Timestamps: got %d, want 100", len(cr.Timestamps))
	}
	if len(cr.Raws) != 100 {
		t.Errorf("Raws: got %d, want 100", len(cr.Raws))
	}

	// Builtins should be empty (not requested).
	if len(cr.Builtins) != 0 {
		t.Errorf("Builtins: expected empty, got %d entries", len(cr.Builtins))
	}

	// Fields should be empty (not requested).
	if len(cr.Fields) != 0 {
		t.Errorf("Fields: expected empty, got %d entries", len(cr.Fields))
	}
}

// TestReadColumnar_FieldsOnly verifies reading specific user fields without builtins.
func TestReadColumnar_FieldsOnly(t *testing.T) {
	events := generateTestEvents(100)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// Read _time + status (int) + latency (float).
	cr, err := r.ReadColumnar([]string{"_time", "status", "latency"}, nil)
	if err != nil {
		t.Fatalf("ReadColumnar: %v", err)
	}

	if cr.Count != 100 {
		t.Fatalf("Count: got %d, want 100", cr.Count)
	}

	// Raws should be nil (not requested).
	if cr.Raws != nil {
		t.Errorf("Raws: expected nil, got len %d", len(cr.Raws))
	}

	// Status should be present.
	if _, ok := cr.Fields["status"]; !ok {
		t.Error("missing field 'status'")
	}

	// Latency should be present.
	if _, ok := cr.Fields["latency"]; !ok {
		t.Error("missing field 'latency'")
	}

	// Level should NOT be present.
	if _, ok := cr.Fields["level"]; ok {
		t.Error("unexpected field 'level' (not requested)")
	}
}

// TestReadColumnarFiltered verifies that ReadColumnarFiltered with predicates
// produces the same rows as ReadEventsFiltered.
func TestReadColumnarFiltered(t *testing.T) {
	events := generateTestEvents(500)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	preds := []Predicate{{Field: "status", Op: "=", Value: "200"}}

	// Reference: row-oriented path.
	refEvents, err := r.ReadEventsFiltered(preds, nil, nil)
	if err != nil {
		t.Fatalf("ReadEventsFiltered: %v", err)
	}

	// Columnar path.
	cr, err := r.ReadColumnarFiltered(preds, nil, nil)
	if err != nil {
		t.Fatalf("ReadColumnarFiltered: %v", err)
	}

	// Handle nil results (no matches).
	if refEvents == nil && cr == nil {
		return
	}
	if (refEvents == nil) != (cr == nil) {
		t.Fatalf("mismatch: refEvents nil=%v, cr nil=%v", refEvents == nil, cr == nil)
	}

	if cr.Count != len(refEvents) {
		t.Fatalf("Count: got %d, want %d", cr.Count, len(refEvents))
	}

	for i := range refEvents {
		expected := refEvents[i].Time.UnixNano()
		if cr.Timestamps[i] != expected {
			t.Errorf("Timestamps[%d]: got %d, want %d", i, cr.Timestamps[i], expected)
		}
	}
}

// TestReadColumnarFiltered_StringPredicate tests string field predicate filtering.
func TestReadColumnarFiltered_StringPredicate(t *testing.T) {
	events := generateTestEvents(500)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	preds := []Predicate{{Field: "level", Op: "=", Value: "ERROR"}}

	refEvents, err := r.ReadEventsFiltered(preds, nil, nil)
	if err != nil {
		t.Fatalf("ReadEventsFiltered: %v", err)
	}

	cr, err := r.ReadColumnarFiltered(preds, nil, nil)
	if err != nil {
		t.Fatalf("ReadColumnarFiltered: %v", err)
	}

	if refEvents == nil && cr == nil {
		return
	}
	if (refEvents == nil) != (cr == nil) {
		t.Fatalf("mismatch: refEvents nil=%v, cr nil=%v", refEvents == nil, cr == nil)
	}

	if cr.Count != len(refEvents) {
		t.Fatalf("Count: got %d, want %d", cr.Count, len(refEvents))
	}
}

// TestFilterByTimeRange verifies time range filtering on ColumnarResult.
func TestFilterByTimeRange(t *testing.T) {
	cr := &ColumnarResult{
		Timestamps: []int64{
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
			time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano(),
			time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC).UnixNano(),
			time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC).UnixNano(),
			time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC).UnixNano(),
		},
		Raws:     []string{"a", "b", "c", "d", "e"},
		Builtins: map[string][]string{"host": {"h1", "h2", "h3", "h4", "h5"}},
		Fields: map[string][]event.Value{
			"level": {
				event.StringValue("INFO"),
				event.StringValue("WARN"),
				event.StringValue("ERROR"),
				event.StringValue("DEBUG"),
				event.StringValue("INFO"),
			},
		},
		Count: 5,
	}

	// Filter: Jan 2 to Jan 4 (inclusive).
	earliest := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	latest := time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC)
	cr.FilterByTimeRange(earliest, latest)

	if cr.Count != 3 {
		t.Fatalf("Count: got %d, want 3", cr.Count)
	}
	if len(cr.Timestamps) != 3 {
		t.Fatalf("Timestamps: got %d, want 3", len(cr.Timestamps))
	}
	if cr.Raws[0] != "b" || cr.Raws[1] != "c" || cr.Raws[2] != "d" {
		t.Errorf("Raws: got %v, want [b c d]", cr.Raws)
	}
	if cr.Builtins["host"][0] != "h2" || cr.Builtins["host"][2] != "h4" {
		t.Errorf("Builtins host: got %v, want [h2 h3 h4]", cr.Builtins["host"])
	}
	if cr.Fields["level"][0].AsString() != "WARN" || cr.Fields["level"][2].AsString() != "DEBUG" {
		t.Errorf("Fields level: unexpected values after filter")
	}
}

// TestFilterByTimeRange_NoOp verifies that zero time bounds are a no-op.
func TestFilterByTimeRange_NoOp(t *testing.T) {
	cr := &ColumnarResult{
		Timestamps: []int64{1, 2, 3},
		Raws:       []string{"a", "b", "c"},
		Count:      3,
	}

	cr.FilterByTimeRange(time.Time{}, time.Time{})
	if cr.Count != 3 {
		t.Errorf("Count: got %d, want 3", cr.Count)
	}
}

// TestFilterByTimeRange_AllFiltered verifies that filtering all rows produces empty result.
func TestFilterByTimeRange_AllFiltered(t *testing.T) {
	cr := &ColumnarResult{
		Timestamps: []int64{
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
			time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano(),
		},
		Raws:  []string{"a", "b"},
		Count: 2,
	}

	// Filter: only June 2024 — no rows match.
	earliest := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	latest := time.Date(2024, 6, 30, 0, 0, 0, 0, time.UTC)
	cr.FilterByTimeRange(earliest, latest)

	if cr.Count != 0 {
		t.Errorf("Count: got %d, want 0", cr.Count)
	}
}

// TestReadColumnar_EmptySegment verifies columnar read on a minimal segment.
func TestReadColumnar_EmptyBitmap(t *testing.T) {
	events := generateTestEvents(100)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// Empty bitmap should return zero rows.
	bitmap := roaring.New()
	cr, err := r.ReadColumnar(nil, bitmap)
	if err != nil {
		t.Fatalf("ReadColumnar: %v", err)
	}

	if cr.Count != 0 {
		t.Errorf("Count: got %d, want 0", cr.Count)
	}
}

// TestReadColumnar_LargeDataset verifies the columnar path with multiple row groups.
func TestReadColumnar_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large dataset test in short mode")
	}

	events := generateTestEvents(100000)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.RowGroupCount() <= 1 {
		t.Skipf("need multiple row groups, got %d (DefaultRowGroupSize=%d)", r.RowGroupCount(), DefaultRowGroupSize)
	}

	// Reference: row-oriented path.
	refEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}

	// Columnar path.
	cr, err := r.ReadColumnar(nil, nil)
	if err != nil {
		t.Fatalf("ReadColumnar: %v", err)
	}

	if cr.Count != len(refEvents) {
		t.Fatalf("Count: got %d, want %d", cr.Count, len(refEvents))
	}

	// Spot-check first, middle, and last rows.
	checkpoints := []int{0, cr.Count / 2, cr.Count - 1}
	for _, i := range checkpoints {
		if cr.Timestamps[i] != refEvents[i].Time.UnixNano() {
			t.Errorf("Timestamps[%d]: got %d, want %d", i, cr.Timestamps[i], refEvents[i].Time.UnixNano())
		}
		if cr.Raws[i] != refEvents[i].Raw {
			t.Errorf("Raws[%d]: mismatch", i)
		}
	}
}

// TestReadColumnar_LargeWithBitmap verifies bitmap filtering across multiple row groups.
func TestReadColumnar_LargeWithBitmap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large dataset test in short mode")
	}

	events := generateTestEvents(100000)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.RowGroupCount() <= 1 {
		t.Skipf("need multiple row groups, got %d", r.RowGroupCount())
	}

	// Bitmap spanning across row group boundaries: rows 60000-70000.
	bitmap := roaring.New()
	bitmap.AddRange(60000, 70000)

	refEvents, err := r.ReadEventsByBitmap(bitmap, nil)
	if err != nil {
		t.Fatalf("ReadEventsByBitmap: %v", err)
	}

	cr, err := r.ReadColumnar(nil, bitmap)
	if err != nil {
		t.Fatalf("ReadColumnar: %v", err)
	}

	if cr.Count != len(refEvents) {
		t.Fatalf("Count: got %d, want %d", cr.Count, len(refEvents))
	}

	for i := range refEvents {
		if cr.Timestamps[i] != refEvents[i].Time.UnixNano() {
			t.Errorf("Timestamps[%d]: got %d, want %d", i, cr.Timestamps[i], refEvents[i].Time.UnixNano())

			break
		}
	}
}

// TestReadColumnarWithHints verifies the hint-based columnar read with time pruning.
func TestReadColumnarWithHints(t *testing.T) {
	events := generateTestEvents(500)

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// Read with column projection via hints.
	cr, err := r.ReadColumnarWithHints(QueryHints{
		Columns: []string{"_time", "_raw", "host"},
	})
	if err != nil {
		t.Fatalf("ReadColumnarWithHints: %v", err)
	}

	if cr.Count != 500 {
		t.Fatalf("Count: got %d, want 500", cr.Count)
	}

	// Verify host is present.
	if hosts, ok := cr.Builtins["host"]; !ok || len(hosts) != 500 {
		t.Errorf("host: expected 500 entries, got %d", len(hosts))
	}

	// Verify level is NOT present (not requested).
	if _, ok := cr.Fields["level"]; ok {
		t.Error("unexpected field 'level' (not requested)")
	}
}

// BenchmarkReadColumnar benchmarks the columnar read path against the row-oriented path.
func BenchmarkReadColumnar(b *testing.B) {
	events := generateTestEvents(10000)
	var buf bytes.Buffer
	w := NewWriter(&buf)
	_, _ = w.Write(events)
	data := buf.Bytes()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := OpenSegment(data)
		_, _ = r.ReadColumnar(nil, nil)
	}
}

// BenchmarkReadColumnar_Projected benchmarks columnar read with column projection.
func BenchmarkReadColumnar_Projected(b *testing.B) {
	events := generateTestEvents(10000)
	var buf bytes.Buffer
	w := NewWriter(&buf)
	_, _ = w.Write(events)
	data := buf.Bytes()
	cols := []string{"_time", "_raw", "status"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := OpenSegment(data)
		_, _ = r.ReadColumnar(cols, nil)
	}
}

// BenchmarkReadEvents_Baseline benchmarks the existing row-oriented path for comparison.
func BenchmarkReadEvents_Baseline(b *testing.B) {
	events := generateTestEvents(10000)
	var buf bytes.Buffer
	w := NewWriter(&buf)
	_, _ = w.Write(events)
	data := buf.Bytes()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := OpenSegment(data)
		_, _ = r.ReadEvents()
	}
}

// Test helpers

func compareBuiltinStrings(t *testing.T, got []string, events []*event.Event, getter func(*event.Event) string) {
	t.Helper()
	if got == nil {
		return // column may not be present
	}
	for i, ev := range events {
		if got[i] != getter(ev) {
			t.Errorf("builtin[%d]: got %q, want %q", i, got[i], getter(ev))
		}
	}
}

func compareStringField(t *testing.T, got []event.Value, events []*event.Event, field string) {
	t.Helper()
	if got == nil {
		t.Fatalf("missing field %q in columnar result", field)
	}
	for i, ev := range events {
		ref := ev.GetField(field)
		if ref.IsNull() {
			if !got[i].IsNull() {
				t.Errorf("%s[%d]: got %v, want null", field, i, got[i])
			}

			continue
		}
		if got[i].AsString() != ref.AsString() {
			t.Errorf("%s[%d]: got %q, want %q", field, i, got[i].AsString(), ref.AsString())
		}
	}
}

func compareIntField(t *testing.T, got []event.Value, events []*event.Event, field string) {
	t.Helper()
	if got == nil {
		t.Fatalf("missing field %q in columnar result", field)
	}
	for i, ev := range events {
		ref := ev.GetField(field)
		if ref.IsNull() {
			continue
		}
		if got[i].AsInt() != ref.AsInt() {
			t.Errorf("%s[%d]: got %d, want %d", field, i, got[i].AsInt(), ref.AsInt())
		}
	}
}

func compareFloatField(t *testing.T, got []event.Value, events []*event.Event, field string) {
	t.Helper()
	if got == nil {
		t.Fatalf("missing field %q in columnar result", field)
	}
	for i, ev := range events {
		ref := ev.GetField(field)
		if ref.IsNull() {
			continue
		}
		if math.Abs(got[i].AsFloat()-ref.AsFloat()) > 1e-10 {
			t.Errorf("%s[%d]: got %v, want %v", field, i, got[i].AsFloat(), ref.AsFloat())
		}
	}
}
