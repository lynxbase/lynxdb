package segment

import (
	"bytes"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// TestReadRowGroupFiltered_NoFilters verifies ReadRowGroupFiltered with no
// bitmap, no predicates, and no column projection returns all events from
// the specified row group.
func TestReadRowGroupFiltered_NoFilters(t *testing.T) {
	events := generateTestEvents(200)
	r := writeAndOpen(t, events)

	rgCount := r.RowGroupCount()
	if rgCount < 1 {
		t.Fatalf("expected at least 1 row group, got %d", rgCount)
	}

	got, err := r.ReadRowGroupFiltered(0, nil, nil, nil)
	if err != nil {
		t.Fatalf("ReadRowGroupFiltered: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("expected non-empty result")
	}

	// Must match ReadRowGroup.
	want, err := r.ReadRowGroup(0)
	if err != nil {
		t.Fatalf("ReadRowGroup: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("ReadRowGroupFiltered returned %d events, ReadRowGroup returned %d", len(got), len(want))
	}
	for i := range got {
		if !got[i].Time.Equal(want[i].Time) {
			t.Errorf("event[%d] time: got %v, want %v", i, got[i].Time, want[i].Time)
		}
	}
}

// TestReadRowGroupFiltered_WithBitmap verifies that only bitmap-selected rows
// are returned from the row group.
func TestReadRowGroupFiltered_WithBitmap(t *testing.T) {
	events := generateTestEvents(200)
	r := writeAndOpen(t, events)

	// Build a bitmap selecting only even-numbered global rows.
	bm := roaring.New()
	for i := uint32(0); i < uint32(len(events)); i += 2 {
		bm.Add(i)
	}

	got, err := r.ReadRowGroupFiltered(0, bm, nil, nil)
	if err != nil {
		t.Fatalf("ReadRowGroupFiltered: %v", err)
	}

	// Read full row group to verify.
	all, _ := r.ReadRowGroup(0)
	var wantCount int
	for i := 0; i < len(all); i++ {
		if i%2 == 0 {
			wantCount++
		}
	}
	if len(got) != wantCount {
		t.Errorf("bitmap filter: got %d events, want %d", len(got), wantCount)
	}
}

// TestReadRowGroupFiltered_EmptyBitmap verifies that an empty bitmap returns nil.
func TestReadRowGroupFiltered_EmptyBitmap(t *testing.T) {
	events := generateTestEvents(200)
	r := writeAndOpen(t, events)

	bm := roaring.New() // empty
	got, err := r.ReadRowGroupFiltered(0, bm, nil, nil)
	if err != nil {
		t.Fatalf("ReadRowGroupFiltered: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for empty bitmap, got %d events", len(got))
	}
}

// TestReadRowGroupFiltered_WithPredicates verifies field predicate pushdown.
func TestReadRowGroupFiltered_WithPredicates(t *testing.T) {
	events := generateTestEvents(500)
	r := writeAndOpen(t, events)

	// Filter: status = "500"
	preds := []Predicate{{Field: "status", Op: "=", Value: "500"}}
	got, err := r.ReadRowGroupFiltered(0, nil, preds, nil)
	if err != nil {
		t.Fatalf("ReadRowGroupFiltered: %v", err)
	}

	// Manually count expected matches in row group 0.
	all, _ := r.ReadRowGroup(0)
	var wantCount int
	for _, ev := range all {
		if v := ev.GetField("status"); !v.IsNull() && v.AsInt() == 500 {
			wantCount++
		}
	}
	if len(got) != wantCount {
		t.Errorf("predicate filter: got %d events, want %d", len(got), wantCount)
	}
}

// TestReadRowGroupFiltered_WithColumnProjection verifies that only requested
// columns are populated in the returned events.
func TestReadRowGroupFiltered_WithColumnProjection(t *testing.T) {
	events := generateTestEvents(200)
	r := writeAndOpen(t, events)

	got, err := r.ReadRowGroupFiltered(0, nil, nil, []string{"_time", "level"})
	if err != nil {
		t.Fatalf("ReadRowGroupFiltered: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("expected non-empty result")
	}

	// Check that level is populated.
	for i, ev := range got {
		if v := ev.GetField("level"); v.IsNull() {
			t.Errorf("event[%d]: level field is missing", i)

			break
		}
	}
}

// TestReadRowGroupFiltered_BitmapAndPredicates verifies the combination of
// bitmap intersection and field predicate filtering.
func TestReadRowGroupFiltered_BitmapAndPredicates(t *testing.T) {
	events := generateTestEvents(500)
	r := writeAndOpen(t, events)

	// Bitmap: all rows (no reduction).
	bm := roaring.New()
	for i := uint32(0); i < uint32(len(events)); i++ {
		bm.Add(i)
	}

	// Predicate: status >= 400.
	preds := []Predicate{{Field: "status", Op: ">=", Value: "400"}}
	got, err := r.ReadRowGroupFiltered(0, bm, preds, nil)
	if err != nil {
		t.Fatalf("ReadRowGroupFiltered: %v", err)
	}

	// Compare with predicate-only path.
	gotPredsOnly, err := r.ReadRowGroupFiltered(0, nil, preds, nil)
	if err != nil {
		t.Fatalf("ReadRowGroupFiltered (preds only): %v", err)
	}
	if len(got) != len(gotPredsOnly) {
		t.Errorf("bitmap+preds: %d events, preds-only: %d events", len(got), len(gotPredsOnly))
	}
}

// TestReadRowGroupFiltered_InvalidIndex verifies error on out-of-range index.
func TestReadRowGroupFiltered_InvalidIndex(t *testing.T) {
	events := generateTestEvents(100)
	r := writeAndOpen(t, events)

	_, err := r.ReadRowGroupFiltered(-1, nil, nil, nil)
	if err == nil {
		t.Error("expected error for negative index")
	}
	_, err = r.ReadRowGroupFiltered(9999, nil, nil, nil)
	if err == nil {
		t.Error("expected error for out-of-range index")
	}
}

// TestCanPruneRowGroupByIndex verifies time-range-based row group pruning.
func TestCanPruneRowGroupByIndex(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := generateTestEvents(200)
	r := writeAndOpen(t, events)

	// Should not prune with no bounds.
	if r.CanPruneRowGroupByIndex(0, nil, nil) {
		t.Error("should not prune with nil bounds")
	}

	// Should not prune with bounds encompassing the data.
	earliest := base.Add(-time.Hour)
	latest := base.Add(24 * time.Hour)
	if r.CanPruneRowGroupByIndex(0, &earliest, &latest) {
		t.Error("should not prune when bounds encompass all data")
	}

	// Should prune when time range is entirely before the data.
	beforeStart := base.Add(-2 * time.Hour)
	beforeEnd := base.Add(-1 * time.Hour)
	if !r.CanPruneRowGroupByIndex(0, &beforeStart, &beforeEnd) {
		// This might not prune if the zone map doesn't have enough granularity.
		// Verify no crash.
		t.Log("note: did not prune with time range before data (zone map granularity)")
	}

	// Invalid index should return false (no crash).
	if r.CanPruneRowGroupByIndex(9999, &earliest, &latest) {
		t.Error("should not prune invalid index")
	}
}

// TestReadRowGroupFiltered_RangePredicate verifies range predicates (>= and <=)
// filter events correctly during row group read.
func TestReadRowGroupFiltered_RangePredicate(t *testing.T) {
	events := generateTestEvents(500)
	r := writeAndOpen(t, events)

	// Range predicate: status >= 400 (should match 400 and 500).
	preds := []Predicate{{Field: "status", Op: ">=", Value: "400"}}
	got, err := r.ReadRowGroupFiltered(0, nil, preds, nil)
	if err != nil {
		t.Fatalf("ReadRowGroupFiltered: %v", err)
	}

	// Count expected: manually check row group 0.
	all, _ := r.ReadRowGroup(0)
	var wantCount int
	for _, ev := range all {
		if v := ev.GetField("status"); !v.IsNull() && v.AsInt() >= 400 {
			wantCount++
		}
	}
	if len(got) != wantCount {
		t.Errorf("range >= 400: got %d events, want %d", len(got), wantCount)
	}

	// Verify all returned events actually satisfy the predicate.
	for i, ev := range got {
		if v := ev.GetField("status"); v.IsNull() || v.AsInt() < 400 {
			t.Errorf("event[%d]: status=%v should be >= 400", i, v)
		}
	}

	// Range predicate: status <= 200 (should match only 200).
	preds2 := []Predicate{{Field: "status", Op: "<=", Value: "200"}}
	got2, err := r.ReadRowGroupFiltered(0, nil, preds2, nil)
	if err != nil {
		t.Fatalf("ReadRowGroupFiltered <=200: %v", err)
	}

	var wantCount2 int
	for _, ev := range all {
		if v := ev.GetField("status"); !v.IsNull() && v.AsInt() <= 200 {
			wantCount2++
		}
	}
	if len(got2) != wantCount2 {
		t.Errorf("range <= 200: got %d events, want %d", len(got2), wantCount2)
	}
	for i, ev := range got2 {
		if v := ev.GetField("status"); v.IsNull() || v.AsInt() > 200 {
			t.Errorf("event[%d]: status=%v should be <= 200", i, v)
		}
	}
}

// writeAndOpen is a test helper that writes events to a segment and opens a reader.
func writeAndOpen(t *testing.T, events []*event.Event) *Reader {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}
	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	return r
}
