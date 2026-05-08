package pipeline

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

func TestIntegration_SegmentStream_BSIRowMaskFiltersRowsWithoutRowPredicates(t *testing.T) {
	eventsA := makeSegmentStreamBSIEvents(t, 1024, func(i int) int64 {
		return int64(200 + i%400)
	})
	eventsB := makeSegmentStreamBSIEvents(t, 1024, func(i int) int64 {
		return int64(500 + i%100)
	})
	sources := []*SegmentSource{
		writeSegmentStreamBSISource(t, eventsA, "main", 512),
		writeSegmentStreamBSISource(t, eventsB, "main", 512),
	}

	hints := &SegmentStreamHints{
		RangePreds: []spl2.RangePredicate{{Field: "status", Min: "500"}},
	}
	iter := NewSegmentStreamIterator(
		sources,
		nil,
		hints,
		128,
		memgov.NewTestBudget("test", 0).NewAccount("test"),
	)
	defer iter.Close()
	iter.segPreds = nil

	got := drainIterator(t, iter)
	want := countSegmentStreamBSIMatches(eventsA, 500) + countSegmentStreamBSIMatches(eventsB, 500)
	if got != want {
		t.Fatalf("drainIterator = %d, want %d BSI-selected rows", got, want)
	}
	total := len(eventsA) + len(eventsB)
	if got >= total {
		t.Fatalf("drainIterator = %d, want fewer than total rows %d to prove mask consumption", got, total)
	}

	stats := iter.Stats()
	if stats.RGRangeBSIChecks != stats.RowGroupsTotal {
		t.Fatalf("RGRangeBSIChecks = %d, want RowGroupsTotal %d", stats.RGRangeBSIChecks, stats.RowGroupsTotal)
	}
	if stats.RGRangeBSIMaskBytes <= 0 {
		t.Fatalf("RGRangeBSIMaskBytes = %d, want > 0", stats.RGRangeBSIMaskBytes)
	}
	if stats.RGRangeBSISkips != 0 {
		t.Fatalf("RGRangeBSISkips = %d, want 0", stats.RGRangeBSISkips)
	}
}

func TestIntegration_SegmentStream_BSIMaskAboveSelectivityThreshold_IsDropped(t *testing.T) {
	events := makeSegmentStreamBSIEvents(t, 1024, func(i int) int64 {
		return int64(200 + i%400)
	})
	source := writeSegmentStreamBSISource(t, events, "main", 512)

	hints := &SegmentStreamHints{
		RangePreds:                 []spl2.RangePredicate{{Field: "status", Min: "280"}},
		BitmapSelectivityThreshold: 0.30,
	}
	iter := NewSegmentStreamIterator(
		[]*SegmentSource{source},
		nil,
		hints,
		128,
		memgov.NewTestBudget("test", 0).NewAccount("test"),
	)
	defer iter.Close()
	iter.segPreds = nil

	got := drainIterator(t, iter)
	if got != len(events) {
		t.Fatalf("drainIterator = %d, want full row count %d when non-selective BSI mask is dropped", got, len(events))
	}
	if selected := countSegmentStreamBSIMatches(events, 280); selected >= len(events) {
		t.Fatalf("fixture selected rows = %d, want fewer than total %d", selected, len(events))
	}

	stats := iter.Stats()
	if stats.RGRangeBSIChecks != stats.RowGroupsTotal {
		t.Fatalf("RGRangeBSIChecks = %d, want RowGroupsTotal %d", stats.RGRangeBSIChecks, stats.RowGroupsTotal)
	}
	if stats.RGRangeBSISkips != 0 {
		t.Fatalf("RGRangeBSISkips = %d, want 0", stats.RGRangeBSISkips)
	}
	if stats.RGRangeBSIMaskBytes <= 0 {
		t.Fatalf("RGRangeBSIMaskBytes = %d, want > 0", stats.RGRangeBSIMaskBytes)
	}
}

func makeSegmentStreamBSIEvents(t *testing.T, n int, status func(int) int64) []*event.Event {
	t.Helper()
	base := time.Date(2026, 5, 8, 15, 0, 0, 0, time.UTC)
	events := make([]*event.Event, n)
	for i := 0; i < n; i++ {
		statusValue := status(i)
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("status=%d row=%d", statusValue, i))
		e.Host = "stream-bsi-host"
		e.Source = "/var/log/stream-bsi.log"
		e.SourceType = "json"
		e.Index = "main"
		e.SetField("status", event.IntValue(statusValue))
		events[i] = e
	}
	return events
}

func writeSegmentStreamBSISource(t *testing.T, events []*event.Event, indexName string, rowGroupSize int) *SegmentSource {
	t.Helper()
	var buf bytes.Buffer
	w := segment.NewWriter(&buf)
	w.SetRowGroupSize(rowGroupSize)
	w.SetIndexConfig(segment.IndexConfig{
		ProfileOverrides: map[string]segment.IndexProfile{
			"status": segment.IndexProfileRangeBSI,
		},
		BSIMaxBitCount: 64,
	})
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}
	reader, err := segment.OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	if !reader.HasRangeBSI() {
		t.Fatal("HasRangeBSI() = false, want true")
	}

	return &SegmentSource{
		Reader: reader,
		Index:  indexName,
		Meta: SegmentMeta{
			ID:         fmt.Sprintf("bsi-%s-%d", indexName, len(events)),
			MinTime:    events[0].Time,
			MaxTime:    events[len(events)-1].Time,
			EventCount: int64(len(events)),
			SizeBytes:  int64(buf.Len()),
		},
	}
}

func countSegmentStreamBSIMatches(events []*event.Event, minStatus int64) int {
	var count int
	for _, e := range events {
		status, ok := e.GetField("status").TryAsInt()
		if ok && status >= minStatus {
			count++
		}
	}
	return count
}
