package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/stats"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

// TestSegmentStreamIterator_MemtableOnly verifies that the iterator yields
// all memtable events when there are no segments.
func TestSegmentStreamIterator_MemtableOnly(t *testing.T) {
	events := makeStreamTestEvents(50)
	iter := NewSegmentStreamIterator(nil, events, nil, 0, nil)
	defer iter.Close()

	got := drainIterator(t, iter)
	if got != 50 {
		t.Errorf("memtable-only: got %d events, want 50", got)
	}
}

// TestSegmentStreamIterator_SegmentOnly verifies streaming from a single
// segment with no memtable events.
func TestSegmentStreamIterator_SegmentOnly(t *testing.T) {
	events := makeStreamTestEvents(200)
	src := writeSegmentSource(t, events, "main")

	iter := NewSegmentStreamIterator(
		[]*SegmentSource{src}, nil, nil, 0,
		stats.NewBudgetMonitor("test", 0).NewAccount("test"),
	)
	defer iter.Close()

	got := drainIterator(t, iter)
	if got != 200 {
		t.Errorf("segment-only: got %d events, want 200", got)
	}

	st := iter.Stats()
	if st.SegmentsScanned != 1 {
		t.Errorf("segments scanned: got %d, want 1", st.SegmentsScanned)
	}
	if st.EventsScanned != 200 {
		t.Errorf("events scanned: got %d, want 200", st.EventsScanned)
	}
}

// TestSegmentStreamIterator_MemtablePlusSegment verifies both phases yield
// data correctly: memtable events first, then segment events.
func TestSegmentStreamIterator_MemtablePlusSegment(t *testing.T) {
	memEvents := makeStreamTestEvents(30)
	segEvents := makeStreamTestEvents(100)
	src := writeSegmentSource(t, segEvents, "main")

	iter := NewSegmentStreamIterator(
		[]*SegmentSource{src}, memEvents, nil, 0,
		stats.NewBudgetMonitor("test", 0).NewAccount("test"),
	)
	defer iter.Close()

	got := drainIterator(t, iter)
	if got != 130 {
		t.Errorf("mem+segment: got %d events, want 130", got)
	}
}

// TestSegmentStreamIterator_MultipleSegments verifies iteration across
// multiple segments.
func TestSegmentStreamIterator_MultipleSegments(t *testing.T) {
	src1 := writeSegmentSource(t, makeStreamTestEvents(100), "main")
	src2 := writeSegmentSource(t, makeStreamTestEvents(150), "main")
	src3 := writeSegmentSource(t, makeStreamTestEvents(50), "main")

	iter := NewSegmentStreamIterator(
		[]*SegmentSource{src1, src2, src3}, nil, nil, 0,
		stats.NewBudgetMonitor("test", 0).NewAccount("test"),
	)
	defer iter.Close()

	got := drainIterator(t, iter)
	if got != 300 {
		t.Errorf("multi-segment: got %d events, want 300", got)
	}

	st := iter.Stats()
	if st.SegmentsScanned != 3 {
		t.Errorf("segments scanned: got %d, want 3", st.SegmentsScanned)
	}
}

// TestSegmentStreamIterator_Limit verifies early termination via hints.Limit.
func TestSegmentStreamIterator_Limit(t *testing.T) {
	events := makeStreamTestEvents(500)
	src := writeSegmentSource(t, events, "main")

	hints := &SegmentStreamHints{Limit: 10}
	iter := NewSegmentStreamIterator(
		[]*SegmentSource{src}, nil, hints, 0,
		stats.NewBudgetMonitor("test", 0).NewAccount("test"),
	)
	defer iter.Close()

	got := drainIterator(t, iter)
	if got != 10 {
		t.Errorf("limit: got %d events, want 10", got)
	}
}

// TestSegmentStreamIterator_LimitWithMemtable verifies that limit works
// across memtable + segment phases.
func TestSegmentStreamIterator_LimitWithMemtable(t *testing.T) {
	memEvents := makeStreamTestEvents(5)
	segEvents := makeStreamTestEvents(200)
	src := writeSegmentSource(t, segEvents, "main")

	hints := &SegmentStreamHints{Limit: 8}
	iter := NewSegmentStreamIterator(
		[]*SegmentSource{src}, memEvents, hints, 0,
		stats.NewBudgetMonitor("test", 0).NewAccount("test"),
	)
	defer iter.Close()

	got := drainIterator(t, iter)
	if got != 8 {
		t.Errorf("limit across phases: got %d events, want 8", got)
	}
}

// TestSegmentStreamIterator_IndexFilter verifies segment-level skip by index name.
func TestSegmentStreamIterator_IndexFilter(t *testing.T) {
	src1 := writeSegmentSource(t, makeStreamTestEvents(100), "web")
	src2 := writeSegmentSource(t, makeStreamTestEvents(100), "api")

	hints := &SegmentStreamHints{IndexName: "web"}
	iter := NewSegmentStreamIterator(
		[]*SegmentSource{src1, src2}, nil, hints, 0,
		stats.NewBudgetMonitor("test", 0).NewAccount("test"),
	)
	defer iter.Close()

	got := drainIterator(t, iter)
	if got != 100 {
		t.Errorf("index filter: got %d events, want 100 (only 'web')", got)
	}

	st := iter.Stats()
	if st.SegmentsSkipped != 1 {
		t.Errorf("segments skipped: got %d, want 1", st.SegmentsSkipped)
	}
}

// TestSegmentStreamIterator_TimeBoundsFilter verifies segment-level time skip.
func TestSegmentStreamIterator_TimeBoundsFilter(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Segment with events at base..base+10s.
	earlyEvents := makeStreamTestEventsAt(100, base)
	src1 := writeSegmentSource(t, earlyEvents, "main")

	// Segment with events at base+1h..base+1h+10s.
	lateEvents := makeStreamTestEventsAt(100, base.Add(time.Hour))
	src2 := writeSegmentSource(t, lateEvents, "main")

	// Only query the late segment.
	hints := &SegmentStreamHints{
		TimeBounds: &spl2.TimeBounds{
			Earliest: base.Add(30 * time.Minute),
			Latest:   base.Add(2 * time.Hour),
		},
	}
	iter := NewSegmentStreamIterator(
		[]*SegmentSource{src1, src2}, nil, hints, 0,
		stats.NewBudgetMonitor("test", 0).NewAccount("test"),
	)
	defer iter.Close()

	got := drainIterator(t, iter)
	if got != 100 {
		t.Errorf("time filter: got %d events, want 100 (only late segment)", got)
	}
}

// TestSegmentStreamIterator_BudgetReturnsError verifies that the iterator
// returns an explicit error when memory budget is exceeded under genuine
// pressure, instead of silently truncating.
func TestSegmentStreamIterator_BudgetReturnsError(t *testing.T) {
	events := makeStreamTestEvents(1000)
	src := writeSegmentSource(t, events, "main")

	// Tiny budget: ~2KB — should fail with error on first batch.
	mon := stats.NewBudgetMonitor("test", 2048)
	acct := mon.NewAccount("test")

	iter := NewSegmentStreamIterator(
		[]*SegmentSource{src}, nil, nil, 0, acct,
	)
	defer iter.Close()

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	var sawError bool
	for {
		_, err := iter.Next(ctx)
		if err != nil {
			sawError = true

			break
		}
	}
	if !sawError {
		t.Error("expected an error when budget is exceeded, not silent EOF")
	}
}

// TestSegmentStreamIterator_ShrinkAfterYield verifies that the memory account
// tracks only the current batch (not cumulative throughput) via Shrink-after-yield.
func TestSegmentStreamIterator_ShrinkAfterYield(t *testing.T) {
	events := makeStreamTestEvents(200)
	src := writeSegmentSource(t, events, "main")

	// Large budget — no pressure. We just want to verify Shrink behavior.
	mon := stats.NewBudgetMonitor("test", 0)
	acct := mon.NewAccount("test")

	iter := NewSegmentStreamIterator(
		[]*SegmentSource{src}, nil, nil, 32, acct,
	)
	defer iter.Close()

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	var batchCount int
	var maxUsed int64
	for {
		batch, err := iter.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if batch == nil {
			break
		}
		batchCount++
		used := acct.Used()
		if used > maxUsed {
			maxUsed = used
		}
	}

	if batchCount < 2 {
		t.Fatalf("expected multiple batches, got %d", batchCount)
	}

	// After Close, account should be near zero (last batch shrunk).
	iter.Close()
	if acct.Used() != 0 {
		t.Errorf("expected account Used()=0 after Close, got %d", acct.Used())
	}
}

// TestSegmentStreamIterator_EmptySegment verifies that a segment with no
// events is handled gracefully (no panic, yields zero events).
func TestSegmentStreamIterator_EmptySegment(t *testing.T) {
	// Write a segment with 0 events. This is unusual but shouldn't crash.
	events := makeStreamTestEvents(0)
	iter := NewSegmentStreamIterator(nil, events, nil, 0, nil)
	defer iter.Close()

	got := drainIterator(t, iter)
	if got != 0 {
		t.Errorf("empty: got %d events, want 0", got)
	}
}

// TestSegmentStreamIterator_ProgressCallback verifies the progress callback
// is invoked during scanning.
func TestSegmentStreamIterator_ProgressCallback(t *testing.T) {
	src := writeSegmentSource(t, makeStreamTestEvents(200), "main")

	iter := NewSegmentStreamIterator(
		[]*SegmentSource{src}, nil, nil, 0,
		stats.NewBudgetMonitor("test", 0).NewAccount("test"),
	)
	defer iter.Close()

	var progressCalls int
	iter.SetOnProgress(func(_ SegmentStreamProgress) {
		progressCalls++
	})

	_ = drainIterator(t, iter)
	if progressCalls == 0 {
		t.Error("expected at least one progress callback")
	}
}

// TestSegmentStreamIterator_ContextCancellation verifies that the iterator
// respects context cancellation.
func TestSegmentStreamIterator_ContextCancellation(t *testing.T) {
	src := writeSegmentSource(t, makeStreamTestEvents(10000), "main")

	iter := NewSegmentStreamIterator(
		[]*SegmentSource{src}, nil, nil, 0,
		stats.NewBudgetMonitor("test", 0).NewAccount("test"),
	)
	defer iter.Close()

	ctx, cancel := context.WithCancel(context.Background())
	if err := iter.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// Read one batch, then cancel.
	_, err := iter.Next(ctx)
	if err != nil {
		t.Fatalf("first Next: %v", err)
	}
	cancel()

	// Next call should return the canceled context error.
	_, err = iter.Next(ctx)
	if err == nil {
		t.Error("expected context cancellation error")
	}
}

// TestSegmentStreamIterator_StatsAccumulation verifies that scan statistics
// are properly accumulated across segments.
func TestSegmentStreamIterator_StatsAccumulation(t *testing.T) {
	src1 := writeSegmentSource(t, makeStreamTestEvents(100), "main")
	src2 := writeSegmentSource(t, makeStreamTestEvents(200), "main")

	iter := NewSegmentStreamIterator(
		[]*SegmentSource{src1, src2}, nil, nil, 0,
		stats.NewBudgetMonitor("test", 0).NewAccount("test"),
	)
	defer iter.Close()

	_ = drainIterator(t, iter)

	st := iter.Stats()
	if st.SegmentsTotal != 2 {
		t.Errorf("SegmentsTotal: got %d, want 2", st.SegmentsTotal)
	}
	if st.SegmentsScanned != 2 {
		t.Errorf("SegmentsScanned: got %d, want 2", st.SegmentsScanned)
	}
	if st.EventsScanned != 300 {
		t.Errorf("EventsScanned: got %d, want 300", st.EventsScanned)
	}
	if st.EventsMatched != 300 {
		t.Errorf("EventsMatched: got %d, want 300", st.EventsMatched)
	}
	if st.RowGroupsScanned == 0 {
		t.Error("RowGroupsScanned should be > 0")
	}
}

// TestSegmentStreamIterator_PerTypeSkipCounters verifies that per-type skip
// counters (scope, time, bloom) match the total SegmentsSkipped count.
func TestSegmentStreamIterator_PerTypeSkipCounters(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Segment 1: index "web", old time range (will be time-skipped).
	earlyEvents := makeStreamTestEventsAt(50, base)
	for _, ev := range earlyEvents {
		ev.Index = "web"
	}
	src1 := writeSegmentSource(t, earlyEvents, "web")

	// Segment 2: index "api" (will be scope-skipped when querying "web").
	apiEvents := makeStreamTestEventsAt(50, base.Add(2*time.Hour))
	for _, ev := range apiEvents {
		ev.Index = "api"
	}
	src2 := writeSegmentSource(t, apiEvents, "api")

	// Segment 3: index "web", in time range (will be scanned).
	lateEvents := makeStreamTestEventsAt(100, base.Add(2*time.Hour))
	for _, ev := range lateEvents {
		ev.Index = "web"
	}
	src3 := writeSegmentSource(t, lateEvents, "web")

	hints := &SegmentStreamHints{
		IndexName: "web",
		TimeBounds: &spl2.TimeBounds{
			Earliest: base.Add(time.Hour),
			Latest:   base.Add(3 * time.Hour),
		},
	}

	iter := NewSegmentStreamIterator(
		[]*SegmentSource{src1, src2, src3}, nil, hints, 0,
		stats.NewBudgetMonitor("test", 0).NewAccount("test"),
	)
	defer iter.Close()

	got := drainIterator(t, iter)
	if got != 100 {
		t.Errorf("events: got %d, want 100", got)
	}

	st := iter.Stats()

	// Total skipped should be 2 (src1 time-skipped, src2 scope-skipped).
	if st.SegmentsSkipped != 2 {
		t.Errorf("SegmentsSkipped: got %d, want 2", st.SegmentsSkipped)
	}

	// Per-type breakdown should sum to total.
	perTypeSum := st.SegmentTimeSkips + st.SegmentBloomSkips + st.ScopeSkips + st.EmptyBitmapSkips
	if perTypeSum != st.SegmentsSkipped {
		t.Errorf("per-type sum (%d) != SegmentsSkipped (%d)", perTypeSum, st.SegmentsSkipped)
	}

	// src1 should be time-skipped.
	if st.SegmentTimeSkips != 1 {
		t.Errorf("SegmentTimeSkips: got %d, want 1", st.SegmentTimeSkips)
	}

	// src2 should be scope-skipped.
	if st.ScopeSkips != 1 {
		t.Errorf("ScopeSkips: got %d, want 1", st.ScopeSkips)
	}

	if st.SegmentsScanned != 1 {
		t.Errorf("SegmentsScanned: got %d, want 1", st.SegmentsScanned)
	}
}

// TestSegmentStreamIterator_ProgressReportsSkipTypes verifies that per-type
// skip counters appear in progress callbacks.
func TestSegmentStreamIterator_ProgressReportsSkipTypes(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a segment outside the time bounds.
	oldEvents := makeStreamTestEventsAt(50, base)
	src1 := writeSegmentSource(t, oldEvents, "main")

	// Create a segment within the time bounds.
	newEvents := makeStreamTestEventsAt(100, base.Add(2*time.Hour))
	src2 := writeSegmentSource(t, newEvents, "main")

	hints := &SegmentStreamHints{
		TimeBounds: &spl2.TimeBounds{
			Earliest: base.Add(time.Hour),
			Latest:   base.Add(3 * time.Hour),
		},
	}

	iter := NewSegmentStreamIterator(
		[]*SegmentSource{src1, src2}, nil, hints, 0,
		stats.NewBudgetMonitor("test", 0).NewAccount("test"),
	)
	defer iter.Close()

	var lastProg SegmentStreamProgress
	iter.SetOnProgress(func(p SegmentStreamProgress) {
		lastProg = p
	})

	_ = drainIterator(t, iter)

	// The final progress should include the time skip.
	if lastProg.SegmentsSkippedTime != 1 {
		t.Errorf("progress SegmentsSkippedTime: got %d, want 1", lastProg.SegmentsSkippedTime)
	}
	if lastProg.SegmentsSkipped != 1 {
		t.Errorf("progress SegmentsSkipped: got %d, want 1", lastProg.SegmentsSkipped)
	}
}

// Test helpers

// makeStreamTestEvents creates n test events with incrementing timestamps and fields.
func makeStreamTestEvents(n int) []*event.Event {
	return makeStreamTestEventsAt(n, time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC))
}

// makeStreamTestEventsAt creates n test events starting at the given base time.
func makeStreamTestEventsAt(n int, base time.Time) []*event.Event {
	events := make([]*event.Event, n)
	for i := 0; i < n; i++ {
		ts := base.Add(time.Duration(i*100) * time.Millisecond)
		raw := fmt.Sprintf("event-%d level=INFO host=web-01 status=200", i)
		ev := event.NewEvent(ts, raw)
		ev.Host = "web-01"
		ev.Source = "test"
		ev.Index = "main"
		ev.SetField("level", event.StringValue("INFO"))
		ev.SetField("status", event.IntValue(200))
		events[i] = ev
	}

	return events
}

// writeSegmentSource writes events to a segment and returns a SegmentSource.
func writeSegmentSource(t *testing.T, events []*event.Event, index string) *SegmentSource {
	t.Helper()
	if len(events) == 0 {
		return &SegmentSource{
			Reader: nil,
			Index:  index,
			Meta: SegmentMeta{
				ID: "empty",
			},
		}
	}

	var buf bytes.Buffer
	w := segment.NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("segment.Write: %v", err)
	}
	r, err := segment.OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("segment.OpenSegment: %v", err)
	}

	return &SegmentSource{
		Reader: r,
		Index:  index,
		Meta: SegmentMeta{
			ID:         fmt.Sprintf("seg-%s-%d", index, len(events)),
			MinTime:    events[0].Time,
			MaxTime:    events[len(events)-1].Time,
			EventCount: int64(len(events)),
			SizeBytes:  int64(buf.Len()),
		},
	}
}

// drainIterator reads all batches and returns total event count.
func drainIterator(t *testing.T, iter *SegmentStreamIterator) int {
	t.Helper()
	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	total := 0
	for {
		batch, err := iter.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if batch == nil {
			break
		}
		total += batch.Len
	}

	return total
}
