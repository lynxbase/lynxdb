package compaction

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/model"
	segment "github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment"
)

func makeSegment(t testing.TB, id, index string, level int, events []*event.Event) *SegmentInfo {
	t.Helper()
	var buf bytes.Buffer
	sw := segment.NewWriter(&buf)
	written, err := sw.Write(events)
	if err != nil {
		t.Fatalf("write segment %s: %v", id, err)
	}

	return &SegmentInfo{
		Meta: model.SegmentMeta{
			ID:         id,
			Index:      index,
			MinTime:    events[0].Time,
			MaxTime:    events[len(events)-1].Time,
			EventCount: int64(len(events)),
			SizeBytes:  written,
			Level:      level,
			CreatedAt:  time.Now(),
		},
		Data: buf.Bytes(),
	}
}

func makeEvents(base time.Time, count int, host string) []*event.Event {
	events := make([]*event.Event, count)
	for i := 0; i < count; i++ {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Second), fmt.Sprintf("msg=%d host=%s", i, host))
		e.Host = host
		e.Source = "/var/log/test"
		e.SourceType = "raw"
		e.Index = "main"
		events[i] = e
	}

	return events
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestCompactor_AddAndSegmentsByLevel(t *testing.T) {
	c := NewCompactor(testLogger())

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	seg1 := makeSegment(t, "s1", "main", L0, makeEvents(base, 10, "web-01"))
	seg2 := makeSegment(t, "s2", "main", L0, makeEvents(base.Add(10*time.Second), 10, "web-02"))
	seg3 := makeSegment(t, "s3", "main", L1, makeEvents(base, 20, "web-01"))

	c.AddSegment(seg1)
	c.AddSegment(seg2)
	c.AddSegment(seg3)

	l0 := c.SegmentsByLevel("main", L0)
	if len(l0) != 2 {
		t.Errorf("L0 count: got %d, want 2", len(l0))
	}

	l1 := c.SegmentsByLevel("main", L1)
	if len(l1) != 1 {
		t.Errorf("L1 count: got %d, want 1", len(l1))
	}

	// Verify L0 segments are sorted by MinTime.
	if l0[0].Meta.ID != "s1" || l0[1].Meta.ID != "s2" {
		t.Errorf("L0 order: %s, %s", l0[0].Meta.ID, l0[1].Meta.ID)
	}
}

func TestCompactor_PlanCompaction_L0ToL1(t *testing.T) {
	c := NewCompactor(testLogger())

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < L0CompactionThreshold; i++ {
		seg := makeSegment(t, fmt.Sprintf("s%d", i), "main", L0,
			makeEvents(base.Add(time.Duration(i)*100*time.Second), 10, "web-01"))
		c.AddSegment(seg)
	}

	plan := c.PlanCompaction("main")
	if plan == nil {
		t.Fatal("expected a compaction plan")
	}
	if plan.OutputLevel != L1 {
		t.Errorf("output level: got %d, want %d", plan.OutputLevel, L1)
	}
	if len(plan.InputSegments) != L0CompactionThreshold {
		t.Errorf("input count: got %d, want %d", len(plan.InputSegments), L0CompactionThreshold)
	}
}

func TestCompactor_PlanCompaction_L1ToL2(t *testing.T) {
	c := NewCompactor(testLogger())

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < L1CompactionThreshold; i++ {
		seg := makeSegment(t, fmt.Sprintf("l1-%d", i), "main", L1,
			makeEvents(base.Add(time.Duration(i)*100*time.Second), 10, "web-01"))
		c.AddSegment(seg)
	}

	plan := c.PlanCompaction("main")
	if plan == nil {
		t.Fatal("expected a compaction plan")
	}
	if plan.OutputLevel != L2 {
		t.Errorf("output level: got %d, want %d", plan.OutputLevel, L2)
	}
}

func TestCompactor_PlanCompaction_NoPlan(t *testing.T) {
	c := NewCompactor(testLogger())

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	// Add fewer than threshold.
	seg := makeSegment(t, "s0", "main", L0, makeEvents(base, 10, "web-01"))
	c.AddSegment(seg)

	plan := c.PlanCompaction("main")
	if plan != nil {
		t.Error("expected no compaction plan")
	}
}

func TestCompactor_Execute_MergesAndSorts(t *testing.T) {
	c := NewCompactor(testLogger())
	ctx := context.Background()

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create overlapping segments with interleaved timestamps.
	seg1 := makeSegment(t, "s1", "main", L0, makeEvents(base, 50, "web-01"))
	seg2 := makeSegment(t, "s2", "main", L0, makeEvents(base.Add(25*time.Second), 50, "web-02"))

	plan := &Plan{
		InputSegments: []*SegmentInfo{seg1, seg2},
		OutputLevel:   L1,
	}

	output, err := c.Execute(ctx, plan)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if output.Meta.EventCount != 100 {
		t.Errorf("event count: got %d, want 100", output.Meta.EventCount)
	}
	if output.Meta.Level != L1 {
		t.Errorf("level: got %d, want %d", output.Meta.Level, L1)
	}
	if output.Meta.Index != "main" {
		t.Errorf("index: got %q, want %q", output.Meta.Index, "main")
	}

	// Verify events are sorted by timestamp.
	reader, err := segment.OpenSegment(output.Data)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	events, err := reader.ReadEvents()
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if len(events) != 100 {
		t.Fatalf("read %d events, want 100", len(events))
	}
	for i := 1; i < len(events); i++ {
		if events[i].Time.Before(events[i-1].Time) {
			t.Errorf("events not sorted at index %d: %v before %v", i, events[i].Time, events[i-1].Time)

			break
		}
	}

	// Verify min/max time.
	if !output.Meta.MinTime.Equal(base) {
		t.Errorf("min time: got %v, want %v", output.Meta.MinTime, base)
	}
	expectedMax := base.Add(74 * time.Second) // max of seg2: base+25+49=base+74
	if !output.Meta.MaxTime.Equal(expectedMax) {
		t.Errorf("max time: got %v, want %v", output.Meta.MaxTime, expectedMax)
	}
}

func TestCompactor_ApplyCompaction(t *testing.T) {
	c := NewCompactor(testLogger())
	ctx := context.Background()

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Add 4 L0 segments.
	for i := 0; i < 4; i++ {
		seg := makeSegment(t, fmt.Sprintf("s%d", i), "main", L0,
			makeEvents(base.Add(time.Duration(i)*100*time.Second), 25, "web-01"))
		c.AddSegment(seg)
	}

	plan := c.PlanCompaction("main")
	if plan == nil {
		t.Fatal("expected plan")
	}

	output, err := c.ApplyCompaction(ctx, plan)
	if err != nil {
		t.Fatalf("ApplyCompaction: %v", err)
	}

	// Input segments should be removed.
	l0 := c.SegmentsByLevel("main", L0)
	if len(l0) != 0 {
		t.Errorf("L0 should be empty, got %d", len(l0))
	}

	// Output segment should be tracked at L1.
	l1 := c.SegmentsByLevel("main", L1)
	if len(l1) != 1 {
		t.Errorf("L1 should have 1 segment, got %d", len(l1))
	}

	if output.Meta.EventCount != 100 {
		t.Errorf("event count: got %d, want 100", output.Meta.EventCount)
	}
}

func TestCompactor_MultiLevelCompaction(t *testing.T) {
	c := NewCompactor(testLogger())
	ctx := context.Background()

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Simulate: add 16 L0 segments, compact in rounds.
	for i := 0; i < 16; i++ {
		seg := makeSegment(t, fmt.Sprintf("s%d", i), "main", L0,
			makeEvents(base.Add(time.Duration(i)*50*time.Second), 10, "web-01"))
		c.AddSegment(seg)
	}

	// Round 1: L0 → L1 compactions (should produce 4 L1 segments from 16 L0).
	for {
		plan := c.PlanCompaction("main")
		if plan == nil || plan.OutputLevel != L1 {
			break
		}
		if _, err := c.ApplyCompaction(ctx, plan); err != nil {
			t.Fatalf("L0→L1: %v", err)
		}
	}

	l0 := c.SegmentsByLevel("main", L0)
	l1 := c.SegmentsByLevel("main", L1)
	if len(l0) != 0 {
		t.Errorf("L0 remaining: %d", len(l0))
	}
	if len(l1) != 4 {
		t.Errorf("L1 count: got %d, want 4", len(l1))
	}

	// Round 2: L1 → L2.
	plan := c.PlanCompaction("main")
	if plan == nil {
		t.Fatal("expected L1→L2 plan")
	}
	if plan.OutputLevel != L2 {
		t.Errorf("output level: got %d, want %d", plan.OutputLevel, L2)
	}

	output, err := c.ApplyCompaction(ctx, plan)
	if err != nil {
		t.Fatalf("L1→L2: %v", err)
	}

	l1 = c.SegmentsByLevel("main", L1)
	l2 := c.SegmentsByLevel("main", L2)
	if len(l1) != 0 {
		t.Errorf("L1 remaining: %d", len(l1))
	}
	if len(l2) != 1 {
		t.Errorf("L2 count: got %d, want 1", len(l2))
	}
	if output.Meta.EventCount != 160 {
		t.Errorf("total events: got %d, want 160", output.Meta.EventCount)
	}
}

func TestCompactor_RemoveSegment(t *testing.T) {
	c := NewCompactor(testLogger())

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	seg := makeSegment(t, "s1", "main", L0, makeEvents(base, 10, "web-01"))
	c.AddSegment(seg)

	c.RemoveSegment("s1")

	segs := c.Segments()
	if len(segs) != 0 {
		t.Errorf("expected 0 segments, got %d", len(segs))
	}
}

func TestCompactor_DifferentIndexes(t *testing.T) {
	c := NewCompactor(testLogger())

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i < 4; i++ {
		events := makeEvents(base.Add(time.Duration(i)*10*time.Second), 10, "web-01")
		for _, e := range events {
			e.Index = "main"
		}
		c.AddSegment(makeSegment(t, fmt.Sprintf("main-%d", i), "main", L0, events))
	}
	for i := 0; i < 2; i++ {
		events := makeEvents(base.Add(time.Duration(i)*10*time.Second), 10, "web-01")
		for _, e := range events {
			e.Index = "security"
		}
		c.AddSegment(makeSegment(t, fmt.Sprintf("sec-%d", i), "security", L0, events))
	}

	// Only "main" should have a compaction plan.
	plan := c.PlanCompaction("main")
	if plan == nil {
		t.Fatal("expected main compaction plan")
	}
	if len(plan.InputSegments) != 4 {
		t.Errorf("input count: %d", len(plan.InputSegments))
	}

	secPlan := c.PlanCompaction("security")
	if secPlan != nil {
		t.Error("security should not need compaction yet")
	}
}
