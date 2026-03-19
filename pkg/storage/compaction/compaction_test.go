package compaction

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/model"
	segment "github.com/lynxbase/lynxdb/pkg/storage/segment"
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
	// Use overlapping time ranges (5s offset, 10 events each spanning 10s)
	// so that the planner produces a merge plan instead of trivial moves.
	for i := 0; i < L0CompactionThreshold; i++ {
		seg := makeSegment(t, fmt.Sprintf("s%d", i), "main", L0,
			makeEvents(base.Add(time.Duration(i)*5*time.Second), 10, "web-01"))
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

	// Add 4 L0 segments with overlapping time ranges (10s offset, 25 events each).
	for i := 0; i < 4; i++ {
		seg := makeSegment(t, fmt.Sprintf("s%d", i), "main", L0,
			makeEvents(base.Add(time.Duration(i)*10*time.Second), 25, "web-01"))
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

	// Simulate: add 16 L0 segments with overlapping ranges (5s offset, 10 events
	// spanning 10s each). Overlapping ensures segments go through merge, not
	// trivial move.
	for i := 0; i < 16; i++ {
		seg := makeSegment(t, fmt.Sprintf("s%d", i), "main", L0,
			makeEvents(base.Add(time.Duration(i)*5*time.Second), 10, "web-01"))
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

	// Use overlapping time ranges (5s offset, 10 events each) for merge.
	for i := 0; i < 4; i++ {
		events := makeEvents(base.Add(time.Duration(i)*5*time.Second), 10, "web-01")
		for _, e := range events {
			e.Index = "main"
		}
		c.AddSegment(makeSegment(t, fmt.Sprintf("main-%d", i), "main", L0, events))
	}
	for i := 0; i < 2; i++ {
		events := makeEvents(base.Add(time.Duration(i)*5*time.Second), 10, "web-01")
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

func TestSizeTiered_TrivialMove_NonOverlapping(t *testing.T) {
	st := &SizeTiered{Threshold: 4}

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create 4 non-overlapping L0 segments (100s apart, 10s span each).
	var segs []*SegmentInfo
	for i := 0; i < 4; i++ {
		minT := base.Add(time.Duration(i) * 100 * time.Second)
		maxT := minT.Add(9 * time.Second)
		segs = append(segs, &SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("s%d", i),
				Level:     L0,
				SizeBytes: 500 << 10,
				MinTime:   minT,
				MaxTime:   maxT,
			},
		})
	}

	plans := st.Plan(segs)

	// All 4 segments are non-overlapping, so each should be trivially moved.
	trivialCount := 0
	for _, p := range plans {
		if p.TrivialMove {
			trivialCount++
			if len(p.InputSegments) != 1 {
				t.Errorf("trivial move plan should have 1 input, got %d", len(p.InputSegments))
			}
			if p.OutputLevel != L1 {
				t.Errorf("trivial move output level: got %d, want %d", p.OutputLevel, L1)
			}
		}
	}
	if trivialCount != 4 {
		t.Errorf("expected 4 trivial moves, got %d (total plans: %d)", trivialCount, len(plans))
	}
}

func TestSizeTiered_TrivialMove_MixedOverlapping(t *testing.T) {
	st := &SizeTiered{Threshold: 4}

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// 2 overlapping + 2 non-overlapping L0 segments.
	segs := []*SegmentInfo{
		{Meta: model.SegmentMeta{ID: "overlap-0", Level: L0, SizeBytes: 500 << 10,
			MinTime: base, MaxTime: base.Add(20 * time.Second)}},
		{Meta: model.SegmentMeta{ID: "overlap-1", Level: L0, SizeBytes: 500 << 10,
			MinTime: base.Add(10 * time.Second), MaxTime: base.Add(30 * time.Second)}},
		{Meta: model.SegmentMeta{ID: "isolated-0", Level: L0, SizeBytes: 500 << 10,
			MinTime: base.Add(100 * time.Second), MaxTime: base.Add(110 * time.Second)}},
		{Meta: model.SegmentMeta{ID: "isolated-1", Level: L0, SizeBytes: 500 << 10,
			MinTime: base.Add(200 * time.Second), MaxTime: base.Add(210 * time.Second)}},
	}

	plans := st.Plan(segs)

	trivialIDs := make(map[string]bool)
	for _, p := range plans {
		if p.TrivialMove {
			for _, s := range p.InputSegments {
				trivialIDs[s.Meta.ID] = true
			}
		}
	}

	// The two isolated segments should be trivially moved.
	if !trivialIDs["isolated-0"] {
		t.Error("isolated-0 should be trivially moved")
	}
	if !trivialIDs["isolated-1"] {
		t.Error("isolated-1 should be trivially moved")
	}
	// The two overlapping segments should NOT be trivially moved.
	if trivialIDs["overlap-0"] {
		t.Error("overlap-0 should NOT be trivially moved")
	}
	if trivialIDs["overlap-1"] {
		t.Error("overlap-1 should NOT be trivially moved")
	}
}

func TestSizeTiered_TrivialMove_SkipsZeroMaxTime(t *testing.T) {
	st := &SizeTiered{Threshold: 4}

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create 4 segments with zero MaxTime — trivial move should be skipped.
	var segs []*SegmentInfo
	for i := 0; i < 4; i++ {
		segs = append(segs, &SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("s%d", i),
				Level:     L0,
				SizeBytes: 500 << 10,
				MinTime:   base.Add(time.Duration(i) * 100 * time.Second),
				// MaxTime is zero — cannot determine overlap.
			},
		})
	}

	plans := st.Plan(segs)

	for _, p := range plans {
		if p.TrivialMove {
			t.Error("no trivial moves expected when MaxTime is zero")
		}
	}
	// Should still produce a regular tier-based plan.
	if len(plans) == 0 {
		t.Error("expected at least one regular plan")
	}
}

func TestSizeTiered_EmergencyCompaction(t *testing.T) {
	st := &SizeTiered{Threshold: 4}

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create 8 L0 segments spread across different size tiers so no single
	// tier has >= 4 segments. This forces the emergency path.
	var segs []*SegmentInfo
	sizes := []int64{
		100 << 10,  // 100KB — tier 0
		500 << 10,  // 500KB — tier 0
		2 << 20,    // 2MB — tier 1
		8 << 20,    // 8MB — tier 1
		20 << 20,   // 20MB — tier 2
		80 << 20,   // 80MB — tier 2
		200 << 20,  // 200MB — tier 3
		500 << 20,  // 500MB — tier 3
	}
	for i := 0; i < 8; i++ {
		segs = append(segs, &SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("s%d", i),
				Level:     L0,
				SizeBytes: sizes[i],
				MinTime:   base.Add(time.Duration(i) * time.Hour),
				// MaxTime zero — trivial move will be skipped.
			},
		})
	}

	plans := st.Plan(segs)

	// No tier has >= 4 segments, so no tier-based plans.
	// But L0 count == L0EmergencyThreshold, so emergency merge should trigger.
	var emergencyPlan *Plan
	for _, p := range plans {
		if !p.TrivialMove && len(p.InputSegments) == 8 {
			emergencyPlan = p
		}
	}
	if emergencyPlan == nil {
		t.Fatalf("expected emergency compaction plan merging all 8 L0 segments, got %d plans", len(plans))
	}
	if emergencyPlan.OutputLevel != L1 {
		t.Errorf("emergency plan output level: got %d, want %d", emergencyPlan.OutputLevel, L1)
	}

	// Verify segments are sorted by MinTime in the emergency plan.
	for i := 1; i < len(emergencyPlan.InputSegments); i++ {
		if emergencyPlan.InputSegments[i].Meta.MinTime.Before(emergencyPlan.InputSegments[i-1].Meta.MinTime) {
			t.Errorf("emergency plan inputs not sorted at position %d", i)
		}
	}
}

func TestSizeTiered_NoEmergencyWhenTierPlansExist(t *testing.T) {
	st := &SizeTiered{Threshold: 4}

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create 8 L0 segments: 4 in the same tier + 4 spread across other tiers.
	// The first 4 form a regular tier plan; emergency should NOT trigger.
	var segs []*SegmentInfo
	for i := 0; i < 4; i++ {
		segs = append(segs, &SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("same-%d", i),
				Level:     L0,
				SizeBytes: 500 << 10, // all in tier 0
				MinTime:   base.Add(time.Duration(i) * time.Hour),
			},
		})
	}
	sizes := []int64{2 << 20, 20 << 20, 200 << 20, 1 << 30}
	for i := 0; i < 4; i++ {
		segs = append(segs, &SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("diff-%d", i),
				Level:     L0,
				SizeBytes: sizes[i],
				MinTime:   base.Add(time.Duration(i+4) * time.Hour),
			},
		})
	}

	plans := st.Plan(segs)

	for _, p := range plans {
		if !p.TrivialMove && len(p.InputSegments) == 8 {
			t.Error("emergency merge should not trigger when tier-based plans exist")
		}
	}
}

func TestTimeWindow_ColdPartition(t *testing.T) {
	tw := &TimeWindow{ColdThreshold: 1 * time.Hour} // short threshold for testing

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create 3 L2 segments that were created > 1 hour ago.
	segs := []*SegmentInfo{
		{Meta: model.SegmentMeta{ID: "l2-0", Level: L2, CreatedAt: base}},
		{Meta: model.SegmentMeta{ID: "l2-1", Level: L2, CreatedAt: base.Add(time.Hour)}},
		{Meta: model.SegmentMeta{ID: "l2-2", Level: L2, CreatedAt: base.Add(2 * time.Hour)}},
	}

	plans := tw.Plan(segs)
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	if plans[0].OutputLevel != L3 {
		t.Errorf("output level: got %d, want %d", plans[0].OutputLevel, L3)
	}
	if len(plans[0].InputSegments) != 3 {
		t.Errorf("input count: got %d, want 3", len(plans[0].InputSegments))
	}
}

func TestTimeWindow_WarmPartition(t *testing.T) {
	tw := &TimeWindow{ColdThreshold: 48 * time.Hour}

	// One segment created now — partition is warm.
	segs := []*SegmentInfo{
		{Meta: model.SegmentMeta{ID: "l2-0", Level: L2, CreatedAt: time.Now().Add(-72 * time.Hour)}},
		{Meta: model.SegmentMeta{ID: "l2-1", Level: L2, CreatedAt: time.Now()}}, // too recent
	}

	plans := tw.Plan(segs)
	if len(plans) != 0 {
		t.Errorf("expected no plans for warm partition, got %d", len(plans))
	}
}

func TestTimeWindow_TooFewSegments(t *testing.T) {
	tw := &TimeWindow{ColdThreshold: 1 * time.Hour}

	// Only 1 L2 segment — need at least 2 to justify consolidation.
	segs := []*SegmentInfo{
		{Meta: model.SegmentMeta{ID: "l2-0", Level: L2,
			CreatedAt: time.Now().Add(-72 * time.Hour)}},
	}

	plans := tw.Plan(segs)
	if len(plans) != 0 {
		t.Errorf("expected no plans for single L2 segment, got %d", len(plans))
	}
}

func TestTimeWindow_IgnoresNonL2(t *testing.T) {
	tw := &TimeWindow{ColdThreshold: 1 * time.Hour}

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Mix of L0, L1, and L2 segments — only L2 should be considered.
	segs := []*SegmentInfo{
		{Meta: model.SegmentMeta{ID: "l0-0", Level: L0, CreatedAt: base}},
		{Meta: model.SegmentMeta{ID: "l1-0", Level: L1, CreatedAt: base}},
		{Meta: model.SegmentMeta{ID: "l2-0", Level: L2, CreatedAt: base}},
	}

	plans := tw.Plan(segs)
	if len(plans) != 0 {
		t.Errorf("expected no plans (only 1 L2), got %d", len(plans))
	}
}

func TestCompactor_PlanCompaction_L2ToL3(t *testing.T) {
	c := NewCompactor(testLogger())

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Add 3 L2 segments created long ago.
	for i := 0; i < 3; i++ {
		c.AddSegment(&SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("l2-%d", i),
				Index:     "main",
				Level:     L2,
				SizeBytes: 500 << 20,
				MinTime:   base.Add(time.Duration(i) * time.Hour),
				MaxTime:   base.Add(time.Duration(i)*time.Hour + 59*time.Minute),
				CreatedAt: base, // created far in the past
			},
		})
	}

	plan := c.PlanCompaction("main")
	if plan == nil {
		t.Fatal("expected L2→L3 plan")
	}
	if plan.OutputLevel != L3 {
		t.Errorf("output level: got %d, want %d", plan.OutputLevel, L3)
	}
	if len(plan.InputSegments) != 3 {
		t.Errorf("input count: got %d, want 3", len(plan.InputSegments))
	}
}

func TestCompactor_PlanAllCompactions_IncludesL2ToL3(t *testing.T) {
	c := NewCompactor(testLogger())

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Add 3 old L2 segments — should produce an L2→L3 job.
	for i := 0; i < 3; i++ {
		c.AddSegment(&SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("l2-%d", i),
				Index:     "main",
				Level:     L2,
				SizeBytes: 500 << 20,
				MinTime:   base.Add(time.Duration(i) * time.Hour),
				MaxTime:   base.Add(time.Duration(i)*time.Hour + 59*time.Minute),
				CreatedAt: base,
			},
		})
	}

	jobs := c.PlanAllCompactions("main")

	var hasL2ToL3 bool
	for _, j := range jobs {
		if j.Priority == PriorityL2ToL3 {
			hasL2ToL3 = true
			if j.Plan.OutputLevel != L3 {
				t.Errorf("L2→L3 job output level: got %d, want %d", j.Plan.OutputLevel, L3)
			}
		}
	}
	if !hasL2ToL3 {
		t.Error("expected an L2→L3 job in PlanAllCompactions")
	}
}

// --- Partition-scoped compaction tests (C1) ---

func TestCompactor_PartitionScopedPlanning(t *testing.T) {
	c := NewCompactor(testLogger())

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Add 4 L0 segments in partition "2024-01-01" and 2 in "2024-01-02".
	// Only the first partition should trigger compaction (threshold=4).
	for i := 0; i < 4; i++ {
		seg := makeSegment(t, fmt.Sprintf("p1-s%d", i), "main", L0,
			makeEvents(base.Add(time.Duration(i)*5*time.Second), 10, "web-01"))
		seg.Meta.Partition = "2024-01-01"
		c.AddSegment(seg)
	}
	for i := 0; i < 2; i++ {
		seg := makeSegment(t, fmt.Sprintf("p2-s%d", i), "main", L0,
			makeEvents(base.Add(24*time.Hour+time.Duration(i)*5*time.Second), 10, "web-01"))
		seg.Meta.Partition = "2024-01-02"
		c.AddSegment(seg)
	}

	jobs := c.PlanAllCompactions("main")

	// Only partition "2024-01-01" should have plans.
	for _, job := range jobs {
		if job.Partition == "2024-01-02" {
			t.Error("partition 2024-01-02 should not have compaction plans (only 2 L0 segments)")
		}
		// Verify plans never mix partitions.
		for _, seg := range job.Plan.InputSegments {
			if seg.Meta.Partition != job.Partition {
				t.Errorf("plan mixes partitions: job partition %q, segment partition %q",
					job.Partition, seg.Meta.Partition)
			}
		}
	}

	if len(jobs) == 0 {
		t.Fatal("expected at least one compaction job for partition 2024-01-01")
	}
}

func TestScheduler_ConcurrentPartitions(t *testing.T) {
	c := NewCompactor(testLogger())
	logger := testLogger()

	s := NewScheduler(c, SchedulerConfig{Workers: 4}, logger)

	// Submit jobs for different partitions of the same index.
	// They should be able to execute concurrently.
	started := make(chan string, 10)
	done := make(chan string, 10)

	s.SetExecutor(func(ctx context.Context, job *Job) error {
		started <- job.Partition
		// Simulate work.
		time.Sleep(50 * time.Millisecond)
		done <- job.Partition
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)
	defer s.Stop()

	// Submit 3 jobs for different partitions.
	for _, part := range []string{"2024-01", "2024-02", "2024-03"} {
		s.Submit(&Job{
			Plan:      &Plan{InputSegments: []*SegmentInfo{{Meta: model.SegmentMeta{ID: "seg-" + part}}}},
			Priority:  PriorityL0ToL1,
			Index:     "main",
			Partition: part,
		})
	}

	// All 3 should start within a reasonable time (concurrent).
	startCount := 0
	timeout := time.After(2 * time.Second)
	for startCount < 3 {
		select {
		case <-started:
			startCount++
		case <-timeout:
			t.Fatalf("only %d/3 partitions started concurrently", startCount)
		}
	}

	// Wait for all to finish.
	doneCount := 0
	timeout = time.After(2 * time.Second)
	for doneCount < 3 {
		select {
		case <-done:
			doneCount++
		case <-timeout:
			t.Fatalf("only %d/3 partitions completed", doneCount)
		}
	}
}

func TestTimeWindow_ColdPartitionIsolation(t *testing.T) {
	c := NewCompactor(testLogger())

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Cold partition: 3 L2 segments created long ago.
	for i := 0; i < 3; i++ {
		c.AddSegment(&SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("cold-l2-%d", i),
				Index:     "main",
				Partition: "2024-01-01",
				Level:     L2,
				SizeBytes: 500 << 20,
				MinTime:   base.Add(time.Duration(i) * time.Hour),
				MaxTime:   base.Add(time.Duration(i)*time.Hour + 59*time.Minute),
				CreatedAt: base, // created far in the past
			},
		})
	}

	// Hot partition: 1 L2 segment created recently — should NOT trigger L2→L3.
	c.AddSegment(&SegmentInfo{
		Meta: model.SegmentMeta{
			ID:        "hot-l2-0",
			Index:     "main",
			Partition: "2024-03-20",
			Level:     L2,
			SizeBytes: 500 << 20,
			MinTime:   time.Now().Add(-1 * time.Hour),
			MaxTime:   time.Now(),
			CreatedAt: time.Now(),
		},
	})

	jobs := c.PlanAllCompactions("main")

	// Only cold partition should have L2→L3 plan.
	var l2ToL3Count int
	for _, job := range jobs {
		if job.Priority == PriorityL2ToL3 {
			l2ToL3Count++
			if job.Partition != "2024-01-01" {
				t.Errorf("L2→L3 should only apply to cold partition, got %q", job.Partition)
			}
		}
	}
	if l2ToL3Count != 1 {
		t.Errorf("expected exactly 1 L2→L3 plan (cold partition), got %d", l2ToL3Count)
	}
}

// --- Adaptive pause tests (C2) ---

func TestAdaptiveController_AutoPauseOnHighLatency(t *testing.T) {
	ac := NewAdaptiveController(AdaptiveConfig{
		TargetP99:  500 * time.Millisecond,
		WindowSize: 10,
	})

	if ac.Paused() {
		t.Error("should not be paused initially")
	}

	// Record latencies > 2× target (1000ms).
	for i := 0; i < 10; i++ {
		ac.RecordLatency(1200 * time.Millisecond)
	}
	ac.Adjust()

	if !ac.Paused() {
		t.Error("should be paused when P99 > 2× target")
	}

	// Record latencies below target to resume.
	for i := 0; i < 10; i++ {
		ac.RecordLatency(200 * time.Millisecond)
	}
	ac.Adjust()

	if ac.Paused() {
		t.Error("should resume when P99 < target")
	}
}

// --- CPU semaphore test (C3) ---

func TestScheduler_CPUSemaphore(t *testing.T) {
	c := NewCompactor(testLogger())
	logger := testLogger()

	s := NewScheduler(c, SchedulerConfig{Workers: 8}, logger)

	// The CPU semaphore should be max(1, GOMAXPROCS/2).
	// With at least GOMAXPROCS=2, that's >= 1.
	if cap(s.cpuSem) < 1 {
		t.Errorf("CPU semaphore capacity %d, want >= 1", cap(s.cpuSem))
	}
}

func TestCompactor_SegmentsByLevelPartition(t *testing.T) {
	c := NewCompactor(testLogger())

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Add segments across two partitions.
	for i := 0; i < 3; i++ {
		seg := makeSegment(t, fmt.Sprintf("p1-s%d", i), "main", L0,
			makeEvents(base.Add(time.Duration(i)*10*time.Second), 5, "web-01"))
		seg.Meta.Partition = "2024-01-01"
		c.AddSegment(seg)
	}
	for i := 0; i < 2; i++ {
		seg := makeSegment(t, fmt.Sprintf("p2-s%d", i), "main", L0,
			makeEvents(base.Add(24*time.Hour+time.Duration(i)*10*time.Second), 5, "web-01"))
		seg.Meta.Partition = "2024-01-02"
		c.AddSegment(seg)
	}

	p1L0 := c.SegmentsByLevelPartition("main", "2024-01-01", L0)
	if len(p1L0) != 3 {
		t.Errorf("partition 2024-01-01 L0 count: got %d, want 3", len(p1L0))
	}

	p2L0 := c.SegmentsByLevelPartition("main", "2024-01-02", L0)
	if len(p2L0) != 2 {
		t.Errorf("partition 2024-01-02 L0 count: got %d, want 2", len(p2L0))
	}

	// No L1 segments.
	p1L1 := c.SegmentsByLevelPartition("main", "2024-01-01", L1)
	if len(p1L1) != 0 {
		t.Errorf("partition 2024-01-01 L1 count: got %d, want 0", len(p1L1))
	}
}
