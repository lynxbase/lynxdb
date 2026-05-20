package compaction

import (
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/model"
)

// smallL2 builds an L2 SegmentInfo with the given size and a deterministic
// MinTime offset. No segment bytes are produced -- IntraL2.Plan inspects
// Meta only.
func smallL2(id string, sizeBytes int64, offset time.Duration) *SegmentInfo {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return &SegmentInfo{
		Meta: model.SegmentMeta{
			ID:         id,
			Index:      "main",
			Partition:  "2026-01-01",
			MinTime:    base.Add(offset),
			MaxTime:    base.Add(offset).Add(time.Minute),
			SizeBytes:  sizeBytes,
			Level:      L2,
			EventCount: 100,
			CreatedAt:  base,
		},
	}
}

func TestIntraL2_BelowThreshold(t *testing.T) {
	il := &IntraL2{Threshold: 4, TargetSize: 1 << 30, MinSize: 512 << 20}
	segs := []*SegmentInfo{
		smallL2("a", 10<<20, 0),
		smallL2("b", 10<<20, time.Hour),
		smallL2("c", 10<<20, 2*time.Hour),
	}
	if plans := il.Plan(segs); plans != nil {
		t.Fatalf("expected no plan below threshold, got %d plans", len(plans))
	}
}

func TestIntraL2_GroupsSmallParts(t *testing.T) {
	il := &IntraL2{Threshold: 4, TargetSize: 1 << 30, MinSize: 512 << 20}
	// 8 small L2 parts of 10MB each. Total 80MB -- well below TargetSize,
	// so a single group is emitted that still meets the count threshold.
	var segs []*SegmentInfo
	for i := 0; i < 8; i++ {
		segs = append(segs, smallL2(fmt.Sprintf("s%d", i), 10<<20, time.Duration(i)*time.Hour))
	}
	plans := il.Plan(segs)
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	if len(plans[0].InputSegments) != 8 {
		t.Errorf("expected 8 inputs, got %d", len(plans[0].InputSegments))
	}
	if plans[0].OutputLevel != L2 {
		t.Errorf("output level: got %d, want L2", plans[0].OutputLevel)
	}
}

func TestIntraL2_SplitsAtTargetSize(t *testing.T) {
	// Use a small target so a few parts fill a group quickly.
	il := &IntraL2{Threshold: 4, TargetSize: 40 << 20, MinSize: 30 << 20}
	// 8 parts × 10MB = 80MB total. Each group should close at 40MB (4 parts).
	var segs []*SegmentInfo
	for i := 0; i < 8; i++ {
		segs = append(segs, smallL2(fmt.Sprintf("s%d", i), 10<<20, time.Duration(i)*time.Hour))
	}
	plans := il.Plan(segs)
	if len(plans) != 2 {
		t.Fatalf("expected 2 plans, got %d", len(plans))
	}
	for i, p := range plans {
		if len(p.InputSegments) != 4 {
			t.Errorf("plan %d: expected 4 inputs, got %d", i, len(p.InputSegments))
		}
	}
}

func TestIntraL2_SkipsLargeL2(t *testing.T) {
	il := &IntraL2{Threshold: 4, TargetSize: 1 << 30, MinSize: 512 << 20}
	// 3 large parts (> MinSize) + 2 small. Large ones are filtered out, only
	// 2 small remain -- below threshold -> no plan.
	segs := []*SegmentInfo{
		smallL2("big-a", 800<<20, 0),
		smallL2("big-b", 800<<20, time.Hour),
		smallL2("big-c", 800<<20, 2*time.Hour),
		smallL2("tiny-a", 10<<20, 3*time.Hour),
		smallL2("tiny-b", 10<<20, 4*time.Hour),
	}
	if plans := il.Plan(segs); plans != nil {
		t.Fatalf("expected no plan when small parts < threshold, got %d", len(plans))
	}
}

func TestIntraL2_IgnoresOtherLevels(t *testing.T) {
	il := &IntraL2{Threshold: 2, TargetSize: 1 << 30, MinSize: 512 << 20}
	segs := []*SegmentInfo{
		{Meta: model.SegmentMeta{ID: "l0-a", Level: L0, SizeBytes: 10 << 20, MinTime: time.Now()}},
		{Meta: model.SegmentMeta{ID: "l0-b", Level: L0, SizeBytes: 10 << 20, MinTime: time.Now()}},
		{Meta: model.SegmentMeta{ID: "l1-a", Level: L1, SizeBytes: 10 << 20, MinTime: time.Now()}},
		{Meta: model.SegmentMeta{ID: "l1-b", Level: L1, SizeBytes: 10 << 20, MinTime: time.Now()}},
	}
	if plans := il.Plan(segs); plans != nil {
		t.Fatalf("expected no plan when no L2 parts present, got %d", len(plans))
	}
}

func TestIntraL2_DropsTrailingShortGroup(t *testing.T) {
	// TargetSize 40MB, MinSize 30MB. 6 × 10MB parts.
	// Group 1: 4 parts (40MB) -> plan.
	// Group 2: 2 parts (20MB) -> below threshold of 4 -> dropped.
	il := &IntraL2{Threshold: 4, TargetSize: 40 << 20, MinSize: 30 << 20}
	var segs []*SegmentInfo
	for i := 0; i < 6; i++ {
		segs = append(segs, smallL2(fmt.Sprintf("s%d", i), 10<<20, time.Duration(i)*time.Hour))
	}
	plans := il.Plan(segs)
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan (trailing partial group dropped), got %d", len(plans))
	}
	if len(plans[0].InputSegments) != 4 {
		t.Errorf("expected 4 inputs in first plan, got %d", len(plans[0].InputSegments))
	}
}

func TestIntraL2_DefaultsApplied(t *testing.T) {
	il := &IntraL2{} // no fields set
	// 4 small L2 parts at default MinSize=L2TargetSize/2.
	var segs []*SegmentInfo
	for i := 0; i < 4; i++ {
		segs = append(segs, smallL2(fmt.Sprintf("s%d", i), 10<<20, time.Duration(i)*time.Hour))
	}
	plans := il.Plan(segs)
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan with defaults, got %d", len(plans))
	}
}

func TestCompactor_PlanCompaction_IntraL2(t *testing.T) {
	c := NewCompactor(testLogger())

	// 4 small L2 parts in the same partition. No L0 / L1 plans available,
	// so PlanCompaction should fall through to IntraL2.
	for i := 0; i < L1CompactionThreshold; i++ {
		c.AddSegment(smallL2(fmt.Sprintf("l2-%d", i), 10<<20, time.Duration(i)*time.Hour))
	}

	plan := c.PlanCompaction("main")
	if plan == nil {
		t.Fatal("expected intra-L2 compaction plan")
	}
	if plan.OutputLevel != L2 {
		t.Errorf("output level: got %d, want L2", plan.OutputLevel)
	}
	if len(plan.InputSegments) < L1CompactionThreshold {
		t.Errorf("input count: got %d, want >= %d", len(plan.InputSegments), L1CompactionThreshold)
	}
	for _, seg := range plan.InputSegments {
		if seg.Meta.Level != L2 {
			t.Errorf("input level: got %d, want L2", seg.Meta.Level)
		}
	}
}

func TestCompactor_PlanAllCompactions_IntraL2Priority(t *testing.T) {
	c := NewCompactor(testLogger())
	for i := 0; i < L1CompactionThreshold; i++ {
		c.AddSegment(smallL2(fmt.Sprintf("l2-%d", i), 10<<20, time.Duration(i)*time.Hour))
	}

	jobs := c.PlanAllCompactions("main")
	if len(jobs) == 0 {
		t.Fatal("expected at least one job")
	}
	found := false
	for _, j := range jobs {
		if j.Priority == PriorityIntraL2 {
			found = true
			if j.Plan.OutputLevel != L2 {
				t.Errorf("intra-L2 output level: got %d, want L2", j.Plan.OutputLevel)
			}
		}
	}
	if !found {
		t.Errorf("expected an IntraL2 job, got jobs with priorities: %v", jobPriorities(jobs))
	}
}

func jobPriorities(jobs []*Job) []JobPriority {
	out := make([]JobPriority, len(jobs))
	for i, j := range jobs {
		out[i] = j.Priority
	}
	return out
}
