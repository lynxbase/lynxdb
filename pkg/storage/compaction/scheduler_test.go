package compaction

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/model"
)

func TestSizeTiered_GroupsBySize(t *testing.T) {
	st := &SizeTiered{Threshold: 4}

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create 4 small segments (~500KB each) and 2 large segments (~50MB each).
	var segs []*SegmentInfo
	for i := 0; i < 4; i++ {
		segs = append(segs, &SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("small-%d", i),
				Level:     L0,
				SizeBytes: 500 << 10, // 500KB
				MinTime:   base.Add(time.Duration(i) * time.Hour),
			},
		})
	}
	for i := 0; i < 2; i++ {
		segs = append(segs, &SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("large-%d", i),
				Level:     L0,
				SizeBytes: 50 << 20, // 50MB
				MinTime:   base.Add(time.Duration(i+4) * time.Hour),
			},
		})
	}

	plans := st.Plan(segs)
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan (from small tier), got %d", len(plans))
	}

	plan := plans[0]
	if plan.OutputLevel != L1 {
		t.Errorf("output level: got %d, want %d", plan.OutputLevel, L1)
	}
	if len(plan.InputSegments) != 4 {
		t.Errorf("input count: got %d, want 4", len(plan.InputSegments))
	}

	// Verify all inputs are small segments.
	for _, s := range plan.InputSegments {
		if s.Meta.SizeBytes != 500<<10 {
			t.Errorf("unexpected segment size: %d", s.Meta.SizeBytes)
		}
	}
}

func TestSizeTiered_SkipsNonL0(t *testing.T) {
	st := &SizeTiered{Threshold: 4}

	var segs []*SegmentInfo
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 6; i++ {
		segs = append(segs, &SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("l1-%d", i),
				Level:     L1, // not L0
				SizeBytes: 1 << 20,
				MinTime:   base.Add(time.Duration(i) * time.Hour),
			},
		})
	}

	plans := st.Plan(segs)
	if len(plans) != 0 {
		t.Errorf("expected no plans for L1 segments, got %d", len(plans))
	}
}

func TestSizeTiered_MultipleTiers(t *testing.T) {
	st := &SizeTiered{Threshold: 4}

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	var segs []*SegmentInfo
	// 4 small (500KB) + 4 medium (5MB)
	for i := 0; i < 4; i++ {
		segs = append(segs, &SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("small-%d", i),
				Level:     L0,
				SizeBytes: 500 << 10,
				MinTime:   base.Add(time.Duration(i) * time.Hour),
			},
		})
	}
	for i := 0; i < 4; i++ {
		segs = append(segs, &SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("medium-%d", i),
				Level:     L0,
				SizeBytes: 5 << 20,
				MinTime:   base.Add(time.Duration(i+4) * time.Hour),
			},
		})
	}

	plans := st.Plan(segs)
	if len(plans) != 2 {
		t.Fatalf("expected 2 plans (one per tier), got %d", len(plans))
	}
}

func TestLevelBased_TimePartitioned(t *testing.T) {
	lb := &LevelBased{Threshold: 4, TargetSize: 1 << 30}

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	var segs []*SegmentInfo
	for i := 0; i < 4; i++ {
		segs = append(segs, &SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("l1-%d", i),
				Level:     L1,
				SizeBytes: 100 << 20, // 100MB each
				MinTime:   base.Add(time.Duration(i) * time.Hour),
				MaxTime:   base.Add(time.Duration(i)*time.Hour + 59*time.Minute),
			},
		})
	}

	plans := lb.Plan(segs)
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}

	plan := plans[0]
	if plan.OutputLevel != L2 {
		t.Errorf("output level: got %d, want %d", plan.OutputLevel, L2)
	}
	if len(plan.InputSegments) != 4 {
		t.Errorf("input count: got %d, want 4", len(plan.InputSegments))
	}

	// Verify inputs are sorted by MinTime.
	for i := 1; i < len(plan.InputSegments); i++ {
		if plan.InputSegments[i].Meta.MinTime.Before(plan.InputSegments[i-1].Meta.MinTime) {
			t.Errorf("inputs not sorted by time at position %d", i)
		}
	}
}

func TestLevelBased_SplitsByTargetSize(t *testing.T) {
	lb := &LevelBased{Threshold: 2, TargetSize: 300 << 20} // 300MB target

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	var segs []*SegmentInfo
	for i := 0; i < 6; i++ {
		segs = append(segs, &SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("l1-%d", i),
				Level:     L1,
				SizeBytes: 200 << 20, // 200MB each
				MinTime:   base.Add(time.Duration(i) * time.Hour),
				MaxTime:   base.Add(time.Duration(i)*time.Hour + 59*time.Minute),
			},
		})
	}

	plans := lb.Plan(segs)
	// 6 x 200MB with 300MB target → groups of 2 (200+200=400 >= 300)
	if len(plans) != 3 {
		t.Fatalf("expected 3 plans, got %d", len(plans))
	}

	for _, plan := range plans {
		if plan.OutputLevel != L2 {
			t.Errorf("output level: got %d, want %d", plan.OutputLevel, L2)
		}
	}
}

func TestLevelBased_SkipsNonL1(t *testing.T) {
	lb := &LevelBased{Threshold: 4}

	var segs []*SegmentInfo
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 6; i++ {
		segs = append(segs, &SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("l0-%d", i),
				Level:     L0,
				SizeBytes: 1 << 20,
				MinTime:   base.Add(time.Duration(i) * time.Hour),
			},
		})
	}

	plans := lb.Plan(segs)
	if len(plans) != 0 {
		t.Errorf("expected no plans for L0 segments, got %d", len(plans))
	}
}

func TestCompactor_PlanAllCompactions(t *testing.T) {
	c := NewCompactor(testLogger())
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Add 4 L0 + 4 L1 segments.
	for i := 0; i < 4; i++ {
		c.AddSegment(&SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("l0-%d", i),
				Index:     "main",
				Level:     L0,
				SizeBytes: 1 << 20,
				MinTime:   base.Add(time.Duration(i) * time.Hour),
			},
			Data: makeSegment(t, fmt.Sprintf("l0-%d", i), "main", L0,
				makeEvents(base.Add(time.Duration(i)*100*time.Second), 10, "web-01")).Data,
		})
	}
	for i := 0; i < 4; i++ {
		c.AddSegment(&SegmentInfo{
			Meta: model.SegmentMeta{
				ID:        fmt.Sprintf("l1-%d", i),
				Index:     "main",
				Level:     L1,
				SizeBytes: 10 << 20,
				MinTime:   base.Add(time.Duration(i+4) * time.Hour),
			},
			Data: makeSegment(t, fmt.Sprintf("l1-%d", i), "main", L1,
				makeEvents(base.Add(time.Duration(i+4)*100*time.Second), 10, "web-01")).Data,
		})
	}

	jobs := c.PlanAllCompactions("main")
	if len(jobs) < 2 {
		t.Fatalf("expected >= 2 jobs, got %d", len(jobs))
	}

	// First job should be L0→L1 (highest priority).
	if jobs[0].Priority != PriorityL0ToL1 {
		t.Errorf("first job priority: got %d, want %d", jobs[0].Priority, PriorityL0ToL1)
	}
	// Second job should be L1→L2.
	if jobs[1].Priority != PriorityL1ToL2 {
		t.Errorf("second job priority: got %d, want %d", jobs[1].Priority, PriorityL1ToL2)
	}
}

func TestScheduler_PriorityOrdering(t *testing.T) {
	c := NewCompactor(testLogger())
	sched := NewScheduler(c, SchedulerConfig{Workers: 1, RateBytesPerSec: 1 << 30}, testLogger())

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Submit jobs in reverse priority order.
	jobs := []*Job{
		{Plan: &Plan{OutputLevel: L2}, Priority: PriorityL1ToL2, Index: "main"},
		{Plan: &Plan{OutputLevel: L2}, Priority: PriorityMaint, Index: "main"},
		{Plan: &Plan{OutputLevel: L1}, Priority: PriorityL0ToL1, Index: "main"},
	}

	var mu sync.Mutex
	var order []JobPriority

	sched.SetOnComplete(func(job *Job, _ *SegmentInfo, _ error) {
		mu.Lock()
		order = append(order, job.Priority)
		mu.Unlock()
	})

	// Create minimal valid segments for each job.
	for i, job := range jobs {
		seg := makeSegment(t, fmt.Sprintf("s%d", i), "main", L0,
			makeEvents(base.Add(time.Duration(i)*100*time.Second), 5, "web-01"))
		job.Plan.InputSegments = []*SegmentInfo{seg}
	}

	sched.SubmitAll(jobs)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sched.Start(ctx)

	// Wait for all jobs to complete.
	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		n := len(order)
		mu.Unlock()
		if n >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for jobs")
		case <-time.After(10 * time.Millisecond):
		}
	}

	sched.Stop()

	// Verify priority ordering.
	if order[0] != PriorityL0ToL1 {
		t.Errorf("first job: got priority %d, want %d", order[0], PriorityL0ToL1)
	}
	if order[1] != PriorityL1ToL2 {
		t.Errorf("second job: got priority %d, want %d", order[1], PriorityL1ToL2)
	}
	if order[2] != PriorityMaint {
		t.Errorf("third job: got priority %d, want %d", order[2], PriorityMaint)
	}
}

func TestTokenBucket_RateLimiting(t *testing.T) {
	// Rate: 10MB/s.
	tb := NewTokenBucket(10 << 20)

	// Consume initial tokens.
	ctx := context.Background()
	tb.Wait(ctx, 10<<20) // drain all initial tokens

	// Now consuming 20MB should take ~2 seconds.
	start := time.Now()
	tb.Wait(ctx, 20<<20)
	elapsed := time.Since(start)

	if elapsed < 1500*time.Millisecond {
		t.Errorf("rate limiter too fast: elapsed %v, expected ~2s", elapsed)
	}
	if elapsed > 4*time.Second {
		t.Errorf("rate limiter too slow: elapsed %v, expected ~2s", elapsed)
	}
}

func TestTokenBucket_TryConsume(t *testing.T) {
	tb := NewTokenBucket(100)

	if !tb.TryConsume(50) {
		t.Error("should consume 50 tokens")
	}
	if !tb.TryConsume(50) {
		t.Error("should consume another 50 tokens")
	}
	if tb.TryConsume(50) {
		t.Error("should fail — no tokens left")
	}
}

func TestTokenBucket_ContextCancellation(t *testing.T) {
	tb := NewTokenBucket(1) // very slow: 1 byte/sec

	// Drain initial tokens.
	tb.TryConsume(2)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	tb.Wait(ctx, 1<<30) // request 1GB — will be canceled quickly
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Errorf("should have returned quickly due to context cancellation, took %v", elapsed)
	}
}

func TestScheduler_MultipleWorkers(t *testing.T) {
	c := NewCompactor(testLogger())
	sched := NewScheduler(c, SchedulerConfig{Workers: 2, RateBytesPerSec: 1 << 30}, testLogger())

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	var mu sync.Mutex
	var completed int

	sched.SetOnComplete(func(_ *Job, _ *SegmentInfo, _ error) {
		mu.Lock()
		completed++
		mu.Unlock()
	})

	// Submit 4 jobs.
	for i := 0; i < 4; i++ {
		seg := makeSegment(t, fmt.Sprintf("s%d", i), "main", L0,
			makeEvents(base.Add(time.Duration(i)*100*time.Second), 5, "web-01"))
		sched.Submit(&Job{
			Plan:     &Plan{InputSegments: []*SegmentInfo{seg}, OutputLevel: L1},
			Priority: PriorityL0ToL1,
			Index:    "main",
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sched.Start(ctx)

	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		n := completed
		mu.Unlock()
		if n >= 4 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout: completed %d/4", completed)
		case <-time.After(10 * time.Millisecond):
		}
	}

	sched.Stop()
}

func BenchmarkCompactionThroughput(b *testing.B) {
	c := NewCompactor(testLogger())
	ctx := context.Background()

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Pre-build segments using a helper that accepts testing.TB.
	var segs []*SegmentInfo
	for i := 0; i < 4; i++ {
		segs = append(segs, makeSegment(b, fmt.Sprintf("s%d", i), "main", L0,
			makeEvents(base.Add(time.Duration(i)*1000*time.Second), 1000, "web-01")))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plan := &Plan{InputSegments: segs, OutputLevel: L1}
		_, err := c.Execute(ctx, plan)
		if err != nil {
			b.Fatal(err)
		}
	}
}
