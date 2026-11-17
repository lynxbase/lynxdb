package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/planner"
)

// TestTailCatchupOverFlushedSegments verifies that BuildStreamingPipeline
// returns events that live only in flushed segments (not in the batcher
// buffer). This is the tail catchup scenario: after FlushBatcher, the
// buffer is empty and all data lives in .lsg segments. The streaming
// pipeline must read those segments.
func TestTailCatchupOverFlushedSegments(t *testing.T) {
	queryCfg := config.DefaultConfig().Query
	queryCfg.SpillDir = t.TempDir()
	fsync := false

	e := NewEngine(Config{
		DataDir: t.TempDir(),
		Storage: config.DefaultConfig().Storage,
		Logger:  discardLogger(),
		Query:   queryCfg,
		Ingest:  config.IngestConfig{FSync: &fsync},
	})

	ctx, cancel := context.WithCancel(context.Background())
	if err := e.Start(ctx); err != nil {
		cancel()
		t.Fatalf("engine start: %v", err)
	}
	defer cancel()
	defer func() {
		if err := e.Shutdown(5 * time.Second); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	}()

	const eventCount = 12
	base := time.Now().UTC()
	events := make([]*event.Event, eventCount)
	for i := range events {
		ev := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("tail-event-%d", i))
		ev.Index = "main"
		ev.Fields = map[string]event.Value{
			"level": event.StringValue("warn"),
		}
		events[i] = ev
	}
	if err := e.Ingest(events); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// Flush: buffer → segments.
	if err := e.FlushBatcher(); err != nil {
		t.Fatalf("flush batcher: %v", err)
	}
	if n := e.BufferedEventCount(); n != 0 {
		t.Fatalf("buffered events after flush = %d, want 0", n)
	}
	if n := e.SegmentCount(); n == 0 {
		t.Fatalf("segment count after flush = 0, want > 0")
	}

	// Plan a query with hints (mirrors TailService.Plan + catchupRing).
	p := planner.New()
	plan, err := p.Plan(planner.PlanRequest{
		Query: "source=main",
	})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	// BuildStreamingPipeline with hints — the new signature.
	// No external time bounds: we want to test that ALL flushed events are read.
	iter, stats, err := e.BuildStreamingPipeline(ctx, plan.Plan, plan.Hints, nil)
	if err != nil {
		t.Fatalf("BuildStreamingPipeline: %v", err)
	}
	defer iter.Close()

	// Drain the iterator.
	var rows int
	for {
		batch, bErr := iter.Next(ctx)
		if bErr != nil {
			t.Fatalf("Next: %v", bErr)
		}
		if batch == nil {
			break
		}
		rows += batch.Len
	}

	if rows != eventCount {
		t.Fatalf("tail catchup returned %d rows, want %d (segment-resident data lost)", rows, eventCount)
	}

	// Stats should show segments were present.
	if stats.SegmentsTotal == 0 {
		t.Errorf("SegmentsTotal = 0, want > 0")
	}
}

// TestBuildStreamingPipelineWithNilHints verifies backward compatibility:
// passing nil hints should not panic or error.
func TestBuildStreamingPipelineWithNilHints(t *testing.T) {
	queryCfg := config.DefaultConfig().Query
	queryCfg.SpillDir = t.TempDir()
	fsync := false

	e := NewEngine(Config{
		DataDir: t.TempDir(),
		Storage: config.DefaultConfig().Storage,
		Logger:  discardLogger(),
		Query:   queryCfg,
		Ingest:  config.IngestConfig{FSync: &fsync},
	})

	ctx, cancel := context.WithCancel(context.Background())
	if err := e.Start(ctx); err != nil {
		cancel()
		t.Fatalf("engine start: %v", err)
	}
	defer cancel()
	defer func() {
		if err := e.Shutdown(5 * time.Second); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	}()

	p := planner.New()
	plan, err := p.Plan(planner.PlanRequest{Query: "source=main"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	// nil hints — should default to empty hints.
	iter, _, err := e.BuildStreamingPipeline(ctx, plan.Plan, nil, nil)
	if err != nil {
		t.Fatalf("BuildStreamingPipeline with nil hints: %v", err)
	}
	defer iter.Close()

	// Drain — should return zero rows (no data ingested).
	for {
		batch, bErr := iter.Next(ctx)
		if bErr != nil {
			t.Fatalf("Next: %v", bErr)
		}
		if batch == nil {
			break
		}
	}
}

// TestBuildStreamingPipelineEpochUnpin verifies that closing the returned
// iterator does not panic (epoch unpin is safe). We call Close() twice to
// verify the double-close guard.
func TestBuildStreamingPipelineEpochUnpin(t *testing.T) {
	queryCfg := config.DefaultConfig().Query
	queryCfg.SpillDir = t.TempDir()
	fsync := false

	e := NewEngine(Config{
		DataDir: t.TempDir(),
		Storage: config.DefaultConfig().Storage,
		Logger:  discardLogger(),
		Query:   queryCfg,
		Ingest:  config.IngestConfig{FSync: &fsync},
	})

	ctx, cancel := context.WithCancel(context.Background())
	if err := e.Start(ctx); err != nil {
		cancel()
		t.Fatalf("engine start: %v", err)
	}
	defer cancel()
	defer func() {
		if err := e.Shutdown(5 * time.Second); err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	}()

	p := planner.New()
	plan, err := p.Plan(planner.PlanRequest{Query: "source=main"})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	hints := &model.QueryHints{}
	iter, _, err := e.BuildStreamingPipeline(ctx, plan.Plan, hints, nil)
	if err != nil {
		t.Fatalf("BuildStreamingPipeline: %v", err)
	}

	// Close once.
	if err := iter.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Close again — should not panic.
	if err := iter.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
