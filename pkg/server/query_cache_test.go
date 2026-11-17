package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/planner"
)

func TestQueryCacheUsesGenerationAfterPreQueryFlush(t *testing.T) {
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

	base := time.Now().UTC()
	events := make([]*event.Event, 8)
	for i := range events {
		ev := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("event-%d", i))
		ev.Index = "main"
		events[i] = ev
	}
	if err := e.Ingest(events); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	query := "source=main | stats count() as count"
	first := submitAndWait(t, e, query)
	if first.Stats.CacheHit {
		t.Fatalf("first query unexpectedly hit cache")
	}
	if first.Status != JobStatusDone {
		t.Fatalf("first query status = %s, error=%s", first.Status, first.Error)
	}

	second := submitAndWait(t, e, query)
	if second.Status != JobStatusDone {
		t.Fatalf("second query status = %s, error=%s", second.Status, second.Error)
	}
	if !second.Stats.CacheHit {
		t.Fatalf("second query did not hit cache")
	}
}

func TestQueryCacheSkippedWhenRequested(t *testing.T) {
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

	base := time.Now().UTC()
	events := make([]*event.Event, 3)
	for i := range events {
		ev := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("dynamic-%d", i))
		ev.Index = "main"
		events[i] = ev
	}
	if err := e.Ingest(events); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	for i := 0; i < 2; i++ {
		got := submitAndWaitWithSkipResultCache(t, e, "source=main | stats count() as count", true)
		if got.Status != JobStatusDone {
			t.Fatalf("query[%d] status = %s, error=%s", i, got.Status, got.Error)
		}
		if got.Stats.CacheHit {
			t.Fatalf("query[%d] unexpectedly hit cache", i)
		}
		if entries := e.CacheStats().EntryCount; entries != 0 {
			t.Fatalf("query[%d] cache entries = %d, want 0", i, entries)
		}
	}
}

func TestQueryReadsBufferedEventsWithoutCreatingParts(t *testing.T) {
	queryCfg := config.DefaultConfig().Query
	queryCfg.SpillDir = t.TempDir()
	fsync := false
	storageCfg := config.DefaultConfig().Storage
	storageCfg.FlushThreshold = 1 * config.GB
	storageCfg.FlushIdleTimeout = time.Hour

	e := NewEngine(Config{
		DataDir: t.TempDir(),
		Storage: storageCfg,
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

	base := time.Now().UTC()
	events := make([]*event.Event, 3)
	for i := range events {
		ev := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("buffered-%d", i))
		ev.Index = "main"
		events[i] = ev
	}
	if err := e.Ingest(events); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	if got := e.partRegistry.Count(); got != 0 {
		t.Fatalf("parts before query: got %d, want 0", got)
	}

	for i := 0; i < 3; i++ {
		result := submitAndWait(t, e, "source=main | stats count() as count")
		if result.Status != JobStatusDone {
			t.Fatalf("query[%d] status = %s, error=%s", i, result.Status, result.Error)
		}
		if got := e.partRegistry.Count(); got != 0 {
			t.Fatalf("parts after query[%d]: got %d, want 0", i, got)
		}
	}
}

func submitAndWait(t *testing.T, e *Engine, query string) JobSnapshot {
	return submitAndWaitWithSkipResultCache(t, e, query, false)
}

func submitAndWaitWithSkipResultCache(t *testing.T, e *Engine, query string, skipResultCache bool) JobSnapshot {
	t.Helper()

	p := planner.New()
	result, err := p.Plan(planner.PlanRequest{Query: query})
	if err != nil {
		t.Fatalf("plan query: %v", err)
	}
	prog := result.Plan

	job, err := e.SubmitQuery(context.Background(), QueryParams{
		Query:           query,
		Plan:            prog,
		Hints:           result.Hints,
		SkipResultCache: skipResultCache,
		ResultType:      DetectResultType(prog),
	})
	if err != nil {
		t.Fatalf("submit query: %v", err)
	}

	select {
	case <-job.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("query timed out")
	}

	e.jobsWG.Wait()
	return job.Snapshot()
}
