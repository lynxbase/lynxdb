package server

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// discardLogger returns a logger that suppresses all output below error level.
// Used in tests to keep output clean while still surfacing real problems.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// newTestEngine creates an in-memory Engine suitable for unit tests.
func newTestEngine(t *testing.T) *Engine {
	t.Helper()

	queryCfg := config.DefaultConfig().Query
	queryCfg.SpillDir = t.TempDir()

	cfg := Config{
		DataDir: "",
		Storage: config.DefaultConfig().Storage,
		Logger:  discardLogger(),
		Query:   queryCfg,
	}

	e := NewEngine(cfg)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("engine start: %v", err)
	}

	t.Cleanup(func() { _ = e.Shutdown(5 * time.Second) })

	return e
}

// ingestEvents injects events directly into the engine with the given source.
func ingestEvents(t *testing.T, e *Engine, source string, count int) {
	t.Helper()

	now := time.Now()
	events := make([]*event.Event, count)

	for i := 0; i < count; i++ {
		ev := &event.Event{
			Time:   now.Add(time.Duration(i) * time.Millisecond),
			Raw:    "test event from " + source,
			Source: source,
			Index:  source,
			Fields: map[string]event.Value{
				"source": event.StringValue(source),
				"level":  event.StringValue("info"),
			},
		}
		events[i] = ev
	}

	if err := e.Ingest(events); err != nil {
		t.Fatalf("ingest %s: %v", source, err)
	}
}

func TestMultiSource_SourceRegistryPopulated(t *testing.T) {
	e := newTestEngine(t)

	ingestEvents(t, e, "nginx", 5)
	ingestEvents(t, e, "postgres", 3)
	ingestEvents(t, e, "redis", 2)

	reg := e.SourceRegistry()
	if reg.Count() != 3 {
		t.Fatalf("expected 3 sources, got %d", reg.Count())
	}
	if !reg.Contains("nginx") {
		t.Error("expected registry to contain nginx")
	}
	if !reg.Contains("postgres") {
		t.Error("expected registry to contain postgres")
	}
	if !reg.Contains("redis") {
		t.Error("expected registry to contain redis")
	}
}

func TestMultiSource_MatchesSourceScope_SingleIndex(t *testing.T) {
	// Single IndexName: existing behavior, should match only that index.
	hints := &spl2.QueryHints{IndexName: "nginx"}
	if !matchesSourceScope("nginx", hints) {
		t.Error("expected nginx to match IndexName=nginx")
	}
	if matchesSourceScope("postgres", hints) {
		t.Error("expected postgres NOT to match IndexName=nginx")
	}
}

func TestMultiSource_MatchesSourceScope_NoFilter(t *testing.T) {
	// No filter: should match everything.
	hints := &spl2.QueryHints{}
	if !matchesSourceScope("nginx", hints) {
		t.Error("expected nginx to match with no filter")
	}
	if !matchesSourceScope("anything", hints) {
		t.Error("expected anything to match with no filter")
	}
}

func TestMultiSource_MatchesSourceScope_List(t *testing.T) {
	hints := &spl2.QueryHints{
		SourceScopeType:    spl2.SourceScopeList,
		SourceScopeSources: []string{"nginx", "postgres"},
	}
	if !matchesSourceScope("nginx", hints) {
		t.Error("expected nginx to match list [nginx, postgres]")
	}
	if !matchesSourceScope("postgres", hints) {
		t.Error("expected postgres to match list [nginx, postgres]")
	}
	if matchesSourceScope("redis", hints) {
		t.Error("expected redis NOT to match list [nginx, postgres]")
	}
}

func TestMultiSource_MatchesSourceScope_Glob(t *testing.T) {
	hints := &spl2.QueryHints{
		SourceScopeType:    spl2.SourceScopeGlob,
		SourceScopePattern: "log*",
	}
	if !matchesSourceScope("logs-web", hints) {
		t.Error("expected logs-web to match glob log*")
	}
	if matchesSourceScope("nginx", hints) {
		t.Error("expected nginx NOT to match glob log*")
	}
}

func TestMultiSource_MatchesSourceScope_All(t *testing.T) {
	hints := &spl2.QueryHints{
		SourceScopeType: spl2.SourceScopeAll,
	}
	if !matchesSourceScope("anything", hints) {
		t.Error("expected anything to match all")
	}
}

func TestMultiSource_MatchesSourceScope_SourceIndices(t *testing.T) {
	// Parser-level SourceIndices (FROM a, b, c).
	hints := &spl2.QueryHints{
		SourceIndices: []string{"nginx", "redis"},
	}
	if !matchesSourceScope("nginx", hints) {
		t.Error("expected nginx to match SourceIndices")
	}
	if !matchesSourceScope("redis", hints) {
		t.Error("expected redis to match SourceIndices")
	}
	if matchesSourceScope("postgres", hints) {
		t.Error("expected postgres NOT to match SourceIndices")
	}
}

func TestMultiSource_MatchesSourceScope_SourceGlob(t *testing.T) {
	// Parser-level SourceGlob (FROM logs*).
	hints := &spl2.QueryHints{
		SourceGlob: "logs*",
	}
	if !matchesSourceScope("logs-web", hints) {
		t.Error("expected logs-web to match SourceGlob=logs*")
	}
	if matchesSourceScope("nginx", hints) {
		t.Error("expected nginx NOT to match SourceGlob=logs*")
	}
}

func TestMultiSource_MatchesSourceScope_SourceGlobStar(t *testing.T) {
	// FROM * — match everything.
	hints := &spl2.QueryHints{
		SourceGlob: "*",
	}
	if !matchesSourceScope("anything", hints) {
		t.Error("expected anything to match SourceGlob=*")
	}
}

func TestMultiSource_ResolveSourceScope(t *testing.T) {
	e := newTestEngine(t)

	// Register sources.
	e.sourceRegistry.Register("logs-web")
	e.sourceRegistry.Register("logs-api")
	e.sourceRegistry.Register("logs-db")
	e.sourceRegistry.Register("metrics")

	// Test glob resolution.
	hints := &spl2.QueryHints{
		SourceScopeType:    spl2.SourceScopeGlob,
		SourceScopePattern: "logs*",
	}
	resolved, _ := e.resolveSourceScope(hints)
	if resolved.SourceScopeType != spl2.SourceScopeList {
		t.Fatalf("expected resolved type to be list, got %s", resolved.SourceScopeType)
	}
	if len(resolved.SourceScopeSources) != 3 {
		t.Fatalf("expected 3 matched sources, got %d: %v",
			len(resolved.SourceScopeSources), resolved.SourceScopeSources)
	}

	// Test parser-level glob resolution.
	hintsGlob := &spl2.QueryHints{
		SourceGlob: "logs*",
	}
	resolvedGlob, _ := e.resolveSourceScope(hintsGlob)
	if resolvedGlob.SourceScopeType != spl2.SourceScopeList {
		t.Fatalf("expected resolved type to be list, got %s", resolvedGlob.SourceScopeType)
	}
	if len(resolvedGlob.SourceScopeSources) != 3 {
		t.Fatalf("expected 3 matched sources, got %d", len(resolvedGlob.SourceScopeSources))
	}

	// Test no-match glob: should return original hints with warning.
	hintsNoMatch := &spl2.QueryHints{
		SourceScopeType:    spl2.SourceScopeGlob,
		SourceScopePattern: "nonexistent*",
	}
	resolvedNoMatch, noMatchWarnings := e.resolveSourceScope(hintsNoMatch)
	if resolvedNoMatch != hintsNoMatch {
		t.Error("expected no-match glob to return original hints")
	}
	if len(noMatchWarnings) == 0 {
		t.Error("expected warning for no-match glob")
	}

	// Test non-glob: should return original hints.
	hintsPlain := &spl2.QueryHints{IndexName: "nginx"}
	resolvedPlain, _ := e.resolveSourceScope(hintsPlain)
	if resolvedPlain != hintsPlain {
		t.Error("expected non-glob hints to return original hints")
	}
}

func TestMultiSource_SegmentSkipping(t *testing.T) {
	// Verify shouldSkipSegment respects multi-source scope.
	seg := &segmentHandle{
		index: "nginx",
		meta:  model.SegmentMeta{MinTime: time.Now().Add(-time.Hour), MaxTime: time.Now()},
	}

	// List that includes nginx: should NOT skip.
	hints := &spl2.QueryHints{
		SourceScopeType:    spl2.SourceScopeList,
		SourceScopeSources: []string{"nginx", "postgres"},
	}
	var ss storeStats
	if shouldSkipSegment(seg, hints, &ss) {
		t.Error("expected nginx segment NOT to be skipped by list [nginx, postgres]")
	}

	// List that excludes nginx: should skip.
	hintsExclude := &spl2.QueryHints{
		SourceScopeType:    spl2.SourceScopeList,
		SourceScopeSources: []string{"postgres", "redis"},
	}
	ss = storeStats{}
	if !shouldSkipSegment(seg, hintsExclude, &ss) {
		t.Error("expected nginx segment to be skipped by list [postgres, redis]")
	}
	if ss.SegmentsSkippedIdx != 1 {
		t.Errorf("expected SegmentsSkippedIdx=1, got %d", ss.SegmentsSkippedIdx)
	}

	// All scope: should NOT skip.
	hintsAll := &spl2.QueryHints{
		SourceScopeType: spl2.SourceScopeAll,
	}
	ss = storeStats{}
	if shouldSkipSegment(seg, hintsAll, &ss) {
		t.Error("expected nginx segment NOT to be skipped by all scope")
	}
}

func TestMultiSource_SourceIndexSet(t *testing.T) {
	// Short list: should return nil (linear scan is faster).
	h := &spl2.QueryHints{
		SourceScopeSources: []string{"a", "b", "c"},
	}
	if h.SourceIndexSet() != nil {
		t.Error("expected nil for short list")
	}

	// Long list: should return a map.
	long := make([]string, 20)
	for i := range long {
		long[i] = string(rune('a' + i))
	}
	h2 := &spl2.QueryHints{
		SourceScopeSources: long,
	}
	set := h2.SourceIndexSet()
	if set == nil {
		t.Fatal("expected non-nil set for long list")
	}
	if _, ok := set["a"]; !ok {
		t.Error("expected set to contain 'a'")
	}
	if len(set) != 20 {
		t.Errorf("expected 20 entries, got %d", len(set))
	}

	// Second call should return cached set.
	set2 := h2.SourceIndexSet()
	if set2 == nil {
		t.Error("expected cached set on second call")
	}
}

func TestMultiSource_IsMultiSource(t *testing.T) {
	tests := []struct {
		name   string
		hints  *spl2.QueryHints
		expect bool
	}{
		{"nil", nil, false},
		{"empty", &spl2.QueryHints{}, false},
		{"single index", &spl2.QueryHints{IndexName: "main"}, false},
		{"source indices", &spl2.QueryHints{SourceIndices: []string{"a", "b"}}, true},
		{"source glob", &spl2.QueryHints{SourceGlob: "logs*"}, true},
		{"scope all", &spl2.QueryHints{SourceScopeType: spl2.SourceScopeAll}, true},
		{"scope list", &spl2.QueryHints{SourceScopeType: spl2.SourceScopeList}, true},
		{"scope glob", &spl2.QueryHints{SourceScopeType: spl2.SourceScopeGlob}, true},
		{"scope single", &spl2.QueryHints{SourceScopeType: spl2.SourceScopeSingle}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.hints.IsMultiSource()
			if got != tt.expect {
				t.Errorf("IsMultiSource() = %v, want %v", got, tt.expect)
			}
		})
	}
}
