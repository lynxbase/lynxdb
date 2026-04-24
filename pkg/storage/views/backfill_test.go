package views

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

// mockSource is a test implementation of EventSource.
type mockSource struct {
	events []*event.Event
}

func (m *mockSource) ScanEvents(cursor string, limit int) ([]*event.Event, string, bool, error) {
	start := 0
	if cursor != "" {
		n, _ := strconv.Atoi(cursor)
		start = n
	}
	end := start + limit
	if end > len(m.events) {
		end = len(m.events)
	}
	more := end < len(m.events)

	return m.events[start:end], fmt.Sprintf("%d", end), more, nil
}

func TestBackfiller_Run(t *testing.T) {
	dir := t.TempDir()
	viewsDir := filepath.Join(dir, "views")
	os.MkdirAll(viewsDir, 0o755)

	reg, _ := Open(viewsDir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	def := ViewDefinition{
		Name:    "mv_backfill",
		Version: 1,
		Type:    ViewTypeProjection,
		Filter:  "_source=nginx",
		Columns: []ColumnDef{{Name: "_time", Type: event.FieldTypeTimestamp}},
		Status:  ViewStatusBackfill,
	}
	reg.Create(def)

	source := &mockSource{
		events: []*event.Event{
			makeTestEvent("nginx", "/a", "200"),
			makeTestEvent("api", "/b", "200"),
			makeTestEvent("nginx", "/c", "500"),
		},
	}

	var dispatched []*event.Event
	dispatch := func(events []*event.Event) error {
		dispatched = append(dispatched, events...)

		return nil
	}

	backfiller := NewBackfiller(reg, logger)
	err := backfiller.Run(context.Background(), "mv_backfill", source, dispatch)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Only nginx events should be dispatched.
	if len(dispatched) != 2 {
		t.Errorf("dispatched: got %d, want 2", len(dispatched))
	}

	// View should be active after backfill.
	stored, _ := reg.Get("mv_backfill")
	if stored.Status != ViewStatusActive {
		t.Errorf("status: got %v, want active", stored.Status)
	}
}

func TestBackfiller_CancelledContext(t *testing.T) {
	dir := t.TempDir()
	viewsDir := filepath.Join(dir, "views")
	os.MkdirAll(viewsDir, 0o755)

	reg, _ := Open(viewsDir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	def := ViewDefinition{
		Name:    "mv_cancel",
		Version: 1,
		Type:    ViewTypeProjection,
		Filter:  "",
		Columns: []ColumnDef{{Name: "_time", Type: event.FieldTypeTimestamp}},
		Status:  ViewStatusBackfill,
	}
	reg.Create(def)

	var events []*event.Event
	for i := 0; i < 5000; i++ {
		events = append(events, makeTestEvent("nginx", fmt.Sprintf("/%d", i), "200"))
	}
	source := &mockSource{events: events}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	dispatched := 0
	dispatch := func(events []*event.Event) error {
		dispatched += len(events)

		return nil
	}

	backfiller := NewBackfiller(reg, logger)
	err := backfiller.Run(ctx, "mv_cancel", source, dispatch)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestBackfiller_WrongStatus(t *testing.T) {
	dir := t.TempDir()
	viewsDir := filepath.Join(dir, "views")
	os.MkdirAll(viewsDir, 0o755)

	reg, _ := Open(viewsDir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	def := ViewDefinition{
		Name:    "mv_active",
		Version: 1,
		Columns: []ColumnDef{{Name: "_time", Type: event.FieldTypeTimestamp}},
		Status:  ViewStatusActive, // Not backfill.
	}
	reg.Create(def)

	source := &mockSource{}
	backfiller := NewBackfiller(reg, logger)
	err := backfiller.Run(context.Background(), "mv_active", source, nil)
	if err == nil {
		t.Error("expected error for non-backfill view")
	}
}

func TestBackfiller_WithBudget(t *testing.T) {
	dir := t.TempDir()
	viewsDir := filepath.Join(dir, "views")
	os.MkdirAll(viewsDir, 0o755)

	reg, _ := Open(viewsDir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	def := ViewDefinition{
		Name:    "mv_budget",
		Version: 1,
		Type:    ViewTypeProjection,
		Filter:  "",
		Columns: []ColumnDef{{Name: "_time", Type: event.FieldTypeTimestamp}},
		Status:  ViewStatusBackfill,
	}
	reg.Create(def)

	source := &mockSource{
		events: []*event.Event{
			makeTestEvent("nginx", "/a", "200"),
			makeTestEvent("nginx", "/b", "200"),
		},
	}

	// Create a Governor with 1GB limit to back the backfill.
	gov := memgov.NewGovernor(memgov.GovernorConfig{TotalLimit: 1 << 30})

	var dispatched int
	dispatch := func(events []*event.Event) error {
		dispatched += len(events)

		return nil
	}

	cfg := BackfillConfig{
		MaxMemoryBytes:   512 << 20, // 512MB
		BackpressureWait: 10 * time.Millisecond,
		MaxRetries:       3,
	}
	backfiller := NewBackfillerWithBudget(reg, gov, cfg, logger)
	err := backfiller.Run(context.Background(), "mv_budget", source, dispatch)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if dispatched != 2 {
		t.Errorf("dispatched: got %d, want 2", dispatched)
	}

	stored, _ := reg.Get("mv_budget")
	if stored.Status != ViewStatusActive {
		t.Errorf("status: got %v, want active", stored.Status)
	}
}

// poolExhaustedSource returns PoolExhaustedError on the first N calls,
// then delegates to the inner source.
type poolExhaustedSource struct {
	inner     EventSource
	failCount int
	callsMade int
}

func (p *poolExhaustedSource) ScanEvents(cursor string, limit int) ([]*event.Event, string, bool, error) {
	p.callsMade++
	if p.callsMade <= p.failCount {
		return nil, "", false, &memgov.PoolExhaustedError{
			Pool:      "test-pool",
			Requested: 1024,
			Current:   1 << 30,
			Limit:     1 << 30,
		}
	}

	return p.inner.ScanEvents(cursor, limit)
}

func TestBackfiller_Backpressure(t *testing.T) {
	dir := t.TempDir()
	viewsDir := filepath.Join(dir, "views")
	os.MkdirAll(viewsDir, 0o755)

	reg, _ := Open(viewsDir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	def := ViewDefinition{
		Name:    "mv_bp",
		Version: 1,
		Type:    ViewTypeProjection,
		Filter:  "",
		Columns: []ColumnDef{{Name: "_time", Type: event.FieldTypeTimestamp}},
		Status:  ViewStatusBackfill,
	}
	reg.Create(def)

	inner := &mockSource{
		events: []*event.Event{
			makeTestEvent("nginx", "/a", "200"),
		},
	}

	// First 2 calls fail with pool exhausted, third succeeds.
	source := &poolExhaustedSource{inner: inner, failCount: 2}

	var dispatched int
	dispatch := func(events []*event.Event) error {
		dispatched += len(events)

		return nil
	}

	cfg := BackfillConfig{
		BackpressureWait: 1 * time.Millisecond, // Fast for tests.
		MaxRetries:       5,
	}
	backfiller := NewBackfillerWithBudget(reg, nil, cfg, logger)
	err := backfiller.Run(context.Background(), "mv_bp", source, dispatch)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Source should have been called 3 times (2 failures + 1 success).
	if source.callsMade != 3 {
		t.Errorf("source calls: got %d, want 3", source.callsMade)
	}

	if dispatched != 1 {
		t.Errorf("dispatched: got %d, want 1", dispatched)
	}
}

func TestBackfiller_BackpressureMaxRetries(t *testing.T) {
	dir := t.TempDir()
	viewsDir := filepath.Join(dir, "views")
	os.MkdirAll(viewsDir, 0o755)

	reg, _ := Open(viewsDir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	def := ViewDefinition{
		Name:    "mv_maxretry",
		Version: 1,
		Type:    ViewTypeProjection,
		Filter:  "",
		Columns: []ColumnDef{{Name: "_time", Type: event.FieldTypeTimestamp}},
		Status:  ViewStatusBackfill,
	}
	reg.Create(def)

	// Source always returns pool exhausted.
	source := &poolExhaustedSource{
		inner:     &mockSource{events: []*event.Event{makeTestEvent("nginx", "/a", "200")}},
		failCount: 100, // Always fail.
	}

	cfg := BackfillConfig{
		BackpressureWait: 1 * time.Millisecond,
		MaxRetries:       3,
	}
	backfiller := NewBackfillerWithBudget(reg, nil, cfg, logger)
	err := backfiller.Run(context.Background(), "mv_maxretry", source, nil)
	if err == nil {
		t.Fatal("expected error after max retries")
	}

	if !memgov.IsPoolExhausted(err) {
		t.Errorf("expected PoolExhaustedError in chain, got: %v", err)
	}
}

func TestBackfiller_CreateBudgetAdapter_AutoCompute(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Governor with 10GB limit -> backfill should get 10% = 1GB.
	gov := memgov.NewGovernor(memgov.GovernorConfig{TotalLimit: 10 << 30})

	backfiller := NewBackfillerWithBudget(nil, gov, BackfillConfig{}, logger)
	adapter := backfiller.createBudgetAdapter("mv_test")
	if adapter == nil {
		t.Fatal("expected non-nil adapter")
	}
	adapter.Close()
}

func TestBackfiller_CreateBudgetAdapter_NilRoot(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Without a Governor, budget adapter should be nil.
	backfiller := NewBackfiller(nil, logger)
	adapter := backfiller.createBudgetAdapter("mv_test")
	if adapter != nil {
		t.Error("expected nil adapter when no Governor")
	}
}
