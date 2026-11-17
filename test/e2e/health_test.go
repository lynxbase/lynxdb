//go:build e2e

package e2e

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestE2E_Health_ReturnsHealthy(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	result, err := h.Client().Health(ctx)
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if result.Status != "healthy" {
		t.Errorf("expected status=healthy, got %q", result.Status)
	}
}

func TestE2E_Status_ReturnsServerInfo(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()
	ingestThreeEvents(t, h)

	status, err := h.Client().Status(ctx)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Version == "" {
		t.Error("expected non-empty version")
	}
	if status.Health != "healthy" {
		t.Errorf("expected health=healthy, got %q", status.Health)
	}
	if status.Events.Total != 3 {
		t.Errorf("expected events.total=3, got %d", status.Events.Total)
	}
	if status.Storage.UsedBytes <= 0 {
		t.Errorf("expected storage.used_bytes > 0, got %d", status.Storage.UsedBytes)
	}
}

func TestE2E_Stats_ReturnsIndexes(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()
	ingestThreeEvents(t, h)

	stats, err := h.Client().Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.TotalEvents != 3 || stats.IndexCount != 1 {
		t.Errorf("expected 3 events in 1 index, got total_events=%d index_count=%d", stats.TotalEvents, stats.IndexCount)
	}
	if len(stats.Indexes) != 1 || stats.Indexes[0].Name != "main" || stats.Indexes[0].Events != 3 {
		t.Errorf("expected index main with 3 events, got %+v", stats.Indexes)
	}
	if stats.OldestEvent == "" || stats.NewestEvent == "" {
		t.Errorf("expected oldest and newest event times, got %q and %q", stats.OldestEvent, stats.NewestEvent)
	}
	for _, state := range []string{"running", "done", "error", "canceled"} {
		if n, ok := stats.Jobs[state]; !ok || n != 0 {
			t.Errorf("expected jobs[%s]=0, got %v (present=%v)", state, n, ok)
		}
	}

	indexes, err := h.Client().Indexes(ctx)
	if err != nil {
		t.Fatalf("Indexes: %v", err)
	}
	if len(indexes) != 1 || indexes[0].Name != "main" || indexes[0].Events != 3 || indexes[0].Parts < 1 {
		t.Errorf("expected index main with 3 events in at least one part, got %+v", indexes)
	}
}

// ingestThreeEvents ingests three events into the main index.
func ingestThreeEvents(t *testing.T, h *Harness) {
	t.Helper()

	result, err := h.Client().IngestEvents(context.Background(), []client.IngestEvent{
		{Event: "first", Host: "web-01"},
		{Event: "second", Host: "web-02"},
		{Event: "third", Host: "web-01"},
	})
	if err != nil {
		t.Fatalf("IngestEvents: %v", err)
	}
	if result.Accepted != 3 {
		t.Fatalf("expected 3 accepted, got %d", result.Accepted)
	}
}
