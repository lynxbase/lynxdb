//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/client"
)

func TestE2E_QuerySync_SimpleCount(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/OpenSSH_2k.log")

	result := h.MustQuery(`FROM idx_ssh | STATS count`)
	requireAggValue(t, result, "count", 2000)
}

func TestE2E_QuerySync_EventsResult(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/OpenSSH_2k.log")

	result := h.MustQuery(`FROM idx_ssh | HEAD 5`)
	if result.Type != client.ResultTypeEvents {
		t.Errorf("expected result type=%s, got %s", client.ResultTypeEvents, result.Type)
	}
	if result.Events == nil {
		t.Fatal("expected non-nil Events")
	}
	if len(result.Events.Events) != 5 {
		t.Errorf("expected 5 events, got %d", len(result.Events.Events))
	}
}

func TestE2E_QuerySync_AggregateResult(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/OpenSSH_2k.log")

	// Use BIN + STATS BY — does not depend on REX (which is broken, see bugs-e2e.md)
	// and does not end with HEAD (which changes the result type to "events").
	result := h.MustQuery(`FROM idx_ssh | BIN _time span=1h AS hour | STATS count BY hour`)
	if result.Type != client.ResultTypeAggregate {
		t.Errorf("expected result type=%s, got %s", client.ResultTypeAggregate, result.Type)
	}
	if result.Aggregate == nil {
		t.Fatal("expected non-nil Aggregate")
	}
	if len(result.Aggregate.Rows) == 0 {
		t.Error("expected at least 1 aggregate row")
	}
	// Verify the columns include both the group-by key and the aggregation.
	colSet := map[string]bool{}
	for _, c := range result.Aggregate.Columns {
		colSet[c] = true
	}
	if !colSet["hour"] {
		t.Errorf("expected 'hour' column in aggregate result, got columns: %v", result.Aggregate.Columns)
	}
	if !colSet["count"] {
		t.Errorf("expected 'count' column in aggregate result, got columns: %v", result.Aggregate.Columns)
	}
}

func TestE2E_QueryGet_ReturnsResults(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/OpenSSH_2k.log")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := h.Client().QueryGet(ctx, `FROM idx_ssh | STATS count`, "", "", 100)
	if err != nil {
		t.Fatalf("QueryGet: %v", err)
	}
	// QueryGet uses the sync path (no Wait parameter), so the server should
	// return 200 with results directly. If it returned a job handle, that
	// means it was promoted to async — PollJob is broken (see query_async_test.go).
	if result.Type == client.ResultTypeJob {
		t.Fatalf("QueryGet returned job handle (promoted to async); expected sync result. PollJob is broken, cannot follow up.")
	}
	requireAggValue(t, result, "count", 2000)
}

func TestE2E_QuerySync_InvalidSPL_ReturnsError(t *testing.T) {
	h := NewHarness(t)

	ctx := context.Background()
	_, err := h.Client().QuerySync(ctx, `THIS IS NOT VALID SPL !!!`, "", "")
	if err == nil {
		t.Fatal("expected error for invalid SPL, got nil")
	}
	t.Logf("invalid SPL correctly returned error: %v", err)
}

func TestE2E_QuerySync_NonexistentIndex_ReturnsEmptyOrError(t *testing.T) {
	h := NewHarness(t)

	result := h.MustQuery(`FROM nonexistent_idx_12345 | STATS count`)
	total := GetInt(result, "count")
	if total != 0 {
		t.Errorf("expected 0 events in nonexistent index, got %d", total)
	}
}

// TestE2E_QuerySync_CountAlias verifies that STATS count AS <alias>
// correctly applies the alias to the output column.
func TestE2E_QuerySync_CountAlias_Bug(t *testing.T) {
	// Previously broken: bare "count" did not respect AS alias.
	// Fixed: convertAggs() and countStarOnly shortcut both check a.Alias.
	h := NewHarness(t)

	ctx := context.Background()
	events := []map[string]interface{}{
		{"host": "a"}, {"host": "b"},
	}
	_, _ = h.Client().Ingest(ctx, events)

	result := h.MustQuery(`FROM main | STATS count AS total`)
	if result.Aggregate == nil {
		t.Fatal("expected aggregate result")
	}

	// Check if the alias was applied.
	hasTotal := false
	hasCount := false
	for _, col := range result.Aggregate.Columns {
		if col == "total" {
			hasTotal = true
		}
		if col == "count" {
			hasCount = true
		}
	}

	if !hasTotal {
		t.Errorf("'STATS count AS total' should produce column 'total'; got columns: %v", result.Aggregate.Columns)
	}
	if hasCount {
		t.Errorf("'STATS count AS total' should not produce column 'count'; got columns: %v", result.Aggregate.Columns)
	}
}
