//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestE2E_QuerySync_SimpleCount(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")

	result := h.MustQuery(`source=idx_ssh | stats count() as count`)
	requireAggValue(t, result, "count", 2000)
}

func TestE2E_QuerySync_EventsResult(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")

	result := h.MustQuery(`source=idx_ssh | head 5`)
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
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")

	// The query ends with stats, not head, so the result type is aggregate.
	result := h.MustQuery(`source=idx_ssh | bin _time span=1h as hour | stats count() as count by hour`)
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
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := h.Client().QueryGet(ctx, `source=idx_ssh | stats count() as count`, "", "", 100)
	if err != nil {
		t.Fatalf("QueryGet: %v", err)
	}
	// QueryGet uses the sync path (no Wait parameter), so the server should
	// return 200 with results directly rather than a job handle.
	if result.Type == client.ResultTypeJob {
		t.Fatalf("QueryGet returned job handle (promoted to async); expected sync result")
	}
	requireAggValue(t, result, "count", 2000)
}

func TestE2E_QuerySync_InvalidQuery_ReturnsError(t *testing.T) {
	h := NewHarness(t)

	ctx := context.Background()
	_, err := h.Client().QuerySync(ctx, `THIS IS NOT A VALID QUERY !!!`, "", "")
	if err == nil {
		t.Fatal("expected error for invalid query, got nil")
	}
	if !client.IsInvalidQuery(err) {
		t.Errorf("expected an invalid-query error, got %v", err)
	}
	t.Logf("invalid query correctly returned error: %v", err)
}

// TestE2E_QuerySync_NonexistentIndex_NotFound requires a query of an index
// that is not there to answer 404 NOT_FOUND with a message that names it.
func TestE2E_QuerySync_NonexistentIndex_NotFound(t *testing.T) {
	h := NewHarness(t)

	_, err := h.Client().Query(context.Background(), client.QueryRequest{Q: `source=nonexistent_idx_12345 | stats count() as count`})
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) || apiErr.HTTPStatus != http.StatusNotFound || apiErr.Code != "NOT_FOUND" ||
		!strings.Contains(apiErr.Message, "no such index: nonexistent_idx_12345") {
		t.Errorf("query of an index that is not there: %v", err)
	}
}

// TestE2E_QuerySync_FieldNoIndexHolds_Warns requires the response to warn of
// a field that the query reads and that no event of the index holds.
func TestE2E_QuerySync_FieldNoIndexHolds_Warns(t *testing.T) {
	h := NewHarness(t)

	ctx := context.Background()
	if _, err := h.Client().IngestEvents(ctx, []client.IngestEvent{{Event: `{"host":"a"}`, Host: "a"}}); err != nil {
		t.Fatal(err)
	}
	result := h.MustQuery(`source=main | where nosuchfield > 1 | stats count() as n`)
	want := "no event of the index main holds the field nosuchfield"
	if !slices.Contains(result.Meta.Warnings, want) {
		t.Errorf("warnings %q, want %q", result.Meta.Warnings, want)
	}
	if result := h.MustQuery(`source=main | eval made = 1 | where made > 0 | stats count() as n`); len(result.Meta.Warnings) != 0 {
		t.Errorf("warnings for a field the query makes: %q", result.Meta.Warnings)
	}
}

// TestE2E_QuerySync_CountAlias verifies that stats count() as <alias>
// applies the alias to the output column.
func TestE2E_QuerySync_CountAlias(t *testing.T) {
	h := NewHarness(t)

	ctx := context.Background()
	events := []client.IngestEvent{
		{Event: `{"host":"a"}`, Host: "a"},
		{Event: `{"host":"b"}`, Host: "b"},
	}
	_, _ = h.Client().IngestEvents(ctx, events)

	result := h.MustQuery(`source=main | stats count() as total`)
	if result.Aggregate == nil {
		t.Fatal("expected aggregate result")
	}

	hasTotal := false
	hasCount := false
	for _, col := range result.Aggregate.Columns {
		if col == "total" {
			hasTotal = true
		}
		if col == "count" || col == "count()" {
			hasCount = true
		}
	}

	if !hasTotal {
		t.Errorf("'stats count() as total' should produce column 'total'; got columns: %v", result.Aggregate.Columns)
	}
	if hasCount {
		t.Errorf("'stats count() as total' should not produce a count column; got columns: %v", result.Aggregate.Columns)
	}
}

// TestE2E_QuerySync_ReverseHeadN verifies that "| reverse | head N" returns
// exactly N events, or every event when N exceeds the dataset.
func TestE2E_QuerySync_ReverseHeadN(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")

	for _, tc := range []struct {
		n, want int
	}{
		{50, 50},
		{10, 10},
		{5, 5},
		{100, 100},
		{5000, 2000},
	} {
		result := h.MustQuery(fmt.Sprintf(`source=idx_ssh | reverse | head %d`, tc.n))
		if got := EventCount(result); got != tc.want {
			t.Fatalf("reverse | head %d: expected %d events, got %d", tc.n, tc.want, got)
		}
	}
}
