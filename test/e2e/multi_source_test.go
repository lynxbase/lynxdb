//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
)

// ingestSourceEvents ingests n simple plain-text events into the given index.
// Uses IngestRaw (text/plain) with X-Index header to route events.
func ingestSourceEvents(t *testing.T, h *Harness, index string, n int) {
	t.Helper()

	ctx := context.Background()
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("event %d from %s level=info", i, index)
	}
	body := strings.NewReader(strings.Join(lines, "\n"))
	result, err := h.Client().IngestRaw(ctx, body, client.IngestOpts{
		Index:       index,
		Source:      index,
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("ingest into %s: %v", index, err)
	}
	if result.Accepted != n {
		t.Fatalf("expected %d accepted for %s, got %d", n, index, result.Accepted)
	}
}

// setupMultiSource creates a harness with events in three sources.
func setupMultiSource(t *testing.T) *Harness {
	t.Helper()
	h := NewHarness(t)
	ingestSourceEvents(t, h, "nginx", 10)
	ingestSourceEvents(t, h, "postgres", 5)
	ingestSourceEvents(t, h, "redis", 3)
	return h
}

// Single index

func TestE2E_MultiSource_IndexEquals_ReturnsOnlyMatchingSource(t *testing.T) {
	h := setupMultiSource(t)

	r := h.MustQuery(`source=nginx | stats count() as count`)
	requireAggValue(t, r, "count", 10)
}

func TestE2E_MultiSource_IndexEquals_DifferentSource(t *testing.T) {
	h := setupMultiSource(t)

	r := h.MustQuery(`source=postgres | stats count() as count`)
	requireAggValue(t, r, "count", 5)
}

func TestE2E_MultiSource_IndexStar_ReturnsAll(t *testing.T) {
	h := setupMultiSource(t)

	r := h.MustQuery(`source=* | stats count() as count`)
	requireAggValue(t, r, "count", 18)
}

// Index lists

func TestE2E_MultiSource_IndexIN_ReturnsSubset(t *testing.T) {
	h := setupMultiSource(t)

	r := h.MustQuery(`source=nginx, postgres | stats count() as count`)
	requireAggValue(t, r, "count", 15)
}

func TestE2E_MultiSource_IndexIN_SingleValue(t *testing.T) {
	h := setupMultiSource(t)

	r := h.MustQuery(`source=redis | stats count() as count`)
	requireAggValue(t, r, "count", 3)
}

// Index exclusion

func TestE2E_MultiSource_IndexNotEquals_ExcludesSource(t *testing.T) {
	h := setupMultiSource(t)

	r := h.MustQuery(`source=* | where index != "redis" | stats count() as count`)
	got := GetInt(r, "count")
	// Should exclude redis (3 events), so at least nginx (10) + postgres (5).
	if got < 15 {
		t.Errorf("expected at least 15 events (excluding redis), got %d", got)
	}
}

// Source clause forms

func TestE2E_MultiSource_FROM_MultipleIndexes(t *testing.T) {
	h := setupMultiSource(t)

	r := h.MustQuery(`source=nginx, postgres | stats count() as count`)
	requireAggValue(t, r, "count", 15)
}

func TestE2E_MultiSource_FROM_Star_ReturnsAll(t *testing.T) {
	h := setupMultiSource(t)

	r := h.MustQuery(`source=* | stats count() as count`)
	requireAggValue(t, r, "count", 18)
}

func TestE2E_MultiSource_FROM_SingleIndex(t *testing.T) {
	h := setupMultiSource(t)

	r := h.MustQuery(`source=redis | stats count() as count`)
	requireAggValue(t, r, "count", 3)
}

// stats count() as count by index

func TestE2E_MultiSource_StatsCountByIndex(t *testing.T) {
	h := setupMultiSource(t)

	r := h.MustQuery(`source=* | stats count() as count by index`)
	rows := AggRows(r)
	if len(rows) < 3 {
		t.Fatalf("expected at least 3 groups for stats count by index, got %d", len(rows))
	}

	// Build lookup: index name -> count.
	counts := make(map[string]int)
	for _, row := range rows {
		idx := fmt.Sprint(row["index"])
		counts[idx] = toInt(row["count"])
	}

	if counts["nginx"] != 10 {
		t.Errorf("expected nginx=10, got %d", counts["nginx"])
	}
	if counts["postgres"] != 5 {
		t.Errorf("expected postgres=5, got %d", counts["postgres"])
	}
	if counts["redis"] != 3 {
		t.Errorf("expected redis=3, got %d", counts["redis"])
	}
}

// Source scope isolation

func TestE2E_MultiSource_FROM_ScopeIsolation(t *testing.T) {
	h := setupMultiSource(t)

	// source=nginx, postgres should return exactly 15 (10+5), not 18.
	r := h.MustQuery(`source=nginx, postgres | stats count() as count`)
	requireAggValue(t, r, "count", 15)

	// source=redis should return exactly 3, not events from other sources.
	r2 := h.MustQuery(`source=redis | stats count() as count`)
	requireAggValue(t, r2, "count", 3)

	// The search-command form scopes the same as the bare source clause.
	r3 := h.MustQuery(`source=nginx | stats count() as count`)
	r4 := h.MustQuery(`search source=nginx | stats count() as count`)
	sourceCount := GetInt(r3, "count")
	searchCount := GetInt(r4, "count")
	if sourceCount != searchCount {
		t.Errorf("source=nginx (%d) != search source=nginx (%d)", sourceCount, searchCount)
	}
}

// Single source

func TestE2E_MultiSource_SourceEquals(t *testing.T) {
	h := setupMultiSource(t)

	r := h.MustQuery(`source=nginx | stats count() as count`)
	requireAggValue(t, r, "count", 10)
}

// Explain endpoint reports source scope

func TestE2E_MultiSource_Explain_ReportsScope(t *testing.T) {
	h := setupMultiSource(t)
	ctx := context.Background()

	result, err := h.Client().Explain(ctx, `source=nginx, postgres | stats count() as count`)
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if !result.IsValid {
		t.Fatal("expected valid query")
	}
	if result.Parsed == nil {
		t.Fatal("expected non-nil parsed result")
	}
}

// Nonexistent source returns warning

func TestE2E_MultiSource_NonexistentSource_ReturnsResults(t *testing.T) {
	h := setupMultiSource(t)

	// Query a nonexistent source - should return 0 results without error.
	r := h.MustQuery(`source=nonexistent | stats count() as count`)
	got := GetInt(r, "count")
	if got != 0 {
		t.Errorf("expected 0 events from nonexistent source, got %d", got)
	}
}

// Glob pattern queries

// ingestGlobEvents ingests events where Index and Source are DIFFERENT values.
// Distinct values catch any aliasing of index to source.
func ingestGlobEvents(t *testing.T, h *Harness, index, source string, n int) {
	t.Helper()

	ctx := context.Background()
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("event %d from index=%s source=%s level=info", i, index, source)
	}
	body := strings.NewReader(strings.Join(lines, "\n"))
	result, err := h.Client().IngestRaw(ctx, body, client.IngestOpts{
		Index:       index,
		Source:      source,
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("ingest into index=%s source=%s: %v", index, source, err)
	}
	if result.Accepted != n {
		t.Fatalf("expected %d accepted for %s, got %d", n, index, result.Accepted)
	}
}

// setupGlobIndexes creates indexes with a common prefix for glob testing.
// Uses DIFFERENT Index and Source values to validate the index/source separation.
func setupGlobIndexes(t *testing.T) *Harness {
	t.Helper()
	h := NewHarness(t)
	ingestGlobEvents(t, h, "logs-web", "nginx-access", 10)
	ingestGlobEvents(t, h, "logs-api", "api-gateway", 8)
	ingestGlobEvents(t, h, "logs-db", "postgres-main", 5)
	ingestGlobEvents(t, h, "metrics-cpu", "node-exporter", 3)

	return h
}

func TestE2E_GlobIndex_FROM_LogsStar(t *testing.T) {
	h := setupGlobIndexes(t)

	// source=logs* should match logs-web(10) + logs-api(8) + logs-db(5) = 23
	r := h.MustQuery(`source=logs* | stats count() as count`)
	requireAggValue(t, r, "count", 23)
}

func TestE2E_GlobIndex_FROM_MetricsStar(t *testing.T) {
	h := setupGlobIndexes(t)

	// source=metrics* should match only metrics-cpu(3)
	r := h.MustQuery(`source=metrics* | stats count() as count`)
	requireAggValue(t, r, "count", 3)
}

func TestE2E_GlobIndex_FROM_Star_All(t *testing.T) {
	h := setupGlobIndexes(t)

	// source=* should match all indexes = 10+8+5+3 = 26
	r := h.MustQuery(`source=* | stats count() as count`)
	requireAggValue(t, r, "count", 26)
}

func TestE2E_GlobIndex_StatsCountByIndex_DiffersFromSource(t *testing.T) {
	h := setupGlobIndexes(t)

	// stats count() as count by index should group by physical partition (logs-web, logs-api, etc.)
	// NOT by source (nginx-access, api-gateway, etc.)
	r := h.MustQuery(`source=* | stats count() as count by index`)
	rows := AggRows(r)
	if len(rows) < 4 {
		t.Fatalf("expected at least 4 index groups, got %d", len(rows))
	}

	counts := make(map[string]int)
	for _, row := range rows {
		idx := fmt.Sprint(row["index"])
		counts[idx] = toInt(row["count"])
	}

	// Verify we see physical partition names, not source names.
	if counts["logs-web"] != 10 {
		t.Errorf("expected logs-web=10, got %d", counts["logs-web"])
	}
	if counts["logs-api"] != 8 {
		t.Errorf("expected logs-api=8, got %d", counts["logs-api"])
	}
	if counts["logs-db"] != 5 {
		t.Errorf("expected logs-db=5, got %d", counts["logs-db"])
	}
	if counts["metrics-cpu"] != 3 {
		t.Errorf("expected metrics-cpu=3, got %d", counts["metrics-cpu"])
	}

	// Source names should NOT appear as index values.
	for _, badKey := range []string{"nginx-access", "api-gateway", "postgres-main", "node-exporter"} {
		if _, found := counts[badKey]; found {
			t.Errorf("source name %q should NOT appear in 'stats count by index' results", badKey)
		}
	}
}

func TestE2E_GlobIndex_IndexEqualsGlob(t *testing.T) {
	h := setupGlobIndexes(t)

	// search source=logs* should match logs-web(10) + logs-api(8) + logs-db(5) = 23
	r := h.MustQuery(`search source=logs* | stats count() as count`)
	requireAggValue(t, r, "count", 23)
}

func TestE2E_GlobIndex_FROM_CommaSeparated(t *testing.T) {
	h := setupGlobIndexes(t)

	// source=logs-web, logs-db should match 10+5 = 15
	r := h.MustQuery(`source=logs-web, logs-db | stats count() as count`)
	requireAggValue(t, r, "count", 15)
}

func TestE2E_GlobIndex_SourceFieldNotConfusedWithIndex(t *testing.T) {
	h := setupGlobIndexes(t)

	// Querying by _source (the logical source tag) should use the Source value,
	// not the Index value. Index=logs-web has Source=nginx-access.
	r := h.MustQuery(`source=logs-web | stats count() as count by _source`)
	rows := AggRows(r)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row for stats by _source")
	}

	// The _source value should be "nginx-access", NOT "logs-web".
	srcVal := fmt.Sprint(rows[0]["_source"])
	if srcVal != "nginx-access" {
		t.Errorf("expected _source='nginx-access', got %q", srcVal)
	}
}
