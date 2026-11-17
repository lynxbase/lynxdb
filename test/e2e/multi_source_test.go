//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
)

// ingestSourceEvents ingests n plain-text events into an index whose source
// field is the index name.
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

// setupMultiSource creates a harness with events in three indexes.
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

// Source field exclusion

func TestE2E_MultiSource_SourceFieldNotEquals_ExcludesSource(t *testing.T) {
	h := setupMultiSource(t)

	// The source field of every event names its index: nginx (10) + postgres (5).
	r := h.MustQuery(`source=* | where source != "redis" | stats count() as count`)
	requireAggValue(t, r, "count", 15)
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

// stats count() as count by source

func TestE2E_MultiSource_StatsCountBySource(t *testing.T) {
	h := setupMultiSource(t)

	r := h.MustQuery(`source=* | stats count() as count by source`)
	rows := AggRows(r)
	if len(rows) != 3 {
		t.Fatalf("expected 3 groups for stats count by source, got %d: %v", len(rows), rows)
	}

	counts := make(map[string]int)
	for _, row := range rows {
		counts[fmt.Sprint(row["source"])] = toInt(row["count"])
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
	scope := result.Parsed.SourceScope
	if scope == nil {
		t.Fatal("expected a source scope")
	}
	if scope.Type != "list" || strings.Join(scope.Sources, ",") != "nginx,postgres" {
		t.Errorf("expected scope list [nginx postgres], got type=%q sources=%v", scope.Type, scope.Sources)
	}
}

// Nonexistent source

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

// ingestGlobEvents ingests events whose index and source differ, which
// catches any aliasing of index to source.
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

// setupGlobIndexes creates indexes with a common prefix for glob testing,
// each with a source that differs from its name.
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

func TestE2E_GlobIndex_StatsCountBySource_DiffersFromIndex(t *testing.T) {
	h := setupGlobIndexes(t)

	// The source field holds the source each event was sent with, not its index.
	r := h.MustQuery(`source=* | stats count() as count by source`)
	rows := AggRows(r)
	if len(rows) != 4 {
		t.Fatalf("expected 4 source groups, got %d: %v", len(rows), rows)
	}

	counts := make(map[string]int)
	for _, row := range rows {
		counts[fmt.Sprint(row["source"])] = toInt(row["count"])
	}
	for source, want := range map[string]int{"nginx-access": 10, "api-gateway": 8, "postgres-main": 5, "node-exporter": 3} {
		if counts[source] != want {
			t.Errorf("expected %s=%d, got %d", source, want, counts[source])
		}
	}
	for _, index := range []string{"logs-web", "logs-api", "logs-db", "metrics-cpu"} {
		if _, found := counts[index]; found {
			t.Errorf("index name %q should not appear as a source value", index)
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

	// Index logs-web holds the events sent with source nginx-access.
	r := h.MustQuery(`source=logs-web | stats count() as count by source`)
	rows := AggRows(r)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row for stats by source, got %d: %v", len(rows), rows)
	}
	if srcVal := fmt.Sprint(rows[0]["source"]); srcVal != "nginx-access" {
		t.Errorf("expected source='nginx-access', got %q", srcVal)
	}
	if got := toInt(rows[0]["count"]); got != 10 {
		t.Errorf("expected count=10, got %d", got)
	}

	requireAggValue(t, h.MustQuery(`source=* | where source = "nginx-access" | stats count() as count`), "count", 10)
	requireAggValue(t, h.MustQuery(`source=* | where source = "logs-web" | stats count() as count`), "count", 0)
}
