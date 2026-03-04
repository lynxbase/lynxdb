package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// setupMultiIndexServer starts a test server and ingests data into 3 separate
// indexes to test multi-index queries. Returns the server base URL.
//
// Index inventory:
//   - idx_access:  1000 events from testdata/logs/access.log  (kv format: host, level, status, method, path, response_time, bytes, msg)
//   - idx_backend: 26 events from testdata/logs/backend_server.log (JSON: service, instance, duration_ms, memory_mb, cpu_pct, level)
//   - idx_nginx:   34 events from testdata/logs/nginx_access.log   (nginx combined log format)
func setupMultiIndexServer(t *testing.T) string {
	t.Helper()

	baseURL := newTestServer(t)
	ingestTestData(t, baseURL, "idx_access", "testdata/logs/access.log")
	ingestTestData(t, baseURL, "idx_backend", "testdata/logs/backend_server.log")
	ingestTestData(t, baseURL, "idx_nginx", "testdata/logs/nginx_access.log")

	return baseURL
}

// ═══════════════════════════════════════════════════════════════════════════
// Group 1: Multi-Index Query Correctness
// ═══════════════════════════════════════════════════════════════════════════

func TestServerQuery_MultiIndex_CountPerIndex(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	tests := []struct {
		name  string
		index string
		want  int
	}{
		{"idx_access_has_1000_events", "idx_access", 1000},
		{"idx_backend_has_26_events", "idx_backend", 26},
		{"idx_nginx_has_34_events", "idx_nginx", 34},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
				"FROM "+tt.index+" | stats count")
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			got := jsonCount(t, stdout)
			if got != tt.want {
				t.Errorf("FROM %s | stats count = %d, want %d", tt.index, got, tt.want)
			}
		})
	}
}

func TestServerQuery_MultiIndex_DefaultIndex_IsMain(t *testing.T) {
	baseURL := newTestServer(t)
	ingestTestData(t, baseURL, "main", "testdata/logs/access.log")

	// Query without explicit FROM should default to "main".
	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"level=error | stats count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 294 {
		t.Errorf("implicit main index: count = %d, want 294", got)
	}
}

func TestServerQuery_WHERE_FilterByLevel_CorrectCount(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	tests := []struct {
		level string
		want  int
	}{
		{"ERROR", 294},
		{"INFO", 359},
		{"WARN", 347},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
				"FROM idx_access | where level=\""+tt.level+"\" | stats count")
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			got := jsonCount(t, stdout)
			if got != tt.want {
				t.Errorf("level=%s count = %d, want %d", tt.level, got, tt.want)
			}
		})
	}
}

func TestServerQuery_WHERE_NumericFilter_StatusGTE500(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | where status >= 500 | stats count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	// access.log has 204 events with status >= 500 (500, 502, 503).
	if got != 204 {
		t.Errorf("status >= 500 count = %d, want 204", got)
	}
}

func TestServerQuery_WHERE_CombinedFilters(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	// Use chained WHERE clauses to combine filters — equivalent to AND.
	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		`FROM idx_access | where level="ERROR" | where status >= 500 | stats count`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got <= 0 {
		t.Errorf("combined filter (ERROR + status>=500) returned count=%d, expected > 0", got)
	}
	if got > 294 {
		t.Errorf("combined filter returned count=%d, cannot exceed total ERROR count of 294", got)
	}
}

func TestServerQuery_SEARCH_FullText_MatchesExpected(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		`FROM idx_access | search "ERROR" | stats count`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 294 {
		t.Errorf("full-text search ERROR count = %d, want 294", got)
	}
}

// toFloat64 coerces a JSON value to float64. The server may return numeric
// aggregation results as either float64 or string depending on the codec path.
func toFloat64(t *testing.T, field string, v interface{}) (float64, bool) {
	t.Helper()

	switch val := v.(type) {
	case float64:
		return val, true
	case string:
		var f float64
		if _, err := fmt.Sscanf(val, "%f", &f); err != nil {
			t.Errorf("%s: cannot parse string %q as float: %v", field, val, err)

			return 0, false
		}

		return f, true
	case json.Number:
		f, err := val.Float64()
		if err != nil {
			t.Errorf("%s: cannot convert json.Number %v: %v", field, val, err)

			return 0, false
		}

		return f, true
	default:
		t.Errorf("%s: unexpected type %T", field, v)

		return 0, false
	}
}

// TestServerQuery_STATS_MultipleAggs_CountAvgMinMax verifies that count, avg,
// min, and max aggregation functions all return values and that count is correct.
//
// Previously broken: min()/max() performed STRING comparison instead of
// NUMERIC comparison on kv-extracted fields. Fixed via vm.CompareValues()
// which now performs ParseFloat promotion for string-typed numeric fields.
func TestServerQuery_STATS_MultipleAggs_CountAvgMinMax(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | stats count, avg(response_time) AS avg_rt, min(response_time) AS min_rt, max(response_time) AS max_rt")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	row := rows[0]

	// Verify count.
	countVal, ok := row["count"]
	if !ok {
		t.Fatal("missing 'count' field")
	}
	if int(countVal.(float64)) != 1000 {
		t.Errorf("count = %v, want 1000", countVal)
	}

	// Verify all aggregation fields exist.
	for _, field := range []string{"avg_rt", "min_rt", "max_rt"} {
		if _, ok := row[field]; !ok {
			t.Errorf("missing %q field in output", field)
		}
	}

	// Verify the invariant min <= avg <= max holds (previously broken
	// due to string comparison; now fixed via numeric promotion).
	minRT, minOK := toFloat64(t, "min_rt", row["min_rt"])
	avgRT, avgOK := toFloat64(t, "avg_rt", row["avg_rt"])
	maxRT, maxOK := toFloat64(t, "max_rt", row["max_rt"])

	if minOK && avgOK && minRT > avgRT {
		t.Errorf("min_rt (%f) > avg_rt (%f) — invariant violated", minRT, avgRT)
	}
	if avgOK && maxOK && avgRT > maxRT {
		t.Errorf("avg_rt (%f) > max_rt (%f) — invariant violated", avgRT, maxRT)
	}
}

func TestServerQuery_STATS_GroupBy_CorrectGroups(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | stats count by level | sort level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 level groups, got %d", len(rows))
	}

	levelCounts := make(map[string]int)
	for _, row := range rows {
		level, _ := row["level"].(string)
		count := int(row["count"].(float64))
		levelCounts[level] = count
	}

	expected := map[string]int{"ERROR": 294, "INFO": 359, "WARN": 347}
	for level, want := range expected {
		if got := levelCounts[level]; got != want {
			t.Errorf("level=%s: count = %d, want %d", level, got, want)
		}
	}
}

func TestServerQuery_STATS_DistinctCount(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | stats dc(host) AS unique_hosts")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	v, ok := rows[0]["unique_hosts"]
	if !ok {
		t.Fatal("missing 'unique_hosts' field")
	}

	dc := int(v.(float64))
	// access.log has 5 distinct hosts: web-01, web-02, web-03, api-01, api-02.
	if dc != 5 {
		t.Errorf("dc(host) = %d, want 5", dc)
	}
}

func TestServerQuery_SORT_Descending_OrderVerified(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | stats count by host | sort -count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) < 2 {
		t.Fatalf("expected multiple rows, got %d", len(rows))
	}

	for i := 1; i < len(rows); i++ {
		prev := rows[i-1]["count"].(float64)
		curr := rows[i]["count"].(float64)
		if prev < curr {
			t.Errorf("sort -count violated at row %d: %v < %v", i, prev, curr)
		}
	}
}

func TestServerQuery_SORT_Ascending_OrderVerified(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | stats count by host | sort count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) < 2 {
		t.Fatalf("expected multiple rows, got %d", len(rows))
	}

	for i := 1; i < len(rows); i++ {
		prev := rows[i-1]["count"].(float64)
		curr := rows[i]["count"].(float64)
		if prev > curr {
			t.Errorf("sort count violated at row %d: %v > %v", i, prev, curr)
		}
	}
}

func TestServerQuery_HEAD_LimitsResults(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | head 7")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 7 {
		t.Errorf("head 7 returned %d rows, want 7", len(rows))
	}
}

func TestServerQuery_EVAL_ComputedField(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		`FROM idx_access | eval is_error=if(level="ERROR","yes","no") | stats count by is_error`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 2 {
		t.Fatalf("expected 2 groups (yes/no), got %d", len(rows))
	}

	total := 0
	for _, row := range rows {
		isErr, _ := row["is_error"].(string)
		if isErr != "yes" && isErr != "no" {
			t.Errorf("unexpected is_error value: %q", isErr)
		}
		total += int(row["count"].(float64))
	}

	if total != 1000 {
		t.Errorf("sum of groups = %d, want 1000", total)
	}
}

func TestServerQuery_DEDUP_UniqueValues(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | dedup level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Errorf("dedup level returned %d rows, want 3", len(rows))
	}

	seen := make(map[string]bool)
	for _, row := range rows {
		level, _ := row["level"].(string)
		if seen[level] {
			t.Errorf("duplicate level %q after dedup", level)
		}
		seen[level] = true
	}
}

func TestServerQuery_RENAME_FieldRenamed(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | stats count by level | rename count AS total")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) == 0 {
		t.Fatal("expected rows from rename query")
	}

	for i, row := range rows {
		if _, ok := row["total"]; !ok {
			t.Errorf("row %d missing 'total' field after rename", i)
		}
		if _, ok := row["count"]; ok {
			t.Errorf("row %d still has 'count' field after rename to 'total'", i)
		}
	}
}

func TestServerQuery_FIELDS_Projection(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | fields level, host | head 5")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}

	for i, row := range rows {
		if _, ok := row["level"]; !ok {
			t.Errorf("row %d missing projected field 'level'", i)
		}
		if _, ok := row["host"]; !ok {
			t.Errorf("row %d missing projected field 'host'", i)
		}
		// Should not have non-projected data fields.
		for k := range row {
			switch k {
			case "level", "host", "_time", "_timestamp", "_raw":
				// allowed
			default:
				t.Errorf("row %d has unexpected field %q after projection", i, k)
			}
		}
	}
}

func TestServerQuery_FILLNULL_NoError(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		`FROM idx_access | head 3 | fillnull value="N/A"`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Errorf("expected 3 rows after fillnull, got %d", len(rows))
	}
}

func TestServerQuery_TOP_ReturnsTopN(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | top 3 level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Fatalf("top 3 level returned %d rows, want 3", len(rows))
	}

	// Each row should have "level", "count", and "percent" fields.
	for i, row := range rows {
		if _, ok := row["level"]; !ok {
			t.Errorf("row %d missing 'level'", i)
		}
		if _, ok := row["count"]; !ok {
			t.Errorf("row %d missing 'count'", i)
		}
		if _, ok := row["percent"]; !ok {
			t.Errorf("row %d missing 'percent'", i)
		}
	}

	// Top row should have the highest count.
	if len(rows) >= 2 {
		first := rows[0]["count"].(float64)
		second := rows[1]["count"].(float64)
		if first < second {
			t.Errorf("top command not sorted: first count %v < second %v", first, second)
		}
	}
}

func TestServerQuery_EmptyResult_StatsCountZero(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		`FROM idx_access | where level="NONEXISTENT" | stats count`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 0 {
		t.Errorf("nonexistent filter: count = %d, want 0", got)
	}
}

func TestServerQuery_NonexistentIndex_CountZero(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_nonexistent_12345 | stats count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 0 {
		t.Errorf("nonexistent index: count = %d, want 0", got)
	}
}

func TestServerQuery_STATS_Sum(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | stats sum(bytes) AS total_bytes")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	v, ok := rows[0]["total_bytes"]
	if !ok {
		t.Fatal("missing 'total_bytes' field")
	}

	totalBytes := v.(float64)
	if totalBytes <= 0 {
		t.Errorf("sum(bytes) = %f, expected > 0", totalBytes)
	}
}

func TestServerQuery_GroupByHost_SumsTo1000(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | stats count by host")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 5 {
		t.Errorf("expected 5 host groups, got %d", len(rows))
	}

	total := 0
	for _, row := range rows {
		total += int(row["count"].(float64))
	}

	if total != 1000 {
		t.Errorf("sum of counts by host = %d, want 1000", total)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Group 2: Output Format Correctness
// ═══════════════════════════════════════════════════════════════════════════

func TestServerFormat_JSON_ValidNDJSON(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | head 10")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) != 10 {
		t.Errorf("expected 10 NDJSON lines, got %d", len(lines))
	}

	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Errorf("line %d is not valid JSON: %v\nline: %q", i, err, line)
		}
	}
}

func TestServerFormat_JSON_AggregateResult(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | stats count by level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 JSON rows, got %d", len(rows))
	}

	for i, row := range rows {
		if _, ok := row["count"]; !ok {
			t.Errorf("row %d missing 'count'", i)
		}
		if _, ok := row["level"]; !ok {
			t.Errorf("row %d missing 'level'", i)
		}
	}
}

func TestServerFormat_CSV_ValidRFC4180(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "csv",
		"FROM idx_access | stats count by level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	reader := csv.NewReader(strings.NewReader(strings.TrimSpace(stdout)))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("CSV parse error: %v", err)
	}

	// Header + 3 data rows.
	if len(records) != 4 {
		t.Errorf("expected 4 CSV records (1 header + 3 data), got %d", len(records))
	}

	header := strings.Join(records[0], ",")
	if !strings.Contains(header, "count") {
		t.Errorf("CSV header missing 'count', got: %v", records[0])
	}
	if !strings.Contains(header, "level") {
		t.Errorf("CSV header missing 'level', got: %v", records[0])
	}
}

func TestServerFormat_CSV_SpecialCharacters(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "csv",
		"FROM idx_access | fields msg | head 5")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	reader := csv.NewReader(strings.NewReader(strings.TrimSpace(stdout)))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("CSV parse error (quoting issue?): %v\nraw output:\n%s", err, stdout)
	}

	if len(records) < 2 {
		t.Errorf("expected at least header + 1 data row, got %d records", len(records))
	}
}

func TestServerFormat_TSV_TabSeparated(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "tsv",
		"FROM idx_access | stats count by level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	// Header + 3 data rows.
	if len(lines) != 4 {
		t.Errorf("expected 4 TSV lines (1 header + 3 data), got %d", len(lines))
	}

	for i, line := range lines {
		if !strings.Contains(line, "\t") {
			t.Errorf("line %d has no tab characters — not TSV: %q", i, line)
		}
	}
}

func TestServerFormat_Table_ContainsHeaders(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "table",
		"FROM idx_access | stats count by level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if !strings.Contains(stdout, "count") {
		t.Errorf("table output missing 'count' header")
	}
	if !strings.Contains(stdout, "level") {
		t.Errorf("table output missing 'level' header")
	}
	if !strings.Contains(stdout, "ERROR") {
		t.Errorf("table output missing 'ERROR' value")
	}
	if !strings.Contains(stdout, "INFO") {
		t.Errorf("table output missing 'INFO' value")
	}
	if !strings.Contains(stdout, "WARN") {
		t.Errorf("table output missing 'WARN' value")
	}
}

func TestServerFormat_Raw_LinesMatchCount(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "raw",
		"FROM idx_access | head 5")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	var nonEmpty int
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if strings.TrimSpace(line) != "" {
			nonEmpty++
		}
	}

	if nonEmpty != 5 {
		t.Errorf("raw output has %d non-empty lines, want 5", nonEmpty)
	}
}

func TestServerFormat_ConsistentAcrossFormats(t *testing.T) {
	baseURL := setupMultiIndexServer(t)
	query := "FROM idx_access | stats count by level"

	// JSON.
	jsonStdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json", query)
	if err != nil {
		t.Fatalf("JSON query failed: %v", err)
	}
	jsonRows := mustParseJSON(t, jsonStdout)

	// CSV.
	csvStdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "csv", query)
	if err != nil {
		t.Fatalf("CSV query failed: %v", err)
	}
	csvReader := csv.NewReader(strings.NewReader(strings.TrimSpace(csvStdout)))
	csvRecords, _ := csvReader.ReadAll()
	csvDataRows := len(csvRecords) - 1 // subtract header

	if len(jsonRows) != csvDataRows {
		t.Errorf("format mismatch: JSON has %d rows, CSV has %d data rows", len(jsonRows), csvDataRows)
	}

	// Extract levels from JSON to verify data consistency.
	jsonLevels := make(map[string]bool)
	for _, row := range jsonRows {
		if level, ok := row["level"].(string); ok {
			jsonLevels[level] = true
		}
	}

	if !jsonLevels["ERROR"] || !jsonLevels["INFO"] || !jsonLevels["WARN"] {
		t.Errorf("JSON missing expected levels, got: %v", jsonLevels)
	}
}

func TestServerFormat_NDJSON_EqualsJSON(t *testing.T) {
	baseURL := setupMultiIndexServer(t)
	query := "FROM idx_access | stats count by level | sort level"

	jsonStdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json", query)
	if err != nil {
		t.Fatalf("JSON query failed: %v", err)
	}

	ndjsonStdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "ndjson", query)
	if err != nil {
		t.Fatalf("NDJSON query failed: %v", err)
	}

	jsonRows := mustParseJSON(t, jsonStdout)
	ndjsonRows := mustParseJSON(t, ndjsonStdout)

	if len(jsonRows) != len(ndjsonRows) {
		t.Errorf("json=%d rows, ndjson=%d rows", len(jsonRows), len(ndjsonRows))
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Group 3: Ingest CLI Command
// ═══════════════════════════════════════════════════════════════════════════

func TestServerIngest_FromFile_ThenQueryReturnsData(t *testing.T) {
	baseURL := newTestServer(t)

	_, _, err := runCmd(t, "--server", baseURL, "ingest",
		testdataPath("logs/access.log"), "--index", "cli_ingest_test")
	if err != nil {
		t.Fatalf("ingest command failed: %v", err)
	}

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM cli_ingest_test | stats count")
	if err != nil {
		t.Fatalf("query after ingest failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 1000 {
		t.Errorf("after CLI ingest: count = %d, want 1000", got)
	}
}

func TestServerIngest_WithIndex_QueriedByIndex(t *testing.T) {
	baseURL := newTestServer(t)

	_, _, err := runCmd(t, "--server", baseURL, "ingest",
		testdataPath("logs/backend_server.log"), "--index", "custom_idx")
	if err != nil {
		t.Fatalf("ingest failed: %v", err)
	}

	// Data should be in custom_idx.
	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM custom_idx | stats count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 26 {
		t.Errorf("FROM custom_idx count = %d, want 26", got)
	}

	// Data should NOT be in main (nothing ingested there).
	stdout2, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM main | stats count")
	if err != nil {
		t.Fatalf("query main failed: %v", err)
	}

	gotMain := jsonCount(t, stdout2)
	if gotMain != 0 {
		t.Errorf("FROM main count = %d, want 0 (data should be in custom_idx only)", gotMain)
	}
}

func TestServerIngest_WithSourceMetadata(t *testing.T) {
	baseURL := newTestServer(t)

	_, _, err := runCmd(t, "--server", baseURL, "ingest",
		testdataPath("logs/access.log"), "--source", "webserver", "--index", "meta_test")
	if err != nil {
		t.Fatalf("ingest with --source failed: %v", err)
	}

	// Verify data is queryable.
	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM meta_test | stats count")
	if err != nil {
		t.Fatalf("query after ingest failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 1000 {
		t.Errorf("after ingest with --source: count = %d, want 1000", got)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Group 4: Non-Query CLI Commands
// ═══════════════════════════════════════════════════════════════════════════

func TestServerStatus_AfterIngest_EventCountCorrect(t *testing.T) {
	baseURL := newTestServer(t)
	ingestTestData(t, baseURL, "main", "testdata/logs/access.log")

	stdout, _, err := runCmd(t, "--server", baseURL, "status", "--format", "json")
	if err != nil {
		t.Fatalf("status failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &result); err != nil {
		t.Fatalf("parse status JSON: %v\noutput: %q", err, stdout)
	}

	totalEvents, ok := result["total_events"]
	if !ok {
		t.Fatal("status JSON missing 'total_events'")
	}

	te := int(totalEvents.(float64))
	if te < 1000 {
		t.Errorf("total_events = %d, want >= 1000", te)
	}
}

func TestServerStatus_ContainsAllKeys(t *testing.T) {
	baseURL := newTestServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "status", "--format", "json")
	if err != nil {
		t.Fatalf("status failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &result); err != nil {
		t.Fatalf("parse status JSON: %v", err)
	}

	required := []string{"uptime_seconds", "total_events", "segment_count", "health"}
	for _, key := range required {
		if _, ok := result[key]; !ok {
			t.Errorf("status JSON missing required key %q, got keys: %v", key, cliMapKeys(result))
		}
	}
}

func TestServerHealth_Returns_OK(t *testing.T) {
	baseURL := newTestServer(t)

	_, _, err := runCmd(t, "--server", baseURL, "health")
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
}

func TestServerFields_AfterIngest_DiscoveredFields(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "fields", "--format", "json")
	if err != nil {
		t.Fatalf("fields failed: %v", err)
	}

	fieldNames := make(map[string]bool)
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("parse field JSON: %v\nline: %q", err, line)
		}
		if name, ok := entry["name"].(string); ok {
			fieldNames[name] = true
		}
	}

	// access.log should contribute these fields.
	expected := []string{"level", "host", "status"}
	for _, f := range expected {
		if !fieldNames[f] {
			t.Errorf("expected field %q in catalog, got: %v", f, fieldNames)
		}
	}
}

func TestServerFields_HasTypeInfo(t *testing.T) {
	baseURL := newTestServer(t)
	ingestTestData(t, baseURL, "main", "testdata/logs/access.log")

	stdout, _, err := runCmd(t, "--server", baseURL, "fields", "--format", "json")
	if err != nil {
		t.Fatalf("fields failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) == 0 {
		t.Fatal("no fields returned")
	}

	// Check first field entry has name and type.
	var first map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(lines[0])), &first); err != nil {
		t.Fatalf("parse first field: %v", err)
	}

	if _, ok := first["name"]; !ok {
		t.Error("field entry missing 'name'")
	}
	if _, ok := first["type"]; !ok {
		t.Error("field entry missing 'type'")
	}
}

func TestServerCount_AllEvents(t *testing.T) {
	baseURL := newTestServer(t)
	ingestTestData(t, baseURL, "main", "testdata/logs/access.log")

	stdout, _, err := runCmd(t, "--server", baseURL, "count", "--format", "json")
	if err != nil {
		t.Fatalf("count command failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 1000 {
		t.Errorf("count = %d, want 1000", got)
	}
}

func TestServerCount_WithFilter(t *testing.T) {
	baseURL := newTestServer(t)
	ingestTestData(t, baseURL, "main", "testdata/logs/access.log")

	stdout, _, err := runCmd(t, "--server", baseURL, "count", "--format", "json",
		`where level="ERROR"`)
	if err != nil {
		t.Fatalf("count with filter failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 294 {
		t.Errorf("filtered count = %d, want 294", got)
	}
}

func TestServerExplain_ValidQuery(t *testing.T) {
	baseURL := newTestServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "explain", "--format", "json",
		"level=error | stats count")
	if err != nil {
		t.Fatalf("explain failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &result); err != nil {
		t.Fatalf("parse explain JSON: %v\noutput: %q", err, stdout)
	}

	if _, ok := result["parsed"]; !ok {
		t.Error("explain JSON missing 'parsed' key")
	}
}

func TestServerExplain_InvalidQuery(t *testing.T) {
	baseURL := newTestServer(t)

	stdout, _, _ := runCmd(t, "--server", baseURL, "explain", "--format", "json", "| where")

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &result); err != nil {
		t.Fatalf("parse explain JSON: %v\noutput: %q", err, stdout)
	}

	isValid, ok := result["is_valid"].(bool)
	if !ok {
		t.Fatalf("explain JSON missing 'is_valid', got keys: %v", cliMapKeys(result))
	}
	if isValid {
		t.Error("expected is_valid=false for incomplete WHERE, got true")
	}
}

func TestServerSample_ReturnsSubset(t *testing.T) {
	baseURL := newTestServer(t)
	ingestTestData(t, baseURL, "main", "testdata/logs/access.log")

	stdout, _, err := runCmd(t, "--server", baseURL, "sample", "--format", "json", "3")
	if err != nil {
		t.Fatalf("sample 3 failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Errorf("sample 3 returned %d rows, want 3", len(rows))
	}
}

func TestServerSample_EventsHaveFields(t *testing.T) {
	baseURL := newTestServer(t)
	ingestTestData(t, baseURL, "main", "testdata/logs/access.log")

	stdout, _, err := runCmd(t, "--server", baseURL, "sample", "--format", "json", "1")
	if err != nil {
		t.Fatalf("sample 1 failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 1 {
		t.Fatalf("expected 1 sampled event, got %d", len(rows))
	}

	if len(rows[0]) == 0 {
		t.Error("sampled event has no fields")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Group 5: Flags and Options
// ═══════════════════════════════════════════════════════════════════════════

func TestServerQuery_QuietFlag_NoStderr(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	_, stderr, err := runCmd(t, "--server", baseURL, "--quiet", "query", "--format", "json",
		"FROM idx_access | stats count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if trimmed := strings.TrimSpace(stderr); trimmed != "" {
		t.Errorf("expected empty stderr with --quiet, got: %q", trimmed)
	}
}

func TestServerQuery_FailOnEmpty_ReturnsError(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	_, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"--fail-on-empty",
		`FROM idx_access | where level="NONEXISTENT_LEVEL_XYZ"`)
	if err == nil {
		t.Fatal("expected error with --fail-on-empty and zero results, got nil")
	}
}

func TestServerQuery_OutputToFile(t *testing.T) {
	baseURL := setupMultiIndexServer(t)
	outFile := filepath.Join(t.TempDir(), "results.json")

	_, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"--file", testdataPath("logs/access.log"), "--output", outFile,
		"| stats count by level")
	if err != nil {
		t.Fatalf("query with --output failed: %v", err)
	}

	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}

	rows := mustParseJSON(t, string(data))
	if len(rows) != 3 {
		t.Errorf("output file has %d rows, want 3", len(rows))
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Group 6: Error Handling
// ═══════════════════════════════════════════════════════════════════════════

func TestServerQuery_ParseError_ReturnsError(t *testing.T) {
	baseURL := newTestServer(t)

	_, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json", "| where")
	if err == nil {
		t.Fatal("expected error for incomplete WHERE clause, got nil")
	}
}

func TestServerQuery_ConnectionRefused_ReturnsError(t *testing.T) {
	// Port 1 is almost certainly not listening.
	_, _, err := runCmd(t, "--server", "http://127.0.0.1:1", "query", "--format", "json",
		"| stats count")
	if err == nil {
		t.Fatal("expected connection error, got nil")
	}
}

func TestServerQuery_IncompleteFromTo_ReturnsError(t *testing.T) {
	baseURL := newTestServer(t)

	// --from without --to should fail.
	_, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"--from", "2026-01-01T00:00:00Z",
		"| stats count")
	if err == nil {
		t.Fatal("expected error for --from without --to, got nil")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Group 7: Multi-Index Cross-Queries
// ═══════════════════════════════════════════════════════════════════════════

func TestServerQuery_BackendIndex_JSONFields(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	// backend_server.log is JSON format with service, level, duration_ms fields.
	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_backend | stats count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 26 {
		t.Errorf("idx_backend count = %d, want 26", got)
	}
}

func TestServerQuery_NginxIndex_HasEvents(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	// Use stats count instead of HEAD to avoid the LimitIterator panic bug
	// documented in TestServerQuery_HEAD_SmallIndex_Panics below.
	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_nginx | stats count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 34 {
		t.Errorf("idx_nginx count = %d, want 34", got)
	}
}

// TestServerQuery_HEAD_SmallIndex_Panics verifies that HEAD N on a small index
// works correctly after the Batch.Slice sparse-column fix.
//
// Previously: Batch.Slice panicked with "slice bounds out of range [:5] with capacity 1"
// when columns had different lengths due to AddRow not padding missing fields.
func TestServerQuery_HEAD_SmallIndex_Panics(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_nginx | head 5")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 5 {
		t.Errorf("idx_nginx head 5 = %d rows, want 5", len(rows))
	}
}

func TestServerQuery_IndexIsolation_NoLeaks(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	// Query idx_access should NOT contain backend events.
	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | stats count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 1000 {
		t.Errorf("idx_access count = %d, want exactly 1000 (no cross-index leakage)", got)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Group 8: SPL2 Pipeline Chaining (server mode)
// ═══════════════════════════════════════════════════════════════════════════

func TestServerQuery_Pipeline_WhereEvalStats(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		`FROM idx_access | where level="ERROR" | eval slow=if(response_time>500,"slow","fast") | stats count by slow`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) < 1 {
		t.Fatal("expected at least 1 row from pipeline")
	}

	total := 0
	for _, row := range rows {
		total += int(row["count"].(float64))
	}

	if total != 294 {
		t.Errorf("chained pipeline sum = %d, want 294 (all ERROR events)", total)
	}
}

func TestServerQuery_Pipeline_SortHeadFields(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | stats count by host | sort -count | head 3 | fields host, count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Verify sort order preserved after head and fields.
	for i := 1; i < len(rows); i++ {
		prev := rows[i-1]["count"].(float64)
		curr := rows[i]["count"].(float64)
		if prev < curr {
			t.Errorf("sort order broken at row %d: %v < %v", i, prev, curr)
		}
	}

	// Verify only projected fields present.
	for i, row := range rows {
		for k := range row {
			if k != "host" && k != "count" {
				t.Errorf("row %d has unexpected field %q after fields projection", i, k)
			}
		}
	}
}

func TestServerQuery_Pipeline_StatsGroupByMethod(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"FROM idx_access | stats count by method | sort level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	// access.log has 4 methods: GET, POST, PUT, DELETE.
	if len(rows) != 4 {
		t.Errorf("expected 4 method groups, got %d", len(rows))
	}

	total := 0
	methods := make([]string, 0, len(rows))
	for _, row := range rows {
		method, _ := row["method"].(string)
		methods = append(methods, method)
		total += int(row["count"].(float64))
	}

	if total != 1000 {
		t.Errorf("sum across all methods = %d, want 1000", total)
	}

	sort.Strings(methods)
	expected := []string{"DELETE", "GET", "POST", "PUT"}
	sort.Strings(expected)
	for i := range expected {
		if i < len(methods) && methods[i] != expected[i] {
			t.Errorf("method[%d] = %q, want %q", i, methods[i], expected[i])
		}
	}
}

func TestServerQuery_Pipeline_SearchThenStats(t *testing.T) {
	baseURL := setupMultiIndexServer(t)

	// Full-text search for "web-01" then aggregate.
	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		`FROM idx_access | search "web-01" | stats count`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got <= 0 {
		t.Errorf("search 'web-01' | stats count = %d, want > 0", got)
	}
	if got > 1000 {
		t.Errorf("search 'web-01' count = %d, cannot exceed 1000", got)
	}
}
