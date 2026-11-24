package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

// testLevelError is the ERROR level string used in testdata/access.log.
// Defined as a variable to avoid goconst triggering across the package.
var testLevelError = "ERROR" //nolint:gochecknoglobals // test-only constant

// File-mode query tests (no server needed)

func TestQueryFile_StatsCount_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json", "source=main | stats count() as count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 1000 {
		t.Errorf("expected count=1000, got %d", got)
	}
}

func TestQueryFile_StatsCount_DefaultColumnName(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"source=main | stats count()")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	count, ok := rows[0]["count()"]
	if !ok {
		t.Fatalf("missing 'count()' column in result: %v", rows[0])
	}
	if c, ok := count.(float64); !ok || c != 1000 {
		t.Errorf("count() = %v, want 1000", count)
	}
}

func TestQueryFile_NDJSONTempFile(t *testing.T) {
	tmpFile := t.TempDir() + "/events.ndjson"
	lines := []string{
		`{"level":"error","service":"api","duration_ms":150}`,
		`{"level":"info","service":"web","duration_ms":50}`,
		`{"level":"error","service":"api","duration_ms":250}`,
		`{"level":"info","service":"api","duration_ms":30}`,
	}
	if err := os.WriteFile(tmpFile, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	stdout, _, err := runCmd(t, "query", "--file", tmpFile, "--format", "json", "--raw",
		`source=main | where level = "error" | stats count()`)
	if err != nil {
		t.Fatalf("NDJSON query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 2 {
		t.Errorf("count = %d, want 2", got)
	}
}

// boundsServer starts a server that answers every query with no events and
// returns its URL and a func that returns the from and to of each query it
// got, nil where a request left one out.
func boundsServer(t *testing.T) (string, func() [][2]any) {
	t.Helper()
	var (
		mu     sync.Mutex
		bounds [][2]any
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		mu.Lock()
		bounds = append(bounds, [2]any{req["from"], req["to"]})
		mu.Unlock()

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"type":     "events",
				"events":   []map[string]interface{}{},
				"total":    0,
				"has_more": false,
			},
			"meta": map[string]interface{}{},
		})
	}))
	t.Cleanup(srv.Close)

	return srv.URL, func() [][2]any {
		mu.Lock()
		defer mu.Unlock()
		return bounds
	}
}

// TestQueryTimeFlagsSendText pins the bounds that the time flags send: text
// that the server resolves on its own clock, and no from for --to alone.
func TestQueryTimeFlagsSendText(t *testing.T) {
	for _, c := range []struct {
		args     []string
		from, to any
	}{
		{nil, nil, nil},
		{[]string{"--since", "1h"}, "-1h", "now"},
		{[]string{"--since", "1d@d"}, "-1d@d", "now"},
		{[]string{"--since", "@d"}, "@d", "now"},
		{[]string{"--from", "2025-01-15T08:00:00Z"}, "2025-01-15T08:00:00Z", "now"},
		{[]string{"--to", "2025-01-15T08:00:00Z"}, nil, "2025-01-15T08:00:00Z"},
		{[]string{"--from", "-1d@d", "--to", "@d"}, "-1d@d", "@d"},
	} {
		url, bounds := boundsServer(t)
		args := append([]string{"--server", url, "--quiet", "query"}, c.args...)
		if _, _, err := runCmd(t, append(args, "search source=main error")...); err != nil {
			t.Fatalf("%q: %v", c.args, err)
		}
		if got := bounds(); len(got) != 1 || got[0] != [2]any{c.from, c.to} {
			t.Errorf("%q: sent %v, want one query with from %v and to %v", c.args, got, c.from, c.to)
		}
	}
}

// TestQueryRefusedTimeFlagsSendNothing pins that time flags that conflict
// or hold a value that is not valid fail as usage errors before a request.
func TestQueryRefusedTimeFlagsSendNothing(t *testing.T) {
	for _, args := range [][]string{
		{"--since", "15m", "--from", "-1h"},
		{"--since", "15m", "--to", "now"},
		{"--since", "banana"},
		{"--from", "banana"},
		{"--to", "banana"},
		{"--from", "-1h", "--to", "-2h"},
		{"--explain", "--since", "banana"},
	} {
		url, bounds := boundsServer(t)
		_, _, err := runCmd(t, append(append([]string{"--server", url, "query"}, args...), "source=main")...)
		var usage usageError
		if !errors.As(err, &usage) {
			t.Errorf("%q: got %v, want a usage error", args, err)
		}
		if got := bounds(); len(got) != 0 {
			t.Errorf("%q: sent %v, want no request", args, got)
		}
	}
}

// TestQueryDefaultSince pins that default_since of .lynxdbrc bounds a
// server query that gives no time flag and gives way to any time flag.
func TestQueryDefaultSince(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".lynxdbrc"), []byte("default_since: 6h\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)

	for _, c := range []struct {
		args     []string
		from, to any
	}{
		{nil, "-6h", "now"},
		{[]string{"--since", "1h"}, "-1h", "now"},
		{[]string{"--from", "-1d"}, "-1d", "now"},
		{[]string{"--to", "-1d"}, nil, "-1d"},
	} {
		url, bounds := boundsServer(t)
		args := append([]string{"--server", url, "--quiet", "query"}, c.args...)
		if _, _, err := runCmd(t, append(args, "source=main")...); err != nil {
			t.Fatalf("%q: %v", c.args, err)
		}
		if got := bounds(); len(got) != 1 || got[0] != [2]any{c.from, c.to} {
			t.Errorf("%q: sent %v, want one query with from %v and to %v", c.args, got, c.from, c.to)
		}
	}

	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"stats count() as count")
	if err != nil {
		t.Fatal(err)
	}
	if got := jsonCount(t, stdout); got != 1000 {
		t.Errorf("local query: count = %d, want all 1000 events", got)
	}
}

// TestQueryInvalidDefaultSince pins that a default_since that is not valid
// fails as a usage error that names it, before a request.
func TestQueryInvalidDefaultSince(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".lynxdbrc"), []byte("default_since: banana\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)

	url, bounds := boundsServer(t)
	_, _, err := runCmd(t, "--server", url, "query", "source=main")
	var usage usageError
	if !errors.As(err, &usage) || !strings.HasPrefix(err.Error(), `invalid default_since "banana": `) {
		t.Errorf("got %v, want a usage error that names default_since", err)
	}
	if got := bounds(); len(got) != 0 {
		t.Errorf("sent %v, want no request", got)
	}
}

func TestQueryFile_StatsCountByLevel_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json", "source=main | stats count() as count by level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows (one per level), got %d", len(rows))
	}

	// Build a map of level -> count.
	levelCounts := make(map[string]int)
	total := 0

	for _, row := range rows {
		level, _ := row["level"].(string)
		count := int(row["count"].(float64))
		levelCounts[level] = count
		total += count
	}

	if total != 1000 {
		t.Errorf("expected total count=1000, got %d", total)
	}

	if levelCounts[testLevelError] != 294 {
		t.Errorf("expected ERROR=294, got %d", levelCounts[testLevelError])
	}

	if levelCounts["INFO"] != 359 {
		t.Errorf("expected INFO=359, got %d", levelCounts["INFO"])
	}

	if levelCounts["WARN"] != 347 {
		t.Errorf("expected WARN=347, got %d", levelCounts["WARN"])
	}
}

func TestQueryFile_Filter_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"source=main | where level = \""+testLevelError+"\" | stats count() as count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 294 {
		t.Errorf("expected count=294, got %d", got)
	}
}

func TestQueryFile_Head_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json", "source=main | head 5")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(rows))
	}
}

func TestQueryFile_SortDescHead_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"source=main | stats count() as count by level | sort - count | head 3")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Each row should have "level" and "count" fields.
	for i, row := range rows {
		if _, ok := row["level"]; !ok {
			t.Errorf("row %d missing 'level' field", i)
		}

		if _, ok := row["count"]; !ok {
			t.Errorf("row %d missing 'count' field", i)
		}
	}

	// First row should have the highest count (INFO=359).
	first := rows[0]["count"].(float64)
	second := rows[1]["count"].(float64)
	if first < second {
		t.Errorf("expected first row count >= second, got %v < %v", first, second)
	}
}

func TestQueryFile_FieldsProjection_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"source=main | fields host, level | head 3")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	for i, row := range rows {
		if _, ok := row["host"]; !ok {
			t.Errorf("row %d missing 'host' field", i)
		}

		if _, ok := row["level"]; !ok {
			t.Errorf("row %d missing 'level' field", i)
		}

		// Should not have other data fields (besides default event metadata).
		for k := range row {
			if k != "host" && k != "level" && k != "_time" && k != "_timestamp" &&
				k != "_raw" && k != "_source" && k != "_sourcetype" {
				t.Errorf("row %d has unexpected field %q", i, k)
			}
		}
	}
}

func TestQueryFile_Search_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"search source=main "+testLevelError+" | stats count() as count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	// Full-text search for "ERROR" should match all ERROR-level events.
	if got != 294 {
		t.Errorf("expected count=294, got %d", got)
	}
}

func TestQueryFile_Sort_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"source=main | stats count() as count by host | sort - count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) < 2 {
		t.Fatalf("expected multiple rows, got %d", len(rows))
	}

	// Verify rows are in descending count order.
	for i := 1; i < len(rows); i++ {
		prev := rows[i-1]["count"].(float64)
		curr := rows[i]["count"].(float64)
		if prev < curr {
			t.Errorf("row %d count %v < row %d count %v — not sorted descending", i-1, prev, i, curr)
		}
	}
}

func TestQueryFile_Eval_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		`source=main | eval fast = if(response_time < 100, "yes", "no") | stats count() as count by fast`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) < 1 {
		t.Fatal("expected at least 1 row")
	}

	// Check that rows have "fast" and "count" fields.
	for i, row := range rows {
		fast, ok := row["fast"].(string)
		if !ok {
			t.Errorf("row %d missing or non-string 'fast' field", i)

			continue
		}

		if fast != "yes" && fast != "no" {
			t.Errorf("row %d has unexpected fast value: %q", i, fast)
		}
	}
}

func TestQueryFile_Dedup_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"source=main | dedup level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Errorf("expected 3 rows (one per unique level), got %d", len(rows))
	}

	// Verify all levels are distinct.
	seen := make(map[string]bool)
	for _, row := range rows {
		level, _ := row["level"].(string)
		if seen[level] {
			t.Errorf("duplicate level %q in dedup output", level)
		}

		seen[level] = true
	}
}

func TestQueryFile_Rename_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"source=main | stats count() as count by level | rename count as total")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row")
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

func TestQueryFile_CSVFormat(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "csv",
		"source=main | stats count() as count by level")
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
		t.Errorf("expected 4 CSV records (header + 3 rows), got %d", len(records))
	}

	// Header should contain "count" and "level".
	header := records[0]
	headerStr := strings.Join(header, ",")
	if !strings.Contains(headerStr, "count") || !strings.Contains(headerStr, "level") {
		t.Errorf("CSV header missing expected columns, got: %v", header)
	}
}

func TestQueryFile_TableFormat(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "table",
		"source=main | stats count() as count by level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// Table output should contain column headers.
	if !strings.Contains(stdout, "count") {
		t.Errorf("table output missing 'count' column header")
	}

	if !strings.Contains(stdout, "level") {
		t.Errorf("table output missing 'level' column header")
	}

	// Should contain all level values.
	if !strings.Contains(stdout, testLevelError) {
		t.Errorf("table output missing %s level", testLevelError)
	}

	if !strings.Contains(stdout, "INFO") {
		t.Errorf("table output missing INFO level")
	}

	if !strings.Contains(stdout, "WARN") {
		t.Errorf("table output missing WARN level")
	}
}

func TestQueryFile_TableFormat_FieldsDoesNotAutoAddEventMetadata(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "table",
		"source=main | fields level | head 3")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if strings.Contains(stdout, "_source") {
		t.Error("table output should not auto-add '_source' column header")
	}
	if strings.Contains(stdout, "_sourcetype") {
		t.Error("table output should not auto-add '_sourcetype' column header")
	}
	if !strings.Contains(stdout, "level") {
		t.Error("table output missing 'level' column header")
	}
	if strings.Contains(stdout, " sourcetype ") || strings.Contains(stdout, " source ") {
		t.Error("table output should not include source/sourcetype aliases")
	}
}

func TestQueryFile_TableFormat_ExplicitEventMetadata(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "table",
		"source=main | fields level, source | head 3")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if !strings.Contains(stdout, "source") {
		t.Error("table output missing explicit 'source' column header")
	}
	if !strings.Contains(stdout, "level") {
		t.Error("table output missing 'level' column header")
	}
}

func TestQueryFile_RawFormat(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "raw",
		"source=main | head 3")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 raw lines, got %d", len(lines))
	}
}

func TestQueryFile_SingleValue_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"source=main | stats count() as count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 1 {
		t.Errorf("expected exactly 1 JSON row, got %d", len(rows))
	}

	if len(rows) > 0 && len(rows[0]) != 1 {
		t.Errorf("expected 1 key in row, got %d keys: %v", len(rows[0]), cliMapKeys(rows[0]))
	}
}

func TestQueryFile_EmptyResult_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		`source=main | where level = "NONEXISTENT" | stats count() as count`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 0 {
		t.Errorf("expected count=0 for nonexistent filter, got %d", got)
	}
}

func TestQueryFile_QuietFlag(t *testing.T) {
	_, stderr, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"--quiet", "source=main | stats count() as count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if strings.TrimSpace(stderr) != "" {
		t.Errorf("expected empty stderr with --quiet, got: %q", stderr)
	}
}

func TestQueryFile_ParseError(t *testing.T) {
	_, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "source=main | where")
	if err == nil {
		t.Fatal("expected error for incomplete WHERE clause, got nil")
	}
}

func TestQueryFile_JSONValidNDJSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"source=main | head 10")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) != 10 {
		t.Errorf("expected 10 lines, got %d", len(lines))
	}

	// Each line must be individually valid JSON.
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Errorf("line %d is not valid JSON: %q", i, line)
		}
	}
}

func TestQueryFile_CSVQuoting(t *testing.T) {
	// The "msg" field in access.log contains quoted strings that may need CSV escaping.
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "csv",
		"source=main | fields msg | head 5")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// Verify it parses as valid CSV.
	reader := csv.NewReader(strings.NewReader(strings.TrimSpace(stdout)))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("CSV parse error (quoting issue?): %v", err)
	}

	// Header + 5 data rows.
	if len(records) < 2 {
		t.Errorf("expected at least header + 1 data row, got %d records", len(records))
	}
}

func TestQueryFile_GlobPattern(t *testing.T) {
	// testdata/logs/access.log should be matched by the glob.
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access*.log"), "--format", "json",
		"source=main | stats count() as count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 1000 {
		t.Errorf("expected count=1000 from glob pattern, got %d", got)
	}
}

func TestQueryFile_StatsCountByHost_SumsTo1000(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"source=main | stats count() as count by host")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	total := 0

	for _, row := range rows {
		count := int(row["count"].(float64))
		total += count
	}

	if total != 1000 {
		t.Errorf("sum of counts by host should be 1000, got %d", total)
	}
}

func TestQueryFile_Fillnull_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"source=main | fillnull with 0 in response_time | head 3")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows))
	}
}

func TestQueryFile_MultipleFormats_ConsistentCounts(t *testing.T) {
	// Run the same aggregation in JSON and CSV formats; both should show 3 rows.
	stdoutJSON, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"source=main | stats count() as count by level")
	if err != nil {
		t.Fatalf("JSON query failed: %v", err)
	}

	stdoutCSV, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "csv",
		"source=main | stats count() as count by level")
	if err != nil {
		t.Fatalf("CSV query failed: %v", err)
	}

	jsonRows := mustParseJSON(t, stdoutJSON)

	csvReader := csv.NewReader(strings.NewReader(strings.TrimSpace(stdoutCSV)))
	csvRecords, _ := csvReader.ReadAll()

	// CSV has header + data rows, JSON is just data rows.
	csvDataRows := len(csvRecords) - 1
	if csvDataRows != len(jsonRows) {
		t.Errorf("JSON returned %d rows but CSV returned %d data rows", len(jsonRows), csvDataRows)
	}
}

func TestQueryFile_Sort_Ascending_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"source=main | stats count() as count by level | sort count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) < 2 {
		t.Fatalf("expected multiple rows, got %d", len(rows))
	}

	// Verify ascending order.
	for i := 1; i < len(rows); i++ {
		prev := rows[i-1]["count"].(float64)
		curr := rows[i]["count"].(float64)
		if prev > curr {
			t.Errorf("row %d count %v > row %d count %v — not sorted ascending", i-1, prev, i, curr)
		}
	}
}

func TestQueryFile_StatsMultipleAggs_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"source=main | stats count() as count, avg(response_time) as avg_rt by level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	for i, row := range rows {
		if _, ok := row["count"]; !ok {
			t.Errorf("row %d missing 'count'", i)
		}

		if _, ok := row["avg_rt"]; !ok {
			t.Errorf("row %d missing 'avg_rt'", i)
		}
	}
}

func TestQueryFile_LevelValues_Exhaustive(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"source=main | stats count() as count by level | sort level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)

	levels := make([]string, 0, len(rows))
	for _, row := range rows {
		levels = append(levels, row["level"].(string))
	}

	sort.Strings(levels)
	expected := []string{testLevelError, "INFO", "WARN"}
	sort.Strings(expected)

	if len(levels) != len(expected) {
		t.Fatalf("expected levels %v, got %v", expected, levels)
	}

	for i := range expected {
		if levels[i] != expected[i] {
			t.Errorf("level[%d] = %q, want %q", i, levels[i], expected[i])
		}
	}
}

// TestCLI_BareWordIsACommandName requires arguments that name no command to
// fail as such, whatever standard input is: read as a query, a mistyped or
// removed command would search standard input for its own name and succeed.
func TestCLI_BareWordIsACommandName(t *testing.T) {
	for _, args := range [][]string{{"health"}, {"staus"}, {"import", "events.json"}} {
		_, _, err := runCmd(t, args...)
		var unknown unknownCommandError
		if !errors.As(err, &unknown) {
			t.Errorf("lynxdb %s: error %v, want an unknown command", strings.Join(args, " "), err)
		}
	}
	_, _, err := runCmd(t, "staus")
	if err == nil || !strings.Contains(err.Error(), "did you mean status") {
		t.Errorf("lynxdb staus: error %v, want a suggestion of status", err)
	}
}
