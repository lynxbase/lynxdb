package main

import (
	"encoding/csv"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
)

// testLevelError is the ERROR level string used in testdata/access.log.
// Defined as a variable to avoid goconst triggering across the package.
var testLevelError = "ERROR" //nolint:gochecknoglobals // test-only constant

// File-mode query tests (no server needed)

func TestQueryFile_StatsCount_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json", "| stats count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 1000 {
		t.Errorf("expected count=1000, got %d", got)
	}
}

func TestQueryQueriesFile_FileMode_NDJSONEnvelopes(t *testing.T) {
	queryFile := t.TempDir() + "/queries.spl2"
	if err := os.WriteFile(queryFile, []byte("# generated\n| stats count\n\n| where level=\""+testLevelError+"\" | stats count\n"), 0o600); err != nil {
		t.Fatalf("write queries: %v", err)
	}

	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--queries-file", queryFile)
	if err != nil {
		t.Fatalf("query --queries-file failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 2 {
		t.Fatalf("expected 2 envelopes, got %d", len(rows))
	}
	if rows[0]["line_number"].(float64) != 2 {
		t.Fatalf("first line_number = %v, want 2", rows[0]["line_number"])
	}
	if rows[1]["line_number"].(float64) != 4 {
		t.Fatalf("second line_number = %v, want 4", rows[1]["line_number"])
	}
	if rows[0]["error"] != "" || rows[1]["error"] != "" {
		t.Fatalf("unexpected envelope errors: %#v %#v", rows[0]["error"], rows[1]["error"])
	}
	results := rows[0]["results"].([]interface{})
	firstResult := results[0].(map[string]interface{})
	if int(firstResult["count"].(float64)) != 1000 {
		t.Fatalf("first query count = %v, want 1000", firstResult["count"])
	}
}

func TestQueryQueriesFile_MutuallyExclusiveWithQueryArg(t *testing.T) {
	queryFile := t.TempDir() + "/queries.spl2"
	if err := os.WriteFile(queryFile, []byte("| stats count\n"), 0o600); err != nil {
		t.Fatalf("write queries: %v", err)
	}

	_, _, err := runCmd(t, "query", "--queries-file", queryFile, "| stats count")
	if err == nil {
		t.Fatal("expected mutual exclusion error")
	}
	if !strings.Contains(err.Error(), "--queries-file cannot be used with a positional query") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRewritePreview_TruncatesLongValues(t *testing.T) {
	value := strings.Repeat("x", maxRewritePreviewBytes+100)

	got := rewritePreview(value)
	if len(got) != maxRewritePreviewBytes {
		t.Fatalf("len(rewritePreview) = %d, want %d", len(got), maxRewritePreviewBytes)
	}
	if !strings.HasSuffix(got, "... [truncated]") {
		t.Fatalf("rewritePreview missing truncation suffix: %q", got[len(got)-20:])
	}
}

func TestQueryNoSuggestions_SendsServerFlag(t *testing.T) {
	reqCh := make(chan struct {
		Lint        *bool `json:"lint"`
		Suggestions *bool `json:"suggestions"`
	}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/query" {
			t.Errorf("path = %s, want /api/v1/query", r.URL.Path)
		}

		var req struct {
			Lint        *bool `json:"lint"`
			Suggestions *bool `json:"suggestions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		reqCh <- req

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
	defer srv.Close()

	_, _, err := runCmd(t, "--server", srv.URL, "--quiet", "query", "--format", "json", "--no-suggestions", "error")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	req := <-reqCh
	if req.Suggestions == nil || *req.Suggestions {
		t.Fatalf("suggestions = %v, want pointer to false", req.Suggestions)
	}
	if req.Lint != nil {
		t.Fatalf("lint = %v, want omitted", req.Lint)
	}
}

func TestQueryQueriesFileNoSuggestions_SendsServerFlag(t *testing.T) {
	reqCh := make(chan *bool, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Suggestions *bool `json:"suggestions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		reqCh <- req.Suggestions

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
	defer srv.Close()

	queryFile := t.TempDir() + "/queries.spl2"
	if err := os.WriteFile(queryFile, []byte("error\n"), 0o600); err != nil {
		t.Fatalf("write queries: %v", err)
	}

	_, _, err := runCmd(t, "--server", srv.URL, "--quiet", "query", "--queries-file", queryFile, "--no-suggestions")
	if err != nil {
		t.Fatalf("query --queries-file failed: %v", err)
	}

	suggestions := <-reqCh
	if suggestions == nil || *suggestions {
		t.Fatalf("suggestions = %v, want pointer to false", suggestions)
	}
}

func TestQueryQueriesFile_ReadsQueriesFromStdin(t *testing.T) {
	resetAllFlags(t)
	t.Setenv("LYNXDB_CONFIG", "")
	t.Setenv("LYNXDB_SERVER", "")
	t.Setenv("LYNXDB_TOKEN", "")

	oldStdin := os.Stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	if _, err := w.WriteString("| stats count\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close stdin writer: %v", err)
	}
	os.Stdin = r
	t.Cleanup(func() {
		os.Stdin = oldStdin
		r.Close()
	})

	rootCmd.SetArgs([]string{"query", "--file", testdataPath("logs/access.log"), "--queries-file", "-"})
	stdout, _, err := captureOutput(t, rootCmd.Execute)
	if err != nil {
		t.Fatalf("query --queries-file - failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 1 {
		t.Fatalf("expected 1 envelope, got %d", len(rows))
	}
	if rows[0]["source_file"] != "stdin" {
		t.Fatalf("source_file = %v, want stdin", rows[0]["source_file"])
	}
}

func TestQueryFile_StatsCountByLevel_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json", "| stats count by level")
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
		"| where level=\""+testLevelError+"\" | stats count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 294 {
		t.Errorf("expected count=294, got %d", got)
	}
}

func TestQueryFile_Head_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json", "| head 5")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(rows))
	}
}

func TestQueryFile_SortDescHead_JSON(t *testing.T) {
	// Equivalent to "top 3 level" via stats + sort + head pipeline.
	// The native `top` command uses a different code path in the ephemeral
	// engine that doesn't extract kv fields, so we test the pipeline form.
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"| stats count by level | sort -count | head 3")
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
		"| fields host, level | head 3")
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

		// Should not have other data fields (besides _time/_timestamp which are internal).
		for k := range row {
			if k != "host" && k != "level" && k != "_time" && k != "_timestamp" && k != "_raw" {
				t.Errorf("row %d has unexpected field %q", i, k)
			}
		}
	}
}

func TestQueryFile_Search_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"| search \""+testLevelError+"\" | stats count")
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
		"| stats count by host | sort -count")
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
		`| eval fast=if(response_time<100,"yes","no") | stats count by fast`)
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
		"| dedup level")
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
		"| stats count by level | rename count AS total")
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
		"| stats count by level")
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
		"| stats count by level")
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

func TestQueryFile_RawFormat(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "raw",
		"| head 3")
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
		"| stats count")
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
		`| where level="NONEXISTENT" | stats count`)
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
		"--quiet", "| stats count")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if strings.TrimSpace(stderr) != "" {
		t.Errorf("expected empty stderr with --quiet, got: %q", stderr)
	}
}

func TestQueryFile_ParseError(t *testing.T) {
	// "| where" without a predicate is a genuine parse error.
	_, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "| where")
	if err == nil {
		t.Fatal("expected error for incomplete WHERE clause, got nil")
	}
}

func TestQueryFile_JSONValidNDJSON(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "json",
		"| head 10")
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
		"| fields msg | head 5")
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
		"| stats count")
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
		"| stats count by host")
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
		"| head 3 | fillnull value=MISSING")
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
		"| stats count by level")
	if err != nil {
		t.Fatalf("JSON query failed: %v", err)
	}

	stdoutCSV, _, err := runCmd(t, "query", "--file", testdataPath("logs/access.log"), "--format", "csv",
		"| stats count by level")
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
		"| stats count by level | sort count")
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
		"| stats count, avg(response_time) as avg_rt by level")
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
		"| stats count by level | sort level")
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
