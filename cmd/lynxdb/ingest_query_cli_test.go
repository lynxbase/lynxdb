package main

import (
	"strings"
	"testing"
)

// setupServerWithData starts a test server and ingests access.log into the "main" index.
func setupServerWithData(t *testing.T) string {
	t.Helper()

	baseURL := newTestServer(t)
	ingestTestData(t, baseURL, "main", "testdata/logs/access.log")

	return baseURL
}

func TestIngestAndQuery_StatsCount_JSON(t *testing.T) {
	baseURL := setupServerWithData(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"source=main | stats count()")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 1000 {
		t.Errorf("expected count=1000, got %d", got)
	}
}

func TestIngestAndQuery_FilteredCount_JSON(t *testing.T) {
	baseURL := setupServerWithData(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"source=main | where level = \""+testLevelError+"\" | stats count()")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 294 {
		t.Errorf("expected count=294 for ERROR filter, got %d", got)
	}
}

func TestIngestAndQuery_StatsCountByLevel_JSON(t *testing.T) {
	baseURL := setupServerWithData(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"source=main | stats count() by level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows (one per level), got %d", len(rows))
	}

	// Build a map of level -> count.
	levelCounts := make(map[string]int)
	for _, row := range rows {
		level, _ := row["level"].(string)
		count := int(row["count()"].(float64))
		levelCounts[level] = count
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

func TestIngestAndQuery_FieldsProjection_JSON(t *testing.T) {
	baseURL := setupServerWithData(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		"source=main | fields level | head 5")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}

	for i, row := range rows {
		if _, ok := row["level"]; !ok {
			t.Errorf("row %d missing 'level' field", i)
		}
	}
}

func TestIngestAndQuery_NDJSON(t *testing.T) {
	baseURL := setupServerWithData(t)

	// Server-mode query always returns NDJSON regardless of --format flag.
	stdout, _, err := runCmd(t, "--server", baseURL, "query",
		"source=main | stats count() by level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 NDJSON rows (one per level), got %d", len(rows))
	}

	for _, row := range rows {
		if _, ok := row["level"]; !ok {
			t.Errorf("NDJSON row missing 'level' key: %v", row)
		}

		if _, ok := row["count()"]; !ok {
			t.Errorf("NDJSON row missing 'count()' key: %v", row)
		}
	}
}

func TestIngestAndQuery_Table(t *testing.T) {
	baseURL := setupServerWithData(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "table",
		"source=main | stats count() by level")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if !strings.Contains(stdout, "count()") {
		t.Errorf("expected 'count()' column in table output")
	}

	if !strings.Contains(stdout, "level") {
		t.Errorf("expected 'level' column in table output")
	}
}
