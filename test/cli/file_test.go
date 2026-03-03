//go:build clitest

package cli_test

import (
	"encoding/json"
	"strings"
	"testing"
)

// access.log known counts: total=1000, ERROR=294, INFO=359, WARN=347
// 5 hosts: web-01, web-02, web-03, api-01, api-02
// 4 methods: GET, POST, PUT, DELETE

func TestFile_StatsCount(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"),
		"--format", "json", "| stats count")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got != 1000 {
		t.Errorf("expected count=1000, got %d", got)
	}
}

func TestFile_CountByLevel(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"),
		"--format", "json", "| stats count by level")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 groups (ERROR, INFO, WARN), got %d", len(rows))
	}

	levelCounts := make(map[string]int)
	total := 0
	for _, row := range rows {
		level, _ := row["level"].(string)
		count := int(row["count"].(float64))
		levelCounts[level] = count
		total += count
	}

	if total != 1000 {
		t.Errorf("expected total=1000, got %d", total)
	}

	expected := map[string]int{"ERROR": 294, "INFO": 359, "WARN": 347}
	for level, want := range expected {
		if got := levelCounts[level]; got != want {
			t.Errorf("level=%s: expected %d, got %d", level, want, got)
		}
	}
}

func TestFile_WhereFilter(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"),
		"--format", "json", `| where level="ERROR" | stats count`)
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got != 294 {
		t.Errorf("expected count=294 for ERROR, got %d", got)
	}
}

func TestFile_Head5(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"),
		"--format", "json", "| head 5")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(rows))
	}
}

func TestFile_SortDescHead3(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"),
		"--format", "json", "| stats count by level | sort -count | head 3")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Verify descending order.
	for i := 1; i < len(rows); i++ {
		prev := rows[i-1]["count"].(float64)
		curr := rows[i]["count"].(float64)
		if prev < curr {
			t.Errorf("not sorted descending at row %d: %v < %v", i, prev, curr)
		}
	}

	// First should be INFO (359), highest count.
	first := int(rows[0]["count"].(float64))
	if first != 359 {
		t.Errorf("expected first count=359 (INFO), got %d", first)
	}
}

func TestFile_EvalExpression(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"),
		"--format", "json",
		`| eval fast=if(response_time<100,"yes","no") | stats count by fast`)
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) < 1 {
		t.Fatal("expected at least 1 row from eval")
	}

	total := 0
	for _, row := range rows {
		fast, _ := row["fast"].(string)
		if fast != "yes" && fast != "no" {
			t.Errorf("unexpected fast value: %q", fast)
		}

		total += int(row["count"].(float64))
	}

	if total != 1000 {
		t.Errorf("expected groups to sum to 1000, got %d", total)
	}
}

func TestFile_FieldsProjection(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"),
		"--format", "json", "| fields host, level | head 3")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	for i, row := range rows {
		if _, ok := row["host"]; !ok {
			t.Errorf("row %d missing projected field 'host'", i)
		}

		if _, ok := row["level"]; !ok {
			t.Errorf("row %d missing projected field 'level'", i)
		}

		// Should only have projected fields (plus internal _time/_timestamp/_raw).
		for k := range row {
			switch k {
			case "host", "level", "_time", "_timestamp", "_raw":
				// allowed
			default:
				t.Errorf("row %d has unexpected field %q after projection", i, k)
			}
		}
	}
}

func TestFile_Search_FullText(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"),
		"--format", "json", `| search "ERROR" | stats count`)
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got != 294 {
		t.Errorf("expected count=294 for full-text search ERROR, got %d", got)
	}
}

func TestFile_GlobPattern(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access*.log"),
		"--format", "json", "| stats count")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got != 1000 {
		t.Errorf("expected count=1000 from glob pattern, got %d", got)
	}
}

func TestFile_NginxLog(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("nginx_access.log"),
		"--format", "json", "| stats count")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got != 34 {
		t.Errorf("expected count=34, got %d", got)
	}
}

func TestFile_BackendJSON(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("backend_server.log"),
		"--format", "json", "| stats count")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got != 26 {
		t.Errorf("expected count=26, got %d", got)
	}
}

func TestFile_NonexistentFile(t *testing.T) {
	r := runLynxDB(t, "query", "--file", "/nonexistent/path/file.log",
		"--format", "json", "| stats count")

	if r.ExitCode == 0 {
		t.Errorf("expected non-zero exit code for nonexistent file, got 0")
	}
}

func TestFile_ValidNDJSON_Output(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"),
		"--format", "json", "| head 10")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	lines := strings.Split(strings.TrimSpace(r.Stdout), "\n")
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
