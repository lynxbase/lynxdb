//go:build clitest

package cli_test

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// generateJSONLines creates n NDJSON lines with a level field.
// Levels rotate: info, error, warn.
func generateJSONLines(n int) string {
	levels := []string{"info", "error", "warn"}
	var sb strings.Builder

	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, `{"level":"%s","n":%d,"msg":"event-%d"}`+"\n",
			levels[i%len(levels)], i, i)
	}

	return sb.String()
}

func TestPipe_StatsCount(t *testing.T) {
	input := generateJSONLines(10)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json", "| stats count")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got != 10 {
		t.Errorf("expected count=10, got %d", got)
	}
}

func TestPipe_StatsCountByLevel(t *testing.T) {
	// 12 events: 4 info, 4 error, 4 warn (evenly distributed by modulo 3).
	input := generateJSONLines(12)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json", "| stats count by level")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(rows))
	}

	levelCounts := make(map[string]int)
	for _, row := range rows {
		level, _ := row["level"].(string)
		count := int(row["count"].(float64))
		levelCounts[level] = count
	}

	for _, level := range []string{"info", "error", "warn"} {
		if levelCounts[level] != 4 {
			t.Errorf("expected %s=4, got %d", level, levelCounts[level])
		}
	}
}

func TestPipe_WhereFilter(t *testing.T) {
	// 12 events: 4 error events (indices 1,4,7,10).
	input := generateJSONLines(12)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| where level="error" | stats count`)
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got != 4 {
		t.Errorf("expected count=4, got %d", got)
	}
}

func TestPipe_Head_LimitsResults(t *testing.T) {
	input := generateJSONLines(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json", "| head 5")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(rows))
	}
}

func TestPipe_SortDescending(t *testing.T) {
	input := generateJSONLines(12)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		"| stats count by level | sort -count")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(rows))
	}

	for i := 1; i < len(rows); i++ {
		prev := rows[i-1]["count"].(float64)
		curr := rows[i]["count"].(float64)
		if prev < curr {
			t.Errorf("not sorted descending at row %d: %v < %v", i, prev, curr)
		}
	}
}

func TestPipe_EvalComputedField(t *testing.T) {
	input := generateJSONLines(12)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| eval category=if(level="error","bad","ok") | stats count by category`)
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) < 1 {
		t.Fatal("expected at least 1 row")
	}

	total := 0
	for _, row := range rows {
		cat, _ := row["category"].(string)
		if cat != "bad" && cat != "ok" {
			t.Errorf("unexpected category: %q", cat)
		}

		total += int(row["count"].(float64))
	}

	if total != 12 {
		t.Errorf("expected total=12, got %d", total)
	}
}

func TestPipe_Dedup_UniqueRows(t *testing.T) {
	input := generateJSONLines(12)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json", "| dedup level")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 3 {
		t.Errorf("expected 3 deduplicated rows, got %d", len(rows))
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

func TestPipe_Rename(t *testing.T) {
	input := generateJSONLines(6)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		"| stats count by level | rename count AS total")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
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

func TestPipe_FormatJSON_ValidNDJSON(t *testing.T) {
	input := generateJSONLines(5)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json", "| head 5")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	lines := strings.Split(strings.TrimSpace(r.Stdout), "\n")
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

func TestPipe_FormatCSV(t *testing.T) {
	input := generateJSONLines(6)

	r := runLynxDBWithStdin(t, input, "query", "--format", "csv",
		"| stats count by level")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	reader := csv.NewReader(strings.NewReader(strings.TrimSpace(r.Stdout)))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("CSV parse error: %v\nraw: %q", err, r.Stdout)
	}

	// Header + 3 data rows.
	if len(records) != 4 {
		t.Errorf("expected 4 CSV records (header + 3 data), got %d", len(records))
	}

	// Verify header contains expected columns.
	header := strings.Join(records[0], ",")
	if !strings.Contains(header, "count") || !strings.Contains(header, "level") {
		t.Errorf("CSV header missing expected columns, got: %v", records[0])
	}
}

func TestPipe_FormatTable(t *testing.T) {
	input := generateJSONLines(6)

	r := runLynxDBWithStdin(t, input, "query", "--format", "table",
		"| stats count by level")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	if !strings.Contains(r.Stdout, "count") {
		t.Errorf("table output missing 'count' column header")
	}

	if !strings.Contains(r.Stdout, "level") {
		t.Errorf("table output missing 'level' column header")
	}
}

func TestPipe_FormatTSV(t *testing.T) {
	input := generateJSONLines(6)

	r := runLynxDBWithStdin(t, input, "query", "--format", "tsv",
		"| stats count by level")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	lines := strings.Split(strings.TrimSpace(r.Stdout), "\n")
	// Header + 3 data rows.
	if len(lines) != 4 {
		t.Errorf("expected 4 TSV lines, got %d", len(lines))
	}

	for i, line := range lines {
		if !strings.Contains(line, "\t") {
			t.Errorf("line %d has no tab character — not TSV: %q", i, line)
		}
	}
}

func TestPipe_FormatRaw(t *testing.T) {
	input := generateJSONLines(5)

	r := runLynxDBWithStdin(t, input, "query", "--format", "raw", "| head 5")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	var nonEmpty int
	for _, line := range strings.Split(strings.TrimSpace(r.Stdout), "\n") {
		if strings.TrimSpace(line) != "" {
			nonEmpty++
		}
	}

	if nonEmpty != 5 {
		t.Errorf("expected 5 non-empty raw lines, got %d", nonEmpty)
	}
}

func TestPipe_LargeInput_1000Events(t *testing.T) {
	input := generateJSONLines(1000)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json", "| stats count")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got != 1000 {
		t.Errorf("expected count=1000, got %d (data loss at scale)", got)
	}
}

func TestPipe_ParseError_Stderr(t *testing.T) {
	input := generateJSONLines(5)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json", "| where")

	if r.ExitCode == 0 {
		t.Errorf("expected non-zero exit code for parse error, got 0")
	}

	if strings.TrimSpace(r.Stderr) == "" {
		t.Errorf("expected error message on stderr, got empty")
	}
}
