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

	r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "json", "source=main | stats count() as count")
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

	r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "json", "source=main | stats count() as count by level")
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

	r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "json",
		`source=main | where level = "error" | stats count() as count`)
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

	r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "json", "source=main | head 5")
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

	r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "json",
		"source=main | stats count() as count by level | sort - count")
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

	r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "json",
		`source=main | eval category = if(level = "error", "bad", "ok") | stats count() as count by category`)
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

	r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "json", "source=main | dedup level")
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

	r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "json",
		"source=main | stats count() as count by level | rename count as total")
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

	r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "json", "source=main | head 5")
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

	r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "csv",
		"source=main | stats count() as count by level")
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

	r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "table",
		"source=main | stats count() as count by level")
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

	r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "tsv",
		"source=main | stats count() as count by level")
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

	r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "raw", "source=main | head 5")
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

	r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "json", "source=main | stats count() as count")
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

	r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "json", "source=main | where")

	if r.ExitCode == 0 {
		t.Errorf("expected non-zero exit code for parse error, got 0")
	}

	if strings.TrimSpace(r.Stderr) == "" {
		t.Errorf("expected error message on stderr, got empty")
	}
}

// TestPipe_BareQueryReadsStdin pins the root command with a query: it reads
// standard input, and the query may omit the source.
func TestPipe_BareQueryReadsStdin(t *testing.T) {
	r := runLynxDBWithStdin(t, generateJSONLines(12), "--format", "json",
		`where level = "error" | stats count() as count`)
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	if got := jsonCount(t, r.Stdout); got != 4 {
		t.Errorf("expected count=4, got %d", got)
	}
}

// TestPipe_BareWithoutQueryShowsWelcome pins that the root command without
// arguments never reads standard input.
func TestPipe_BareWithoutQueryShowsWelcome(t *testing.T) {
	r := runLynxDBWithStdin(t, generateJSONLines(3))
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	if !strings.Contains(r.Stdout, "Quick start") || strings.Contains(r.Stdout, "event-") {
		t.Errorf("expected the welcome text and no events, got:\n%s", r.Stdout)
	}
}

// TestPipe_DefaultSource pins the forms of a local query that names no
// source: each reads the index main.
func TestPipe_DefaultSource(t *testing.T) {
	input := generateJSONLines(12)

	for _, q := range []string{
		`where level = "error" | stats count() as count`,
		`| where level = "error" | stats count() as count`,
		`level=error | stats count() as count`,
		`source=main level=error | stats count() as count`,
	} {
		r := runLynxDBWithStdin(t, input, "query", "--file", "-", "--format", "json", q)
		if r.ExitCode != 0 {
			t.Errorf("%s: exit code %d, stderr: %s", q, r.ExitCode, r.Stderr)
			continue
		}
		if got := jsonCount(t, r.Stdout); got != 4 {
			t.Errorf("%s: expected count=4, got %d", q, got)
		}
	}
}

func TestPipe_LeadingPipeNeedsCommand(t *testing.T) {
	r := runLynxDBWithStdin(t, generateJSONLines(3), "query", "--file", "-", "| tstats count where index=main by host")

	if r.ExitCode != 4 {
		t.Errorf("expected exit code 4 (parse error), got %d\nstdout: %s", r.ExitCode, r.Stdout)
	}
	if !strings.Contains(r.Stderr, `unknown command "tstats"`) {
		t.Errorf("expected an unknown command error, got:\n%s", r.Stderr)
	}
}

// minuteLines returns n text lines, one per minute from 08:00, with n=<i>.
func minuteLines(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, "2025-01-15T08:%02d:00.000Z level=info n=%d\n", i, i)
	}

	return sb.String()
}

// TestPipe_TimeRange pins that --from and --to bound a local query to
// [from, to).
func TestPipe_TimeRange(t *testing.T) {
	r := runLynxDBWithStdin(t, minuteLines(10), "query", "--file", "-", "--format", "json",
		"--from", "2025-01-15T08:02:00Z", "--to", "2025-01-15T08:05:00Z",
		"stats count() as count, min(n) as lo, max(n) as hi")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 1 || rows[0]["count"] != 3.0 || rows[0]["lo"] != 2.0 || rows[0]["hi"] != 4.0 {
		t.Errorf("expected count=3 lo=2 hi=4, got %v", rows)
	}
}

// TestPipe_SinceCountsFromAfterLoad pins that --since counts from after the
// input loaded, so lines without a timestamp, which take their load time,
// fall inside it.
func TestPipe_SinceCountsFromAfterLoad(t *testing.T) {
	r := runLynxDBWithStdin(t, "first\nsecond\n", "query", "--file", "-", "--format", "json",
		"--since", "1m", "stats count() as count")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	if got := jsonCount(t, r.Stdout); got != 2 {
		t.Errorf("expected count=2, got %d", got)
	}
}

// TestPipe_FromOrToAlone pins that --from alone reads from that time until
// now and --to alone reads every event before that time: an event at from
// is in the range, an event at to is not.
func TestPipe_FromOrToAlone(t *testing.T) {
	for _, c := range []struct {
		flag, value   string
		count, lo, hi float64
	}{
		{"--from", "2025-01-15T08:02:00Z", 8, 2, 9},
		{"--to", "2025-01-15T08:05:00Z", 5, 0, 4},
	} {
		r := runLynxDBWithStdin(t, minuteLines(10), "query", "--file", "-", "--format", "json",
			c.flag, c.value, "stats count() as count, min(n) as lo, max(n) as hi")
		if r.ExitCode != 0 {
			t.Fatalf("%s %s: exit code %d, stderr: %s", c.flag, c.value, r.ExitCode, r.Stderr)
		}
		rows := mustParseJSON(t, r.Stdout)
		if len(rows) != 1 || rows[0]["count"] != c.count || rows[0]["lo"] != c.lo || rows[0]["hi"] != c.hi {
			t.Errorf("%s %s: got %v, want count=%v lo=%v hi=%v", c.flag, c.value, rows, c.count, c.lo, c.hi)
		}
	}
}

// TestPipe_FromCountsFromAfterLoad pins that a relative --from alone counts
// from after the input loaded, as --since does.
func TestPipe_FromCountsFromAfterLoad(t *testing.T) {
	r := runLynxDBWithStdin(t, "first\nsecond\n", "query", "--file", "-", "--format", "json",
		"--from", "-1m", "stats count() as count")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	if got := jsonCount(t, r.Stdout); got != 2 {
		t.Errorf("expected count=2, got %d", got)
	}
}

// TestPipe_ExplainPrefixPrintsPlan pins that a local query starting with
// explain prints its plan instead of rows.
func TestPipe_ExplainPrefixPrintsPlan(t *testing.T) {
	r := runLynxDBWithStdin(t, generateJSONLines(3), "query", "--file", "-", "explain level=error | stats count()")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	if !strings.Contains(r.Stdout, "Logical plan:") || !strings.Contains(r.Stdout, "Scan(main)") {
		t.Errorf("expected the plan, got:\n%s", r.Stdout)
	}
}
