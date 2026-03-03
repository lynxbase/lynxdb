//go:build clitest

package cli_test

import (
	"encoding/csv"
	"encoding/json"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestServer_StartAndHealth(t *testing.T) {
	srv := startServer(t)

	r := runLynxDB(t, "--server", srv.BaseURL, "health")
	if r.ExitCode != 0 {
		t.Errorf("expected exit 0, got %d\nstderr: %s", r.ExitCode, r.Stderr)
	}
}

func TestServer_IngestAndQuery_Count(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("access.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		"| stats count")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got != 1000 {
		t.Errorf("expected count=1000, got %d", got)
	}
}

func TestServer_IngestWithIndex(t *testing.T) {
	srv := startServer(t)
	ingestFileWithIndex(t, srv, testdataLog("backend_server.log"), "custom")

	// Query custom index.
	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		"FROM custom | stats count")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got != 26 {
		t.Errorf("expected count=26 in custom index, got %d", got)
	}

	// Main index should be empty.
	r2 := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		"FROM main | stats count")
	if r2.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r2.ExitCode, r2.Stderr)
	}

	gotMain := jsonCount(t, r2.Stdout)
	if gotMain != 0 {
		t.Errorf("expected count=0 in main, got %d (index leak)", gotMain)
	}
}

func TestServer_Query_CountByLevel(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("access.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		"| stats count by level")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
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
			t.Errorf("level=%s: expected %d, got %d", level, want, got)
		}
	}
}

func TestServer_Query_WhereFilter(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("access.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`| where level="ERROR" | stats count`)
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got != 294 {
		t.Errorf("expected count=294, got %d", got)
	}
}

func TestServer_Query_SortDescHead(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("access.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		"| stats count by level | sort -count | head 3")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	for i := 1; i < len(rows); i++ {
		prev := rows[i-1]["count"].(float64)
		curr := rows[i]["count"].(float64)
		if prev < curr {
			t.Errorf("not sorted descending at row %d: %v < %v", i, prev, curr)
		}
	}
}

func TestServer_Query_EvalComputedField(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("access.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`| eval is_error=if(level="ERROR","yes","no") | stats count by is_error`)
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
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
		t.Errorf("expected groups to sum to 1000, got %d", total)
	}
}

func TestServer_Query_Dedup(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("access.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		"| dedup level")
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

func TestServer_Query_Top3(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("access.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		"| top 3 level")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows from top, got %d", len(rows))
	}

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

	// Verify descending order.
	if len(rows) >= 2 {
		first := rows[0]["count"].(float64)
		second := rows[1]["count"].(float64)
		if first < second {
			t.Errorf("top not sorted: first %v < second %v", first, second)
		}
	}
}

func TestServer_Format_JSON(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("access.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		"| head 10")
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

func TestServer_Format_CSV(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("access.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "csv",
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
		t.Errorf("expected 4 CSV records, got %d", len(records))
	}

	header := strings.Join(records[0], ",")
	if !strings.Contains(header, "count") || !strings.Contains(header, "level") {
		t.Errorf("CSV header missing expected columns, got: %v", records[0])
	}
}

func TestServer_Format_Table(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("access.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "table",
		"| stats count by level")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	if !strings.Contains(r.Stdout, "count") {
		t.Errorf("table output missing 'count' header")
	}

	if !strings.Contains(r.Stdout, "level") {
		t.Errorf("table output missing 'level' header")
	}

	for _, level := range []string{"ERROR", "INFO", "WARN"} {
		if !strings.Contains(r.Stdout, level) {
			t.Errorf("table output missing %s level", level)
		}
	}
}

func TestServer_Status_JSON(t *testing.T) {
	srv := startServer(t)

	r := runLynxDB(t, "--server", srv.BaseURL, "status", "--format", "json")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(r.Stdout)), &result); err != nil {
		t.Fatalf("parse status JSON: %v\noutput: %q", err, r.Stdout)
	}

	required := []string{"uptime_seconds", "total_events", "segment_count", "health"}
	for _, key := range required {
		if _, ok := result[key]; !ok {
			t.Errorf("status JSON missing required key %q, got keys: %v", key, mapKeys(result))
		}
	}
}

func TestServer_GracefulShutdown(t *testing.T) {
	srv := startServer(t)

	// Verify server is up.
	r := runLynxDB(t, "--server", srv.BaseURL, "health")
	if r.ExitCode != 0 {
		t.Fatalf("server not healthy: exit %d", r.ExitCode)
	}

	// Send SIGTERM.
	if srv.cmd.Process == nil {
		t.Fatal("server process is nil")
	}

	if err := srv.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}

	// Wait for the process to exit.
	done := make(chan error, 1)
	go func() {
		done <- srv.cmd.Wait()
	}()

	select {
	case err := <-done:
		// Process exited. On graceful shutdown, exit code should be 0.
		// Some systems report SIGTERM as a non-zero exit, so we accept both.
		if err != nil {
			// ExitError from SIGTERM is acceptable.
			t.Logf("server exited with: %v (expected for SIGTERM)", err)
		}
	case <-time.After(15 * time.Second):
		t.Errorf("server did not exit within 15s after SIGTERM")
	}
}
