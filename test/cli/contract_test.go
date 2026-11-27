//go:build clitest

package cli_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestContract_EnvServer_Override(t *testing.T) {
	// LYNXDB_SERVER should control the target server.
	// Point it at a non-listening port — should get connection error, not default.
	r := runLynxDBWithEnv(t,
		map[string]string{"LYNXDB_SERVER": "http://127.0.0.1:1"},
		"status",
	)

	// Should fail with connection error (exit 3), not the default port.
	if r.ExitCode != 3 {
		t.Errorf("expected exit code 3 (connection refused via LYNXDB_SERVER), got %d\nstderr: %s",
			r.ExitCode, r.Stderr)
	}
}

func TestContract_EnvConfig_Empty(t *testing.T) {
	// LYNXDB_CONFIG="" should prevent loading any config file.
	// This is already set in our cleanEnv(), but verify it explicitly:
	// a successful file query proves no config file interfered.
	r := runLynxDBWithEnv(t,
		map[string]string{"LYNXDB_CONFIG": ""},
		"query", "--file", testdataLog("access.log"),
		"--format", "json", "source=main | stats count() as count",
	)

	if r.ExitCode != 0 {
		t.Errorf("expected exit 0, got %d\nstderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got != 1000 {
		t.Errorf("expected count=1000, got %d", got)
	}
}

func TestContract_NoColor_EnvVar(t *testing.T) {
	// NO_COLOR=1 must suppress ANSI escapes on early error paths too, before
	// the cobra flags are parsed.
	r := runLynxDBWithEnv(t,
		map[string]string{"NO_COLOR": "1"},
		"query", "--file", testdataLog("access.log"), "source=main | where",
	)

	// This triggers a parse error, which writes to stderr.
	// Verify no ANSI in stderr.
	if containsANSI(r.Stderr) {
		t.Errorf("stderr contains ANSI escapes with NO_COLOR=1:\n%s", r.Stderr)
	}
}

func TestContract_NoColor_Flag(t *testing.T) {
	r := runLynxDB(t, "--no-color", "query", "--file", testdataLog("access.log"), "source=main | where")

	if containsANSI(r.Stderr) {
		t.Errorf("stderr contains ANSI escapes with --no-color:\n%s", r.Stderr)
	}
}

func TestContract_Quiet_SuppressesStderr(t *testing.T) {
	// --quiet should suppress non-data output on stderr.
	r := runLynxDB(t, "--quiet", "query", "--file", testdataLog("access.log"),
		"--format", "json", "source=main | stats count() as count")

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	if strings.TrimSpace(r.Stderr) != "" {
		t.Errorf("expected empty stderr with --quiet, got: %q", r.Stderr)
	}
}

func TestContract_OutputToFile(t *testing.T) {
	outFile := filepath.Join(t.TempDir(), "results.json")

	r := runLynxDB(t, "query", "--file", testdataLog("access.log"),
		"--format", "json", "--output", outFile,
		"source=main | stats count() as count by level")
	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
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

func TestContract_Version_ContainsLynxDB(t *testing.T) {
	r := runLynxDB(t, "version")

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d", r.ExitCode)
	}

	if !strings.Contains(r.Stdout, "LynxDB") {
		t.Errorf("version output missing 'LynxDB': %q", r.Stdout)
	}
}

func TestContract_FailOnEmpty_FileMode(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"),
		"--format", "json", "--fail-on-empty",
		`source=main | where level = "NONEXISTENT_LEVEL_XYZ"`)

	if r.ExitCode != 6 {
		t.Errorf("expected exit code 6 (no results with --fail-on-empty), got %d\nstderr: %s",
			r.ExitCode, r.Stderr)
	}
}

func TestNewestEventsInTimeRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.ndjson")
	now := time.Now().UTC().Truncate(time.Second)
	var data strings.Builder
	for i, age := range []int{2, 120, 1, 1, 5} {
		fmt.Fprintf(&data, `{"timestamp":%q,"id":%d}`+"\n", now.Add(-time.Duration(age)*time.Minute).Format(time.RFC3339), i)
	}
	if err := os.WriteFile(path, []byte(data.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	srv := startServer(t)
	ingestFile(t, srv, path)
	for _, base := range [][]string{{"query", "--file", path}, {"--server", srv.BaseURL, "query"}} {
		for _, args := range [][]string{{"--since", "1h", "source=main | fields id | head 3"}, {"source=main earliest=-1h | head 3 | fields id"}} {
			r := runLynxDB(t, append(append(slices.Clone(base), "--format", "json"), args...)...)
			if r.ExitCode != 0 {
				t.Fatal(r.Stderr)
			}
			var ids []int
			for _, row := range mustParseJSON(t, r.Stdout) {
				ids = append(ids, int(row["id"].(float64)))
			}
			if !slices.Equal(ids, []int{2, 3, 0}) {
				t.Errorf("%v %v: IDs %v", base, args, ids)
			}
		}
	}
}
