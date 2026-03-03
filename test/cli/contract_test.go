//go:build clitest

package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
		"--format", "json", "| stats count",
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
	// BUG: This test exposes a real bug in ensureThemeInit() / renderError().
	// Expected: NO_COLOR=1 suppresses ANSI escape sequences in error output.
	// Actual: ANSI escapes are emitted because ensureThemeInit() calls
	//   ui.Init(globalNoColor) where globalNoColor is still false — cobra's
	//   OnInitialize hasn't run yet when early errors occur (e.g., parse errors).
	//   The NO_COLOR env var is not checked independently of the flag.
	// The application code must be fixed — do not modify this test to pass.

	r := runLynxDBWithEnv(t,
		map[string]string{"NO_COLOR": "1"},
		"query", "--file", testdataLog("access.log"), "| where",
	)

	// This triggers a parse error, which writes to stderr.
	// Verify no ANSI in stderr.
	if containsANSI(r.Stderr) {
		t.Errorf("stderr contains ANSI escapes with NO_COLOR=1:\n%s", r.Stderr)
	}
}

func TestContract_NoColor_Flag(t *testing.T) {
	// BUG: Same root cause as TestContract_NoColor_EnvVar.
	// Expected: --no-color flag suppresses ANSI escape sequences.
	// Actual: ANSI escapes still emitted because ensureThemeInit() runs before
	//   cobra parses the --no-color flag on early error paths.
	// The application code must be fixed — do not modify this test to pass.

	r := runLynxDB(t, "--no-color", "query", "--file", testdataLog("access.log"), "| where")

	if containsANSI(r.Stderr) {
		t.Errorf("stderr contains ANSI escapes with --no-color:\n%s", r.Stderr)
	}
}

func TestContract_Quiet_SuppressesStderr(t *testing.T) {
	// --quiet should suppress non-data output on stderr.
	r := runLynxDB(t, "--quiet", "query", "--file", testdataLog("access.log"),
		"--format", "json", "| stats count")

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
		"| stats count by level")
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
		`| where level="NONEXISTENT_LEVEL_XYZ"`)

	if r.ExitCode != 6 {
		t.Errorf("expected exit code 6 (no results with --fail-on-empty), got %d\nstderr: %s",
			r.ExitCode, r.Stderr)
	}
}
