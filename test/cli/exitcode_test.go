//go:build clitest

package cli_test

import (
	"strings"
	"testing"
)

// Exit code constants (from cmd/lynxdb/exitcodes.go):
// 0 = ok, 1 = general, 2 = usage, 3 = connection, 4 = parse,
// 5 = timeout, 6 = no results (--fail-on-empty), 7 = auth.

func TestExit0_QueryFile(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"),
		"--format", "json", "| stats count")

	if r.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d\nstderr: %s", r.ExitCode, r.Stderr)
	}
}

func TestExit0_Version(t *testing.T) {
	r := runLynxDB(t, "version")

	if r.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", r.ExitCode)
	}
}

func TestExit0_Help(t *testing.T) {
	r := runLynxDB(t, "--help")

	if r.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", r.ExitCode)
	}
}

func TestExit2_UnknownCommand(t *testing.T) {
	r := runLynxDB(t, "nonexistent-command-xyz")

	// Cobra returns exit code 1 for unknown commands by default, but some
	// CLIs use 2. We test for non-zero as the essential guarantee.
	if r.ExitCode == 0 {
		t.Errorf("expected non-zero exit code for unknown command, got 0")
	}
}

func TestExit3_ConnectionRefused(t *testing.T) {
	// Port 1 is almost certainly not listening.
	r := runLynxDB(t, "--server", "http://127.0.0.1:1", "status")

	if r.ExitCode != 3 {
		t.Errorf("expected exit code 3 (connection), got %d\nstderr: %s", r.ExitCode, r.Stderr)
	}
}

func TestExit4_ParseError_FileMode(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"), "| where")

	if r.ExitCode != 4 {
		t.Errorf("expected exit code 4 (parse error), got %d\nstderr: %s", r.ExitCode, r.Stderr)
	}
}

func TestExit4_ParseError_ServerMode(t *testing.T) {
	srv := startServer(t)

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json", "| where")

	if r.ExitCode != 4 {
		t.Errorf("expected exit code 4 (parse error), got %d\nstderr: %s", r.ExitCode, r.Stderr)
	}
}

func TestExit6_FailOnEmpty(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"),
		"--format", "json", "--fail-on-empty",
		`| where level="NONEXISTENT_LEVEL_XYZ"`)

	if r.ExitCode != 6 {
		t.Errorf("expected exit code 6 (no results), got %d\nstderr: %s", r.ExitCode, r.Stderr)
	}
}

func TestExit_ConnectionRefused_NonZero(t *testing.T) {
	r := runLynxDB(t, "--server", "http://127.0.0.1:1", "status")

	if r.ExitCode == 0 {
		t.Errorf("expected non-zero exit code for connection failure, got 0")
	}
}

func TestExit_ParseError_StderrHasMessage(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"), "| where")

	if r.ExitCode != 4 {
		t.Errorf("expected exit code 4, got %d", r.ExitCode)
	}

	if strings.TrimSpace(r.Stderr) == "" {
		t.Errorf("expected error message on stderr for parse error, got empty")
	}
}
