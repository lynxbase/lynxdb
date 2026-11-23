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
		"--format", "json", "source=main | stats count() as count")

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

func TestExit_UnknownFlag(t *testing.T) {
	r := runLynxDB(t, "status", "--nonexistent-flag-xyz")

	if r.ExitCode == 0 {
		t.Errorf("expected non-zero exit code for an unknown flag, got 0")
	}
}

// TestExit3_ServerQueryIgnoresStdin pins that a query without --file asks
// the server even when standard input is not a terminal, and that the
// connection error shows how to query standard input instead.
func TestExit3_ServerQueryIgnoresStdin(t *testing.T) {
	r := runLynxDBWithStdin(t, `{"status":503}`+"\n",
		"--server", "http://127.0.0.1:1", "query", "source=web | stats count()")

	if r.ExitCode != 3 {
		t.Errorf("expected exit code 3 (connection), got %d\nstdout: %s\nstderr: %s", r.ExitCode, r.Stdout, r.Stderr)
	}
	if want := "lynxdb query --file - 'source=web | stats count()'"; !strings.Contains(r.Stderr, want) {
		t.Errorf("expected stderr to show %q, got:\n%s", want, r.Stderr)
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
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"), "source=main | where")

	if r.ExitCode != 4 {
		t.Errorf("expected exit code 4 (parse error), got %d\nstderr: %s", r.ExitCode, r.Stderr)
	}
}

func TestExit4_ParseError_ServerMode(t *testing.T) {
	srv := startServer(t)

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json", "source=main | where")

	if r.ExitCode != 4 {
		t.Errorf("expected exit code 4 (parse error), got %d\nstderr: %s", r.ExitCode, r.Stderr)
	}
}

func TestExit6_FailOnEmpty(t *testing.T) {
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"),
		"--format", "json", "--fail-on-empty",
		`source=main | where level = "NONEXISTENT_LEVEL_XYZ"`)

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
	r := runLynxDB(t, "query", "--file", testdataLog("access.log"), "source=main | where")

	if r.ExitCode != 4 {
		t.Errorf("expected exit code 4, got %d", r.ExitCode)
	}

	if strings.TrimSpace(r.Stderr) == "" {
		t.Errorf("expected error message on stderr for parse error, got empty")
	}
}
