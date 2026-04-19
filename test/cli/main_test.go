//go:build clitest

package cli_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// lynxdbBin is the path to the compiled lynxdb binary, set in TestMain.
var lynxdbBin string

// projectRoot is the absolute path to the LynxDB project root, set in TestMain.
var projectRoot string

// testHomeDir isolates CLI tests from the user's real home/config/data dirs.
var testHomeDir string

func TestMain(m *testing.M) {
	// Resolve project root from this test file's location.
	// test/cli/main_test.go → project root is 2 levels up.
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Fprintln(os.Stderr, "FATAL: cannot determine test file location")
		os.Exit(1)
	}

	projectRoot = filepath.Join(filepath.Dir(filename), "..", "..")

	// Create a temp directory for the binary.
	tmpDir, err := os.MkdirTemp("", "lynxdb-clitest-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: create temp dir: %v\n", err)
		os.Exit(1)
	}

	binName := "lynxdb"
	if runtime.GOOS == "windows" {
		binName = "lynxdb.exe"
	}

	lynxdbBin = filepath.Join(tmpDir, binName)
	testHomeDir = filepath.Join(tmpDir, "home")
	if err := os.MkdirAll(filepath.Join(testHomeDir, ".config"), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: create isolated config dir: %v\n", err)
		os.RemoveAll(tmpDir)
		os.Exit(1)
	}
	if err := os.MkdirAll(filepath.Join(testHomeDir, ".local", "share"), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: create isolated data dir: %v\n", err)
		os.RemoveAll(tmpDir)
		os.Exit(1)
	}

	// Build the binary with race detector enabled.
	fmt.Fprintf(os.Stderr, "Building lynxdb binary at %s ...\n", lynxdbBin)

	build := exec.Command("go", "build", "-race", "-o", lynxdbBin, "./cmd/lynxdb/")
	build.Dir = projectRoot
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr

	if err := build.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: go build failed: %v\n", err)
		os.RemoveAll(tmpDir)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "Binary built successfully (%s)\n", lynxdbBin)

	code := m.Run()

	// Cleanup.
	os.RemoveAll(tmpDir)
	os.Exit(code)
}
