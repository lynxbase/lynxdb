//go:build clitest

package cli_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// ─── Result type ─────────────────────────────────────────────────────────────

// Result holds the captured output and exit code from a CLI invocation.
type Result struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// ─── Core runner functions ───────────────────────────────────────────────────

// runLynxDB executes the lynxdb binary with the given args and returns the result.
func runLynxDB(t *testing.T, args ...string) Result {
	t.Helper()

	return runLynxDBWithEnv(t, nil, args...)
}

// runLynxDBWithStdin executes the binary with data piped to stdin.
func runLynxDBWithStdin(t *testing.T, stdin string, args ...string) Result {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, lynxdbBin, args...)
	cmd.Stdin = strings.NewReader(stdin)
	cmd.Env = cleanEnv()

	var outBuf, errBuf strings.Builder
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	err := cmd.Run()

	return buildResult(t, outBuf.String(), errBuf.String(), err)
}

// runLynxDBWithEnv executes the binary with extra environment variables.
func runLynxDBWithEnv(t *testing.T, env map[string]string, args ...string) Result {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, lynxdbBin, args...)
	cmd.Env = cleanEnv()

	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}

	var outBuf, errBuf strings.Builder
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	err := cmd.Run()

	return buildResult(t, outBuf.String(), errBuf.String(), err)
}

// cleanEnv returns a copy of os.Environ() with test-isolation overrides.
func cleanEnv() []string {
	env := os.Environ()
	env = append(env,
		"NO_COLOR=1",
		"LYNXDB_CONFIG=",
		"LYNXDB_SERVER=",
		"LYNXDB_TOKEN=",
	)

	return env
}

// buildResult constructs a Result from captured output and a command error.
func buildResult(t *testing.T, stdout, stderr string, err error) Result {
	t.Helper()

	if err == nil {
		return Result{Stdout: stdout, Stderr: stderr, ExitCode: 0}
	}

	if exitErr, ok := err.(*exec.ExitError); ok {
		return Result{
			Stdout:   stdout,
			Stderr:   stderr,
			ExitCode: exitErr.ExitCode(),
		}
	}

	// Context timeout or other unexpected error.
	t.Fatalf("unexpected error running lynxdb: %v\nstderr: %s", err, stderr)

	return Result{} // unreachable
}

// ─── Server management ───────────────────────────────────────────────────────

// Server represents a running lynxdb server process for testing.
type Server struct {
	BaseURL string
	DataDir string
	cmd     *exec.Cmd
	cancel  context.CancelFunc
}

// startServer starts a lynxdb server on a random port and returns a Server.
// The server is automatically stopped when the test finishes.
func startServer(t *testing.T) *Server {
	t.Helper()

	port := getFreePort(t)
	dataDir := t.TempDir()
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	ctx, cancel := context.WithCancel(context.Background())

	cmd := exec.CommandContext(ctx, lynxdbBin,
		"server",
		"--addr", addr,
		"--data-dir", dataDir,
		"--log-level", "error",
	)
	cmd.Env = cleanEnv()
	cmd.Stdout = os.Stderr // pipe server logs to test stderr for debugging
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start server: %v", err)
	}

	srv := &Server{
		BaseURL: fmt.Sprintf("http://%s", addr),
		DataDir: dataDir,
		cmd:     cmd,
		cancel:  cancel,
	}

	// Wait for server to be healthy.
	waitForHealth(t, srv.BaseURL, 15*time.Second)

	// Register cleanup: graceful SIGTERM, then SIGKILL fallback.
	t.Cleanup(func() {
		cancel()

		// Send SIGTERM for graceful shutdown.
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}

		// Wait with timeout.
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Clean exit.
		case <-time.After(10 * time.Second):
			// Force kill.
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			<-done
		}
	})

	return srv
}

// getFreePort returns an available TCP port on localhost.
func getFreePort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("get free port: %v", err)
	}

	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	return port
}

// waitForHealth polls the server's /health endpoint until it responds HTTP 200
// or the timeout expires. Uses exponential backoff (10ms → 500ms cap).
func waitForHealth(t *testing.T, baseURL string, timeout time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	backoff := 10 * time.Millisecond
	maxBackoff := 500 * time.Millisecond

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/health", http.NoBody)
		if err != nil {
			t.Fatalf("create health request: %v", err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}

		if ctx.Err() != nil {
			t.Fatalf("server at %s did not become healthy within %s", baseURL, timeout)
		}

		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			t.Fatalf("server at %s did not become healthy within %s", baseURL, timeout)
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// ingestFile runs `lynxdb ingest <filePath> --server <url>` and asserts exit 0.
func ingestFile(t *testing.T, srv *Server, filePath string) {
	t.Helper()

	r := runLynxDB(t, "--server", srv.BaseURL, "ingest", filePath)
	if r.ExitCode != 0 {
		t.Fatalf("ingest %s failed (exit %d): %s", filePath, r.ExitCode, r.Stderr)
	}
}

// ingestFileWithIndex runs ingest with a custom index.
func ingestFileWithIndex(t *testing.T, srv *Server, filePath, index string) {
	t.Helper()

	r := runLynxDB(t, "--server", srv.BaseURL, "ingest", filePath, "--index", index)
	if r.ExitCode != 0 {
		t.Fatalf("ingest %s into %s failed (exit %d): %s", filePath, index, r.ExitCode, r.Stderr)
	}
}

// ─── Assertion helpers ───────────────────────────────────────────────────────

// mustParseJSON parses NDJSON lines into a slice of maps. Fatals on error.
func mustParseJSON(t *testing.T, s string) []map[string]interface{} {
	t.Helper()

	var result []map[string]interface{}

	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("parse JSON line %q: %v", line, err)
		}

		result = append(result, m)
	}

	return result
}

// jsonCount extracts the "count" field from the first row of NDJSON output.
func jsonCount(t *testing.T, s string) int {
	t.Helper()

	rows := mustParseJSON(t, s)
	if len(rows) == 0 {
		t.Fatal("expected at least one JSON row, got none")
	}

	v, ok := rows[0]["count"]
	if !ok {
		t.Fatalf("expected 'count' field in row, got keys: %v", mapKeys(rows[0]))
	}

	switch val := v.(type) {
	case float64:
		return int(val)
	case json.Number:
		n, _ := val.Int64()
		return int(n)
	default:
		t.Fatalf("unexpected count type %T: %v", v, v)
		return 0
	}
}

// testdataLog resolves a log file name to an absolute path under testdata/logs/.
func testdataLog(name string) string {
	return filepath.Join(projectRoot, "testdata", "logs", name)
}

// testdataFile resolves a file name to an absolute path under testdata/.
func testdataFile(name string) string {
	return filepath.Join(projectRoot, "testdata", name)
}

// containsANSI checks if a string contains ANSI escape sequences.
func containsANSI(s string) bool {
	return strings.Contains(s, "\033[")
}

// mapKeys returns the keys of a map.
func mapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys
}

// ─── Infra sanity tests ──────────────────────────────────────────────────────

func TestInfra_BinaryExists(t *testing.T) {
	info, err := os.Stat(lynxdbBin)
	if err != nil {
		t.Fatalf("lynxdb binary not found at %s: %v", lynxdbBin, err)
	}

	if info.IsDir() {
		t.Fatalf("lynxdb binary path is a directory: %s", lynxdbBin)
	}

	// On Unix, verify it's executable.
	if runtime.GOOS != "windows" {
		if info.Mode()&0111 == 0 {
			t.Errorf("lynxdb binary is not executable: mode=%v", info.Mode())
		}
	}
}

func TestInfra_Version(t *testing.T) {
	r := runLynxDB(t, "version")

	if r.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d\nstderr: %s", r.ExitCode, r.Stderr)
	}

	if !strings.Contains(r.Stdout, "LynxDB") {
		t.Errorf("expected 'LynxDB' in version output, got: %q", r.Stdout)
	}
}

func TestInfra_TestdataExists(t *testing.T) {
	accessLog := testdataLog("access.log")

	info, err := os.Stat(accessLog)
	if err != nil {
		t.Fatalf("testdata/logs/access.log not found at %s: %v", accessLog, err)
	}

	if info.Size() == 0 {
		t.Fatalf("testdata/logs/access.log is empty")
	}
}
