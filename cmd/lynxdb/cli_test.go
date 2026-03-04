package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/api/rest"
	"github.com/lynxbase/lynxdb/pkg/config"
)

// resetAllFlags clears the Changed state and resets values of all global vars
// and all Cobra PersistentFlag states so tests don't leak state to each other.
func resetAllFlags(t *testing.T) {
	t.Helper()

	// Reset global variables to defaults.
	globalServer = "http://localhost:3100"
	globalToken = ""
	globalFormat = "auto"
	globalProfile = ""
	globalQuiet = false
	globalVerbose = false
	globalNoColor = true // Always disable color in tests to simplify assertions.
	globalDebug = false
	globalTLSSkipVerify = false
	globalHTTPClient = nil
	flagConfigPath = ""
	flagBenchEvents = 100000
	flagInitDataDir = ""
	flagInitRetention = ""
	flagInitNoInteractive = false

	// Reset root persistent flags.
	rootCmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		f.Changed = false
		_ = f.Value.Set(f.DefValue)
	})

	// Reset all subcommand flags.
	for _, sub := range rootCmd.Commands() {
		sub.Flags().VisitAll(func(f *pflag.Flag) {
			f.Changed = false
			_ = f.Value.Set(f.DefValue)
		})
		sub.PersistentFlags().VisitAll(func(f *pflag.Flag) {
			f.Changed = false
			_ = f.Value.Set(f.DefValue)
		})
		// Also reset nested subcommands (e.g., mv create, config init).
		for _, nested := range sub.Commands() {
			nested.Flags().VisitAll(func(f *pflag.Flag) {
				f.Changed = false
				_ = f.Value.Set(f.DefValue)
			})
		}
	}

	// Ensure the UI theme is initialized for tests.
	ui.Init(true)
}

// captureOutput captures both stdout and stderr during the execution of fn.
// Returns the captured stdout, stderr, and any error returned by fn.
func captureOutput(t *testing.T, fn func() error) (string, string, error) {
	t.Helper()

	// Capture stdout.
	oldOut := os.Stdout
	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe: %v", err)
	}

	os.Stdout = wOut

	// Capture stderr.
	oldErr := os.Stderr
	rErr, wErr, err := os.Pipe()
	if err != nil {
		wOut.Close()
		os.Stdout = oldOut
		t.Fatalf("create stderr pipe: %v", err)
	}

	os.Stderr = wErr

	// Re-init UI theme to write to our captured stderr.
	ui.Init(true)

	fnErr := fn()

	wOut.Close()
	wErr.Close()
	os.Stdout = oldOut
	os.Stderr = oldErr

	// Re-init UI theme back to real stderr.
	ui.Init(true)

	var bufOut, bufErr bytes.Buffer
	bufOut.ReadFrom(rOut)
	bufErr.ReadFrom(rErr)

	return bufOut.String(), bufErr.String(), fnErr
}

// runCmd is a convenience: resetAllFlags + rootCmd.SetArgs + captureOutput.
func runCmd(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	resetAllFlags(t)

	// Ensure no config file interferes.
	t.Setenv("LYNXDB_CONFIG", "")
	t.Setenv("LYNXDB_SERVER", "")
	t.Setenv("LYNXDB_TOKEN", "")

	rootCmd.SetArgs(args)
	stdout, stderr, err = captureOutput(t, rootCmd.Execute)

	return
}

// cliProjectRoot returns the project root directory.
func cliProjectRoot() string {
	_, filename, _, _ := runtime.Caller(0)

	return filepath.Join(filepath.Dir(filename), "..", "..")
}

// testdataPath resolves a path relative to the project root's testdata directory.
func testdataPath(rel string) string {
	return filepath.Join(cliProjectRoot(), "testdata", rel)
}

// newTestServer starts an in-memory rest.Server on a random port and returns
// the base URL (e.g., "http://127.0.0.1:PORT"). The server is stopped when
// the test finishes.
func newTestServer(t *testing.T) string {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	storageCfg := config.DefaultConfig().Storage
	storageCfg.CompactionInterval = 1 * time.Hour // disable auto-compaction
	storageCfg.TieringInterval = 1 * time.Hour    // disable auto-tiering

	srv, err := rest.NewServer(rest.Config{
		Addr:    "127.0.0.1:0",
		DataDir: "", // in-memory
		Storage: storageCfg,
		Logger:  logger,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan struct{})

	go func() {
		defer close(startDone)
		if srvErr := srv.Start(ctx); srvErr != nil {
			if ctx.Err() == nil {
				t.Logf("server error: %v", srvErr)
			}
		}
	}()

	srv.WaitReady()

	baseURL := fmt.Sprintf("http://%s", srv.Addr())

	// Poll /health until the server is up.
	healthCtx, healthCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer healthCancel()

	for {
		req, _ := http.NewRequestWithContext(healthCtx, http.MethodGet, baseURL+"/health", http.NoBody)
		resp, reqErr := http.DefaultClient.Do(req)
		if reqErr == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}

		if healthCtx.Err() != nil {
			t.Fatalf("server did not become healthy within 10s")
		}

		runtime.Gosched()
	}

	t.Cleanup(func() {
		cancel()
		select {
		case <-startDone:
		case <-time.After(30 * time.Second):
			t.Fatal("server did not shut down within 30s")
		}
	})

	return baseURL
}

// newTestServerWithDisk starts a disk-backed rest.Server using t.TempDir() and
// returns the base URL. Required for features that need persistence (e.g., materialized views).
func newTestServerWithDisk(t *testing.T) string {
	t.Helper()

	dataDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	storageCfg := config.DefaultConfig().Storage
	storageCfg.CompactionInterval = 1 * time.Hour
	storageCfg.TieringInterval = 1 * time.Hour

	srv, err := rest.NewServer(rest.Config{
		Addr:    "127.0.0.1:0",
		DataDir: dataDir,
		Storage: storageCfg,
		Logger:  logger,
	})
	if err != nil {
		t.Fatalf("NewServer (disk): %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan struct{})

	go func() {
		defer close(startDone)
		if srvErr := srv.Start(ctx); srvErr != nil {
			if ctx.Err() == nil {
				t.Logf("server error: %v", srvErr)
			}
		}
	}()

	srv.WaitReady()

	baseURL := fmt.Sprintf("http://%s", srv.Addr())

	healthCtx, healthCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer healthCancel()

	for {
		req, _ := http.NewRequestWithContext(healthCtx, http.MethodGet, baseURL+"/health", http.NoBody)
		resp, reqErr := http.DefaultClient.Do(req)
		if reqErr == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}

		if healthCtx.Err() != nil {
			t.Fatalf("server did not become healthy within 10s")
		}

		runtime.Gosched()
	}

	t.Cleanup(func() {
		cancel()
		select {
		case <-startDone:
		case <-time.After(30 * time.Second):
			t.Fatal("server did not shut down within 30s")
		}
	})

	return baseURL
}

// ingestTestData reads a file and POSTs its content to the server's raw ingest
// endpoint into the given index.
func ingestTestData(t *testing.T, serverURL, index, relPath string) {
	t.Helper()

	absPath := filepath.Join(cliProjectRoot(), relPath)

	data, err := os.ReadFile(absPath)
	if err != nil {
		t.Fatalf("read file %s: %v", relPath, err)
	}

	req, err := http.NewRequest(http.MethodPost, serverURL+"/api/v1/ingest/raw", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Index", index)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("ingest %s: %v", relPath, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ingest %s: HTTP %d", relPath, resp.StatusCode)
	}

	var env struct {
		Data struct {
			Accepted int `json:"accepted"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode ingest response: %v", err)
	}

	t.Logf("ingested %s into %s: accepted=%d", relPath, index, env.Data.Accepted)
}

// mustParseJSON parses a string containing one or more NDJSON lines into
// a slice of maps.
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

// jsonCount extracts a float64 "count" field from the first row of parsed NDJSON.
func jsonCount(t *testing.T, s string) int {
	t.Helper()

	rows := mustParseJSON(t, s)
	if len(rows) == 0 {
		t.Fatal("expected at least one JSON row, got none")
	}

	v, ok := rows[0]["count"]
	if !ok {
		t.Fatalf("expected 'count' field in row, got keys: %v", cliMapKeys(rows[0]))
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

// cliMapKeys returns the keys of a map.
func cliMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys
}

// Infra sanity tests: verify the test helpers work

func TestCLIInfra_RunCmd_CapturesStdout(t *testing.T) {
	stdout, _, err := runCmd(t, "version")
	if err != nil {
		t.Fatalf("version command failed: %v", err)
	}

	if !strings.Contains(stdout, "LynxDB") {
		t.Errorf("expected 'LynxDB' in version output, got: %q", stdout)
	}
}

func TestCLIInfra_TestdataPath_Resolves(t *testing.T) {
	path := testdataPath("logs/access.log")
	if _, err := os.Stat(path); err != nil {
		t.Errorf("testdataPath should resolve to existing file, got error: %v", err)
	}
}

func TestCLIInfra_MustParseJSON_SingleLine(t *testing.T) {
	rows := mustParseJSON(t, `{"count":42}`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	if rows[0]["count"] != float64(42) {
		t.Errorf("expected count=42, got %v", rows[0]["count"])
	}
}

func TestCLIInfra_MustParseJSON_MultiLine(t *testing.T) {
	rows := mustParseJSON(t, "{\"a\":1}\n{\"b\":2}\n")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestCLIInfra_JsonCount(t *testing.T) {
	got := jsonCount(t, `{"count":294}`)
	if got != 294 {
		t.Errorf("expected 294, got %d", got)
	}
}

func TestCLIInfra_NewTestServer_StartsAndStops(t *testing.T) {
	baseURL := newTestServer(t)
	if !strings.HasPrefix(baseURL, "http://127.0.0.1:") {
		t.Errorf("expected http://127.0.0.1:PORT, got %q", baseURL)
	}

	// Verify server responds.
	resp, err := http.Get(baseURL + "/health")
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestCLIInfra_IngestTestData_AcceptsEvents(t *testing.T) {
	baseURL := newTestServer(t)
	ingestTestData(t, baseURL, "main", "testdata/logs/access.log")
	// If we get here without t.Fatal, ingest succeeded.
}
