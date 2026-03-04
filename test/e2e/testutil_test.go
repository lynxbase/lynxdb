//go:build e2e

package e2e

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
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/api/rest"
	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/config"
)

// Harness

// Harness manages a test server and typed client for E2E tests.
type Harness struct {
	t         *testing.T
	dataDir   string
	srv       *rest.Server
	client    *client.Client
	cancel    context.CancelFunc
	startDone chan struct{} // closed when srv.Start() returns (engine fully shut down)
	inMem     bool
}

// HarnessOption configures a Harness.
type HarnessOption func(*Harness)

// WithDisk creates a persistent data directory for the server.
func WithDisk() HarnessOption {
	return func(h *Harness) {
		h.inMem = false
		h.dataDir = h.t.TempDir()
	}
}

// WithInMemory creates a server with no disk persistence (default).
func WithInMemory() HarnessOption {
	return func(h *Harness) {
		h.inMem = true
		h.dataDir = ""
	}
}

// NewHarness starts a test server on a random port and creates a typed client.
// The server is torn down automatically when the test finishes.
func NewHarness(t *testing.T, opts ...HarnessOption) *Harness {
	t.Helper()

	h := &Harness{t: t, inMem: true}
	for _, opt := range opts {
		opt(h)
	}

	h.startServer()

	t.Cleanup(func() {
		h.stopServer()
	})

	return h
}

func (h *Harness) startServer() {
	h.t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	storageCfg := config.DefaultConfig().Storage
	storageCfg.CompactionInterval = 1 * time.Hour // disable auto-compaction in tests
	storageCfg.TieringInterval = 1 * time.Hour    // disable auto-tiering in tests

	var err error
	h.srv, err = rest.NewServer(rest.Config{
		Addr:    "127.0.0.1:0",
		DataDir: h.dataDir,
		Storage: storageCfg,
		Logger:  logger,
	})
	if err != nil {
		h.t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.startDone = make(chan struct{})

	go func() {
		defer close(h.startDone)
		if err := h.srv.Start(ctx); err != nil {
			if ctx.Err() == nil {
				h.t.Logf("server error: %v", err)
			}
		}
	}()

	h.srv.WaitReady()

	baseURL := fmt.Sprintf("http://%s", h.srv.Addr())
	h.client = client.NewClient(
		client.WithBaseURL(baseURL),
		client.WithTimeout(60*time.Second),
	)

	// Poll /health until the server is fully up. Use context deadline, not Sleep.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	for {
		_, err := h.client.Health(ctx2)
		if err == nil {
			break
		}
		if ctx2.Err() != nil {
			h.t.Fatalf("server did not become healthy within 10s: %v", err)
		}
		// Minimal backoff — this is polling, not sleep-based sync.
		runtime.Gosched()
	}

	h.t.Logf("server started at %s (data-dir: %q)", baseURL, h.dataDir)
}

func (h *Harness) stopServer() {
	h.t.Helper()
	if h.cancel != nil {
		h.cancel()
		h.cancel = nil

		// Wait for srv.Start() to return. Start() blocks on <-shutdownDone
		// which is closed only after the engine flush + WAL close + registry
		// sync complete. This guarantees all data is on disk before we
		// potentially restart the server on the same data directory.
		select {
		case <-h.startDone:
		case <-time.After(30 * time.Second):
			h.t.Fatal("server did not shut down within 30s")
		}

		h.t.Log("server stopped")
	}
}

// RestartServer stops and restarts the server. Uses polling, never Sleep.
func (h *Harness) RestartServer() {
	h.t.Helper()
	h.t.Log("restarting server...")
	h.stopServer()
	h.startServer()
	h.t.Log("server restarted")
}

// Client returns the typed HTTP client.
func (h *Harness) Client() *client.Client {
	return h.client
}

// BaseURL returns the server base URL.
func (h *Harness) BaseURL() string {
	return fmt.Sprintf("http://%s", h.srv.Addr())
}

// Data helpers

func projectRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..")
}

// IngestFile reads a file relative to the project root and ingests it as raw text
// into the specified index. Uses a direct HTTP POST with X-Index header for
// simplicity (avoids constructing an io.Reader from a file path through the client).
func (h *Harness) IngestFile(index, relPath string) {
	h.t.Helper()

	absPath := filepath.Join(projectRoot(), relPath)
	data, err := os.ReadFile(absPath)
	if err != nil {
		h.t.Fatalf("read file %s: %v", relPath, err)
	}

	req, err := http.NewRequest(http.MethodPost, h.BaseURL()+"/api/v1/ingest/raw", bytes.NewReader(data))
	if err != nil {
		h.t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Index", index)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("ingest %s: %v", relPath, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		h.t.Fatalf("ingest %s: HTTP %d", relPath, resp.StatusCode)
	}

	var env struct {
		Data struct {
			Accepted int `json:"accepted"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		h.t.Fatalf("decode ingest response: %v", err)
	}
	h.t.Logf("ingested %s into %s: accepted=%d", relPath, index, env.Data.Accepted)
}

// Query helpers

// MustQuery executes a synchronous query (Wait=nil) and returns the result.
// Uses the sync path which waits server-side for job completion and returns
// the result inline with HTTP 200.
func (h *Harness) MustQuery(q string) *client.QueryResult {
	h.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Clear cache for deterministic results.
	_ = h.client.CacheClear(ctx)

	result, err := h.client.QuerySync(ctx, q, "", "")
	if err != nil {
		h.t.Fatalf("QuerySync %q: %v", q, err)
	}

	// If the server promoted the sync query to async (returned 202), fail fast.
	if result.Type == client.ResultTypeJob {
		h.t.Fatalf("QuerySync %q: server promoted to async (job_id=%s); sync query did not complete within server timeout", q, result.Job.JobID)
	}

	return result
}

// Result helpers

// EventCount returns the number of result rows regardless of result type.
func EventCount(r *client.QueryResult) int {
	if r == nil {
		return 0
	}
	switch r.Type {
	case client.ResultTypeEvents:
		if r.Events != nil {
			return len(r.Events.Events)
		}
	case client.ResultTypeAggregate, client.ResultTypeTimechart:
		if r.Aggregate != nil {
			return len(r.Aggregate.Rows)
		}
	}
	return 0
}

// AggRows converts an AggregateResult into a slice of maps for easy assertion.
func AggRows(r *client.QueryResult) []map[string]interface{} {
	if r == nil || r.Aggregate == nil {
		return nil
	}
	agg := r.Aggregate
	rows := make([]map[string]interface{}, len(agg.Rows))
	for i, row := range agg.Rows {
		m := make(map[string]interface{}, len(agg.Columns))
		for j, col := range agg.Columns {
			if j < len(row) {
				m[col] = row[j]
			}
		}
		rows[i] = m
	}
	return rows
}

// EventRows returns events as-is for EventsResult, or converts AggregateResult.
func EventRows(r *client.QueryResult) []map[string]interface{} {
	if r == nil {
		return nil
	}
	switch r.Type {
	case client.ResultTypeEvents:
		if r.Events != nil {
			return r.Events.Events
		}
	case client.ResultTypeAggregate, client.ResultTypeTimechart:
		return AggRows(r)
	}
	return nil
}

// GetInt returns an integer from the first result row's field.
func GetInt(r *client.QueryResult, field string) int {
	rows := EventRows(r)
	if len(rows) == 0 {
		return 0
	}
	return toInt(rows[0][field])
}

// GetFloat returns a float from the first result row's field.
func GetFloat(r *client.QueryResult, field string) float64 {
	rows := EventRows(r)
	if len(rows) == 0 {
		return 0
	}
	return toFloat(rows[0][field])
}

// GetStr returns a string from the first result row's field.
func GetStr(r *client.QueryResult, field string) string {
	rows := EventRows(r)
	if len(rows) == 0 {
		return ""
	}
	v := rows[0][field]
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func toInt(v interface{}) int {
	switch val := v.(type) {
	case float64:
		return int(val)
	case int64:
		return int(val)
	case int:
		return val
	case json.Number:
		n, _ := val.Int64()
		return int(n)
	default:
		return 0
	}
}

func toFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case int:
		return float64(val)
	case json.Number:
		n, _ := val.Float64()
		return n
	default:
		return 0
	}
}

// Assertion helpers

// requireEventCount fatals if the result doesn't have the expected row count.
func requireEventCount(t *testing.T, r *client.QueryResult, expected int) {
	t.Helper()
	got := EventCount(r)
	if got != expected {
		t.Fatalf("expected %d rows, got %d", expected, got)
	}
}

// requireAggValue asserts the first row's field equals the expected value.
func requireAggValue(t *testing.T, r *client.QueryResult, field string, expected int) {
	t.Helper()
	got := GetInt(r, field)
	if got != expected {
		t.Errorf("expected %s=%d, got %d", field, expected, got)
	}
}

// assertQueryResultsEqual compares pre and post restart query results.
func assertQueryResultsEqual(t *testing.T, name string, pre, post *client.QueryResult) {
	t.Helper()

	preRows := EventRows(pre)
	postRows := EventRows(post)

	if len(preRows) != len(postRows) {
		t.Errorf("[%s] row count mismatch: pre=%d, post=%d", name, len(preRows), len(postRows))
		return
	}

	maxRows := len(preRows)
	for i := 0; i < maxRows; i++ {
		for key, preVal := range preRows[i] {
			postVal := postRows[i][key]
			if fmt.Sprint(preVal) != fmt.Sprint(postVal) {
				t.Errorf("[%s] row %d field %s: pre=%v, post=%v", name, i, key, preVal, postVal)
			}
		}
	}
}

// Unused import guard

var _ = http.StatusOK
