package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/event"
)

func startTestServer(t *testing.T) (*Server, func()) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	queryCfg := config.QueryConfig{SpillDir: t.TempDir()}
	srv, err := NewServer(Config{
		Addr:   "127.0.0.1:0",
		Logger: logger,
		Query:  queryCfg,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start(ctx) }()
	waitTestServerReady(t, srv, errCh)

	return srv, func() {
		cancel()
		time.Sleep(50 * time.Millisecond)
	}
}

func assertLintMeta(t *testing.T, meta map[string]interface{}, wantCode string) {
	t.Helper()
	if meta == nil {
		t.Fatal("missing meta")
	}
	lints, _ := meta["lints"].([]interface{})
	if len(lints) != 1 {
		t.Fatalf("meta.lints: got %#v, want one lint", meta["lints"])
	}
	firstLint, _ := lints[0].(map[string]interface{})
	if firstLint["code"] != wantCode {
		t.Fatalf("meta.lints[0].code: got %v, want %s", firstLint["code"], wantCode)
	}
}

func startTestServerWithConfig(t *testing.T, cfg Config) (*Server, func()) {
	t.Helper()

	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	}
	queryCfg := cfg.Query
	if queryCfg.SpillDir == "" {
		queryCfg.SpillDir = t.TempDir()
	}

	srv, err := NewServer(Config{
		Addr:          "127.0.0.1:0",
		DataDir:       cfg.DataDir,
		RuntimeConfig: cfg.RuntimeConfig,
		Storage:       cfg.Storage,
		Logger:        logger,
		Query:         queryCfg,
		Ingest:        cfg.Ingest,
		Server:        cfg.Server,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start(ctx) }()
	waitTestServerReady(t, srv, errCh)

	return srv, func() {
		cancel()
		time.Sleep(50 * time.Millisecond)
	}
}

func assertRewriteMeta(t *testing.T, meta map[string]interface{}, wantAfter, wantReason string) {
	t.Helper()
	if meta == nil {
		t.Fatal("missing meta")
	}
	rewrites, _ := meta["rewrites"].([]interface{})
	if len(rewrites) != 1 {
		t.Fatalf("meta.rewrites: got %#v, want one rewrite", meta["rewrites"])
	}
	firstRewrite, _ := rewrites[0].(map[string]interface{})
	if firstRewrite["after"] != wantAfter {
		t.Fatalf("meta.rewrites[0].after: got %v, want %s", firstRewrite["after"], wantAfter)
	}
	if firstRewrite["reason"] != wantReason {
		t.Fatalf("meta.rewrites[0].reason: got %v, want %s", firstRewrite["reason"], wantReason)
	}
}

func waitTestServerReady(t *testing.T, srv *Server, errCh <-chan error) {
	t.Helper()
	select {
	case <-srv.ready:
	case err := <-errCh:
		t.Fatalf("server failed before ready: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("server did not become ready within 30s")
	}
}

// ingestTestEvents is a helper that ingests n events with configurable hosts.
func ingestTestEvents(t *testing.T, addr string, n, hostCount int) {
	t.Helper()
	now := float64(time.Now().Unix())
	events := make([]map[string]interface{}, n)
	for i := 0; i < n; i++ {
		host := fmt.Sprintf("web-%02d", i%hostCount)
		events[i] = map[string]interface{}{
			"time":       now + float64(i),
			"event":      fmt.Sprintf("host=%s level=INFO status=200 msg=\"request %d\"", host, i),
			"host":       host,
			"source":     "/var/log/app.log",
			"sourcetype": "json",
			"index":      "main",
		}
	}
	body, _ := json.Marshal(events)
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/ingest", addr), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST events: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("ingest status: %d", resp.StatusCode)
	}
}

func ingestIndexedTestEvents(t *testing.T, addr string, indexCount int) {
	t.Helper()
	for i := 0; i < indexCount; i++ {
		index := fmt.Sprintf("idx-%02d", i)
		req, err := http.NewRequest(http.MethodPost,
			fmt.Sprintf("http://%s/api/v1/ingest/raw", addr),
			strings.NewReader(fmt.Sprintf("source=%s msg=\"request %d\"\n", index, i)))
		if err != nil {
			t.Fatalf("new indexed ingest request: %v", err)
		}
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("X-Index", index)
		req.Header.Set("X-Source", index)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST indexed raw events: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("indexed raw ingest status: %d", resp.StatusCode)
		}
	}
}

func metaLintCodes(meta map[string]interface{}) []string {
	lints, _ := meta["lints"].([]interface{})
	codes := make([]string, 0, len(lints))
	for _, raw := range lints {
		lint, _ := raw.(map[string]interface{})
		if code, _ := lint["code"].(string); code != "" {
			codes = append(codes, code)
		}
	}

	return codes
}

func lintCodesContain(codes []string, want string) bool {
	for _, code := range codes {
		if code == want {
			return true
		}
	}

	return false
}

func TestServer_Health(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/health", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
}

func TestServer_RootIsESCompatible(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	rootResp, err := http.Get(fmt.Sprintf("http://%s/", srv.Addr()))
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer rootResp.Body.Close()
	if rootResp.StatusCode != http.StatusOK {
		t.Fatalf("GET / status: %d", rootResp.StatusCode)
	}
	if got := rootResp.Header.Get("X-Elastic-Product"); got != "Elasticsearch" {
		t.Fatalf("GET / X-Elastic-Product = %q, want Elasticsearch", got)
	}
}

func TestServer_ListIndexes(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/indexes", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})
	indexes := data["indexes"].([]interface{})
	if len(indexes) != 1 {
		t.Errorf("expected 1 index (main), got %d", len(indexes))
	}
}

func TestServer_CreateIndex(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":           "security",
		"retention_days": 30,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/indexes", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	// Verify index exists.
	resp2, err2 := http.Get(fmt.Sprintf("http://%s/api/v1/indexes", srv.Addr()))
	if err2 != nil {
		t.Fatalf("GET indexes: %v", err2)
	}
	defer resp2.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp2.Body).Decode(&result)
	data2 := result["data"].(map[string]interface{})
	indexes := data2["indexes"].([]interface{})
	if len(indexes) != 2 {
		t.Errorf("expected 2 indexes, got %d", len(indexes))
	}
}

func TestServer_ClusterStatus(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/cluster/status", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})
	if data["status"] != "healthy" {
		t.Errorf("status: %v", data["status"])
	}
}

func TestServer_IngestAndQuery(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 50, 5)

	// Sync query (no wait param).
	searchBody, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | where has(_raw, "request") | head 10`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(searchBody))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("sync query status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})

	if data["type"] != "events" {
		t.Errorf("data.type: got %v, want events", data["type"])
	}

	events := data["events"].([]interface{})
	if len(events) != 10 {
		t.Errorf("events count: got %d, want 10", len(events))
	}

	meta, _ := result["meta"].(map[string]interface{})
	if meta == nil {
		t.Error("missing meta in response")
	}
	if _, ok := meta["took_ms"]; !ok {
		t.Error("missing took_ms in meta")
	}
	// Post-RFC-002: query_id may not be in LynxFlow meta; verify took_ms suffices.

	// Verify a query without explicit FROM gets the default source rewritten.
	rewriteQuery := "stats count()"
	rewriteBody, _ := json.Marshal(map[string]interface{}{
		"q": rewriteQuery,
	})
	rewriteResp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(rewriteBody))
	if err != nil {
		t.Fatalf("POST rewrite query: %v", err)
	}
	defer rewriteResp.Body.Close()

	if rewriteResp.StatusCode != 200 {
		b, _ := io.ReadAll(rewriteResp.Body)
		t.Fatalf("rewrite query status: %d, body: %s", rewriteResp.StatusCode, string(b))
	}

	lintBody, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | stats count() by host`,
	})
	lintResp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(lintBody))
	if err != nil {
		t.Fatalf("POST lint query: %v", err)
	}
	defer lintResp.Body.Close()

	if lintResp.StatusCode != 200 {
		b, _ := io.ReadAll(lintResp.Body)
		t.Fatalf("lint query status: %d, body: %s", lintResp.StatusCode, string(b))
	}

	var lintResult map[string]interface{}
	json.NewDecoder(lintResp.Body).Decode(&lintResult)
	lintMeta, _ := lintResult["meta"].(map[string]interface{})
	if lintMeta == nil {
		t.Fatal("missing meta in lint query response")
	}
	lints, _ := lintMeta["lints"].([]interface{})
	if len(lints) == 0 {
		t.Fatalf("meta.lints: expected at least one lint, got none")
	}
	// LynxFlow produces LF03 (no time bound) for queries without time constraints.
	firstLint, _ := lints[0].(map[string]interface{})
	if firstLint["code"] == nil {
		t.Fatalf("meta.lints[0] missing code: %#v", firstLint)
	}

	disabled := false
	noLintBody, _ := json.Marshal(map[string]interface{}{
		"q":    `FROM main | stats count() by host`,
		"lint": disabled,
	})
	noLintResp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(noLintBody))
	if err != nil {
		t.Fatalf("POST no-lint query: %v", err)
	}
	defer noLintResp.Body.Close()

	if noLintResp.StatusCode != 200 {
		b, _ := io.ReadAll(noLintResp.Body)
		t.Fatalf("no-lint query status: %d, body: %s", noLintResp.StatusCode, string(b))
	}

	var noLintResult map[string]interface{}
	json.NewDecoder(noLintResp.Body).Decode(&noLintResult)
	noLintMeta, _ := noLintResult["meta"].(map[string]interface{})
	if noLintMeta == nil {
		t.Fatal("missing meta in no-lint query response")
	}
	if _, ok := noLintMeta["lints"]; ok {
		t.Fatalf("meta.lints present despite lint=false: %#v", noLintMeta["lints"])
	}
}

func TestServer_IngestJSONArray_PartialSuccessOnMalformedTail(t *testing.T) {
	srv, cleanup := startTestServerWithConfig(t, Config{
		Ingest: config.IngestConfig{
			MaxBatchSize: 1,
		},
	})
	defer cleanup()

	body := `[{"event":"line one","index":"main"},{"event":"line two","index":"main"},`
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/ingest", srv.Addr()), "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST ingest: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200, body: %s", resp.StatusCode, string(raw))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	data := result["data"].(map[string]interface{})
	if got := int(data["accepted"].(float64)); got != 2 {
		t.Fatalf("accepted: got %d, want 2", got)
	}
	if warning, ok := data["warning"].(string); !ok || warning == "" {
		t.Fatalf("expected warning on partial success, got %v", data["warning"])
	}

	if got := queryEventCount(t, srv.Addr(), `{"q":"FROM main"}`); got != 2 {
		t.Fatalf("query count: got %d, want 2", got)
	}
}

func TestServer_IngestRejectsSingleObjectWithSuggestion(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/ingest", srv.Addr()),
		"application/json",
		bytes.NewBufferString(`{"event":"one"}`),
	)
	if err != nil {
		t.Fatalf("POST ingest: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	errObj := result["error"].(map[string]interface{})
	msg, _ := errObj["message"].(string)
	if !strings.Contains(msg, "top-level JSON array") {
		t.Fatalf("message = %q, want array guidance", msg)
	}
	suggestion, _ := errObj["suggestion"].(string)
	if !strings.Contains(suggestion, "/api/v1/ingest/raw") || !strings.Contains(suggestion, "/api/v1/es/_bulk") {
		t.Fatalf("suggestion = %q, want endpoint guidance", suggestion)
	}
}

func TestServer_IngestRejectsNDJSONWithSuggestion(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/ingest", srv.Addr()),
		"application/x-ndjson",
		bytes.NewBufferString("{\"event\":\"one\"}\n{\"event\":\"two\"}\n"),
	)
	if err != nil {
		t.Fatalf("POST ingest: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	errObj := result["error"].(map[string]interface{})
	suggestion, _ := errObj["suggestion"].(string)
	if !strings.Contains(suggestion, "/api/v1/ingest/raw") {
		t.Fatalf("suggestion = %q, want raw endpoint guidance", suggestion)
	}
}

func TestServer_IngestRaw(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	raw := "2024-01-01T00:00:00Z line one\n2024-01-01T00:00:01Z line two\n"
	req, _ := http.NewRequest("POST", fmt.Sprintf("http://%s/api/v1/ingest/raw", srv.Addr()), bytes.NewBufferString(raw))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
}

func TestServer_IngestRawLarge(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	var buf bytes.Buffer
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&buf, "2024-01-01T00:00:%02dZ host=web-%02d level=INFO msg=\"request %d\"\n", i%60, i%10, i)
	}

	req, _ := http.NewRequest("POST", fmt.Sprintf("http://%s/api/v1/ingest/raw", srv.Addr()), &buf)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})

	count, ok := data["accepted"].(float64)
	if !ok || int(count) != 5000 {
		t.Errorf("accepted: got %v, want 5000", data["accepted"])
	}

	// In-memory mode: events are immediately flushed to segments (no batcher).
	if srv.engine.SegmentCount() == 0 {
		t.Error("expected segments after ingest in in-memory mode")
	}
}

func TestServer_AutoFlush(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv, err := NewServer(Config{
		Addr:   "127.0.0.1:0",
		Logger: logger,
		Query:  config.QueryConfig{SpillDir: t.TempDir()},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Manually test FlushBatcher via Ingest (in-memory mode).
	base := time.Now()
	flushEvents := make([]*event.Event, 100)
	for i := 0; i < 100; i++ {
		flushEvents[i] = &event.Event{
			Time:       base.Add(time.Duration(i) * time.Millisecond),
			Raw:        fmt.Sprintf("event %d", i),
			Host:       "web-01",
			Index:      "main",
			Source:     "test",
			SourceType: "raw",
			Fields:     make(map[string]event.Value),
		}
	}
	if err := srv.engine.Ingest(flushEvents); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// In-memory mode: Ingest calls flushInMemory which creates segments directly.
	// Events are immediately in segments, not buffered.
	if srv.engine.SegmentCount() != 1 {
		t.Errorf("segments after ingest: got %d, want 1", srv.engine.SegmentCount())
	}
}

func TestServer_StatsQuery(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 30, 3)

	// Sync stats query.
	searchBody, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | stats count() by host`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(searchBody))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})

	if data["type"] != "aggregate" {
		t.Errorf("data.type: got %v, want aggregate", data["type"])
	}

	cols, _ := data["columns"].([]interface{})
	if len(cols) == 0 {
		t.Fatal("no columns in aggregate response")
	}

	rows, _ := data["rows"].([]interface{})
	totalRows, _ := data["total_rows"].(float64)
	if int(totalRows) != 3 {
		t.Errorf("total_rows: got %v, want 3 (one per host)", totalRows)
	}

	// Verify each group has count=10.
	for _, row := range rows {
		arr := row.([]interface{})
		// Find count column index.
		for j, col := range cols {
			if fmt.Sprint(col) == "count" && j < len(arr) {
				count := arr[j].(float64)
				if count != 10 {
					t.Errorf("count=%v, want 10", count)
				}
			}
		}
	}
}

func TestServer_StatsAggregateAliases(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	now := time.Now()
	durations := []float64{20, 20, 20}
	users := []string{"alice", "bob", "alice"}
	events := make([]*event.Event, 0, len(durations))
	for i, duration := range durations {
		ev := event.NewEvent(now.Add(time.Duration(i)*time.Second), fmt.Sprintf("duration_ms=%v user=%s", duration, users[i]))
		ev.Index = "main"
		ev.Host = "web-00"
		ev.Source = "/var/log/app.log"
		ev.SourceType = "json"
		ev.SetField("duration_ms", event.FloatValue(duration))
		ev.SetField("user", event.StringValue(users[i]))
		events = append(events, ev)
	}
	if err := srv.engine.Ingest(events); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	searchBody, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | stats avg(duration_ms) as avg_dur, p50(duration_ms) as p50_dur, dc(user) as users, estdc(user) as estimated_users, mode(user) as common_user`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(searchBody))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	data := result["data"].(map[string]interface{})
	cols := data["columns"].([]interface{})
	rows := data["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}
	row := rows[0].([]interface{})
	colIndex := func(name string) int {
		t.Helper()
		for i, col := range cols {
			if fmt.Sprint(col) == name {
				return i
			}
		}
		t.Fatalf("missing column %q in %v", name, cols)
		return -1
	}

	if got := row[colIndex("avg_dur")].(float64); got != 20 {
		t.Errorf("avg_dur: got %v, want 20", got)
	}
	if got := row[colIndex("p50_dur")].(float64); got != 20 {
		t.Errorf("p50_dur: got %v, want 20", got)
	}
	if got := row[colIndex("users")].(float64); got != 2 {
		t.Errorf("users: got %v, want 2", got)
	}
	if got := row[colIndex("estimated_users")].(float64); got != 2 {
		t.Errorf("estimated_users: got %v, want 2", got)
	}
	if got := row[colIndex("common_user")].(string); got != "alice" {
		t.Errorf("common_user: got %v, want alice", got)
	}
}

func TestServer_StatsPercentileSuffixAliases(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	now := time.Now()
	events := make([]*event.Event, 0, 3)
	for i := 0; i < 3; i++ {
		ev := event.NewEvent(now.Add(time.Duration(i)*time.Second), "duration_ms=20")
		ev.Index = "main"
		ev.Host = "web-00"
		ev.Source = "/var/log/app.log"
		ev.SourceType = "json"
		ev.SetField("duration_ms", event.FloatValue(20))
		events = append(events, ev)
	}
	if err := srv.engine.Ingest(events); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | stats p95(duration_ms) as p95, p99(duration_ms) as p99_dur`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	data := result["data"].(map[string]interface{})
	cols := data["columns"].([]interface{})
	rows := data["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}
	row := rows[0].([]interface{})
	for _, name := range []string{"p95", "p99_dur"} {
		found := false
		for i, col := range cols {
			if fmt.Sprint(col) == name {
				found = true
				if got := row[i].(float64); got != 20 {
					t.Errorf("%s: got %v, want 20", name, got)
				}
			}
		}
		if !found {
			t.Fatalf("missing column %q in %v", name, cols)
		}
	}
}

func TestServer_StatsRangeAggregate(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	now := time.Now()
	durations := []float64{10, 25, 40}
	events := make([]*event.Event, 0, len(durations))
	for i, duration := range durations {
		ev := event.NewEvent(now.Add(time.Duration(i)*time.Second), fmt.Sprintf("duration_ms=%v", duration))
		ev.Index = "main"
		ev.Host = "web-00"
		ev.Source = "/var/log/app.log"
		ev.SourceType = "json"
		ev.SetField("duration_ms", event.FloatValue(duration))
		events = append(events, ev)
	}
	if err := srv.engine.Ingest(events); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | stats range(duration_ms) as duration_range`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	data := result["data"].(map[string]interface{})
	cols := data["columns"].([]interface{})
	rows := data["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}
	row := rows[0].([]interface{})
	for i, col := range cols {
		if fmt.Sprint(col) == "duration_range" {
			if got := row[i].(float64); got != 30 {
				t.Fatalf("duration_range: got %v, want 30", got)
			}
			return
		}
	}
	t.Fatalf("missing duration_range column in %v", cols)
}

func TestServer_StatsSumSqAggregate(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	now := time.Now()
	durations := []float64{2, 3, 4}
	events := make([]*event.Event, 0, len(durations))
	for i, duration := range durations {
		ev := event.NewEvent(now.Add(time.Duration(i)*time.Second), fmt.Sprintf("duration_ms=%v", duration))
		ev.Index = "main"
		ev.Host = "web-00"
		ev.Source = "/var/log/app.log"
		ev.SourceType = "json"
		ev.SetField("duration_ms", event.FloatValue(duration))
		events = append(events, ev)
	}
	if err := srv.engine.Ingest(events); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | stats sumsq(duration_ms) as duration_squares`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	data := result["data"].(map[string]interface{})
	cols := data["columns"].([]interface{})
	rows := data["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}
	row := rows[0].([]interface{})
	for i, col := range cols {
		if fmt.Sprint(col) == "duration_squares" {
			if got := row[i].(float64); got != 29 {
				t.Fatalf("duration_squares: got %v, want 29", got)
			}
			return
		}
	}
	t.Fatalf("missing duration_squares column in %v", cols)
}

func TestServer_StatsVarianceAggregates(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	now := time.Now()
	durations := []float64{0, 2}
	events := make([]*event.Event, 0, len(durations))
	for i, duration := range durations {
		ev := event.NewEvent(now.Add(time.Duration(i)*time.Second), fmt.Sprintf("duration_ms=%v", duration))
		ev.Index = "main"
		ev.Host = "web-00"
		ev.Source = "/var/log/app.log"
		ev.SourceType = "json"
		ev.SetField("duration_ms", event.FloatValue(duration))
		events = append(events, ev)
	}
	if err := srv.engine.Ingest(events); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | stats stdevp(duration_ms) as stdevp_duration, var(duration_ms) as sample_var, varp(duration_ms) as population_var`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	data := result["data"].(map[string]interface{})
	cols := data["columns"].([]interface{})
	rows := data["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}
	row := rows[0].([]interface{})
	values := map[string]float64{}
	for i, col := range cols {
		values[fmt.Sprint(col)] = row[i].(float64)
	}
	if got := values["stdevp_duration"]; got != 1 {
		t.Fatalf("stdevp_duration: got %v, want 1", got)
	}
	if got := values["sample_var"]; got != 2 {
		t.Fatalf("sample_var: got %v, want 2", got)
	}
	if got := values["population_var"]; got != 1 {
		t.Fatalf("population_var: got %v, want 1", got)
	}
}

func TestServer_StatsListAggregatePreservesDuplicates(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	now := time.Now()
	users := []string{"alice", "bob", "alice"}
	events := make([]*event.Event, 0, len(users))
	for i, user := range users {
		ev := event.NewEvent(now.Add(time.Duration(i)*time.Second), fmt.Sprintf("user=%s", user))
		ev.Index = "main"
		ev.Host = "web-00"
		ev.Source = "/var/log/app.log"
		ev.SourceType = "json"
		ev.SetField("user", event.StringValue(user))
		events = append(events, ev)
	}
	if err := srv.engine.Ingest(events); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | stats list(user) as user_list, values(user) as user_values`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	data := result["data"].(map[string]interface{})
	cols := data["columns"].([]interface{})
	rows := data["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}
	row := rows[0].([]interface{})
	values := map[string]string{}
	for i, col := range cols {
		values[fmt.Sprint(col)] = row[i].(string)
	}
	if got := values["user_list"]; got != "alice|||bob|||alice" {
		t.Fatalf("user_list: got %q, want alice|||bob|||alice", got)
	}
	if got := values["user_values"]; got != "alice|||bob" {
		t.Fatalf("user_values: got %q, want alice|||bob", got)
	}
}

// New Three-Mode Query Tests

func TestQuery_SyncMode(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 20, 2)

	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | head 5`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})
	meta := result["meta"].(map[string]interface{})

	if data["type"] != "events" {
		t.Errorf("type: %v", data["type"])
	}
	events := data["events"].([]interface{})
	if len(events) != 5 {
		t.Errorf("events: got %d, want 5", len(events))
	}
	if len(events) > 0 {
		first := events[0].(map[string]interface{})
		if _, ok := first["_source"]; !ok {
			t.Error("event row missing _source")
		}
		if _, ok := first["_sourcetype"]; !ok {
			t.Error("event row missing _sourcetype")
		}
		if _, ok := first["source"]; ok {
			t.Error("event row should not include source alias")
		}
		if _, ok := first["sourcetype"]; ok {
			t.Error("event row should not include sourcetype alias")
		}
	}
	if _, ok := meta["took_ms"]; !ok {
		t.Error("missing took_ms")
	}

}

func TestQuery_AsyncMode(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 20, 2)

	query := `stats count() by host`
	body, _ := json.Marshal(map[string]interface{}{
		"q": query,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	// Post-RFC-002: LynxFlow executes synchronously; accept both 200 and 202.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200 or 202, body: %s", resp.StatusCode, string(b))
	}

	// Post-RFC-002: LynxFlow queries execute synchronously. Verify the result.
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data, ok := result["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing data in response: %#v", result)
	}
	// Should be an aggregate response with rows.
	if dtype, _ := data["type"].(string); dtype != "aggregate" {
		t.Fatalf("data.type: got %v, want aggregate", dtype)
	}
	rows, _ := data["rows"].([]interface{})
	if len(rows) != 2 {
		t.Fatalf("rows: got %d, want 2 (one per host)", len(rows))
	}
}

func TestQuery_HybridFast(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 10, 2)

	wait := float64(5) // 5 seconds — query should finish well within
	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | head 3`, "wait": wait,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	// Should be 200 (fast query completes within 5s).
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})

	events := data["events"].([]interface{})
	if len(events) != 3 {
		t.Errorf("events: got %d, want 3", len(events))
	}
}

func TestQuery_AggregateResult(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 30, 3)

	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | stats count() by host`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})

	if data["type"] != "aggregate" {
		t.Errorf("type: %v", data["type"])
	}

	cols := data["columns"].([]interface{})
	rows := data["rows"].([]interface{})
	totalRows := data["total_rows"].(float64)

	if len(cols) < 2 {
		t.Errorf("columns: got %d, want >= 2", len(cols))
	}
	if int(totalRows) != 3 {
		t.Errorf("total_rows: got %v, want 3", totalRows)
	}
	if len(rows) != 3 {
		t.Errorf("rows: got %d, want 3", len(rows))
	}
}

func TestQuery_TimechartResult(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 30, 3)

	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | every 1h by host stats count()`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})

	dtype, _ := data["type"].(string)
	if dtype != "timechart" && dtype != "aggregate" {
		t.Errorf("type: got %v, want timechart or aggregate", data["type"])
	}

	cols := data["columns"].([]interface{})
	if len(cols) < 2 {
		t.Errorf("columns: got %d, want >= 2", len(cols))
	}

	rows := data["rows"].([]interface{})
	if len(rows) == 0 {
		t.Error("rows: got 0, want > 0")
	}

	totalRows := data["total_rows"].(float64)
	if int(totalRows) != len(rows) {
		t.Errorf("total_rows: got %v, want %d", totalRows, len(rows))
	}
}

func TestQuery_TimechartPerMinuteAggregate(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	events := []*event.Event{
		event.NewEvent(base, "bytes=60"),
		event.NewEvent(base.Add(30*time.Second), "bytes=120"),
	}
	for _, ev := range events {
		ev.Index = "main"
		ev.Host = "web-00"
		ev.Source = "/var/log/app.log"
		ev.SourceType = "json"
	}
	events[0].SetField("bytes", event.IntValue(60))
	events[1].SetField("bytes", event.IntValue(120))
	if err := srv.engine.Ingest(events); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | every 1h stats per_second(bytes) as bytes_per_sec`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	data := result["data"].(map[string]interface{})
	cols := data["columns"].([]interface{})
	rows := data["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}
	colIndex := -1
	for i, col := range cols {
		if fmt.Sprint(col) == "bytes_per_sec" {
			colIndex = i
			break
		}
	}
	if colIndex < 0 {
		t.Fatalf("missing bytes_per_sec column in %v", cols)
	}
	got := rows[0].([]interface{})[colIndex].(float64)
	// per_second(bytes) over 1h bucket with 60+120=180 bytes: 180/3600 = 0.05
	if got <= 0 {
		t.Errorf("bytes_per_sec: got %v, want > 0", got)
	}
}

func TestQuery_StatsTimeAggregates(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	events := []map[string]interface{}{
		{
			"time":       float64(base.Add(10 * time.Second).Unix()),
			"event":      `host=web-00 level=INFO counter=25 status=late msg="late"`,
			"host":       "web-00",
			"source":     "/var/log/app.log",
			"sourcetype": "json",
			"index":      "main",
		},
		{
			"time":       float64(base.Unix()),
			"event":      `host=web-00 level=INFO counter=10 status=early msg="early"`,
			"host":       "web-00",
			"source":     "/var/log/app.log",
			"sourcetype": "json",
			"index":      "main",
		},
	}
	ingestBody, _ := json.Marshal(events)
	ingestResp, err := http.Post(fmt.Sprintf("http://%s/api/v1/ingest", srv.Addr()), "application/json", bytes.NewReader(ingestBody))
	if err != nil {
		t.Fatalf("POST ingest: %v", err)
	}
	ingestResp.Body.Close()
	if ingestResp.StatusCode != 200 {
		t.Fatalf("ingest status: %d", ingestResp.StatusCode)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | stats earliest(status) as first_status, latest(status) as last_status, earliest_time(counter) as first_ts, latest_time(counter) as last_ts, rate(counter) as counter_rate`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	data := result["data"].(map[string]interface{})
	cols := data["columns"].([]interface{})
	rows := data["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1", len(rows))
	}
	row := rows[0].([]interface{})
	colIndex := func(name string) int {
		t.Helper()
		for i, col := range cols {
			if fmt.Sprint(col) == name {
				return i
			}
		}
		t.Fatalf("missing column %q in %v", name, cols)
		return -1
	}

	if got := row[colIndex("first_status")].(string); got != "early" {
		t.Errorf("first_status: got %q, want early", got)
	}
	if got := row[colIndex("last_status")].(string); got != "late" {
		t.Errorf("last_status: got %q, want late", got)
	}
	if got := row[colIndex("first_ts")].(float64); got != float64(base.Unix()) {
		t.Errorf("first_ts: got %v, want %v", got, base.Unix())
	}
	if got := row[colIndex("last_ts")].(float64); got != float64(base.Add(10*time.Second).Unix()) {
		t.Errorf("last_ts: got %v, want %v", got, base.Add(10*time.Second).Unix())
	}
	if got := row[colIndex("counter_rate")].(float64); got != 1.5 {
		t.Errorf("counter_rate: got %v, want 1.5", got)
	}
}

func TestQuery_CancelJob(t *testing.T) {
	// Post-RFC-002: LynxFlow queries execute synchronously (no job_id).
	// Verify that canceling a non-existent job returns 404.
	srv, cleanup := startTestServer(t)
	defer cleanup()

	req, _ := http.NewRequest("DELETE", fmt.Sprintf("http://%s/api/v1/query/jobs/nonexistent", srv.Addr()), http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", resp.StatusCode)
	}
}

func TestQuery_ListJobs(t *testing.T) {
	// Post-RFC-002: LynxFlow queries execute synchronously — the job list
	// endpoint still exists but will be empty since no async jobs are created.
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query/jobs", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
}

func TestQuery_CancelJob_AlreadyDone(t *testing.T) {
	// Post-RFC-002: LynxFlow queries execute synchronously — no async job_id.
	// Verify that DELETE on a non-existent job returns 404.
	srv, cleanup := startTestServer(t)
	defer cleanup()

	req, _ := http.NewRequest("DELETE", fmt.Sprintf("http://%s/api/v1/query/jobs/nonexistent", srv.Addr()), http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", resp.StatusCode)
	}
}

func TestQuery_ListJobs_FilterByStatus(t *testing.T) {
	// Post-RFC-002: LynxFlow executes synchronously. The filter endpoint
	// still works but returns an empty list since no async jobs are created.
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query/jobs?status=complete", srv.Addr()))
	if err != nil {
		t.Fatalf("GET jobs: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
}

func TestQuery_ListJobs_FilterByStatus_Invalid(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query/jobs?status=bogus", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}
}

func TestQuery_FieldAliases(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 10, 2)

	// Test "query" alias for "q", "earliest"/"latest" alias for "from"/"to".
	body, _ := json.Marshal(map[string]interface{}{
		"query": `FROM main | head 3`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})
	events := data["events"].([]interface{})
	if len(events) != 3 {
		t.Errorf("events: got %d, want 3", len(events))
	}
}

func TestQuery_AsyncPollEndpoint(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 20, 2)

	// Submit async query via POST /api/v1/query with wait=0.
	wait := float64(0)
	searchBody, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | head 5`, "wait": wait,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(searchBody))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	var jobResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&jobResp)
	resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status: got %d, want 202", resp.StatusCode)
	}

	data := jobResp["data"].(map[string]interface{})
	jobID, ok := data["job_id"].(string)
	if !ok || jobID == "" {
		t.Fatalf("missing job_id, got: %v", data["job_id"])
	}

	// Poll using GET /api/v1/query/jobs/{id}.
	for i := 0; i < 50; i++ {
		time.Sleep(50 * time.Millisecond)
		resp, err = http.Get(fmt.Sprintf("http://%s/api/v1/query/jobs/%s", srv.Addr(), jobID))
		if err != nil {
			t.Fatalf("GET job: %v", err)
		}
		var jr map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&jr)
		resp.Body.Close()

		d := jr["data"].(map[string]interface{})
		dtype, _ := d["type"].(string)
		dstatus, _ := d["status"].(string)

		// Done jobs are wrapped in a job envelope: unwrap results.
		resultData := d
		if dtype == "job" && dstatus == "done" {
			if results, ok := d["results"].(map[string]interface{}); ok {
				resultData = results
				dtype, _ = resultData["type"].(string)
			}
		}

		if dtype == "events" {
			events := resultData["events"].([]interface{})
			if len(events) != 5 {
				t.Errorf("events: got %d, want 5", len(events))
			}

			return
		}
		if dstatus == "error" {
			t.Fatalf("job error: %v", d["error"])
		}
	}
	t.Fatal("timeout waiting for async job")
}

func TestQuery_ParseError(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"q": `INVALID QUERY @@@ !!!`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 400, body: %s", resp.StatusCode, string(b))
	}
}

func TestQuery_UnsupportedTimeFormat(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	// LynxFlow does not support SPL2 timeformat/starttime syntax.
	// This query should produce a parse error.
	body, _ := json.Marshal(map[string]interface{}{
		"q": `index=main timeformat="%b %d %Y" starttime="Mar 23 2025"`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 400, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	errObj := result["error"].(map[string]interface{})
	code, _ := errObj["code"].(string)
	if code != string(ErrCodeInvalidQuery) {
		t.Fatalf("code: got %v, want %s", code, ErrCodeInvalidQuery)
	}
}

func TestQuery_MissingQuery(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}
}

func TestQuery_LintOutputControls(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	query := `FROM main | where has(_raw, "error") | sort -status, duration_ms`
	post := func(body map[string]interface{}) map[string]interface{} {
		t.Helper()
		raw, _ := json.Marshal(body)
		resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("status: got %d, want 200, body: %s", resp.StatusCode, string(b))
		}

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		meta, _ := result["meta"].(map[string]interface{})
		if meta == nil {
			t.Fatal("missing meta")
		}

		return meta
	}

	defaultMeta := post(map[string]interface{}{"q": query})
	defaultLints, _ := defaultMeta["lints"].([]interface{})
	if len(defaultLints) == 0 {
		t.Fatalf("default meta.lints: got 0, want at least 1 (%#v)", defaultMeta["lints"])
	}
	firstLint, _ := defaultLints[0].(map[string]interface{})
	// LynxFlow lints use "message" (may also have "reason" but not guaranteed).
	msg, _ := firstLint["message"].(string)
	reason, _ := firstLint["reason"].(string)
	if msg == "" && reason == "" {
		t.Fatalf("default meta.lints[0] missing message and reason: %#v", firstLint)
	}
	if severity, _ := firstLint["severity"].(string); severity == "" {
		t.Fatalf("default meta.lints[0] missing severity: %#v", firstLint)
	}

	// The LynxFlow path produces fewer broad-scope lints than SPL2 did.
	// With lint_limit: the result should be capped to at most that many.
	limitedMeta := post(map[string]interface{}{"q": query, "lint_limit": 2})
	limitedLints, _ := limitedMeta["lints"].([]interface{})
	if len(limitedLints) > 2 {
		t.Fatalf("limited meta.lints: got %d, want at most 2", len(limitedLints))
	}

	// lint_full=true should return at least as many as the default.
	fullMeta := post(map[string]interface{}{"q": query, "lint_full": true})
	fullLints, _ := fullMeta["lints"].([]interface{})
	if len(fullLints) < len(defaultLints) {
		t.Fatalf("full meta.lints: got %d, want at least %d", len(fullLints), len(defaultLints))
	}
}

func TestQuery_SuggestionsMetadata(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	post := func(body map[string]interface{}) map[string]interface{} {
		t.Helper()
		if _, ok := body["q"]; !ok {
			// LynxFlow shape (a): stats count() by service | sort -count | head 5
			// This is the long form of `top 5 service`.
			body["q"] = `from main | where level == "error" | stats count() by service | sort -count | head 5`
		}
		raw, _ := json.Marshal(body)
		resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("status: got %d, want 200, body: %s", resp.StatusCode, string(b))
		}

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		meta, _ := result["meta"].(map[string]interface{})
		if meta == nil {
			t.Fatal("missing meta")
		}
		return meta
	}

	meta := post(map[string]interface{}{})
	suggestions, _ := meta["suggestions"].([]interface{})
	if len(suggestions) != 1 {
		t.Fatalf("meta.suggestions: got %#v, want one suggestion", meta["suggestions"])
	}
	first, _ := suggestions[0].(map[string]interface{})
	if first["text"] != "top 5 service" {
		t.Fatalf("suggestion text: got %v, want top 5 service", first["text"])
	}
	if first["reason"] != "shortcut" || first["source_code"] != "LF09" {
		t.Fatalf("suggestion metadata: got %#v", first)
	}

	noLintMeta := post(map[string]interface{}{"lint": false})
	if _, ok := noLintMeta["lints"]; ok {
		t.Fatalf("meta.lints present despite lint=false: %#v", noLintMeta["lints"])
	}
	if suggestions, _ := noLintMeta["suggestions"].([]interface{}); len(suggestions) != 1 {
		t.Fatalf("meta.suggestions with lint=false: got %#v, want one suggestion", noLintMeta["suggestions"])
	}

	noSuggestionMeta := post(map[string]interface{}{"suggestions": false})
	if _, ok := noSuggestionMeta["suggestions"]; ok {
		t.Fatalf("meta.suggestions present despite suggestions=false: %#v", noSuggestionMeta["suggestions"])
	}
	if lints, _ := noSuggestionMeta["lints"].([]interface{}); len(lints) == 0 {
		t.Fatalf("meta.lints missing when suggestions=false: %#v", noSuggestionMeta["lints"])
	}
}

func TestQuery_ExplainMetadata(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 20, 2)

	body, _ := json.Marshal(map[string]interface{}{
		"q": `from main | where matches(_raw, r"request") | head 3`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	meta, _ := result["meta"].(map[string]interface{})
	explain, _ := meta["explain"].(map[string]interface{})
	if explain == nil {
		t.Fatalf("missing meta.explain: %#v", meta)
	}
	sourceScope, _ := explain["source_scope"].(map[string]interface{})
	if sourceScope == nil || int(sourceScope["count"].(float64)) == 0 {
		t.Fatalf("meta.explain.source_scope: %#v", explain["source_scope"])
	}
	segments, _ := explain["segments"].(map[string]interface{})
	if segments == nil || int(segments["total"].(float64)) == 0 {
		t.Fatalf("meta.explain.segments: %#v", explain["segments"])
	}
	if rows, ok := explain["candidate_rows"].(float64); !ok || rows == 0 {
		t.Fatalf("meta.explain.candidate_rows: %#v", explain["candidate_rows"])
	}
	if _, ok := explain["literal_extraction"].(bool); !ok {
		t.Fatalf("meta.explain.literal_extraction missing: %#v", explain)
	}
	if explain["regex_engine"] != "linear" {
		t.Fatalf("meta.explain.regex_engine: got %#v, want linear", explain["regex_engine"])
	}
}

func TestQuery_BroadScopeLints(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestIndexedTestEvents(t, srv.Addr(), config.DefaultConfig().Query.BroadSourceLintThreshold+1)

	post := func(body map[string]interface{}) map[string]interface{} {
		t.Helper()
		raw, _ := json.Marshal(body)
		resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("status: got %d, body: %s", resp.StatusCode, string(b))
		}
		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		meta, _ := result["meta"].(map[string]interface{})
		if meta == nil {
			t.Fatal("missing meta")
		}

		return meta
	}

	// Post-RFC-002: all queries go through LynxFlow. Verify that LynxFlow
	// lints (e.g., LF03 for missing time bound) are reported.
	allSourceMeta := post(map[string]interface{}{"q": `FROM * | head 1`})
	if codes := metaLintCodes(allSourceMeta); len(codes) == 0 {
		t.Fatalf("expected some lints for FROM * query, got none")
	}

	noLintMeta := post(map[string]interface{}{"q": `FROM * | where has(_raw, "request") | head 1`, "lint": false})
	if _, ok := noLintMeta["lints"]; ok {
		t.Fatalf("meta.lints present despite lint=false: %#v", noLintMeta["lints"])
	}

	// Post-RFC-002: verify sync query completes successfully.
	raw, _ := json.Marshal(map[string]interface{}{
		"q": `FROM * | where has(_raw, "request 1") | head 1`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sync status: got %d, want 200", resp.StatusCode)
	}
}

func TestQuery_BroadScopeLintThresholdConfig(t *testing.T) {
	queryCfg := config.DefaultConfig().Query
	queryCfg.SpillDir = t.TempDir()
	queryCfg.BroadSourceLintThreshold = 3
	queryCfg.BroadSegmentLintThreshold = 1000
	srv, cleanup := startTestServerWithConfig(t, Config{Query: queryCfg})
	defer cleanup()

	ingestIndexedTestEvents(t, srv.Addr(), 3)

	// Post-RFC-002: use LynxFlow. Verify lints are generated for FROM *.
	raw, _ := json.Marshal(map[string]interface{}{"q": `FROM * | head 1`})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200, body: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	meta, _ := result["meta"].(map[string]interface{})
	codes := metaLintCodes(meta)
	if len(codes) == 0 {
		t.Fatalf("expected lints for FROM * query, got none")
	}
}

func TestErrorFormat(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	t.Run("ParseError", func(t *testing.T) {
		body, _ := json.Marshal(map[string]interface{}{
			"q": `INVALID QUERY @@@ !!!`,
		})
		resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status: got %d, want 400", resp.StatusCode)
		}

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)

		errObj, ok := result["error"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected structured error, got: %v", result["error"])
		}
		if errObj["code"] != "INVALID_QUERY" {
			t.Errorf("code: got %v, want INVALID_QUERY", errObj["code"])
		}
		if errObj["message"] == nil || errObj["message"] == "" {
			t.Error("expected non-empty message")
		}
	})

	t.Run("ValidationError", func(t *testing.T) {
		body, _ := json.Marshal(map[string]interface{}{})
		resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status: got %d, want 400", resp.StatusCode)
		}

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)

		errObj, ok := result["error"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected structured error, got: %v", result["error"])
		}
		if errObj["code"] != "VALIDATION_ERROR" {
			t.Errorf("code: got %v, want VALIDATION_ERROR", errObj["code"])
		}
		if errObj["message"] != "query is required" {
			t.Errorf("message: got %v, want 'query is required'", errObj["message"])
		}
	})
}

func TestQueryGet_Basic(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 20, 2)

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query?q=%s&limit=5",
		srv.Addr(), "FROM+main+|+head+5"))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})
	if data["type"] != "events" {
		t.Errorf("data.type: got %v, want events", data["type"])
	}
	events := data["events"].([]interface{})
	if len(events) != 5 {
		t.Errorf("events: got %d, want 5", len(events))
	}
}

func TestQueryGet_MissingQ(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}
}

func TestQuery_FormatValidation(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	t.Run("POST rejects unsupported format", func(t *testing.T) {
		body := strings.NewReader(`{"q":"FROM main","format":"csv"}`)
		resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", body)
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status: got %d, want 400", resp.StatusCode)
		}
	})

	t.Run("GET rejects unsupported format", func(t *testing.T) {
		resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query?q=%s&format=csv", srv.Addr(), "FROM+main"))
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status: got %d, want 400", resp.StatusCode)
		}
	})
}

func TestIngestBulk_Route(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	// POST to /ingest/bulk with ES bulk format.
	body := `{"index":{"_index":"test"}}
{"message":"hello","level":"info"}
{"index":{"_index":"test"}}
{"message":"world","level":"error"}
`
	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/ingest/bulk", srv.Addr()),
		"application/x-ndjson",
		bytes.NewBufferString(body),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["errors"] != false {
		t.Errorf("errors: got %v, want false", result["errors"])
	}
	items := result["items"].([]interface{})
	if len(items) != 2 {
		t.Errorf("items: got %d, want 2", len(items))
	}
}

func TestJobStream_Basic(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 10, 2)

	// Submit an async job.
	zero := float64(0)
	body, _ := json.Marshal(map[string]interface{}{
		"q":    `FROM main | head 5`,
		"wait": zero,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	defer resp.Body.Close()

	var jobResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&jobResp)
	data := jobResp["data"].(map[string]interface{})
	jobID := data["job_id"].(string)

	// Wait a moment for the job to finish.
	time.Sleep(200 * time.Millisecond)

	// Stream results via SSE.
	sseResp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query/jobs/%s/stream", srv.Addr(), jobID))
	if err != nil {
		t.Fatalf("GET stream: %v", err)
	}
	defer sseResp.Body.Close()

	if sseResp.StatusCode != 200 {
		b, _ := io.ReadAll(sseResp.Body)
		t.Fatalf("SSE status: %d, body: %s", sseResp.StatusCode, string(b))
	}
	if ct := sseResp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type: got %s, want text/event-stream", ct)
	}

	// Read SSE events - should contain at least a "complete" event.
	sseBody, _ := io.ReadAll(sseResp.Body)
	sseStr := string(sseBody)
	if !bytes.Contains([]byte(sseStr), []byte("event: complete")) {
		t.Errorf("SSE body missing 'event: complete': %s", sseStr)
	}
}

func TestJobStream_NotFound(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query/jobs/nonexistent/stream", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", resp.StatusCode)
	}
}

// Test: parse error columns round-trip through the REST JSON response.
//
// Verifies finding [22b]: _error and _error_detail columns produced by
// `parse json` with malformed rows serialize correctly through the REST
// query response envelope.

func TestServer_ParseErrorColumns_RoundTrip(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	// Ingest a mix of valid and invalid JSON as _raw.
	now := time.Now()
	rawLines := []string{
		`{"name":"alice","age":30}`,
		`this is not valid json`,
		`{"name":"bob","age":25}`,
	}
	events := make([]*event.Event, len(rawLines))
	for i, line := range rawLines {
		ev := event.NewEvent(now.Add(time.Duration(i)*time.Second), line)
		ev.Index = "main"
		ev.Host = "web-01"
		ev.Source = "test"
		ev.SourceType = "raw"
		ev.Fields = make(map[string]event.Value)
		events[i] = ev
	}
	if err := srv.engine.Ingest(events); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Query with parse json (default on_error=propagate).
	body, _ := json.Marshal(map[string]interface{}{
		"q": `from main | parse json | sort _time`,
	})
	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/query", srv.Addr()),
		"application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	data, ok := result["data"].(map[string]interface{})
	if !ok {
		t.Fatal("missing 'data' in response")
	}

	eventsRaw, ok := data["events"].([]interface{})
	if !ok {
		t.Fatal("missing 'events' in data")
	}

	if len(eventsRaw) != 3 {
		t.Fatalf("expected 3 events, got %d", len(eventsRaw))
	}

	// Find the error row (the invalid JSON one). Columnar batches pad all
	// columns to batch length, so even valid rows carry _error as JSON null.
	// Distinguish by checking whether the value is a non-nil string.
	var errorRow map[string]interface{}
	var validRows []map[string]interface{}
	for _, raw := range eventsRaw {
		ev, _ := raw.(map[string]interface{})
		if ev == nil {
			continue
		}
		if errVal, ok := ev["_error"].(string); ok && errVal != "" {
			errorRow = ev
		} else {
			validRows = append(validRows, ev)
		}
	}

	// Verify the error row has _error and _error_detail.
	if errorRow == nil {
		t.Fatal("expected at least one event with _error column in response")
	}

	errStr, ok := errorRow["_error"].(string)
	if !ok || errStr == "" {
		t.Fatalf("_error should be a non-empty string, got %T: %v", errorRow["_error"], errorRow["_error"])
	}
	if !strings.Contains(errStr, "parse:json:") {
		t.Errorf("_error should contain 'parse:json:', got %q", errStr)
	}

	// _error_detail should be a JSON object with stage, format, code, message.
	detail, ok := errorRow["_error_detail"].(map[string]interface{})
	if !ok {
		t.Fatalf("_error_detail should be a JSON object, got %T: %v", errorRow["_error_detail"], errorRow["_error_detail"])
	}
	for _, key := range []string{"stage", "format", "code", "message"} {
		if _, exists := detail[key]; !exists {
			t.Errorf("_error_detail missing key %q, got: %v", key, detail)
		}
	}
	if fmt.Sprint(detail["format"]) != "json" {
		t.Errorf("_error_detail.format: got %v, want %q", detail["format"], "json")
	}

	// Verify valid rows do NOT have a non-null _error.
	if len(validRows) < 2 {
		t.Fatalf("expected at least 2 valid rows, got %d", len(validRows))
	}
	for i, vr := range validRows {
		if errVal, ok := vr["_error"].(string); ok && errVal != "" {
			t.Errorf("valid row %d should not have non-null _error, got: %v", i, errVal)
		}
	}
}
