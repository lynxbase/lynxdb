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
	"github.com/lynxbase/lynxdb/pkg/spl2"
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
		Views:         cfg.Views,
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

func TestServer_UIIsMountedUnderUIAndRootStaysESCompat(t *testing.T) {
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

	noFollow := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	uiResp, err := noFollow.Get(fmt.Sprintf("http://%s/ui", srv.Addr()))
	if err != nil {
		t.Fatalf("GET /ui: %v", err)
	}
	defer uiResp.Body.Close()
	if uiResp.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("GET /ui status: %d", uiResp.StatusCode)
	}
	if got := uiResp.Header.Get("Location"); got != "/ui/" {
		t.Fatalf("GET /ui Location = %q, want /ui/", got)
	}

	indexResp, err := http.Get(fmt.Sprintf("http://%s/ui/", srv.Addr()))
	if err != nil {
		t.Fatalf("GET /ui/: %v", err)
	}
	defer indexResp.Body.Close()
	if indexResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /ui/ status: %d", indexResp.StatusCode)
	}
	body, err := io.ReadAll(indexResp.Body)
	if err != nil {
		t.Fatalf("read /ui/: %v", err)
	}
	if !strings.Contains(string(body), `<div id="app"></div>`) {
		t.Fatalf("GET /ui/ did not return SPA index.html")
	}

	iconResp, err := http.Get(fmt.Sprintf("http://%s/ui/lynxdb-icon.png", srv.Addr()))
	if err != nil {
		t.Fatalf("GET /ui/lynxdb-icon.png: %v", err)
	}
	defer iconResp.Body.Close()
	if iconResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /ui/lynxdb-icon.png status: %d", iconResp.StatusCode)
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
		"q": `FROM main | search "request" | head 10`,
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
	if _, ok := meta["query_id"]; !ok {
		t.Error("missing query_id in meta")
	}

	rewriteQuery := "request"
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

	var rewriteResult map[string]interface{}
	json.NewDecoder(rewriteResp.Body).Decode(&rewriteResult)
	rewriteMeta, _ := rewriteResult["meta"].(map[string]interface{})
	if rewriteMeta == nil {
		t.Fatal("missing meta in rewrite query response")
	}
	rewrites, _ := rewriteMeta["rewrites"].([]interface{})
	if len(rewrites) != 1 {
		t.Fatalf("meta.rewrites: got %#v, want one rewrite", rewriteMeta["rewrites"])
	}
	firstRewrite, _ := rewrites[0].(map[string]interface{})
	if firstRewrite["before"] != rewriteQuery {
		t.Fatalf("meta.rewrites[0].before: got %v, want %s", firstRewrite["before"], rewriteQuery)
	}
	if firstRewrite["after"] != spl2.NormalizeQuery(rewriteQuery) {
		t.Fatalf("meta.rewrites[0].after: got %v, want %s", firstRewrite["after"], spl2.NormalizeQuery(rewriteQuery))
	}
	if firstRewrite["reason"] != "freehand-search" {
		t.Fatalf("meta.rewrites[0].reason: got %v, want freehand-search", firstRewrite["reason"])
	}

	lintBody, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | stats count`,
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
	if len(lints) != 1 {
		t.Fatalf("meta.lints: got %#v, want one lint", lintMeta["lints"])
	}
	firstLint, _ := lints[0].(map[string]interface{})
	if firstLint["code"] != spl2.LintCountWithoutParens {
		t.Fatalf("meta.lints[0].code: got %v, want %s", firstLint["code"], spl2.LintCountWithoutParens)
	}

	disabled := false
	noLintBody, _ := json.Marshal(map[string]interface{}{
		"q":    `FROM main | stats count`,
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
		"q": `FROM main | stats mean(duration_ms) as avg_dur, median(duration_ms) as p50_dur, distinct_count(user) as users, estdc(user) as estimated_users, estdc_error(user) as estimated_user_error, mode(user) as common_user`,
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
	if got := row[colIndex("estimated_user_error")].(float64); got != 0 {
		t.Errorf("estimated_user_error: got %v, want 0", got)
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
		"q": `FROM main | stats percentile95(duration_ms) as p95, exactperc95(duration_ms) as exact_p95, upperperc95(duration_ms) as upper_p95, percentile(duration_ms, 95) as generic_p95`,
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
	for _, name := range []string{"p95", "exact_p95", "upper_p95", "generic_p95"} {
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
	if _, ok := meta["took_ms"]; !ok {
		t.Error("missing took_ms")
	}
}

func TestQuery_AsyncMode(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 20, 2)

	wait := float64(0)
	query := `stats count`
	body, _ := json.Marshal(map[string]interface{}{
		"q": query, "wait": wait,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 202, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})

	jobID, _ := data["job_id"].(string)
	if jobID == "" {
		t.Fatal("missing job_id")
	}
	if data["status"] != "running" {
		t.Errorf("status: %v", data["status"])
	}
	meta := result["meta"].(map[string]interface{})
	assertLintMeta(t, meta, spl2.LintCountWithoutParens)
	assertRewriteMeta(t, meta, spl2.NormalizeQuery(query), "default-source")

	// Poll until done.
	for i := 0; i < 50; i++ {
		time.Sleep(50 * time.Millisecond)
		resp2, err := http.Get(fmt.Sprintf("http://%s/api/v1/query/jobs/%s", srv.Addr(), jobID))
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		var jr map[string]interface{}
		json.NewDecoder(resp2.Body).Decode(&jr)
		resp2.Body.Close()

		d := jr["data"].(map[string]interface{})
		dtype, _ := d["type"].(string)
		dstatus, _ := d["status"].(string)
		if dstatus == "done" || (dtype != "" && dtype != "job") {
			meta := jr["meta"].(map[string]interface{})
			assertLintMeta(t, meta, spl2.LintCountWithoutParens)
			assertRewriteMeta(t, meta, spl2.NormalizeQuery(query), "default-source")

			return
		}
		if dstatus == "error" {
			t.Fatalf("job error: %v", d["error"])
		}
	}
	t.Fatal("timeout waiting for async job")
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
		"q": `FROM main | timechart count() by host`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})

	if data["type"] != "timechart" {
		t.Errorf("type: got %v, want timechart", data["type"])
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
		"q": `FROM main | timechart span=1h per_minute(bytes) as bytes_per_minute`,
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
		if fmt.Sprint(col) == "bytes_per_minute" {
			colIndex = i
			break
		}
	}
	if colIndex < 0 {
		t.Fatalf("missing bytes_per_minute column in %v", cols)
	}
	got := rows[0].([]interface{})[colIndex].(float64)
	if got != 3 {
		t.Errorf("bytes_per_minute: got %v, want 3", got)
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
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 10, 2)

	// Submit async job.
	wait := float64(0)
	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | head 5`, "wait": wait,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()

	data := result["data"].(map[string]interface{})
	jobID := data["job_id"].(string)

	// Cancel.
	req, _ := http.NewRequest("DELETE", fmt.Sprintf("http://%s/api/v1/query/jobs/%s", srv.Addr(), jobID), http.NoBody)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp2.Body.Close()

	var cancelResult map[string]interface{}
	json.NewDecoder(resp2.Body).Decode(&cancelResult)
	d := cancelResult["data"].(map[string]interface{})
	status, _ := d["status"].(string)
	canceled, ok := d["canceled"].(bool)
	if !ok {
		t.Fatalf("missing canceled flag: %v", d)
	}
	switch status {
	case "canceled":
		if !canceled {
			t.Errorf("canceled flag = false for canceled job: %v", d)
		}
	case "done":
		if canceled {
			t.Errorf("canceled flag = true for completed job: %v", d)
		}
	default:
		t.Errorf("unexpected cancel status: %v", d["status"])
	}
}

func TestQuery_ListJobs(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 10, 2)

	// Submit two async jobs.
	wait := float64(0)
	for i := 0; i < 2; i++ {
		body, _ := json.Marshal(map[string]interface{}{
			"q": fmt.Sprintf("FROM main | head %d", i+1), "wait": wait,
		})
		resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		resp.Body.Close()
	}

	time.Sleep(200 * time.Millisecond)

	// List jobs.
	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query/jobs", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})
	jobs := data["jobs"].([]interface{})

	if len(jobs) < 2 {
		t.Errorf("jobs: got %d, want >= 2", len(jobs))
	}
}

func TestQuery_CancelJob_AlreadyDone(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 10, 2)

	wait := float64(0)
	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | head 1`, "wait": wait,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()

	data := result["data"].(map[string]interface{})
	jobID := data["job_id"].(string)

	for i := 0; i < 50; i++ {
		resp, err = http.Get(fmt.Sprintf("http://%s/api/v1/query/jobs/%s", srv.Addr(), jobID))
		if err != nil {
			t.Fatalf("GET job: %v", err)
		}
		var jr map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&jr)
		resp.Body.Close()

		d := jr["data"].(map[string]interface{})
		if d["status"] == "done" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	req, _ := http.NewRequest("DELETE", fmt.Sprintf("http://%s/api/v1/query/jobs/%s", srv.Addr(), jobID), http.NoBody)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()

	var cancelResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&cancelResult)
	d := cancelResult["data"].(map[string]interface{})
	if d["status"] != "done" {
		t.Fatalf("status: got %v, want done", d["status"])
	}
	if d["canceled"] != false {
		t.Fatalf("canceled: got %v, want false", d["canceled"])
	}
}

func TestQuery_ListJobs_FilterByStatus(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 10, 2)

	wait := float64(0)
	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | head 1`, "wait": wait,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	var submitted map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&submitted)
	resp.Body.Close()

	jobID := submitted["data"].(map[string]interface{})["job_id"].(string)

	for i := 0; i < 50; i++ {
		resp, err = http.Get(fmt.Sprintf("http://%s/api/v1/query/jobs/%s", srv.Addr(), jobID))
		if err != nil {
			t.Fatalf("GET job: %v", err)
		}
		var jr map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&jr)
		resp.Body.Close()

		if jr["data"].(map[string]interface{})["status"] == "done" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	resp, err = http.Get(fmt.Sprintf("http://%s/api/v1/query/jobs?status=complete", srv.Addr()))
	if err != nil {
		t.Fatalf("GET jobs: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	jobs := result["data"].(map[string]interface{})["jobs"].([]interface{})
	if len(jobs) == 0 {
		t.Fatal("expected at least one completed job")
	}
	found := false
	for _, job := range jobs {
		entry := job.(map[string]interface{})
		if entry["status"] != "done" {
			t.Fatalf("status: got %v, want done", entry["status"])
		}
		if entry["job_id"] == jobID {
			found = true
		}
	}
	if !found {
		t.Fatalf("job %s not found in filtered results", jobID)
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
	if errObj["code"] != string(ErrCodeUnsupportedCommand) {
		t.Fatalf("code: got %v, want %s", errObj["code"], ErrCodeUnsupportedCommand)
	}
	if suggestion, _ := errObj["suggestion"].(string); !strings.Contains(suggestion, "%Y-%m-%d") {
		t.Fatalf("suggestion missing supported format example: %q", suggestion)
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

	query := `FROM main | search NOT NOT NOT NOT NOT NOT *error OR timeout AND fatal | where _raw = "panic" | fields order | sort status asc, duration_ms desc`
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
	if len(defaultLints) != 5 {
		t.Fatalf("default meta.lints: got %d, want 5 (%#v)", len(defaultLints), defaultMeta["lints"])
	}

	limitedMeta := post(map[string]interface{}{"q": query, "lint_limit": 2})
	limitedLints, _ := limitedMeta["lints"].([]interface{})
	if len(limitedLints) != 2 {
		t.Fatalf("limited meta.lints: got %d, want 2 (%#v)", len(limitedLints), limitedMeta["lints"])
	}

	fullMeta := post(map[string]interface{}{"q": query, "lint_full": true})
	fullLints, _ := fullMeta["lints"].([]interface{})
	if len(fullLints) <= len(defaultLints) {
		t.Fatalf("full meta.lints: got %d, want more than default %d", len(fullLints), len(defaultLints))
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
