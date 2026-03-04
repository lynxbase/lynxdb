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
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func startTestServer(t *testing.T) (*Server, func()) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv, err := NewServer(Config{
		Addr:   "127.0.0.1:0",
		Logger: logger,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go srv.Start(ctx)
	srv.WaitReady()

	return srv, func() {
		cancel()
		time.Sleep(50 * time.Millisecond)
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

	// Build 5000 lines.
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
	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | head 5`, "wait": wait,
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
			// Completed
			if dtype == "events" {
				events := d["events"].([]interface{})
				if len(events) != 5 {
					t.Errorf("events: got %d, want 5", len(events))
				}
			}

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

	// Wait a bit for the job to potentially complete.
	time.Sleep(200 * time.Millisecond)

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
	if d["status"] != "canceled" {
		t.Errorf("cancel status: %v", d["status"])
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
