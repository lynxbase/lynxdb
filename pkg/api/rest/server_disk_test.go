package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/config"
	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

func startDiskTestServer(t *testing.T) (*Server, string, func()) {
	t.Helper()

	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv, err := NewServer(Config{
		Addr:    "127.0.0.1:0",
		DataDir: dir,
		Storage: config.DefaultConfig().Storage,
		Logger:  logger,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go srv.Start(ctx)
	srv.WaitReady()

	return srv, dir, func() {
		cancel()
		time.Sleep(100 * time.Millisecond)
	}
}

func TestServer_DiskFlushAndRecover(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// Start server, insert events, flush, stop.
	srv1, err := NewServer(Config{
		Addr:    "127.0.0.1:0",
		DataDir: dir,
		Storage: config.DefaultConfig().Storage,
		Logger:  logger,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	go srv1.Start(ctx1)
	srv1.WaitReady()

	// Insert events via Ingest (batcher path).
	base := time.Now()
	events := make([]*event.Event, 100)
	for i := 0; i < 100; i++ {
		events[i] = &event.Event{
			Time:       base.Add(time.Duration(i) * time.Millisecond),
			Raw:        fmt.Sprintf("event %d host=web-%02d level=INFO", i, i%5),
			Host:       fmt.Sprintf("web-%02d", i%5),
			Index:      "main",
			Source:     "test",
			SourceType: "raw",
			Fields:     make(map[string]event.Value),
		}
	}
	if err := srv1.engine.Ingest(events); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Flush to disk.
	if err := srv1.engine.FlushBatcher(); err != nil {
		t.Fatalf("FlushBatcher: %v", err)
	}

	// Verify part file exists (parts go to segments/hot/<index>/<date>/*.lsg).
	segFiles, _ := filepath.Glob(filepath.Join(dir, "segments", "hot", "*", "*", "*.lsg"))
	if len(segFiles) == 0 {
		t.Fatal("expected part files on disk")
	}
	t.Logf("part files: %v", segFiles)

	// Verify batcher is empty after flush.
	if srv1.engine.BufferedEventCount() != 0 {
		t.Errorf("batcher should be empty after flush, got %d", srv1.engine.BufferedEventCount())
	}

	// Verify segment count.
	if srv1.engine.SegmentCount() != 1 {
		t.Errorf("expected 1 segment, got %d", srv1.engine.SegmentCount())
	}

	// Stop server.
	cancel1()
	time.Sleep(200 * time.Millisecond)

	// Start new server on same data dir to test recovery.
	srv2, err := NewServer(Config{
		Addr:    "127.0.0.1:0",
		DataDir: dir,
		Storage: config.DefaultConfig().Storage,
		Logger:  logger,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go srv2.Start(ctx2)
	srv2.WaitReady()

	// Verify recovered segments.
	if srv2.engine.SegmentCount() != 1 {
		t.Errorf("after recovery: expected 1 segment, got %d", srv2.engine.SegmentCount())
	}

	// Query the recovered data.
	resultCount := searchAndCount(t, srv2, `FROM main | head 10`)
	if resultCount != 10 {
		t.Errorf("result_count: got %d, want 10", resultCount)
	}
}

func TestServer_DiskMode_IngestAndQuery(t *testing.T) {
	srv, _, cleanup := startDiskTestServer(t)
	defer cleanup()

	// Ingest events via API.
	now := float64(time.Now().Unix())
	events := make([]map[string]interface{}, 30)
	for i := 0; i < 30; i++ {
		events[i] = map[string]interface{}{
			"time":       now + float64(i),
			"event":      fmt.Sprintf("host=web-%02d level=INFO msg=\"request %d\"", i%3, i),
			"host":       fmt.Sprintf("web-%02d", i%3),
			"source":     "/var/log/app.log",
			"sourcetype": "json",
			"index":      "main",
		}
	}

	body, _ := json.Marshal(events)
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/ingest", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST events: %v", err)
	}
	resp.Body.Close()

	// Flush to disk.
	if err := srv.engine.FlushBatcher(); err != nil {
		t.Fatalf("FlushBatcher: %v", err)
	}

	// Query the data.
	resultCount := searchAndCount(t, srv, `FROM main | stats count() by host`)
	if resultCount != 3 {
		t.Errorf("result_count: got %d, want 3 (one per host)", resultCount)
	}
}

func TestServer_BloomFilterSkip(t *testing.T) {
	srv, _, cleanup := startDiskTestServer(t)
	defer cleanup()

	// Insert events with specific terms via Ingest (batcher path).
	base := time.Now()
	bloomEvents := make([]*event.Event, 50)
	for i := 0; i < 50; i++ {
		bloomEvents[i] = &event.Event{
			Time:       base.Add(time.Duration(i) * time.Millisecond),
			Raw:        fmt.Sprintf("event %d host=web-01 level=INFO status=200 msg=\"hello world\"", i),
			Host:       "web-01",
			Index:      "main",
			Source:     "test",
			SourceType: "raw",
			Fields:     make(map[string]event.Value),
		}
	}
	if err := srv.engine.Ingest(bloomEvents); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Flush to create a segment with bloom filter.
	if err := srv.engine.FlushBatcher(); err != nil {
		t.Fatalf("FlushBatcher: %v", err)
	}

	// Verify bloom filter is cached on the segment handle.
	if !srv.engine.HasBloomFilter() {
		t.Fatal("expected at least one segment with cached bloom filter")
	}

	// Search for a term that exists.
	resultCount := searchAndCount(t, srv, `FROM main | search "hello"`)
	if resultCount != 50 {
		t.Errorf("search 'hello': got %d results, want 50", resultCount)
	}
}

func TestServer_DiskMode_ClusterStatus(t *testing.T) {
	srv, dir, cleanup := startDiskTestServer(t)
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
	if data["data_dir"] != dir {
		t.Errorf("data_dir: got %v, want %v", data["data_dir"], dir)
	}
}

func TestServer_BatcherPersistence(t *testing.T) {
	dir, err := os.MkdirTemp("", "lynxdb-test-batcher-*")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	defer os.RemoveAll(dir)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// Start server 1 and ingest events (batcher buffers them).
	srv1, err := NewServer(Config{
		Addr:    "127.0.0.1:0",
		DataDir: dir,
		Storage: config.DefaultConfig().Storage,
		Logger:  logger,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	go srv1.Start(ctx1)
	srv1.WaitReady()

	// Ingest via API (events go to batcher).
	now := float64(time.Now().Unix())
	ingestEvents := make([]map[string]interface{}, 20)
	for i := 0; i < 20; i++ {
		ingestEvents[i] = map[string]interface{}{
			"time":       now + float64(i),
			"event":      fmt.Sprintf("batcher-test event %d host=web-01", i),
			"host":       "web-01",
			"source":     "test",
			"sourcetype": "raw",
			"index":      "main",
		}
	}

	body, _ := json.Marshal(ingestEvents)
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/ingest", srv1.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST events: %v", err)
	}
	resp.Body.Close()

	// Flush batcher to ensure events are persisted as parts.
	if err := srv1.engine.FlushBatcher(); err != nil {
		t.Fatalf("FlushBatcher: %v", err)
	}

	// Stop server.
	cancel1()
	time.Sleep(200 * time.Millisecond)

	// Start server 2 on same data dir and verify parts recovered.
	srv2, err := NewServer(Config{
		Addr:    "127.0.0.1:0",
		DataDir: dir,
		Storage: config.DefaultConfig().Storage,
		Logger:  logger,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	go srv2.Start(ctx2)
	srv2.WaitReady()

	// Events should be available in segments (recovered from parts on disk).
	var totalEvents int64
	for _, meta := range srv2.engine.Segments() {
		if meta.Index == "main" {
			totalEvents += meta.EventCount
		}
	}
	if totalEvents != 20 {
		t.Errorf("after restart: total events = %d, want 20", totalEvents)
	}

	cancel2()
	time.Sleep(200 * time.Millisecond)
}

func TestServer_FlushThenSearch(t *testing.T) {
	srv, _, cleanup := startDiskTestServer(t)
	defer cleanup()

	// Ingest events containing a unique search term via Ingest (batcher path).
	base := time.Now()
	const uniqueTerm = "tm2517090d1_tsrimg120"
	matchCount := 0
	flushEvents := make([]*event.Event, 200)
	for i := 0; i < 200; i++ {
		raw := fmt.Sprintf("event %d host=web-%02d level=INFO", i, i%5)
		if i%50 == 0 {
			raw = fmt.Sprintf("event %d path=/data/%s host=web-%02d level=ERROR", i, uniqueTerm, i%5)
			matchCount++
		}
		flushEvents[i] = &event.Event{
			Time:       base.Add(time.Duration(i) * time.Millisecond),
			Raw:        raw,
			Host:       fmt.Sprintf("web-%02d", i%5),
			Index:      "main",
			Source:     "test",
			SourceType: "raw",
			Fields:     make(map[string]event.Value),
		}
	}
	if err := srv.engine.Ingest(flushEvents); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Flush batcher to disk segments.
	if err := srv.engine.FlushBatcher(); err != nil {
		t.Fatalf("FlushBatcher: %v", err)
	}

	// Verify batcher is empty and segments exist.
	if srv.engine.BufferedEventCount() != 0 {
		t.Errorf("batcher should be empty after flush, got %d", srv.engine.BufferedEventCount())
	}
	if srv.engine.SegmentCount() == 0 {
		t.Fatal("expected at least 1 segment after flush")
	}

	// Search AFTER flush — results should come from segments.
	postFlushCount := searchAndCount(t, srv, fmt.Sprintf("FROM main | search %q", uniqueTerm))
	if postFlushCount != matchCount {
		t.Fatalf("post-flush: got %d results, want %d", postFlushCount, matchCount)
	}
	t.Logf("post-flush: %d results from %d segments", postFlushCount, srv.engine.SegmentCount())
}

// searchAndCount runs a search query via the async query API and returns the result count.
func searchAndCount(t *testing.T, srv *Server, query string) int {
	t.Helper()

	wait := float64(0)
	searchBody, _ := json.Marshal(map[string]interface{}{"q": query, "wait": wait})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(searchBody))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	var jobResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&jobResp)
	resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("query status: %d", resp.StatusCode)
	}

	data := jobResp["data"].(map[string]interface{})
	jobID := data["job_id"].(string)

	for i := 0; i < 100; i++ {
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

		if dstatus == "error" {
			t.Fatalf("job error: %v", d["error"])
		}

		// Done jobs are wrapped in a job envelope: {type: "job", status: "done", results: {...}}.
		// Unwrap the results for type inspection.
		resultData := d
		if dtype == "job" && dstatus == "done" {
			if results, ok := d["results"].(map[string]interface{}); ok {
				resultData = results
				dtype, _ = resultData["type"].(string)
			}
		}

		// Job completed — count results based on response type.
		if dtype == "events" {
			events := resultData["events"].([]interface{})

			return len(events)
		}
		if dtype == "aggregate" || dtype == "timechart" {
			rows := resultData["rows"].([]interface{})

			return len(rows)
		}
		if dstatus != "" && dstatus != "running" {
			t.Fatalf("unexpected job status: %v", dstatus)
		}
	}

	t.Fatal("timeout waiting for query job")

	return 0
}

func TestServer_MetricsPopulated(t *testing.T) {
	srv, _, cleanup := startDiskTestServer(t)
	defer cleanup()

	// Ingest events via API (batcher path).
	now := float64(time.Now().Unix())
	metricEvents := make([]map[string]interface{}, 10)
	for i := 0; i < 10; i++ {
		metricEvents[i] = map[string]interface{}{
			"time":       now + float64(i),
			"event":      fmt.Sprintf("host=web-01 level=INFO msg=\"request %d\"", i),
			"host":       "web-01",
			"source":     "test",
			"sourcetype": "raw",
			"index":      "main",
		}
	}

	body, _ := json.Marshal(metricEvents)
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/ingest", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST events: %v", err)
	}
	resp.Body.Close()

	// Flush batcher to disk to trigger flush metrics.
	if err := srv.engine.FlushBatcher(); err != nil {
		t.Fatalf("FlushBatcher: %v", err)
	}

	// Query to trigger segment read metrics.
	// Use stats count by host to avoid countStarOnly metadata-only optimization.
	_ = searchAndCount(t, srv, `FROM main | stats count by host`)

	// Get metrics.
	resp, err = http.Get(fmt.Sprintf("http://%s/api/v1/metrics", srv.Addr()))
	if err != nil {
		t.Fatalf("GET metrics: %v", err)
	}
	defer resp.Body.Close()

	var envelope map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}
	metrics := envelope["data"].(map[string]interface{})

	// Verify flush metrics are populated.
	flushMetrics := metrics["flush"].(map[string]interface{})
	if flushMetrics["flushes"].(float64) == 0 {
		t.Error("flush.flushes should be > 0 after flush")
	}

	// Verify segment metrics.
	segMetrics := metrics["segment"].(map[string]interface{})
	if segMetrics["count"].(float64) == 0 {
		t.Error("segment.count should be > 0 after flush")
	}
	if segMetrics["reads"].(float64) == 0 {
		t.Error("segment.reads should be > 0 after query")
	}

	// Verify uptime.
	if metrics["uptime_seconds"].(float64) <= 0 {
		t.Error("uptime_seconds should be > 0")
	}
}
