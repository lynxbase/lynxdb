package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/columnar"
	"github.com/lynxbase/lynxdb/pkg/event"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// serveTestServer starts srv and returns its stop, which waits until Start
// has closed the database; calls after the first do nothing.
func serveTestServer(t *testing.T, srv *Server) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start(ctx) }()
	select {
	case <-srv.ready:
	case err := <-errCh:
		cancel()
		t.Fatalf("server failed before ready: %v", err)
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("server did not become ready within 30s")
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			// Shutdown waits five seconds for a pooled connection that never sent a request.
			http.DefaultClient.CloseIdleConnections()
			cancel()
			select {
			case err := <-errCh:
				if err != nil {
					t.Errorf("Start: %v", err)
				}
			case <-time.After(30 * time.Second):
				t.Error("server did not stop within 30s")
			}
		})
	}
}

func startTestServer(t *testing.T) (*Server, func()) {
	t.Helper()

	return startTestServerWithConfig(t, Config{})
}

func startTestServerWithConfig(t *testing.T, cfg Config) (*Server, func()) {
	t.Helper()

	cfg.Addr = "127.0.0.1:0"
	if cfg.Logger == nil {
		cfg.Logger = testLogger()
	}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	return srv, serveTestServer(t, srv)
}

// ingestNDJSON posts NDJSON to /api/v1/ingest?index= and fails the test
// unless every line is accepted.
func ingestNDJSON(t *testing.T, addr, index, body string) {
	t.Helper()
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/ingest?index=%s", addr, url.QueryEscape(index)),
		"application/x-ndjson", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST ingest: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ingest status: %d, body: %s", resp.StatusCode, raw)
	}
	var result struct {
		Data struct {
			Accepted int `json:"accepted"`
			Failed   int `json:"failed"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("decode ingest response: %v (body: %s)", err, raw)
	}
	if want := strings.Count(strings.TrimSpace(body), "\n") + 1; result.Data.Accepted != want || result.Data.Failed != 0 {
		t.Fatalf("ingest: accepted %d failed %d, want %d and 0", result.Data.Accepted, result.Data.Failed, want)
	}
}

// ndjsonLines encodes each object as one line.
func ndjsonLines(t *testing.T, objects ...map[string]interface{}) string {
	t.Helper()
	var buf strings.Builder
	for _, o := range objects {
		line, err := json.Marshal(o)
		if err != nil {
			t.Fatalf("marshal event: %v", err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}

	return buf.String()
}

// ingestTestEvents ingests n events into main, one second apart and ending
// now, spread over hostCount hosts.
func ingestTestEvents(t *testing.T, addr string, n, hostCount int) {
	t.Helper()
	base := time.Now().Add(-time.Duration(n) * time.Second)
	events := make([]map[string]interface{}, n)
	for i := 0; i < n; i++ {
		host := fmt.Sprintf("web-%02d", i%hostCount)
		msg := fmt.Sprintf("request %d", i)
		events[i] = map[string]interface{}{
			"_time":      base.Add(time.Duration(i) * time.Second).UTC().Format(time.RFC3339Nano),
			"_raw":       fmt.Sprintf("host=%s level=INFO status=200 msg=%q", host, msg),
			"host":       host,
			"level":      "INFO",
			"status":     200,
			"msg":        msg,
			"source":     "/var/log/app.log",
			"sourcetype": "json",
		}
	}
	ingestNDJSON(t, addr, "main", ndjsonLines(t, events...))
}

// indexStats returns the stats of an index as the database holds them now.
func indexStats(srv *Server, index string) (columnar.IndexStats, bool) {
	for _, ix := range srv.DB().Stats() {
		if ix.Name == index {
			return ix, true
		}
	}

	return columnar.IndexStats{}, false
}

// postQueryData runs a sync query and returns its data object.
func postQueryData(t *testing.T, addr string, body map[string]interface{}) map[string]interface{} {
	t.Helper()
	raw, _ := json.Marshal(body)
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", addr), "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query status: %d, body: %s", resp.StatusCode, b)
	}
	var result map[string]interface{}
	if err := json.Unmarshal(b, &result); err != nil {
		t.Fatalf("decode query response: %v (body: %s)", err, b)
	}
	data, ok := result["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing data in query response: %s", b)
	}

	return data
}

// aggregateRow returns the only row of an aggregate result by column name.
func aggregateRow(t *testing.T, data map[string]interface{}) map[string]interface{} {
	t.Helper()
	cols, _ := data["columns"].([]interface{})
	rows, _ := data["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("rows: got %d, want 1 (data %v)", len(rows), data)
	}
	row := rows[0].([]interface{})
	out := make(map[string]interface{}, len(cols))
	for i, col := range cols {
		out[fmt.Sprint(col)] = row[i]
	}

	return out
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
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	data := result["data"].(map[string]interface{})
	if data["status"] != "healthy" {
		t.Fatalf("status = %v, want healthy", data["status"])
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

	listIndexes := func() []interface{} {
		t.Helper()
		resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/indexes", srv.Addr()))
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer resp.Body.Close()
		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("decode: %v", err)
		}
		data := result["data"].(map[string]interface{})

		return data["indexes"].([]interface{})
	}

	if indexes := listIndexes(); len(indexes) != 0 {
		t.Fatalf("indexes before ingest: got %v, want none", indexes)
	}

	ingestTestEvents(t, srv.Addr(), 5, 1)

	indexes := listIndexes()
	if len(indexes) != 1 {
		t.Fatalf("expected 1 index (main), got %v", indexes)
	}
	main := indexes[0].(map[string]interface{})
	if main["name"] != "main" {
		t.Errorf("name = %v, want main", main["name"])
	}
	if main["events"] != float64(5) {
		t.Errorf("events = %v, want 5", main["events"])
	}
	if parts, _ := main["parts"].(float64); parts < 1 {
		t.Errorf("parts = %v, want at least 1", main["parts"])
	}
	for _, key := range []string{"bytes", "first_event", "last_event"} {
		if _, ok := main[key]; !ok {
			t.Errorf("index entry missing %s: %v", key, main)
		}
	}
}

func TestServer_IngestAndQuery(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 50, 5)

	searchBody, _ := json.Marshal(map[string]interface{}{
		"q": `search source=main request | head 10`,
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
		t.Fatal("missing meta in response")
	}
	if _, ok := meta["took_ms"]; !ok {
		t.Error("missing took_ms in meta")
	}
	if id, _ := meta["query_id"].(string); !strings.HasPrefix(id, "qry_") {
		t.Errorf("query_id = %v, want qry_ prefix", meta["query_id"])
	}
	if meta["rows"] != float64(10) {
		t.Errorf("rows = %v, want 10", meta["rows"])
	}

	missingSourceBody, _ := json.Marshal(map[string]interface{}{"q": "stats count()"})
	missingSourceResp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(missingSourceBody))
	if err != nil {
		t.Fatalf("POST query without source: %v", err)
	}
	defer missingSourceResp.Body.Close()

	if missingSourceResp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(missingSourceResp.Body)
		t.Fatalf("query without source status: %d, want 400, body: %s", missingSourceResp.StatusCode, string(b))
	}
}

func TestServer_IngestKeepsMalformedLineAsText(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := "{\"event\":\"line one\"}\n{\"event\":\"line two\"}\n{\"event\":"
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/ingest", srv.Addr()), "application/x-ndjson", bytes.NewBufferString(body))
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
	if got := int(data["accepted"].(float64)); got != 3 {
		t.Fatalf("accepted: got %d, want 3", got)
	}
	if got := int(data["failed"].(float64)); got != 0 {
		t.Fatalf("failed: got %d, want 0", got)
	}

	events := queryEvents(t, srv.Addr(), `{"q":"source=main"}`)
	if len(events) != 3 {
		t.Fatalf("query count: got %d, want 3", len(events))
	}
	var objects, texts int
	for _, ev := range events {
		switch {
		case ev["event"] != nil:
			objects++
		case ev["_raw"] == `{"event":`:
			texts++
		}
	}
	if objects != 2 || texts != 1 {
		t.Fatalf("events = %v, want two objects and the malformed line as _raw", events)
	}
}

func TestServer_IngestCountsLinesAcrossPieces(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	// Lines straddle the pieces the body is read in, one is longer than a
	// piece, and blank lines count for nothing.
	var body bytes.Buffer
	want, longs := 0, 0
	for i := 0; body.Len() < 3*bodyPiece; i++ {
		switch i % 1000 {
		case 7:
			fmt.Fprintf(&body, `{"n":%d,"long":"%s"}`+"\r\n", i, strings.Repeat("y", 2*bodyPiece))
			longs++
		case 8:
			body.WriteString(" \n\n")

			continue
		default:
			fmt.Fprintf(&body, `{"n":%d,"msg":"%s"}`+"\n", i, strings.Repeat("x", i%300))
		}
		want++
	}
	body.WriteString(`{"last":true}`)
	want++
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/ingest?index=pieces", srv.Addr()), "application/x-ndjson",
		struct{ io.Reader }{&body})
	if err != nil {
		t.Fatalf("POST ingest: %v", err)
	}
	defer resp.Body.Close()
	var result struct {
		Data struct {
			Accepted int `json:"accepted"`
			Failed   int `json:"failed"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.StatusCode != http.StatusOK || result.Data.Accepted != want || result.Data.Failed != 0 {
		t.Fatalf("status %d, accepted %d, failed %d; want 200, %d, 0", resp.StatusCode, result.Data.Accepted, result.Data.Failed, want)
	}
	if events := queryEvents(t, srv.Addr(), `{"q":"source=pieces | where isnotnull(long) | fields n"}`); len(events) != longs {
		t.Fatalf("%d of the %d lines longer than a piece were taken", len(events), longs)
	}
}

func TestServer_IngestRoutesToIndex(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestNDJSON(t, srv.Addr(), "web", "{\"msg\":\"one\"}\n{\"msg\":\"two\"}\n")

	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/api/v1/ingest", srv.Addr()),
		strings.NewReader("{\"msg\":\"three\"}\n"))
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("X-Index", "api")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST ingest: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("X-Index ingest status: %d", resp.StatusCode)
	}

	for q, want := range map[string]int{"source=web": 2, "source=api": 1, "source=main": 0} {
		body, _ := json.Marshal(map[string]string{"q": q})
		if got := queryEventCount(t, srv.Addr(), string(body)); got != want {
			t.Errorf("%s: got %d events, want %d", q, got, want)
		}
	}
}

func TestServer_IngestInvalidIndexName(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/ingest?index=%s", srv.Addr(), url.QueryEscape("bad/name")),
		"application/x-ndjson", strings.NewReader("{\"msg\":\"one\"}\n"))
	if err != nil {
		t.Fatalf("POST ingest: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}
	if code, _ := decodeErrorResponse(t, resp); code != "VALIDATION_ERROR" {
		t.Fatalf("code = %q, want VALIDATION_ERROR", code)
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
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	data := result["data"].(map[string]interface{})
	if data["accepted"] != float64(2) || data["failed"] != float64(0) {
		t.Fatalf("data = %v, want 2 accepted, 0 failed", data)
	}

	events := queryEvents(t, srv.Addr(), `{"q":"source=main"}`)
	if len(events) != 2 {
		t.Fatalf("events: got %d, want 2", len(events))
	}
	raws := map[interface{}]bool{}
	for _, ev := range events {
		raws[ev["_raw"]] = true
		if ev["source"] != "http" || ev["sourcetype"] != "raw" {
			t.Errorf("source/sourcetype = %v/%v, want http/raw", ev["source"], ev["sourcetype"])
		}
	}
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		if !raws[line] {
			t.Errorf("no event with _raw %q in %v", line, events)
		}
	}
}

func TestServer_IngestRawExtractsLeadingTimestamp(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	req, _ := http.NewRequest("POST", fmt.Sprintf("http://%s/api/v1/ingest/raw", srv.Addr()),
		strings.NewReader("2024-01-01T00:00:00.000Z host=web-01 level=INFO msg=\"request 1\"\n"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	events := queryEvents(t, srv.Addr(), `{"q":"source=main"}`)
	if len(events) != 1 {
		t.Fatalf("events: got %d, want 1", len(events))
	}
	ev := events[0]
	if ev["_time"] != "2024-01-01T00:00:00Z" {
		t.Errorf("_time = %v, want 2024-01-01T00:00:00Z", ev["_time"])
	}
	if ev["host"] != "web-01" || ev["level"] != "INFO" || ev["msg"] != "request 1" {
		t.Errorf("key=value fields = %v, want host, level and msg", ev)
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

	if err := srv.DB().Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	ix, ok := indexStats(srv, "main")
	if !ok {
		t.Fatal("no main index after ingest")
	}
	if ix.Rows != 5000 || ix.Parts == 0 {
		t.Errorf("main: rows %d parts %d, want 5000 rows in at least one part", ix.Rows, ix.Parts)
	}
}

func TestServer_AutoFlush(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

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
	if err := srv.DB().AppendEvents(flushEvents); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}

	// The store writes appended events within its flush interval without a Sync.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if ix, ok := indexStats(srv, "main"); ok && ix.Rows == 100 && ix.Parts > 0 {
			return
		}
		if time.Now().After(deadline) {
			ix, _ := indexStats(srv, "main")
			t.Fatalf("main after flush interval: rows %d parts %d, want 100 rows written", ix.Rows, ix.Parts)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// appendFieldEvents appends one event per value of field to main, a second
// apart.
func appendFieldEvents(t *testing.T, srv *Server, base time.Time, fields ...map[string]event.Value) {
	t.Helper()
	events := make([]*event.Event, 0, len(fields))
	for i, f := range fields {
		parts := make([]string, 0, len(f))
		for k, v := range f {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
		ev := event.NewEvent(base.Add(time.Duration(i)*time.Second), strings.Join(parts, " "))
		ev.Index = "main"
		ev.Host = "web-00"
		ev.Source = "/var/log/app.log"
		ev.SourceType = "json"
		for k, v := range f {
			ev.SetField(k, v)
		}
		events = append(events, ev)
	}
	if err := srv.DB().AppendEvents(events); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}
}

func TestServer_StatsQuery(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 30, 3)

	searchBody, _ := json.Marshal(map[string]interface{}{
		"q": `source=main | stats count() by host`,
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
	if fmt.Sprint(cols) != "[count() host]" {
		t.Fatalf("columns = %v, want [count() host]", cols)
	}

	rows, _ := data["rows"].([]interface{})
	totalRows, _ := data["total_rows"].(float64)
	if int(totalRows) != 3 {
		t.Errorf("total_rows: got %v, want 3 (one per host)", totalRows)
	}

	for _, row := range rows {
		if count := row.([]interface{})[0].(float64); count != 10 {
			t.Errorf("count=%v, want 10", count)
		}
	}
}

func TestServer_StatsAggregateAliases(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	appendFieldEvents(t, srv, time.Now(),
		map[string]event.Value{"duration_ms": event.FloatValue(20), "user": event.StringValue("alice")},
		map[string]event.Value{"duration_ms": event.FloatValue(20), "user": event.StringValue("bob")},
		map[string]event.Value{"duration_ms": event.FloatValue(20), "user": event.StringValue("alice")},
	)

	row := aggregateRow(t, postQueryData(t, srv.Addr(), map[string]interface{}{
		"q": `source=main | stats avg(duration_ms) as avg_dur, p50(duration_ms) as p50_dur, dc(user) as users, distinct_count_approx(user) as estimated_users`,
	}))
	for name, want := range map[string]float64{"avg_dur": 20, "p50_dur": 20, "users": 2, "estimated_users": 2} {
		if got, _ := row[name].(float64); got != want {
			t.Errorf("%s: got %v, want %v", name, row[name], want)
		}
	}
}

func TestServer_StatsPercentileSuffixAliases(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	twenty := map[string]event.Value{"duration_ms": event.FloatValue(20)}
	appendFieldEvents(t, srv, time.Now(), twenty, twenty, twenty)

	row := aggregateRow(t, postQueryData(t, srv.Addr(), map[string]interface{}{
		"q": `source=main | stats p95(duration_ms) as p95, p99(duration_ms) as p99_dur`,
	}))
	for _, name := range []string{"p95", "p99_dur"} {
		if got, _ := row[name].(float64); got != 20 {
			t.Errorf("%s: got %v, want 20", name, row[name])
		}
	}
}

func TestServer_StatsVarianceAggregates(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	appendFieldEvents(t, srv, time.Now(),
		map[string]event.Value{"duration_ms": event.FloatValue(0)},
		map[string]event.Value{"duration_ms": event.FloatValue(2)},
	)

	row := aggregateRow(t, postQueryData(t, srv.Addr(), map[string]interface{}{
		"q": `source=main | stats stddev_pop(duration_ms) as stdevp_duration, var_samp(duration_ms) as sample_var, var_pop(duration_ms) as population_var`,
	}))
	for name, want := range map[string]float64{"stdevp_duration": 1, "sample_var": 2, "population_var": 1} {
		if got, _ := row[name].(float64); got != want {
			t.Errorf("%s: got %v, want %v", name, row[name], want)
		}
	}
}

func TestServer_StatsListAggregatePreservesDuplicates(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	appendFieldEvents(t, srv, time.Now(),
		map[string]event.Value{"user": event.StringValue("alice")},
		map[string]event.Value{"user": event.StringValue("bob")},
		map[string]event.Value{"user": event.StringValue("alice")},
	)

	row := aggregateRow(t, postQueryData(t, srv.Addr(), map[string]interface{}{
		"q": `source=main | stats list(user) as user_list, values(user) as user_values`,
	}))
	if got := fmt.Sprint(row["user_list"]); got != "[alice bob alice]" {
		t.Fatalf("user_list: got %s, want [alice bob alice]", got)
	}
	if got := fmt.Sprint(row["user_values"]); got != "[alice bob]" {
		t.Fatalf("user_values: got %s, want [alice bob]", got)
	}
}

func TestQuery_SyncMode(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 20, 2)

	body, _ := json.Marshal(map[string]interface{}{
		"q": `source=main | head 5`,
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
	if data["total"] != float64(5) || data["has_more"] != false {
		t.Errorf("total/has_more = %v/%v, want 5/false", data["total"], data["has_more"])
	}
	if len(events) > 0 {
		first := events[0].(map[string]interface{})
		if first["source"] != "/var/log/app.log" {
			t.Errorf("source = %v, want /var/log/app.log", first["source"])
		}
		if first["sourcetype"] != "json" {
			t.Errorf("sourcetype = %v, want json", first["sourcetype"])
		}
		for _, name := range []string{"_source", "_sourcetype", "index"} {
			if _, ok := first[name]; ok {
				t.Errorf("event row has %s: %v", name, first)
			}
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

	body, _ := json.Marshal(map[string]interface{}{
		"q": `source=main | stats count() by host`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200 or 202, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data, ok := result["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing data in response: %#v", result)
	}
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

	wait := float64(5)
	body, _ := json.Marshal(map[string]interface{}{
		"q": `source=main | head 3`, "wait": wait,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

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
		"q": `source=main | stats count() by host`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})

	if data["type"] != "aggregate" {
		t.Errorf("type: %v", data["type"])
	}

	cols := data["columns"].([]interface{})
	rows := data["rows"].([]interface{})
	totalRows := data["total_rows"].(float64)

	if len(cols) != 2 {
		t.Errorf("columns: got %v, want 2", cols)
	}
	if int(totalRows) != 3 {
		t.Errorf("total_rows: got %v, want 3", totalRows)
	}
	if len(rows) != 3 {
		t.Errorf("rows: got %d, want 3", len(rows))
	}
	if data["has_more"] != false {
		t.Errorf("has_more = %v, want false", data["has_more"])
	}
}

func TestQuery_TimechartResult(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 30, 3)

	body, _ := json.Marshal(map[string]interface{}{
		"q": `source=main | timechart span=1h count() by host`,
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

	if data["type"] != "aggregate" {
		t.Errorf("type: got %v, want aggregate", data["type"])
	}

	cols := data["columns"].([]interface{})
	for _, want := range []string{"_time", "host", "count()"} {
		found := false
		for _, c := range cols {
			found = found || c == want
		}
		if !found {
			t.Errorf("columns %v missing %s", cols, want)
		}
	}

	rows := data["rows"].([]interface{})
	if len(rows) == 0 {
		t.Error("rows: got 0, want > 0")
	}
	var total float64
	countCol := len(cols) - 1
	for i, c := range cols {
		if c == "count()" {
			countCol = i
		}
	}
	for _, row := range rows {
		total += row.([]interface{})[countCol].(float64)
	}
	if total != 30 {
		t.Errorf("sum of counts: got %v, want 30", total)
	}

	totalRows := data["total_rows"].(float64)
	if int(totalRows) != len(rows) {
		t.Errorf("total_rows: got %v, want %d", totalRows, len(rows))
	}
}

func TestQuery_TimechartPerMinuteAggregate(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	appendFieldEvents(t, srv, time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		map[string]event.Value{"bytes": event.IntValue(60)},
		map[string]event.Value{"bytes": event.IntValue(120)},
	)

	data := postQueryData(t, srv.Addr(), map[string]interface{}{
		"q": `source=main | timechart span=1h per_second(bytes) as bytes_per_sec`,
	})
	row := aggregateRow(t, data)
	got, ok := row["bytes_per_sec"].(float64)
	if !ok {
		t.Fatalf("missing bytes_per_sec in %v", row)
	}
	// 60+120 bytes over a one hour bucket.
	if math.Abs(got-180.0/3600) > 1e-12 {
		t.Errorf("bytes_per_sec: got %v, want %v", got, 180.0/3600)
	}
}

func TestQuery_StatsTimeAggregates(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ingestNDJSON(t, srv.Addr(), "main", ndjsonLines(t,
		map[string]interface{}{
			"_time":  base.Add(10 * time.Second).Format(time.RFC3339),
			"host":   "web-00",
			"status": "late",
		},
		map[string]interface{}{
			"_time":  base.Format(time.RFC3339),
			"host":   "web-00",
			"status": "early",
		},
	))

	row := aggregateRow(t, postQueryData(t, srv.Addr(), map[string]interface{}{
		"q": `source=main | stats earliest(status) as first_status, latest(status) as last_status`,
	}))
	if got := row["first_status"]; got != "early" {
		t.Errorf("first_status: got %v, want early", got)
	}
	if got := row["last_status"]; got != "late" {
		t.Errorf("last_status: got %v, want late", got)
	}
}

func TestQuery_CancelJob(t *testing.T) {
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
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 5, 1)
	postQueryData(t, srv.Addr(), map[string]interface{}{"q": "source=main | head 1"})

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query/jobs", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	var result struct {
		Data struct {
			Jobs []struct {
				JobID  string `json:"job_id"`
				Query  string `json:"query"`
				Status string `json:"status"`
			} `json:"jobs"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(result.Data.Jobs) != 1 {
		t.Fatalf("jobs = %+v, want 1", result.Data.Jobs)
	}
	job := result.Data.Jobs[0]
	if !strings.HasPrefix(job.JobID, "qry_") || job.Query != "source=main | head 1" || job.Status != "done" {
		t.Fatalf("job = %+v, want the done query", job)
	}
}

func TestQuery_CancelJob_AlreadyDone(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	wait := float64(0)
	body, _ := json.Marshal(map[string]interface{}{"q": "source=main | head 1", "wait": wait})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	var handle map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&handle)
	resp.Body.Close()
	jobID := handle["data"].(map[string]interface{})["job_id"].(string)
	job, ok := srv.DB().Job(jobID)
	if !ok {
		t.Fatalf("job %s not found", jobID)
	}
	<-job.Done()

	req, _ := http.NewRequest("DELETE", fmt.Sprintf("http://%s/api/v1/query/jobs/%s", srv.Addr(), jobID), http.NoBody)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})
	if data["canceled"] != false || data["status"] != "done" {
		t.Fatalf("data = %v, want status done and canceled false", data)
	}
	if _, ok := data["completed_at"]; !ok {
		t.Fatalf("data = %v, want completed_at", data)
	}
}

func TestQuery_ListJobs_FilterByStatus(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query/jobs?status=done", srv.Addr()))
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

	// "query" is an alias of "q".
	body, _ := json.Marshal(map[string]interface{}{
		"query": `source=main | head 3`,
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

	wait := float64(0)
	searchBody, _ := json.Marshal(map[string]interface{}{
		"q": `source=main | head 5`, "wait": wait,
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

	for i := 0; i < 100; i++ {
		resp, err = http.Get(fmt.Sprintf("http://%s/api/v1/query/jobs/%s", srv.Addr(), jobID))
		if err != nil {
			t.Fatalf("GET job: %v", err)
		}
		var jr map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&jr)
		resp.Body.Close()

		d := jr["data"].(map[string]interface{})
		if d["type"] != "job" {
			t.Fatalf("data.type = %v, want job", d["type"])
		}
		switch d["status"] {
		case "done":
			results := d["results"].(map[string]interface{})
			if results["type"] != "events" {
				t.Fatalf("results.type = %v, want events", results["type"])
			}
			if events := results["events"].([]interface{}); len(events) != 5 {
				t.Errorf("events: got %d, want 5", len(events))
			}
			if _, ok := d["completed_at"]; !ok {
				t.Errorf("done job without completed_at: %v", d)
			}

			return
		case "error":
			t.Fatalf("job error: %v", d["error"])
		}
		time.Sleep(50 * time.Millisecond)
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
		if errObj["code"] != string(ErrCodeSyntaxError) {
			t.Errorf("code: got %v, want %s", errObj["code"], ErrCodeSyntaxError)
		}
		if _, ok := errObj["position"].(map[string]interface{}); !ok {
			t.Errorf("expected error position, got %v", errObj)
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
		srv.Addr(), url.QueryEscape("source=main | head 5")))
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
		body := strings.NewReader(`{"q":"source=main","format":"csv"}`)
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
		resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query?q=%s&format=csv", srv.Addr(), url.QueryEscape("source=main")))
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status: got %d, want 400", resp.StatusCode)
		}
	})
}

func TestJobStream_Basic(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 10, 2)

	zero := float64(0)
	body, _ := json.Marshal(map[string]interface{}{
		"q":    `source=main | head 5`,
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

	sseBody, _ := io.ReadAll(sseResp.Body)
	sseStr := string(sseBody)
	_, completeData, ok := strings.Cut(sseStr, "event: complete\ndata: ")
	if !ok {
		t.Fatalf("SSE body missing complete event: %s", sseStr)
	}
	completeData, _, _ = strings.Cut(completeData, "\n")
	var complete struct {
		Data struct {
			Type   string        `json:"type"`
			Events []interface{} `json:"events"`
		} `json:"data"`
		Meta struct {
			QueryID string `json:"query_id"`
			Rows    int    `json:"rows"`
		} `json:"meta"`
	}
	if err := json.Unmarshal([]byte(completeData), &complete); err != nil {
		t.Fatalf("decode complete event %q: %v", completeData, err)
	}
	if complete.Data.Type != "events" || len(complete.Data.Events) != 5 {
		t.Errorf("complete data = %+v, want 5 events", complete.Data)
	}
	if complete.Meta.QueryID != jobID || complete.Meta.Rows != 5 {
		t.Errorf("complete meta = %+v, want query_id %s and 5 rows", complete.Meta, jobID)
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
