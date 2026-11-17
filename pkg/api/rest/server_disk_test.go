package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func startDiskTestServer(t *testing.T) (*Server, string, func()) {
	t.Helper()

	dir := t.TempDir()
	srv, cleanup := startTestServerWithConfig(t, Config{DataDir: dir})

	return srv, dir, cleanup
}

// partFiles lists the part files under dir.
func partFiles(t *testing.T, dir string) []string {
	t.Helper()
	var parts []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".part") {
			parts = append(parts, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}

	return parts
}

func textEvents(n int, base time.Time, raw func(i int) string) []*event.Event {
	events := make([]*event.Event, n)
	for i := 0; i < n; i++ {
		events[i] = &event.Event{
			Time:       base.Add(time.Duration(i) * time.Millisecond),
			Raw:        raw(i),
			Host:       fmt.Sprintf("web-%02d", i%5),
			Index:      "main",
			Source:     "test",
			SourceType: "raw",
			Fields:     make(map[string]event.Value),
		}
	}

	return events
}

func TestServer_DiskFlushAndRecover(t *testing.T) {
	dir := t.TempDir()

	srv1, stop1 := startTestServerWithConfig(t, Config{DataDir: dir})
	defer stop1()

	events := textEvents(100, time.Now(), func(i int) string {
		return fmt.Sprintf("event %d host=web-%02d level=INFO", i, i%5)
	})
	if err := srv1.DB().AppendEvents(events); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}
	if err := srv1.DB().Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	if files := partFiles(t, filepath.Join(dir, "indexes", "main")); len(files) == 0 {
		t.Fatal("expected part files on disk")
	}
	ix, ok := indexStats(srv1, "main")
	if !ok || ix.Rows != 100 || ix.Parts == 0 {
		t.Fatalf("main before restart: %+v, want 100 rows in parts", ix)
	}

	stop1()

	srv2, stop2 := startTestServerWithConfig(t, Config{DataDir: dir})
	defer stop2()

	if ix, ok := indexStats(srv2, "main"); !ok || ix.Rows != 100 {
		t.Errorf("after recovery: main = %+v, want 100 rows", ix)
	}

	resultCount := searchAndCount(t, srv2, `source=main | head 10`)
	if resultCount != 10 {
		t.Errorf("result_count: got %d, want 10", resultCount)
	}
}

func TestServer_DiskMode_IngestAndQuery(t *testing.T) {
	srv, _, cleanup := startDiskTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 30, 3)

	resultCount := searchAndCount(t, srv, `source=main | stats count() by host`)
	if resultCount != 3 {
		t.Errorf("result_count: got %d, want 3 (one per host)", resultCount)
	}
}

func TestServer_DiskSearchTerm(t *testing.T) {
	srv, _, cleanup := startDiskTestServer(t)
	defer cleanup()

	events := textEvents(50, time.Now(), func(i int) string {
		return fmt.Sprintf("event %d host=web-01 level=INFO status=200 msg=\"hello world\"", i)
	})
	if err := srv.DB().AppendEvents(events); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}
	if err := srv.DB().Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	if n := searchAndCount(t, srv, `search source=main hello`); n != 50 {
		t.Errorf("search 'hello': got %d results, want 50", n)
	}
	if n := searchAndCount(t, srv, `search source=main absent`); n != 0 {
		t.Errorf("search 'absent': got %d results, want 0", n)
	}
}

func TestServer_ShutdownPersistsAppendedEvents(t *testing.T) {
	dir := t.TempDir()

	srv1, stop1 := startTestServerWithConfig(t, Config{DataDir: dir})
	defer stop1()

	base := time.Now().Add(-time.Minute)
	lines := make([]map[string]interface{}, 20)
	for i := 0; i < 20; i++ {
		lines[i] = map[string]interface{}{
			"_time":      base.Add(time.Duration(i) * time.Second).UTC().Format(time.RFC3339Nano),
			"_raw":       fmt.Sprintf("persist-test event %d host=web-01", i),
			"host":       "web-01",
			"source":     "test",
			"sourcetype": "raw",
		}
	}
	ingestNDJSON(t, srv1.Addr(), "main", ndjsonLines(t, lines...))

	// Closing the database on shutdown writes what the store still buffers.
	stop1()

	srv2, stop2 := startTestServerWithConfig(t, Config{DataDir: dir})
	defer stop2()

	ix, ok := indexStats(srv2, "main")
	if !ok || ix.Rows != 20 {
		t.Errorf("after restart: main = %+v, want 20 rows", ix)
	}
}

func TestServer_FlushThenSearch(t *testing.T) {
	srv, _, cleanup := startDiskTestServer(t)
	defer cleanup()

	const uniqueTerm = "tm2517090d1_tsrimg120"
	matchCount := 0
	events := textEvents(200, time.Now(), func(i int) string {
		if i%50 == 0 {
			matchCount++
			return fmt.Sprintf("event %d path=/data/%s host=web-%02d level=ERROR", i, uniqueTerm, i%5)
		}
		return fmt.Sprintf("event %d host=web-%02d level=INFO", i, i%5)
	})
	if err := srv.DB().AppendEvents(events); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}
	if err := srv.DB().Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	if ix, ok := indexStats(srv, "main"); !ok || ix.Parts == 0 {
		t.Fatalf("main after Sync: %+v, want at least one part", ix)
	}

	postFlushCount := searchAndCount(t, srv, "search source=main "+uniqueTerm)
	if postFlushCount != matchCount {
		t.Fatalf("post-flush: got %d results, want %d", postFlushCount, matchCount)
	}
}

// searchAndCount runs a query and returns the number of its events or rows,
// polling the job when the server answers with a handle.
func searchAndCount(t *testing.T, srv *Server, query string) int {
	t.Helper()

	searchBody, _ := json.Marshal(map[string]interface{}{"q": query})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(searchBody))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	var jobResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&jobResp)
	resp.Body.Close()

	countResult := func(data map[string]interface{}) int {
		switch data["type"] {
		case "events":
			return len(data["events"].([]interface{}))
		case "aggregate":
			return len(data["rows"].([]interface{}))
		}
		t.Fatalf("unexpected result: %v", data)

		return 0
	}

	if resp.StatusCode == http.StatusOK {
		return countResult(jobResp["data"].(map[string]interface{}))
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("query status: %d, body: %v", resp.StatusCode, jobResp)
	}

	jobID := jobResp["data"].(map[string]interface{})["job_id"].(string)
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
		switch d["status"] {
		case "done":
			return countResult(d["results"].(map[string]interface{}))
		case "running":
		default:
			t.Fatalf("job ended %v: %v", d["status"], d["error"])
		}
	}
	t.Fatal("timeout waiting for query job")

	return 0
}
