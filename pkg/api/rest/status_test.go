package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

func getData(t *testing.T, url string) map[string]interface{} {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("GET %s status: %d", url, resp.StatusCode)
	}
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}

	return result["data"].(map[string]interface{})
}

func TestStatus_Unified(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 5, 1)

	data := getData(t, fmt.Sprintf("http://%s/api/v1/status", srv.Addr()))

	if data["version"] == nil {
		t.Fatal("missing version")
	}
	if data["health"] != "healthy" {
		t.Fatalf("health = %v, want healthy", data["health"])
	}
	if data["uptime_seconds"] == nil {
		t.Fatal("missing uptime_seconds")
	}
	storage := data["storage"].(map[string]interface{})
	if used, _ := storage["used_bytes"].(float64); used <= 0 {
		t.Fatalf("storage.used_bytes = %v, want > 0", storage["used_bytes"])
	}
	events := data["events"].(map[string]interface{})
	if events["total"] != float64(5) {
		t.Fatalf("events.total = %v, want 5", events["total"])
	}
	queries := data["queries"].(map[string]interface{})
	if queries["active"] != float64(0) {
		t.Fatalf("queries.active = %v, want 0", queries["active"])
	}
	retention, _ := data["retention"].(map[string]interface{})
	if retention["oldest_event"] == nil {
		t.Fatalf("retention = %v, want oldest_event", data["retention"])
	}
}

func TestStats_IndexesAndJobs(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 5, 1)
	ingestNDJSON(t, srv.Addr(), "web", "{\"msg\":\"one\"}\n")
	postQueryData(t, srv.Addr(), map[string]interface{}{"q": "source=main | head 1"})

	data := getData(t, fmt.Sprintf("http://%s/api/v1/stats", srv.Addr()))
	if data["total_events"] != float64(6) {
		t.Errorf("total_events = %v, want 6", data["total_events"])
	}
	if data["index_count"] != float64(2) {
		t.Errorf("index_count = %v, want 2", data["index_count"])
	}
	if parts, _ := data["part_count"].(float64); parts < 2 {
		t.Errorf("part_count = %v, want at least 2", data["part_count"])
	}
	for _, key := range []string{"uptime_seconds", "storage_bytes", "oldest_event", "newest_event"} {
		if data[key] == nil {
			t.Errorf("missing %s in %v", key, data)
		}
	}
	indexes, _ := data["indexes"].([]interface{})
	if len(indexes) != 2 {
		t.Fatalf("indexes = %v, want main and web", data["indexes"])
	}
	jobs, _ := data["jobs"].(map[string]interface{})
	if jobs["done"] != float64(1) || jobs["running"] != float64(0) {
		t.Errorf("jobs = %v, want one done", data["jobs"])
	}
}
