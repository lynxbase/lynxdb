package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

func postCompact(t *testing.T, addr, index string) (int, map[string]interface{}) {
	t.Helper()
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/indexes/%s/compact", addr, index), "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	data, _ := result["data"].(map[string]interface{})

	return resp.StatusCode, data
}

func TestCompactMergesAnIndexIntoOnePart(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	statsURL := fmt.Sprintf("http://%s/api/v1/stats", srv.Addr())
	// Stats write the buffered events, so every round leaves a run.
	for i := range 3 {
		ingestNDJSON(t, srv.Addr(), "web", fmt.Sprintf("{\"msg\":\"m%d\"}\n{\"msg\":\"n%d\"}\n", i, i))
		getData(t, statsURL)
	}
	ingestNDJSON(t, srv.Addr(), "web", "{\"msg\":\"buffered\"}\n")

	code, data := postCompact(t, srv.Addr(), "web")
	if code != http.StatusOK {
		t.Fatalf("compact status %d: %v", code, data)
	}
	if data["index"] != "web" || data["parts"] != float64(1) {
		t.Fatalf("compact reply %v, want one part of web", data)
	}
	if before, _ := data["parts_before"].(float64); before < 4 {
		t.Fatalf("parts_before = %v, want the four runs written", data["parts_before"])
	}
	for _, key := range []string{"bytes", "bytes_before", "seconds"} {
		if _, ok := data[key].(float64); !ok {
			t.Fatalf("compact reply %v lacks %s", data, key)
		}
	}

	stats := getData(t, statsURL)
	if stats["total_events"] != float64(7) || stats["part_count"] != float64(1) {
		t.Fatalf("stats after compact: %v", stats)
	}
	if stats["merges_running"] != float64(0) || stats["runs_to_merge"] != float64(0) {
		t.Fatalf("stats after compact: merges_running %v, runs_to_merge %v, want both 0", stats["merges_running"], stats["runs_to_merge"])
	}
	index := stats["indexes"].([]interface{})[0].(map[string]interface{})
	if index["merges_running"] != float64(0) || index["runs_to_merge"] != float64(0) {
		t.Fatalf("index stats after compact: %v", index)
	}

	if code, _ := postCompact(t, srv.Addr(), "nosuch"); code != http.StatusNotFound {
		t.Fatalf("compact of an unknown index: status %d, want 404", code)
	}
}
