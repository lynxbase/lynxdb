package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

func TestHandleStatus_BufferPoolAbsent(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/status", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	data := result["data"].(map[string]interface{})

	// Without buffer pool enabled, the key should not be present.
	if _, ok := data["buffer_pool"]; ok {
		t.Fatal("expected buffer_pool to be absent when buffer manager is not enabled")
	}
}

func TestStatus_Unified(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/status", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})

	if data["version"] == nil {
		t.Fatal("missing version")
	}
	if data["health"] == nil {
		t.Fatal("missing health")
	}
	if data["uptime_seconds"] == nil {
		t.Fatal("missing uptime_seconds")
	}
	storage := data["storage"].(map[string]interface{})
	if storage["used_bytes"] == nil {
		t.Fatal("missing storage.used_bytes")
	}
	events := data["events"].(map[string]interface{})
	if events["total"] == nil {
		t.Fatal("missing events.total")
	}
	queries := data["queries"].(map[string]interface{})
	if queries["active"] == nil {
		t.Fatal("missing queries.active")
	}
	views := data["views"].(map[string]interface{})
	if views["total"] == nil {
		t.Fatal("missing views.total")
	}
}
