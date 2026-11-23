package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStatus_JSON_ContainsExpectedKeys(t *testing.T) {
	baseURL := newTestServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "status", "--format", "json")
	if err != nil {
		t.Fatalf("status failed: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &result); err != nil {
		t.Fatalf("parse status JSON: %v\noutput: %q", err, stdout)
	}

	expectedKeys := []string{"uptime_seconds", "total_events", "part_count", "health"}
	for _, k := range expectedKeys {
		if _, ok := result[k]; !ok {
			t.Errorf("status JSON missing key %q, got keys: %v", k, cliMapKeys(result))
		}
	}
}

func TestStatus_Table_ContainsLabels(t *testing.T) {
	baseURL := newTestServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "status", "--format", "table")
	if err != nil {
		t.Fatalf("status failed: %v", err)
	}

	if !strings.Contains(stdout, "Events") {
		t.Errorf("expected 'Events' in table status output")
	}

	if !strings.Contains(stdout, "Storage") {
		t.Errorf("expected 'Storage' in table status output")
	}
}

// TestStatus_FailsWhenNotHealthy pins that status prints what it learned
// and then fails when the server is not healthy.
func TestStatus_FailsWhenNotHealthy(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/stats", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"uptime_seconds": 14, "total_events": 3}})
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"status": "degraded"}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	stdout, _, err := runCmd(t, "--server", srv.URL, "status", "--format", "json")
	if err == nil || !strings.Contains(err.Error(), "server is degraded") {
		t.Fatalf("got %v, want a degraded server error", err)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(stdout), &result); err != nil || result["health"] != "degraded" {
		t.Errorf("status printed no status before failing: %q", stdout)
	}
}
