package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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

// TestInspectionJSON pins that the inspection commands print JSON objects
// keyed by lowercase snake_case names, never table headers, in json,
// ndjson and non-terminal auto output.
func TestInspectionJSON(t *testing.T) {
	baseURL := setupServerWithData(t)
	if _, _, err := runCmd(t, "--server", baseURL, "query", "source=main | head 1"); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		args []string
		keys []string
	}{
		{[]string{"status"}, []string{"uptime_seconds", "storage_bytes", "total_events", "index_count", "part_count", "oldest_event", "newest_event", "health", "indexes"}},
		{[]string{"indexes"}, []string{"name", "events", "bytes", "parts", "first_event", "last_event"}},
		{[]string{"fields"}, []string{"name", "indexes"}},
		{[]string{"fields", "status"}, []string{"name", "indexes"}},
		{[]string{"fields", "status", "--values"}, []string{"value", "count"}},
		{[]string{"jobs"}, []string{"job_id", "status", "query", "created_at"}},
	} {
		for _, format := range []string{"json", "ndjson", "auto"} {
			args := append([]string{"--server", baseURL, "--format", format}, c.args...)
			stdout, _, err := runCmd(t, args...)
			if err != nil {
				t.Fatalf("%v: %v", args, err)
			}
			lines := strings.Split(strings.TrimSpace(stdout), "\n")
			if c.args[0] == "status" && len(lines) != 1 {
				t.Errorf("%v: status printed %d lines, want one object", args, len(lines))
			}
			for _, line := range lines {
				var obj map[string]any
				if err := json.Unmarshal([]byte(line), &obj); err != nil {
					t.Fatalf("%v: %q is not a JSON object: %v", args, line, err)
				}
				for _, k := range c.keys {
					if _, ok := obj[k]; !ok {
						t.Errorf("%v: %s lacks %q", args, line, k)
					}
				}
				for k := range obj {
					if k != strings.ToLower(k) || strings.Contains(k, " ") {
						t.Errorf("%v: key %q is not lowercase snake_case", args, k)
					}
				}
			}
		}
	}
}

func TestListProfilesJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	for _, args := range [][]string{
		{"--config", path, "config", "add-profile", "prod", "--url", "https://logs.example.com", "--token", "secret"},
		{"--config", path, "config", "add-profile", "dev", "--url", "http://localhost:3100"},
	} {
		if _, _, err := runCmd(t, args...); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}

	stdout, _, err := runCmd(t, "--config", path, "--format", "json", "config", "list-profiles")
	if err != nil {
		t.Fatal(err)
	}
	want := `{"name":"dev","url":"http://localhost:3100","auth":false}` + "\n" +
		`{"name":"prod","url":"https://logs.example.com","auth":true}` + "\n"
	if stdout != want {
		t.Errorf("got %q, want %q", stdout, want)
	}
}
