package main

import (
	"encoding/json"
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

func TestHealth_OK(t *testing.T) {
	baseURL := newTestServer(t)

	_, _, err := runCmd(t, "--server", baseURL, "health")
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
}
