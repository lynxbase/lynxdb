package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestExplain_ValidQuery_JSON(t *testing.T) {
	baseURL := newTestServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "explain", "--format", "json",
		"level=error | stats count")
	if err != nil {
		t.Fatalf("explain failed: %v", err)
	}

	// Should be valid JSON.
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &result); err != nil {
		t.Fatalf("parse explain JSON: %v\noutput: %q", err, stdout)
	}

	// Should have a parsed section.
	if _, ok := result["parsed"]; !ok {
		t.Errorf("explain JSON missing 'parsed' key")
	}
}

func TestExplain_ValidQuery_Table(t *testing.T) {
	baseURL := newTestServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "explain", "--format", "table",
		"level=error | stats count")
	if err != nil {
		t.Fatalf("explain failed: %v", err)
	}

	for _, section := range []string{"Query plan", "Pipeline", "Optimizer", "Hints"} {
		if !strings.Contains(stdout, section) {
			t.Errorf("expected %q in explain output, got: %q", section, stdout)
		}
	}
}

func TestExplain_InvalidQuery_ShowsErrors(t *testing.T) {
	baseURL := newTestServer(t)

	// The explain endpoint returns HTTP 200 with is_valid=false for parse errors,
	// so the CLI does not return a Go error. Instead, verify the output contains
	// error information.
	stdout, _, _ := runCmd(t, "--server", baseURL, "explain", "--format", "json", "| where")

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &result); err != nil {
		t.Fatalf("parse explain JSON: %v\noutput: %q", err, stdout)
	}

	isValid, ok := result["is_valid"].(bool)
	if !ok {
		t.Fatalf("explain JSON missing 'is_valid' key, got keys: %v", cliMapKeys(result))
	}

	if isValid {
		t.Errorf("expected is_valid=false for incomplete WHERE, got true")
	}
}

func TestExplain_InvalidQuery_TableShowsDiagnostics(t *testing.T) {
	baseURL := newTestServer(t)

	stdout, _, _ := runCmd(t, "--server", baseURL, "explain", "--format", "table", "| where")

	for _, section := range []string{"Query plan", "Diagnostics", "Hints"} {
		if !strings.Contains(stdout, section) {
			t.Errorf("expected %q in invalid explain output, got: %q", section, stdout)
		}
	}
	if !strings.Contains(strings.ToLower(stdout), "suggest") {
		t.Errorf("expected suggestion in invalid explain output, got: %q", stdout)
	}
}
