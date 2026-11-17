package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestExplain_ValidQuery_JSON(t *testing.T) {
	baseURL := newTestServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "explain", "--format", "json",
		`source=main | where level = "error" | stats count()`)
	if err != nil {
		t.Fatalf("explain failed: %v", err)
	}

	var result struct {
		Explain map[string]interface{} `json:"explain"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &result); err != nil {
		t.Fatalf("parse explain JSON: %v\noutput: %q", err, stdout)
	}

	if result.Explain["is_valid"] != true {
		t.Fatalf("explain JSON: is_valid != true, got %#v", result.Explain)
	}
	parsed, ok := result.Explain["parsed"].(map[string]interface{})
	if !ok {
		t.Fatalf("explain JSON missing 'parsed', got keys: %v", cliMapKeys(result.Explain))
	}
	if plan, _ := parsed["logical_plan"].(string); !strings.Contains(plan, "Aggregate") {
		t.Errorf("logical_plan = %q, want an Aggregate node", plan)
	}
}

func TestExplain_ValidQuery_Table(t *testing.T) {
	baseURL := newTestServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "explain", "--format", "table",
		`source=main | where level = "error" | stats count()`)
	if err != nil {
		t.Fatalf("explain failed: %v", err)
	}

	for _, want := range []string{"Logical plan:", "Optimized plan:", "Scan:", "Aggregate", "Filter"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("explain output missing %q, got: %q", want, stdout)
		}
	}
}

func TestExplain_InvalidQuery_ShowsErrors(t *testing.T) {
	baseURL := newTestServer(t)

	// The explain endpoint returns HTTP 200 with is_valid=false for parse errors,
	// so the CLI does not return a Go error. Instead, verify the output contains
	// error information.
	stdout, _, _ := runCmd(t, "--server", baseURL, "explain", "--format", "json", "source=main | where")

	var result struct {
		Explain map[string]interface{} `json:"explain"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &result); err != nil {
		t.Fatalf("parse explain JSON: %v\noutput: %q", err, stdout)
	}

	isValid, ok := result.Explain["is_valid"].(bool)
	if !ok {
		t.Fatalf("explain JSON missing 'is_valid' key, got keys: %v", cliMapKeys(result.Explain))
	}

	if isValid {
		t.Errorf("expected is_valid=false for incomplete WHERE, got true")
	}
	errs, _ := result.Explain["errors"].([]interface{})
	if len(errs) == 0 {
		t.Errorf("expected at least one error, got %#v", result.Explain["errors"])
	}
}

func TestExplain_InvalidQuery_TableReturnsError(t *testing.T) {
	baseURL := newTestServer(t)

	_, stderr, err := runCmd(t, "--server", baseURL, "explain", "--format", "table", "source=main | where")
	if err == nil {
		t.Fatal("expected an error for an invalid query")
	}
	if !strings.Contains(err.Error(), "invalid query") {
		t.Errorf("error = %v, want invalid query", err)
	}
	if !strings.Contains(stderr, "source=main | where") {
		t.Errorf("expected the query echoed under the error, got stderr: %q", stderr)
	}
}

func TestQuery_ExplainFlag_PrintsPlans(t *testing.T) {
	stdout, _, err := runCmd(t, "query", "--explain", "source=main | where status >= 500 | stats count()")
	if err != nil {
		t.Fatalf("query --explain failed: %v", err)
	}

	for _, want := range []string{"Logical plan:", "Optimized plan:", "Aggregate", "Filter(status >= 500)", "Scan(main)"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("explain output missing %q, got:\n%s", want, stdout)
		}
	}
}
