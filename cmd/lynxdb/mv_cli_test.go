package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// setupDiskServerWithData starts a disk-backed server and ingests access.log.
// Materialized views require disk persistence.
func setupDiskServerWithData(t *testing.T) string {
	t.Helper()

	baseURL := newTestServerWithDisk(t)
	ingestTestData(t, baseURL, "main", "testdata/logs/access.log")

	return baseURL
}

func TestMVList_EmptyServer(t *testing.T) {
	baseURL := newTestServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "mv", "list")
	if err != nil {
		t.Fatalf("mv list failed: %v", err)
	}

	// Empty server should show "No materialized views" or empty output.
	if strings.TrimSpace(stdout) == "" {
		return // acceptable
	}

	if !strings.Contains(stdout, "No materialized views") {
		t.Errorf("expected empty-state message, got: %q", stdout)
	}
}

func TestMVCreateAndList(t *testing.T) {
	baseURL := setupDiskServerWithData(t)

	_, _, err := runCmd(t, "--server", baseURL, "mv", "create",
		"mv_test_levels",
		"level=error | stats count by level")
	if err != nil {
		t.Fatalf("mv create failed: %v", err)
	}

	// List should now include the view.
	stdout, _, err := runCmd(t, "--server", baseURL, "mv", "list", "--format", "json")
	if err != nil {
		t.Fatalf("mv list failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	found := false

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		var view map[string]interface{}
		if err := json.Unmarshal([]byte(line), &view); err != nil {
			t.Fatalf("parse view JSON: %v\nline: %q", err, line)
		}

		if name, _ := view["name"].(string); name == "mv_test_levels" {
			found = true

			break
		}
	}

	if !found {
		t.Errorf("mv list output does not contain 'mv_test_levels':\n%s", stdout)
	}
}

func TestMVStatus(t *testing.T) {
	baseURL := setupDiskServerWithData(t)

	_, _, err := runCmd(t, "--server", baseURL, "mv", "create",
		"mv_status_test",
		"level=error | stats count by level")
	if err != nil {
		t.Fatalf("mv create failed: %v", err)
	}

	stdout, _, err := runCmd(t, "--server", baseURL, "mv", "status",
		"--format", "json", "mv_status_test")
	if err != nil {
		t.Fatalf("mv status failed: %v", err)
	}

	var detail map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &detail); err != nil {
		t.Fatalf("parse status JSON: %v\noutput: %q", err, stdout)
	}

	if name, _ := detail["name"].(string); name != "mv_status_test" {
		t.Errorf("expected name='mv_status_test', got %q", name)
	}

	if _, ok := detail["query"]; !ok {
		t.Errorf("status JSON missing 'query' key")
	}
}

func TestMVDrop_Force(t *testing.T) {
	baseURL := setupDiskServerWithData(t)

	_, _, err := runCmd(t, "--server", baseURL, "mv", "create",
		"mv_drop_test",
		"level=error | stats count")
	if err != nil {
		t.Fatalf("mv create failed: %v", err)
	}

	// Drop with --force (no interactive confirmation).
	_, _, err = runCmd(t, "--server", baseURL, "mv", "drop", "--force", "mv_drop_test")
	if err != nil {
		t.Fatalf("mv drop --force failed: %v", err)
	}

	// List should no longer contain it.
	stdout, _, err := runCmd(t, "--server", baseURL, "mv", "list", "--format", "json")
	if err != nil {
		t.Fatalf("mv list after drop failed: %v", err)
	}

	if strings.Contains(stdout, "mv_drop_test") {
		t.Errorf("view 'mv_drop_test' still present after drop:\n%s", stdout)
	}
}
