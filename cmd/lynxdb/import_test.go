package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizeImportJSONLine_GenericDocument(t *testing.T) {
	line := `{"@timestamp":"2026-02-14T12:00:00Z","host":"web-01","status":200,"message":"ok","nested":{"a":1}}`

	ev, err := normalizeImportJSONLine(line, importOptions{
		source: "default-source",
		index:  "default-index",
	})
	if err != nil {
		t.Fatalf("normalizeImportJSONLine: %v", err)
	}

	if ev.Event != line {
		t.Fatalf("Event = %q, want original JSON line", ev.Event)
	}
	if ev.Host != "web-01" {
		t.Fatalf("Host = %q, want web-01", ev.Host)
	}
	if ev.Index != "default-index" {
		t.Fatalf("Index = %q, want default-index", ev.Index)
	}
	if ev.Time == nil {
		t.Fatal("expected parsed timestamp")
	}
	if got := ev.Fields["status"]; got != int64(200) {
		t.Fatalf("status field = %#v, want int64(200)", got)
	}
	if got := ev.Fields["message"]; got != "ok" {
		t.Fatalf("message field = %#v, want ok", got)
	}
	if _, ok := ev.Fields["nested"]; ok {
		t.Fatal("nested object should stay in raw JSON, not structured fields")
	}
}

func TestNormalizeImportJSONObject_StructuredEnvelope(t *testing.T) {
	ev, err := normalizeImportJSONObject(map[string]interface{}{
		"event": "hello",
		"fields": map[string]interface{}{
			"status": 200,
			"ok":     true,
			"nested": map[string]interface{}{"drop": true},
		},
	}, "", importOptions{source: "cli-source", index: "cli-index"})
	if err != nil {
		t.Fatalf("normalizeImportJSONObject: %v", err)
	}

	if ev.Event != "hello" {
		t.Fatalf("Event = %q, want hello", ev.Event)
	}
	if ev.Source != "cli-source" {
		t.Fatalf("Source = %q, want cli-source", ev.Source)
	}
	if ev.Index != "cli-index" {
		t.Fatalf("Index = %q, want cli-index", ev.Index)
	}
	if got := ev.Fields["status"]; got != 200 {
		t.Fatalf("status field = %#v, want 200", got)
	}
	if _, ok := ev.Fields["nested"]; ok {
		t.Fatal("nested structured field should be dropped")
	}
}

func TestImportCommand_NDJSONEndToEnd(t *testing.T) {
	baseURL := newTestServer(t)
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "events.ndjson")
	body := "" +
		"{\"@timestamp\":\"2026-02-14T12:00:00Z\",\"host\":\"web-01\",\"status\":200,\"message\":\"ok\",\"nested\":{\"k\":1}}\n" +
		"{\"@timestamp\":\"2026-02-14T12:01:00Z\",\"host\":\"web-02\",\"status\":500,\"message\":\"fail\"}\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, _, err := runCmd(t, "--server", baseURL, "import", path, "--index", "imported"); err != nil {
		t.Fatalf("import failed: %v", err)
	}

	stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json",
		`FROM imported | stats count by status`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestImportCommand_ESBulkRejectsIndexOverride(t *testing.T) {
	baseURL := newTestServer(t)
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "events.bulk")
	body := "" +
		"{\"index\":{\"_index\":\"logs\"}}\n" +
		"{\"message\":\"hello\"}\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, _, err := runCmd(t, "--server", baseURL, "import", path, "--format", "esbulk", "--index", "custom")
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); got == "" || !containsAll(got, "--index", "esbulk") {
		t.Fatalf("error = %q, want esbulk index guidance", got)
	}
}

func containsAll(s string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(s, part) {
			return false
		}
	}

	return true
}
