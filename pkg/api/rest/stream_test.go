package rest

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestQueryStream_Basic(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 50, 5)

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/query/stream", srv.Addr()),
		"application/json",
		strings.NewReader(`{"q":"FROM main"}`),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-ndjson" {
		t.Fatalf("content-type: %q", ct)
	}

	scanner := bufio.NewScanner(resp.Body)
	var lines []map[string]interface{}
	for scanner.Scan() {
		var obj map[string]interface{}
		if err := json.Unmarshal(scanner.Bytes(), &obj); err != nil {
			t.Fatalf("invalid JSON line: %v", err)
		}
		lines = append(lines, obj)
	}

	if len(lines) < 2 {
		t.Fatalf("expected at least 2 lines (events + meta), got %d", len(lines))
	}

	// Last line should be __meta.
	last := lines[len(lines)-1]
	meta, ok := last["__meta"].(map[string]interface{})
	if !ok {
		t.Fatalf("last line should be __meta, got: %v", last)
	}
	total := int(meta["total"].(float64))
	if total != 50 {
		t.Fatalf("total: got %d, want 50", total)
	}
	if meta["took_ms"] == nil {
		t.Fatal("missing took_ms")
	}
	if meta["scanned"] == nil {
		t.Fatal("missing scanned")
	}

	// Event lines should have _raw.
	for _, line := range lines[:len(lines)-1] {
		if _, hasRaw := line["_raw"]; !hasRaw {
			t.Fatalf("event line missing _raw: %v", line)
		}
	}
}

func TestQueryStream_EmptyResult(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/query/stream", srv.Addr()),
		"application/json",
		strings.NewReader(`{"q":"FROM main"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	var lines []map[string]interface{}
	for scanner.Scan() {
		var obj map[string]interface{}
		json.Unmarshal(scanner.Bytes(), &obj)
		lines = append(lines, obj)
	}

	if len(lines) != 1 {
		t.Fatalf("expected 1 line (meta only), got %d", len(lines))
	}
	meta := lines[0]["__meta"].(map[string]interface{})
	if int(meta["total"].(float64)) != 0 {
		t.Fatalf("total: %v", meta["total"])
	}
}

func TestQueryStream_StatsQuery(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 30, 3)

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/query/stream", srv.Addr()),
		"application/json",
		strings.NewReader(`{"q":"FROM main | stats count by host"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	var lines []map[string]interface{}
	for scanner.Scan() {
		var obj map[string]interface{}
		json.Unmarshal(scanner.Bytes(), &obj)
		lines = append(lines, obj)
	}

	// Should have 3 data lines + 1 meta line.
	if len(lines) < 2 {
		t.Fatalf("expected at least 2 lines, got %d", len(lines))
	}
	last := lines[len(lines)-1]
	if _, ok := last["__meta"]; !ok {
		t.Fatal("last line should be __meta")
	}
	// Data lines should have host and count.
	for _, line := range lines[:len(lines)-1] {
		if _, ok := line["host"]; !ok {
			t.Fatalf("missing host: %v", line)
		}
		if _, ok := line["count"]; !ok {
			t.Fatalf("missing count: %v", line)
		}
	}
}

func TestQueryStream_ParseError(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/query/stream", srv.Addr()),
		"application/json",
		strings.NewReader(`{"q":"INVALID @@@"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Fatalf("status: %d, want 400", resp.StatusCode)
	}
}

func TestQueryStream_MissingQuery(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/query/stream", srv.Addr()),
		"application/json",
		strings.NewReader(`{}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Fatalf("status: %d, want 400", resp.StatusCode)
	}
}
