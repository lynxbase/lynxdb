package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
)

func TestHistogram_Basic(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 50, 5)

	// Use a generous future window since ingestTestEvents uses now + i seconds.
	u := fmt.Sprintf("http://%s/api/v1/histogram?from=-1h&buckets=10&to=%s",
		srv.Addr(),
		url.QueryEscape("2099-01-01T00:00:00Z"))
	resp, err := http.Get(u)
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

	if data["interval"] == nil {
		t.Fatal("missing interval")
	}
	buckets := data["buckets"].([]interface{})
	if len(buckets) != 10 {
		t.Fatalf("buckets: got %d, want 10", len(buckets))
	}
	total := int(data["total"].(float64))
	if total != 50 {
		t.Fatalf("total: got %d, want 50", total)
	}
}

func TestHistogram_Empty(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	u := fmt.Sprintf("http://%s/api/v1/histogram?from=-1h&to=now", srv.Addr())
	resp, err := http.Get(u)
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
	total := int(data["total"].(float64))
	if total != 0 {
		t.Fatalf("total: got %d, want 0", total)
	}
}

func TestHistogram_WithMeta(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 10, 2)

	u := fmt.Sprintf("http://%s/api/v1/histogram?from=-1h&to=now&buckets=5", srv.Addr())
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	meta, ok := result["meta"].(map[string]interface{})
	if !ok {
		t.Fatal("missing meta")
	}
	if meta["took_ms"] == nil {
		t.Fatal("missing took_ms")
	}
}

func TestHistogram_InvalidRange(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	u := fmt.Sprintf("http://%s/api/v1/histogram?from=%s&to=%s",
		srv.Addr(),
		url.QueryEscape("2026-01-02T00:00:00Z"),
		url.QueryEscape("2026-01-01T00:00:00Z"))
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Fatalf("status: %d, want 400", resp.StatusCode)
	}
}
