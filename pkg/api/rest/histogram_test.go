package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestHistogram_Basic(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 50, 5)

	// One hour around the events, starting on a bucket boundary.
	from := time.Now().UTC().Truncate(5 * time.Minute).Add(-30 * time.Minute)
	to := from.Add(time.Hour)
	u := fmt.Sprintf("http://%s/api/v1/histogram?buckets=12&from=%s&to=%s",
		srv.Addr(), url.QueryEscape(from.Format(time.RFC3339)), url.QueryEscape(to.Format(time.RFC3339)))
	data := getData(t, u)

	if data["interval"] != "5m0s" {
		t.Fatalf("interval = %v, want 5m0s", data["interval"])
	}
	buckets := data["buckets"].([]interface{})
	if len(buckets) != 12 {
		t.Fatalf("buckets: got %d, want 12", len(buckets))
	}
	var sum float64
	for i, b := range buckets {
		bucket := b.(map[string]interface{})
		if want := from.Add(time.Duration(i) * 5 * time.Minute).Format(time.RFC3339); bucket["time"] != want {
			t.Fatalf("bucket %d time = %v, want %s", i, bucket["time"], want)
		}
		sum += bucket["count"].(float64)
	}
	if sum != 50 {
		t.Fatalf("sum of bucket counts: got %v, want 50", sum)
	}
	total := int(data["total"].(float64))
	if total != 50 {
		t.Fatalf("total: got %d, want 50", total)
	}
}

func TestHistogram_Empty(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	data := getData(t, fmt.Sprintf("http://%s/api/v1/histogram?from=-1h&to=now", srv.Addr()))
	total := int(data["total"].(float64))
	if total != 0 {
		t.Fatalf("total: got %d, want 0", total)
	}
}

func TestHistogram_DefaultRangeWithMeta(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 10, 2)

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/histogram?buckets=5", srv.Addr()))
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
	data := result["data"].(map[string]interface{})
	if total := data["total"]; total != float64(10) {
		t.Fatalf("total over the last hour: got %v, want 10", total)
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
