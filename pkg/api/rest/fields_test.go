package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

func TestFieldValues_Basic(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 30, 3)

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/fields/host/values", srv.Addr()))
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

	if data["field"] != "host" {
		t.Fatalf("field: %v", data["field"])
	}
	values := data["values"].([]interface{})
	if len(values) == 0 {
		t.Fatal("no values returned")
	}
	uniqueCount := int(data["unique_count"].(float64))
	if uniqueCount != 3 {
		t.Fatalf("unique_count: %d, want 3", uniqueCount)
	}
}

func TestFieldValues_Limit(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 50, 10)

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/fields/host/values?limit=3", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})
	values := data["values"].([]interface{})
	if len(values) != 3 {
		t.Fatalf("values: got %d, want 3", len(values))
	}
}

func TestFieldValues_NonExistent(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 10, 2)

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/fields/nonexistent/values", srv.Addr()))
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
	if int(data["total_count"].(float64)) != 0 {
		t.Fatalf("total_count: %v", data["total_count"])
	}
}

func TestSources_Basic(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 20, 4)

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/sources", srv.Addr()))
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
	sources := data["sources"].([]interface{})
	if len(sources) == 0 {
		t.Fatal("no sources returned")
	}
	// All events have source "/var/log/app.log".
	src := sources[0].(map[string]interface{})
	if src["name"] != "/var/log/app.log" {
		t.Fatalf("source name: %v", src["name"])
	}
	if int(src["event_count"].(float64)) != 20 {
		t.Fatalf("event_count: %v", src["event_count"])
	}
}

func TestSources_Empty(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/sources", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})
	sources := data["sources"].([]interface{})
	if len(sources) != 0 {
		t.Fatalf("expected 0 sources, got %d", len(sources))
	}
}
