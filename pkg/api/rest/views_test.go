package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
)

func TestViews_PatchRetention(t *testing.T) {
	srv, _, cleanup := startDiskTestServer(t)
	defer cleanup()

	// Create a view first.
	createBody, _ := json.Marshal(map[string]interface{}{
		"name":  "test_view",
		"query": "FROM main | stats count by host",
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/views", srv.Addr()), "application/json", bytes.NewReader(createBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 201 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("create status: %d, body: %s", resp.StatusCode, b)
	}

	// Patch retention.
	retention := "720h" // 30 days
	patchBody, _ := json.Marshal(map[string]interface{}{
		"retention": retention,
	})
	req, _ := http.NewRequest("PATCH", fmt.Sprintf("http://%s/api/v1/views/test_view", srv.Addr()), bytes.NewReader(patchBody))
	req.Header.Set("Content-Type", "application/json")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != 200 {
		b, _ := io.ReadAll(resp2.Body)
		t.Fatalf("patch status: %d, body: %s", resp2.StatusCode, b)
	}

	var result map[string]interface{}
	json.NewDecoder(resp2.Body).Decode(&result)
	data := result["data"].(map[string]interface{})
	if data["updated"] != true {
		t.Fatal("expected updated=true")
	}
}

func TestViews_PatchNotFound(t *testing.T) {
	srv, _, cleanup := startDiskTestServer(t)
	defer cleanup()

	patchBody, _ := json.Marshal(map[string]interface{}{
		"retention": "720h",
	})
	req, _ := http.NewRequest("PATCH", fmt.Sprintf("http://%s/api/v1/views/nonexistent", srv.Addr()), bytes.NewReader(patchBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 404 {
		t.Fatalf("status: %d, want 404", resp.StatusCode)
	}
}

func TestViews_Backfill(t *testing.T) {
	srv, _, cleanup := startDiskTestServer(t)
	defer cleanup()

	// Create a view.
	createBody, _ := json.Marshal(map[string]interface{}{
		"name":  "bf_view",
		"query": "FROM main",
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/views", srv.Addr()), "application/json", bytes.NewReader(createBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 201 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("create status: %d, body: %s", resp.StatusCode, b)
	}

	// Get backfill status.
	resp2, err := http.Get(fmt.Sprintf("http://%s/api/v1/views/bf_view/backfill", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != 200 {
		t.Fatalf("status: %d", resp2.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp2.Body).Decode(&result)
	data := result["data"].(map[string]interface{})

	// After creation, the view starts in "backfill" status and transitions to
	// "active" once the async backfill goroutine completes. With no data in the
	// store, backfill completes almost instantly, so either status is valid.
	status, _ := data["status"].(string)
	if status != "backfill" && status != "active" {
		t.Fatalf("status: %v, want backfill or active", data["status"])
	}
}

func TestViews_BackfillNotFound(t *testing.T) {
	srv, _, cleanup := startDiskTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/views/nonexistent/backfill", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 404 {
		t.Fatalf("status: %d, want 404", resp.StatusCode)
	}
}

func TestViews_RouteAlias(t *testing.T) {
	srv, _, cleanup := startDiskTestServer(t)
	defer cleanup()

	// /api/v1/views should list views.
	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/views", srv.Addr()))
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
	if data["views"] == nil {
		t.Fatal("missing views in response")
	}
}
