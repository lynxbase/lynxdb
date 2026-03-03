package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
)

func TestQueries_CRUD(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	// List — empty.
	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/queries", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("list status: %d", resp.StatusCode)
	}
	var listResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&listResp)
	queries := listResp["data"].([]interface{})
	if len(queries) != 0 {
		t.Fatalf("expected 0 queries, got %d", len(queries))
	}

	// Create.
	body, _ := json.Marshal(map[string]interface{}{
		"name": "error-search",
		"q":    "FROM main | search \"error\"",
		"from": "-1h",
	})
	resp2, err := http.Post(fmt.Sprintf("http://%s/api/v1/queries", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 201 {
		b, _ := io.ReadAll(resp2.Body)
		t.Fatalf("create status: %d, body: %s", resp2.StatusCode, b)
	}
	var createResp map[string]interface{}
	json.NewDecoder(resp2.Body).Decode(&createResp)
	created := createResp["data"].(map[string]interface{})
	id := created["id"].(string)
	if id == "" {
		t.Fatal("missing id")
	}
	if created["name"] != "error-search" {
		t.Fatalf("name: %v", created["name"])
	}

	// Get.
	resp3, err := http.Get(fmt.Sprintf("http://%s/api/v1/queries/%s", srv.Addr(), id))
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != 200 {
		t.Fatalf("get status: %d", resp3.StatusCode)
	}
	var getResp map[string]interface{}
	json.NewDecoder(resp3.Body).Decode(&getResp)
	got := getResp["data"].(map[string]interface{})
	if got["q"] != "FROM main | search \"error\"" {
		t.Fatalf("q: %v", got["q"])
	}

	// Update.
	updateBody, _ := json.Marshal(map[string]interface{}{
		"name": "error-search-v2",
		"q":    "FROM main | search \"error\" | head 100",
	})
	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/api/v1/queries/%s", srv.Addr(), id), bytes.NewReader(updateBody))
	req.Header.Set("Content-Type", "application/json")
	resp4, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp4.Body.Close()
	if resp4.StatusCode != 200 {
		b, _ := io.ReadAll(resp4.Body)
		t.Fatalf("update status: %d, body: %s", resp4.StatusCode, b)
	}
	var updateResp map[string]interface{}
	json.NewDecoder(resp4.Body).Decode(&updateResp)
	updated := updateResp["data"].(map[string]interface{})
	if updated["name"] != "error-search-v2" {
		t.Fatalf("updated name: %v", updated["name"])
	}

	// List — should have 1.
	resp5, err := http.Get(fmt.Sprintf("http://%s/api/v1/queries", srv.Addr()))
	if err != nil {
		t.Fatalf("GET queries: %v", err)
	}
	defer resp5.Body.Close()
	var listResp2 map[string]interface{}
	json.NewDecoder(resp5.Body).Decode(&listResp2)
	queries2 := listResp2["data"].([]interface{})
	if len(queries2) != 1 {
		t.Fatalf("expected 1 query, got %d", len(queries2))
	}

	// Delete.
	delReq, _ := http.NewRequest("DELETE", fmt.Sprintf("http://%s/api/v1/queries/%s", srv.Addr(), id), http.NoBody)
	resp6, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatal(err)
	}
	defer resp6.Body.Close()
	if resp6.StatusCode != 204 {
		t.Fatalf("delete status: %d", resp6.StatusCode)
	}

	// Get after delete — 404.
	resp7, err := http.Get(fmt.Sprintf("http://%s/api/v1/queries/%s", srv.Addr(), id))
	if err != nil {
		t.Fatalf("GET after delete: %v", err)
	}
	defer resp7.Body.Close()
	if resp7.StatusCode != 404 {
		t.Fatalf("get-after-delete status: %d", resp7.StatusCode)
	}
}

func TestQueries_ValidationErrors(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	// Missing name.
	body, _ := json.Marshal(map[string]interface{}{
		"q": "FROM main",
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/queries", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 422 {
		t.Fatalf("status: %d, want 422", resp.StatusCode)
	}

	// Missing query.
	body2, _ := json.Marshal(map[string]interface{}{
		"name": "test",
	})
	resp2, err := http.Post(fmt.Sprintf("http://%s/api/v1/queries", srv.Addr()), "application/json", bytes.NewReader(body2))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 422 {
		t.Fatalf("status: %d, want 422", resp2.StatusCode)
	}
}

func TestQueries_DuplicateName(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name": "my-query",
		"q":    "FROM main",
	})

	// First create — success.
	resp, _ := http.Post(fmt.Sprintf("http://%s/api/v1/queries", srv.Addr()), "application/json", bytes.NewReader(body))
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("first create: %d", resp.StatusCode)
	}

	// Duplicate — conflict.
	resp2, _ := http.Post(fmt.Sprintf("http://%s/api/v1/queries", srv.Addr()), "application/json", bytes.NewReader(body))
	resp2.Body.Close()
	if resp2.StatusCode != 409 {
		t.Fatalf("duplicate: %d, want 409", resp2.StatusCode)
	}
}

func TestQueries_NotFound(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/queries/nonexistent", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("status: %d, want 404", resp.StatusCode)
	}
}
