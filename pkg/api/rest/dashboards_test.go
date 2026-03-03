package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
)

func validDashboardInput() map[string]interface{} {
	return map[string]interface{}{
		"name": "ops-overview",
		"panels": []map[string]interface{}{
			{
				"id":    "p1",
				"title": "Error Rate",
				"type":  "timechart",
				"q":     "FROM main | stats count by host",
				"position": map[string]interface{}{
					"x": 0, "y": 0, "w": 6, "h": 4,
				},
			},
			{
				"id":    "p2",
				"title": "Top Hosts",
				"type":  "table",
				"q":     "FROM main | stats count by host | sort -count",
				"position": map[string]interface{}{
					"x": 6, "y": 0, "w": 6, "h": 4,
				},
			},
		},
	}
}

func TestDashboards_CRUD(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	// List — empty.
	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/dashboards", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("list status: %d", resp.StatusCode)
	}
	var listResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&listResp)
	dashes := listResp["data"].([]interface{})
	if len(dashes) != 0 {
		t.Fatalf("expected 0 dashboards, got %d", len(dashes))
	}

	// Create.
	body, _ := json.Marshal(validDashboardInput())
	resp2, err := http.Post(fmt.Sprintf("http://%s/api/v1/dashboards", srv.Addr()), "application/json", bytes.NewReader(body))
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
	if created["name"] != "ops-overview" {
		t.Fatalf("name: %v", created["name"])
	}
	panels := created["panels"].([]interface{})
	if len(panels) != 2 {
		t.Fatalf("panels: %d", len(panels))
	}

	// Get.
	resp3, err := http.Get(fmt.Sprintf("http://%s/api/v1/dashboards/%s", srv.Addr(), id))
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != 200 {
		t.Fatalf("get status: %d", resp3.StatusCode)
	}

	// Update.
	updateInput := validDashboardInput()
	updateInput["name"] = "ops-overview-v2"
	updateBody, _ := json.Marshal(updateInput)
	req, _ := http.NewRequest("PUT", fmt.Sprintf("http://%s/api/v1/dashboards/%s", srv.Addr(), id), bytes.NewReader(updateBody))
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
	if updated["name"] != "ops-overview-v2" {
		t.Fatalf("updated name: %v", updated["name"])
	}

	// Delete.
	delReq, _ := http.NewRequest("DELETE", fmt.Sprintf("http://%s/api/v1/dashboards/%s", srv.Addr(), id), http.NoBody)
	resp5, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatal(err)
	}
	defer resp5.Body.Close()
	if resp5.StatusCode != 204 {
		t.Fatalf("delete status: %d", resp5.StatusCode)
	}

	// Get after delete — 404.
	resp6, err := http.Get(fmt.Sprintf("http://%s/api/v1/dashboards/%s", srv.Addr(), id))
	if err != nil {
		t.Fatalf("GET after delete: %v", err)
	}
	defer resp6.Body.Close()
	if resp6.StatusCode != 404 {
		t.Fatalf("get-after-delete: %d", resp6.StatusCode)
	}
}

func TestDashboards_ValidationErrors(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	// Missing name.
	body, _ := json.Marshal(map[string]interface{}{
		"panels": []map[string]interface{}{
			{"id": "p1", "title": "X", "type": "table", "q": "FROM main",
				"position": map[string]interface{}{"x": 0, "y": 0, "w": 6, "h": 4}},
		},
	})
	resp, _ := http.Post(fmt.Sprintf("http://%s/api/v1/dashboards", srv.Addr()), "application/json", bytes.NewReader(body))
	resp.Body.Close()
	if resp.StatusCode != 422 {
		t.Fatalf("missing name: %d, want 422", resp.StatusCode)
	}

	// Missing panels.
	body2, _ := json.Marshal(map[string]interface{}{"name": "test"})
	resp2, _ := http.Post(fmt.Sprintf("http://%s/api/v1/dashboards", srv.Addr()), "application/json", bytes.NewReader(body2))
	resp2.Body.Close()
	if resp2.StatusCode != 422 {
		t.Fatalf("missing panels: %d, want 422", resp2.StatusCode)
	}

	// Invalid panel type.
	body3, _ := json.Marshal(map[string]interface{}{
		"name": "test",
		"panels": []map[string]interface{}{
			{"id": "p1", "title": "X", "type": "invalid", "q": "FROM main",
				"position": map[string]interface{}{"x": 0, "y": 0, "w": 6, "h": 4}},
		},
	})
	resp3, _ := http.Post(fmt.Sprintf("http://%s/api/v1/dashboards", srv.Addr()), "application/json", bytes.NewReader(body3))
	resp3.Body.Close()
	if resp3.StatusCode != 422 {
		t.Fatalf("invalid panel type: %d, want 422", resp3.StatusCode)
	}
}

func TestDashboards_DuplicateName(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(validDashboardInput())
	resp, _ := http.Post(fmt.Sprintf("http://%s/api/v1/dashboards", srv.Addr()), "application/json", bytes.NewReader(body))
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("first create: %d", resp.StatusCode)
	}

	resp2, _ := http.Post(fmt.Sprintf("http://%s/api/v1/dashboards", srv.Addr()), "application/json", bytes.NewReader(body))
	resp2.Body.Close()
	if resp2.StatusCode != 409 {
		t.Fatalf("duplicate: %d, want 409", resp2.StatusCode)
	}
}

func TestDashboards_NotFound(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/dashboards/nonexistent", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("status: %d, want 404", resp.StatusCode)
	}
}
