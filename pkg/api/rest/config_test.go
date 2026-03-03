package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

func TestConfig_Get(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/config", srv.Addr()))
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
	if data["log_level"] == nil {
		t.Fatal("missing log_level in config")
	}
}

func TestConfig_Patch(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"log_level": "debug",
	})
	req, _ := http.NewRequest("PATCH", fmt.Sprintf("http://%s/api/v1/config", srv.Addr()), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
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
	cfg := data["config"].(map[string]interface{})
	if cfg["log_level"] != "debug" {
		t.Fatalf("log_level: %v", cfg["log_level"])
	}
}

func TestConfig_PatchRestartRequired(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"listen": "0.0.0.0:4000",
	})
	req, _ := http.NewRequest("PATCH", fmt.Sprintf("http://%s/api/v1/config", srv.Addr()), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
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
	restart, ok := data["restart_required"].([]interface{})
	if !ok || len(restart) == 0 {
		t.Fatal("expected restart_required")
	}
}

func TestConfig_PatchEmpty(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	req, _ := http.NewRequest("PATCH", fmt.Sprintf("http://%s/api/v1/config", srv.Addr()), bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Fatalf("status: %d, want 400", resp.StatusCode)
	}
}

func TestConfig_PatchUnknownKey(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"unknown_field": "value",
	})
	req, _ := http.NewRequest("PATCH", fmt.Sprintf("http://%s/api/v1/config", srv.Addr()), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Fatalf("status: %d, want 400", resp.StatusCode)
	}
}
