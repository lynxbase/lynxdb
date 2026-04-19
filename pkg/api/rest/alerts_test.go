package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
)

func TestServer_AlertsPatchEnabled(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	createBody, _ := json.Marshal(map[string]interface{}{
		"name":     "toggle-me",
		"query":    `FROM main | stats count`,
		"interval": "1m",
		"channels": []map[string]interface{}{
			{
				"type": "webhook",
				"config": map[string]interface{}{
					"url": "https://example.com/hook",
				},
			},
		},
	})
	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/alerts", srv.Addr()),
		"application/json",
		bytes.NewReader(createBody),
	)
	if err != nil {
		t.Fatalf("POST create: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("create status: got %d, body: %s", resp.StatusCode, string(body))
	}

	var created map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	data := created["data"].(map[string]interface{})
	id := data["id"].(string)

	patchBody := bytes.NewBufferString(`{"enabled":false}`)
	req, _ := http.NewRequest(http.MethodPatch,
		fmt.Sprintf("http://%s/api/v1/alerts/%s", srv.Addr(), id),
		patchBody)
	req.Header.Set("Content-Type", "application/json")

	patchResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PATCH: %v", err)
	}
	defer patchResp.Body.Close()
	if patchResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(patchResp.Body)
		t.Fatalf("patch status: got %d, body: %s", patchResp.StatusCode, string(body))
	}

	var patched map[string]interface{}
	if err := json.NewDecoder(patchResp.Body).Decode(&patched); err != nil {
		t.Fatalf("decode patch: %v", err)
	}
	patchedData := patched["data"].(map[string]interface{})
	if enabled := patchedData["enabled"].(bool); enabled {
		t.Fatalf("patched alert enabled = true, want false")
	}

	getResp, err := http.Get(fmt.Sprintf("http://%s/api/v1/alerts/%s", srv.Addr(), id))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer getResp.Body.Close()

	var fetched map[string]interface{}
	if err := json.NewDecoder(getResp.Body).Decode(&fetched); err != nil {
		t.Fatalf("decode get: %v", err)
	}
	fetchedData := fetched["data"].(map[string]interface{})
	if enabled := fetchedData["enabled"].(bool); enabled {
		t.Fatalf("fetched alert enabled = true, want false")
	}
}

func TestServer_AlertsRejectUnsupportedChannel(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":     "unsupported-channel",
		"query":    `FROM main | stats count`,
		"interval": "1m",
		"channels": []map[string]interface{}{
			{
				"type": "pagerduty",
				"config": map[string]interface{}{
					"routing_key": "key",
				},
			},
		},
	})
	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/alerts", srv.Addr()),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnprocessableEntity {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 422, body: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	errObj := result["error"].(map[string]interface{})
	if errObj["code"] != "VALIDATION_ERROR" {
		t.Fatalf("error code = %v, want VALIDATION_ERROR", errObj["code"])
	}
	if msg := errObj["message"].(string); msg != "alerts: unknown channel type" {
		t.Fatalf("error message = %q, want %q", msg, "alerts: unknown channel type")
	}
}
