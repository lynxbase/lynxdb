package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestServer_OTLPIngest(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ts := time.Now().UnixNano()
	payload := fmt.Sprintf(`{
		"resourceLogs": [{
			"resource": {
				"attributes": [
					{"key": "service.name", "value": {"stringValue": "test-svc"}},
					{"key": "host.name", "value": {"stringValue": "host-01"}}
				]
			},
			"scopeLogs": [{
				"scope": {"name": "test-lib"},
				"logRecords": [{
					"timeUnixNano": "%d",
					"severityNumber": 9,
					"body": {"stringValue": "hello from OTLP"}
				}]
			}]
		}]
	}`, ts)

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/otlp/v1/logs", srv.Addr()),
		"application/json",
		bytes.NewBufferString(payload),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200, body: %s", resp.StatusCode, string(body))
	}
}

func TestServer_OTLPIngest_InvalidJSON(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/otlp/v1/logs", srv.Addr()),
		"application/json",
		bytes.NewBufferString("not json{{{"),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	errObj, ok := result["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected structured error, got: %v", result)
	}
	if errObj["code"] != "INVALID_JSON" {
		t.Errorf("code: got %v, want INVALID_JSON", errObj["code"])
	}
}

func TestServer_OTLPIngest_EmptyPayload(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/otlp/v1/logs", srv.Addr()),
		"application/json",
		bytes.NewBufferString(`{"resourceLogs": []}`),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200, body: %s", resp.StatusCode, string(body))
	}
}

func TestServer_OTLPIngest_ProtobufRejected(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	req, _ := http.NewRequest("POST",
		fmt.Sprintf("http://%s/api/v1/otlp/v1/logs", srv.Addr()),
		bytes.NewBufferString("binary data"))
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status: got %d, want 415", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	errObj, ok := result["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected structured error, got: %v", result)
	}
	if errObj["code"] != "INVALID_REQUEST" {
		t.Errorf("code: got %v, want INVALID_REQUEST", errObj["code"])
	}
}

func TestServer_OTLPIngestAndQuery(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ts := time.Now()
	payload := fmt.Sprintf(`{
		"resourceLogs": [{
			"resource": {
				"attributes": [
					{"key": "service.name", "value": {"stringValue": "payment-svc"}},
					{"key": "host.name", "value": {"stringValue": "otlp-host"}}
				]
			},
			"scopeLogs": [{
				"scope": {"name": "payments"},
				"logRecords": [
					{
						"timeUnixNano": "%d",
						"severityNumber": 9,
						"body": {"stringValue": "OTLP event one"}
					},
					{
						"timeUnixNano": "%d",
						"severityNumber": 13,
						"body": {"stringValue": "OTLP event two"}
					},
					{
						"timeUnixNano": "%d",
						"severityNumber": 17,
						"body": {"stringValue": "OTLP event three"}
					}
				]
			}]
		}]
	}`, ts.UnixNano(), ts.Add(time.Second).UnixNano(), ts.Add(2*time.Second).UnixNano())

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/otlp/v1/logs", srv.Addr()),
		"application/json",
		bytes.NewBufferString(payload),
	)
	if err != nil {
		t.Fatalf("POST ingest: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ingest status: %d", resp.StatusCode)
	}

	// In-memory mode: events are immediately flushed to segments (no batcher).
	if srv.engine.SegmentCount() == 0 {
		t.Fatal("expected segments after OTLP ingest in in-memory mode")
	}

	// Query for the OTLP events.
	searchBody, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | search "OTLP" | head 10`,
	})
	qResp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/query", srv.Addr()),
		"application/json",
		bytes.NewReader(searchBody),
	)
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	defer qResp.Body.Close()

	if qResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(qResp.Body)
		t.Fatalf("query status: %d, body: %s", qResp.StatusCode, string(body))
	}

	var result map[string]interface{}
	json.NewDecoder(qResp.Body).Decode(&result)
	data := result["data"].(map[string]interface{})
	events := data["events"].([]interface{})

	if len(events) != 3 {
		t.Errorf("events: got %d, want 3", len(events))
	}
}
