//go:build e2e

package e2e

import (
	"context"
	"strings"
	"testing"
)

func TestE2E_OTLP_IngestLogs(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	// Minimal OTLP JSON log payload.
	otlpJSON := `{
		"resourceLogs": [{
			"resource": {
				"attributes": [
					{"key": "service.name", "value": {"stringValue": "test-svc"}}
				]
			},
			"scopeLogs": [{
				"logRecords": [
					{
						"timeUnixNano": "1700000000000000000",
						"severityText": "INFO",
						"body": {"stringValue": "OTLP test log line 1"}
					},
					{
						"timeUnixNano": "1700000001000000000",
						"severityText": "ERROR",
						"body": {"stringValue": "OTLP test log line 2"}
					}
				]
			}]
		}]
	}`

	resp, err := h.Client().OTLPIngestLogs(ctx, strings.NewReader(otlpJSON), "application/json")
	if err != nil {
		t.Fatalf("OTLPIngestLogs: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		t.Errorf("expected success status, got %d", resp.StatusCode)
	}
	t.Logf("OTLP ingest: status=%d", resp.StatusCode)
}
