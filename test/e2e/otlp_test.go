//go:build e2e

package e2e

import (
	"context"
	"strings"
	"testing"
	"time"
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

	if resp.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	// Without a deployment.environment attribute the records go to main;
	// service.name becomes the source.
	r := h.MustQuery(`source=main | sort _time | fields _time, _raw, level, source, sourcetype`)
	rows := EventRows(r)
	if len(rows) != 2 {
		t.Fatalf("expected 2 events, got %d: %v", len(rows), rows)
	}
	for i, want := range []struct{ time, raw, level string }{
		{time.Unix(1700000000, 0).UTC().Format(time.RFC3339Nano), "OTLP test log line 1", "info"},
		{time.Unix(1700000001, 0).UTC().Format(time.RFC3339Nano), "OTLP test log line 2", "error"},
	} {
		row := rows[i]
		if row["_time"] != want.time || row["_raw"] != want.raw || row["level"] != want.level ||
			row["source"] != "test-svc" || row["sourcetype"] != "otlp" {
			t.Errorf("event %d: expected _time=%s _raw=%q level=%s source=test-svc sourcetype=otlp, got %v",
				i, want.time, want.raw, want.level, row)
		}
	}
}
