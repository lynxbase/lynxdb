//go:build e2e

package e2e

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	logscollector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

func postOTLP(t *testing.T, url, contentType string, body []byte) {
	t.Helper()
	resp, err := http.Post(url, contentType, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s (%s): status %d", url, contentType, resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); got != contentType {
		t.Fatalf("POST %s: response content type %q, want %q", url, got, contentType)
	}
}

// TestE2E_OTLP_IngestLogs sends one record as OTLP/JSON and one as protobuf
// to the OTLP/HTTP listener, and reads both back the same way.
func TestE2E_OTLP_IngestLogs(t *testing.T) {
	h := NewHarness(t, WithOTLPHTTP())

	otlpJSON := `{
		"resourceLogs": [{
			"resource": {"attributes": [
				{"key": "service.name", "value": {"stringValue": "test-svc"}},
				{"key": "deployment.environment", "value": {"stringValue": "prod"}}
			]},
			"scopeLogs": [{
				"logRecords": [{
					"timeUnixNano": "1700000000000000000",
					"severityText": "INFO",
					"traceId": "5b8efff798038103d269b633813fc60c",
					"body": {"stringValue": "OTLP test log line 1"}
				}]
			}]
		}]
	}`
	postOTLP(t, h.OTLPHTTPURL()+"/v1/logs", "application/json", []byte(otlpJSON))

	body, err := proto.Marshal(&logscollector.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-svc"}}},
		}},
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
			TimeUnixNano: 1700000001000000000,
			SeverityText: "ERROR",
			TraceId:      []byte{0x5b, 0x8e, 0xff, 0xf7, 0x98, 0x03, 0x81, 0x03, 0xd2, 0x69, 0xb6, 0x33, 0x81, 0x3f, 0xc6, 0x0c},
			Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "OTLP test log line 2"}},
		}}}},
	}}})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	postOTLP(t, h.OTLPHTTPURL()+"/v1/logs", "application/x-protobuf", body)

	// Both records go to main whatever their resource says; service.name
	// becomes the source.
	r := h.MustQuery(`source=main | sort _time | fields _time, _raw, level, source, sourcetype, trace_id`)
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
			row["source"] != "test-svc" || row["sourcetype"] != "otlp" || row["trace_id"] != "5b8efff798038103d269b633813fc60c" {
			t.Errorf("event %d: expected _time=%s _raw=%q level=%s source=test-svc sourcetype=otlp trace_id=5b8e..., got %v",
				i, want.time, want.raw, want.level, row)
		}
	}
}
