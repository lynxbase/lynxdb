package receiver

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func strPtr(s string) *string     { return &s }
func floatPtr(f float64) *float64 { return &f }
func boolPtr(b bool) *bool        { return &b }

// toEventValue tests

func TestOTLPAnyValue_StringValue(t *testing.T) {
	v := toEventValue(OTLPAnyValue{StringValue: strPtr("hello")})
	if v.Type() != event.FieldTypeString || v.AsString() != "hello" {
		t.Errorf("got %v", v)
	}
}

func TestOTLPAnyValue_IntValue(t *testing.T) {
	v := toEventValue(OTLPAnyValue{IntValue: strPtr("42")})
	if v.Type() != event.FieldTypeInt || v.AsInt() != 42 {
		t.Errorf("got %v", v)
	}
}

func TestOTLPAnyValue_IntValueInvalid(t *testing.T) {
	v := toEventValue(OTLPAnyValue{IntValue: strPtr("not-a-number")})
	if v.Type() != event.FieldTypeString || v.AsString() != "not-a-number" {
		t.Errorf("got %v", v)
	}
}

func TestOTLPAnyValue_DoubleValue(t *testing.T) {
	v := toEventValue(OTLPAnyValue{DoubleValue: floatPtr(3.14)})
	if v.Type() != event.FieldTypeFloat || v.AsFloat() != 3.14 {
		t.Errorf("got %v", v)
	}
}

func TestOTLPAnyValue_BoolValue(t *testing.T) {
	v := toEventValue(OTLPAnyValue{BoolValue: boolPtr(true)})
	if v.Type() != event.FieldTypeBool || v.AsBool() != true {
		t.Errorf("got %v", v)
	}
}

func TestOTLPAnyValue_BoolValueFalse(t *testing.T) {
	v := toEventValue(OTLPAnyValue{BoolValue: boolPtr(false)})
	if v.Type() != event.FieldTypeBool || v.AsBool() != false {
		t.Errorf("got %v", v)
	}
}

func TestOTLPAnyValue_BytesValue(t *testing.T) {
	v := toEventValue(OTLPAnyValue{BytesValue: strPtr("AQID")})
	if v.Type() != event.FieldTypeString || v.AsString() != "AQID" {
		t.Errorf("got %v", v)
	}
}

func TestOTLPAnyValue_ArrayValue(t *testing.T) {
	v := toEventValue(OTLPAnyValue{
		ArrayValue: &OTLPArrayValue{
			Values: []OTLPAnyValue{
				{StringValue: strPtr("a")},
				{IntValue: strPtr("1")},
			},
		},
	})
	if v.Type() != event.FieldTypeString {
		t.Errorf("type: got %v, want string", v.Type())
	}
	// Should be valid JSON array representation.
	s := v.AsString()
	if s == "" {
		t.Error("empty string for array value")
	}
}

func TestOTLPAnyValue_KvlistValue(t *testing.T) {
	v := toEventValue(OTLPAnyValue{
		KvlistValue: &OTLPKvlistValue{
			Values: []OTLPKeyValue{
				{Key: "foo", Value: OTLPAnyValue{StringValue: strPtr("bar")}},
			},
		},
	})
	if v.Type() != event.FieldTypeString {
		t.Errorf("type: got %v, want string", v.Type())
	}
	s := v.AsString()
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	if m["foo"] != "bar" {
		t.Errorf("foo: got %v, want bar", m["foo"])
	}
}

func TestOTLPAnyValue_NullValue(t *testing.T) {
	v := toEventValue(OTLPAnyValue{})
	if !v.IsNull() {
		t.Errorf("expected null, got %v", v)
	}
}

// severity mapping tests

func TestOTLPSeverityMapping(t *testing.T) {
	tests := []struct {
		num  int
		want string
	}{
		{0, ""},
		{1, "trace"}, {2, "trace"}, {3, "trace"}, {4, "trace"},
		{5, "debug"}, {6, "debug"}, {7, "debug"}, {8, "debug"},
		{9, "info"}, {10, "info"}, {11, "info"}, {12, "info"},
		{13, "warn"}, {14, "warn"}, {15, "warn"}, {16, "warn"},
		{17, "error"}, {18, "error"}, {19, "error"}, {20, "error"},
		{21, "fatal"}, {22, "fatal"}, {23, "fatal"}, {24, "fatal"},
		{25, ""},
	}

	for _, tt := range tests {
		got := otlpSeverityText(tt.num, "")
		if got != tt.want {
			t.Errorf("severity(%d): got %q, want %q", tt.num, got, tt.want)
		}
	}
}

func TestOTLPSeverityText_Precedence(t *testing.T) {
	// Text takes precedence over number.
	got := otlpSeverityText(17, "WARNING")
	if got != "warning" {
		t.Errorf("got %q, want %q", got, "warning")
	}
}

// ToEvents tests

func TestToEvents_Basic(t *testing.T) {
	ts := time.Now().UnixNano()
	tsStr := json.Number(time.Now().Format("0")).String()
	_ = tsStr

	req := OTLPLogsRequest{
		ResourceLogs: []OTLPResourceLogs{
			{
				Resource: OTLPResource{
					Attributes: []OTLPKeyValue{
						{Key: "service.name", Value: OTLPAnyValue{StringValue: strPtr("my-service")}},
					},
				},
				ScopeLogs: []OTLPScopeLogs{
					{
						Scope: OTLPScope{Name: "my-lib", Version: "1.0.0"},
						LogRecords: []OTLPLogRecord{
							{
								TimeUnixNano:   formatNanos(ts),
								SeverityNumber: 9,
								Body:           OTLPAnyValue{StringValue: strPtr("log line 1")},
							},
							{
								TimeUnixNano:   formatNanos(ts + 1000),
								SeverityNumber: 17,
								SeverityText:   "ERROR",
								Body:           OTLPAnyValue{StringValue: strPtr("log line 2")},
							},
						},
					},
				},
			},
		},
	}

	events := req.ToEvents()
	if len(events) != 2 {
		t.Fatalf("events: got %d, want 2", len(events))
	}

	// First event.
	e := events[0]
	if e.Raw != "log line 1" {
		t.Errorf("Raw: got %q", e.Raw)
	}
	if e.Source != "my-service" {
		t.Errorf("Source: got %q", e.Source)
	}
	if e.SourceType != "otlp" {
		t.Errorf("SourceType: got %q", e.SourceType)
	}
	if e.Time.UnixNano() != ts {
		t.Errorf("Time: got %v, want %v", e.Time.UnixNano(), ts)
	}
	if e.Fields["level"].AsString() != "info" {
		t.Errorf("level: got %q", e.Fields["level"])
	}
	if e.Fields["otel.scope.name"].AsString() != "my-lib" {
		t.Errorf("otel.scope.name: got %q", e.Fields["otel.scope.name"])
	}
	if e.Fields["otel.scope.version"].AsString() != "1.0.0" {
		t.Errorf("otel.scope.version: got %q", e.Fields["otel.scope.version"])
	}

	// Second event: text precedence.
	e2 := events[1]
	if e2.Fields["level"].AsString() != "error" {
		t.Errorf("level: got %q, want error", e2.Fields["level"])
	}
}

func TestToEvents_ResourceMapping(t *testing.T) {
	req := OTLPLogsRequest{
		ResourceLogs: []OTLPResourceLogs{
			{
				Resource: OTLPResource{
					Attributes: []OTLPKeyValue{
						{Key: "service.name", Value: OTLPAnyValue{StringValue: strPtr("api-gw")}},
						{Key: "host.name", Value: OTLPAnyValue{StringValue: strPtr("prod-01")}},
						{Key: "deployment.environment", Value: OTLPAnyValue{StringValue: strPtr("production")}},
						{Key: "service.version", Value: OTLPAnyValue{StringValue: strPtr("2.1.0")}},
					},
				},
				ScopeLogs: []OTLPScopeLogs{
					{
						LogRecords: []OTLPLogRecord{
							{Body: OTLPAnyValue{StringValue: strPtr("test")}},
						},
					},
				},
			},
		},
	}

	events := req.ToEvents()
	if len(events) != 1 {
		t.Fatalf("events: got %d, want 1", len(events))
	}
	e := events[0]

	if e.Source != "api-gw" {
		t.Errorf("Source: got %q, want api-gw", e.Source)
	}
	if e.Host != "prod-01" {
		t.Errorf("Host: got %q, want prod-01", e.Host)
	}
	if e.Index != "production" {
		t.Errorf("Index: got %q, want production", e.Index)
	}

	// Well-known keys should NOT appear as resource.* fields.
	for _, key := range []string{"resource.service.name", "resource.host.name", "resource.deployment.environment"} {
		if _, ok := e.Fields[key]; ok {
			t.Errorf("well-known key should be consumed: %s", key)
		}
	}

	// Non-well-known resource attrs should appear with prefix.
	if v, ok := e.Fields["resource.service.version"]; !ok || v.AsString() != "2.1.0" {
		t.Errorf("resource.service.version: got %v", e.Fields["resource.service.version"])
	}
}

func TestToEvents_TraceContext(t *testing.T) {
	req := OTLPLogsRequest{
		ResourceLogs: []OTLPResourceLogs{
			{
				ScopeLogs: []OTLPScopeLogs{
					{
						LogRecords: []OTLPLogRecord{
							{
								Body:    OTLPAnyValue{StringValue: strPtr("traced log")},
								TraceID: "0af7651916cd43dd8448eb211c80319c",
								SpanID:  "b7ad6b7169203331",
								Flags:   1,
							},
						},
					},
				},
			},
		},
	}

	events := req.ToEvents()
	if len(events) != 1 {
		t.Fatalf("events: got %d, want 1", len(events))
	}
	e := events[0]

	if e.Fields["trace_id"].AsString() != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("trace_id: got %q", e.Fields["trace_id"])
	}
	if e.Fields["span_id"].AsString() != "b7ad6b7169203331" {
		t.Errorf("span_id: got %q", e.Fields["span_id"])
	}
	if e.Fields["flags"].AsInt() != 1 {
		t.Errorf("flags: got %v", e.Fields["flags"])
	}
}

func TestToEvents_LogRecordAttributes(t *testing.T) {
	req := OTLPLogsRequest{
		ResourceLogs: []OTLPResourceLogs{
			{
				ScopeLogs: []OTLPScopeLogs{
					{
						LogRecords: []OTLPLogRecord{
							{
								Body: OTLPAnyValue{StringValue: strPtr("with attrs")},
								Attributes: []OTLPKeyValue{
									{Key: "http.method", Value: OTLPAnyValue{StringValue: strPtr("GET")}},
									{Key: "http.status_code", Value: OTLPAnyValue{IntValue: strPtr("200")}},
									{Key: "latency_ms", Value: OTLPAnyValue{DoubleValue: floatPtr(12.5)}},
									{Key: "success", Value: OTLPAnyValue{BoolValue: boolPtr(true)}},
								},
							},
						},
					},
				},
			},
		},
	}

	events := req.ToEvents()
	if len(events) != 1 {
		t.Fatalf("events: got %d, want 1", len(events))
	}
	e := events[0]

	if e.Fields["http.method"].AsString() != "GET" {
		t.Errorf("http.method: got %v", e.Fields["http.method"])
	}
	if e.Fields["http.status_code"].AsInt() != 200 {
		t.Errorf("http.status_code: got %v", e.Fields["http.status_code"])
	}
	if e.Fields["latency_ms"].AsFloat() != 12.5 {
		t.Errorf("latency_ms: got %v", e.Fields["latency_ms"])
	}
	if e.Fields["success"].AsBool() != true {
		t.Errorf("success: got %v", e.Fields["success"])
	}
}

func TestToEvents_AttributeConflict(t *testing.T) {
	// Log record attributes should override resource attributes.
	req := OTLPLogsRequest{
		ResourceLogs: []OTLPResourceLogs{
			{
				Resource: OTLPResource{
					Attributes: []OTLPKeyValue{
						{Key: "env", Value: OTLPAnyValue{StringValue: strPtr("resource-env")}},
					},
				},
				ScopeLogs: []OTLPScopeLogs{
					{
						LogRecords: []OTLPLogRecord{
							{
								Body: OTLPAnyValue{StringValue: strPtr("conflict")},
								Attributes: []OTLPKeyValue{
									// This uses the same key "resource.env" that the resource
									// attribute would be mapped to. Record attrs win.
									{Key: "resource.env", Value: OTLPAnyValue{StringValue: strPtr("record-env")}},
								},
							},
						},
					},
				},
			},
		},
	}

	events := req.ToEvents()
	if len(events) != 1 {
		t.Fatalf("events: got %d, want 1", len(events))
	}
	e := events[0]

	if e.Fields["resource.env"].AsString() != "record-env" {
		t.Errorf("resource.env: got %q, want record-env", e.Fields["resource.env"])
	}
}

func TestToEvents_EmptyRequest(t *testing.T) {
	req := OTLPLogsRequest{}
	events := req.ToEvents()
	if events != nil {
		t.Errorf("expected nil, got %d events", len(events))
	}
}

func TestToEvents_MissingTimestamp(t *testing.T) {
	req := OTLPLogsRequest{
		ResourceLogs: []OTLPResourceLogs{
			{
				ScopeLogs: []OTLPScopeLogs{
					{
						LogRecords: []OTLPLogRecord{
							{Body: OTLPAnyValue{StringValue: strPtr("no timestamp")}},
						},
					},
				},
			},
		},
	}

	events := req.ToEvents()
	if len(events) != 1 {
		t.Fatalf("events: got %d, want 1", len(events))
	}
	if !events[0].Time.IsZero() {
		t.Errorf("Time should be zero, got %v", events[0].Time)
	}
}

func TestToEvents_BodyTypes(t *testing.T) {
	t.Run("intValue body", func(t *testing.T) {
		req := OTLPLogsRequest{
			ResourceLogs: []OTLPResourceLogs{
				{
					ScopeLogs: []OTLPScopeLogs{
						{
							LogRecords: []OTLPLogRecord{
								{Body: OTLPAnyValue{IntValue: strPtr("42")}},
							},
						},
					},
				},
			},
		}
		events := req.ToEvents()
		if events[0].Raw != "42" {
			t.Errorf("Raw: got %q, want 42", events[0].Raw)
		}
	})

	t.Run("kvlist body", func(t *testing.T) {
		req := OTLPLogsRequest{
			ResourceLogs: []OTLPResourceLogs{
				{
					ScopeLogs: []OTLPScopeLogs{
						{
							LogRecords: []OTLPLogRecord{
								{
									Body: OTLPAnyValue{
										KvlistValue: &OTLPKvlistValue{
											Values: []OTLPKeyValue{
												{Key: "msg", Value: OTLPAnyValue{StringValue: strPtr("hello")}},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}
		events := req.ToEvents()
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(events[0].Raw), &m); err != nil {
			t.Fatalf("Raw is not valid JSON: %v", err)
		}
		if m["msg"] != "hello" {
			t.Errorf("msg: got %v", m["msg"])
		}
	})
}

func TestOTLPJSON_RoundTrip(t *testing.T) {
	payload := `{
		"resourceLogs": [{
			"resource": {
				"attributes": [
					{"key": "service.name", "value": {"stringValue": "checkout"}},
					{"key": "host.name", "value": {"stringValue": "prod-web-01"}},
					{"key": "deployment.environment", "value": {"stringValue": "production"}}
				]
			},
			"scopeLogs": [{
				"scope": {"name": "checkout.payments", "version": "2.0.0"},
				"logRecords": [
					{
						"timeUnixNano": "1700000000000000000",
						"severityNumber": 9,
						"severityText": "INFO",
						"body": {"stringValue": "Payment processed successfully"},
						"attributes": [
							{"key": "payment.method", "value": {"stringValue": "credit_card"}},
							{"key": "payment.amount", "value": {"doubleValue": 99.99}},
							{"key": "payment.currency", "value": {"stringValue": "USD"}}
						],
						"traceId": "0af7651916cd43dd8448eb211c80319c",
						"spanId": "b7ad6b7169203331"
					},
					{
						"timeUnixNano": "1700000001000000000",
						"severityNumber": 17,
						"body": {"stringValue": "Payment declined"},
						"attributes": [
							{"key": "payment.method", "value": {"stringValue": "debit_card"}},
							{"key": "error.code", "value": {"intValue": "4001"}}
						]
					},
					{
						"timeUnixNano": "1700000002000000000",
						"severityNumber": 5,
						"body": {"stringValue": "Cache miss for user profile"},
						"attributes": [
							{"key": "cache.hit", "value": {"boolValue": false}}
						]
					}
				]
			}]
		}]
	}`

	var req OTLPLogsRequest
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	events := req.ToEvents()
	if len(events) != 3 {
		t.Fatalf("events: got %d, want 3", len(events))
	}

	// Verify first event.
	e0 := events[0]
	if e0.Source != "checkout" {
		t.Errorf("Source: got %q", e0.Source)
	}
	if e0.Host != "prod-web-01" {
		t.Errorf("Host: got %q", e0.Host)
	}
	if e0.Index != "production" {
		t.Errorf("Index: got %q", e0.Index)
	}
	if e0.SourceType != "otlp" {
		t.Errorf("SourceType: got %q", e0.SourceType)
	}
	if e0.Raw != "Payment processed successfully" {
		t.Errorf("Raw: got %q", e0.Raw)
	}
	if e0.Time.UnixNano() != 1700000000000000000 {
		t.Errorf("Time: got %v", e0.Time.UnixNano())
	}
	if e0.Fields["level"].AsString() != "info" {
		t.Errorf("level: got %q", e0.Fields["level"])
	}
	if e0.Fields["otel.scope.name"].AsString() != "checkout.payments" {
		t.Errorf("otel.scope.name: got %q", e0.Fields["otel.scope.name"])
	}
	if e0.Fields["payment.amount"].AsFloat() != 99.99 {
		t.Errorf("payment.amount: got %v", e0.Fields["payment.amount"])
	}
	if e0.Fields["trace_id"].AsString() != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("trace_id: got %q", e0.Fields["trace_id"])
	}

	// Verify second event severity (no text, falls back to number -> error).
	e1 := events[1]
	if e1.Fields["level"].AsString() != "error" {
		t.Errorf("level: got %q, want error", e1.Fields["level"])
	}
	if e1.Fields["error.code"].AsInt() != 4001 {
		t.Errorf("error.code: got %v", e1.Fields["error.code"])
	}

	// Verify third event severity (num=5 -> debug).
	e2 := events[2]
	if e2.Fields["level"].AsString() != "debug" {
		t.Errorf("level: got %q, want debug", e2.Fields["level"])
	}
	if e2.Fields["cache.hit"].AsBool() != false {
		t.Errorf("cache.hit: got %v", e2.Fields["cache.hit"])
	}
}

// formatNanos converts int64 nanoseconds to the string format used by OTLP.
func formatNanos(n int64) string {
	return strconv.FormatInt(n, 10)
}
