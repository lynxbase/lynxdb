package otlphttp

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func TestLogsToEvents_OneRecordMapped(t *testing.T) {
	body := "hello"
	events := LogsToEvents([]*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "api"}}},
			{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "host-1"}}},
		}},
		ScopeLogs: []*logspb.ScopeLogs{{
			LogRecords: []*logspb.LogRecord{{
				TimeUnixNano:   123,
				SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
				Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: body}},
			}},
		}},
	}})

	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	ev := events[0]
	if ev.Raw != body {
		t.Fatalf("Raw = %q, want %q", ev.Raw, body)
	}
	if ev.Source != "api" {
		t.Fatalf("Source = %q, want api", ev.Source)
	}
	if ev.Host != "host-1" {
		t.Fatalf("Host = %q, want host-1", ev.Host)
	}
	if ev.Index != "main" {
		t.Fatalf("Index = %q, want main", ev.Index)
	}
}

func TestLogsToEvents_EmptyBodyDropped(t *testing.T) {
	events := LogsToEvents([]*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{
			LogRecords: []*logspb.LogRecord{{}},
		}},
	}})
	if len(events) != 0 {
		t.Fatalf("events = %d, want 0", len(events))
	}
}
