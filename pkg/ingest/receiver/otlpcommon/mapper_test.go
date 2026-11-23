package otlpcommon

import (
	"testing"
	"time"

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
	if got := ev.GetField("level").AsString(); got != "info" {
		t.Fatalf("level = %q, want info", got)
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

func TestLogsToEventsTakesTheTimeOfTheRecord(t *testing.T) {
	before := time.Now()
	for _, tc := range []struct {
		name           string
		time, observed uint64
		want           time.Time
	}{
		{"event time", 1_700_000_000_000_000_000, 1_700_000_005_000_000_000, time.Unix(1_700_000_000, 0)},
		{"observed time", 0, 1_700_000_005_000_000_000, time.Unix(1_700_000_005, 0)},
		{"arrival time", 0, 0, time.Time{}},
	} {
		events := LogsToEvents([]*logspb.ResourceLogs{{ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
			TimeUnixNano:         tc.time,
			ObservedTimeUnixNano: tc.observed,
			Body:                 &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello"}},
		}}}}}})
		if len(events) != 1 {
			t.Fatalf("%s: %d events", tc.name, len(events))
		}
		got := events[0].Time
		if tc.want.IsZero() {
			if got.Before(before) || got.After(time.Now()) {
				t.Errorf("%s: time %v, want the arrival time", tc.name, got)
			}
			continue
		}
		if !got.Equal(tc.want) {
			t.Errorf("%s: time %v, want %v", tc.name, got, tc.want)
		}
	}
}
