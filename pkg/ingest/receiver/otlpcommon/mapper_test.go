package otlpcommon

import (
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

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
