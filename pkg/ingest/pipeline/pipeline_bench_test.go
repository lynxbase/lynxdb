package pipeline

import (
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// Realistic JSON log line.
const benchJSONLine = `{"timestamp":"2026-02-14T14:23:01.345Z","level":"error","source":"api-gw","msg":"connection refused to /user_service/health","status":502,"duration":1234,"trace_id":"abc123","method":"GET","path":"/api/v1/users"}`

// Realistic unstructured text line.
const benchTextLine = `Feb 14 14:23:01 prod-app-01 api-gw[1234]: connection refused to /user_service/health status=502 duration=1234 trace_id=abc123 method=GET path=/api/v1/users`

func makeBenchEvents(raw string, n int) []*event.Event {
	events := make([]*event.Event, n)
	for i := range events {
		events[i] = event.NewEvent(time.Time{}, raw)
		events[i].Source = "bench"
	}

	return events
}

// BenchmarkDefaultPipeline_JSONLine benchmarks the full 4-stage pipeline on one JSON line.
func BenchmarkDefaultPipeline_JSONLine(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pipe := DefaultPipeline()
		events := makeBenchEvents(benchJSONLine, 1)
		_, _ = pipe.Process(events)
	}
}

// BenchmarkDefaultPipeline_TextLine benchmarks the full 4-stage pipeline on one text line.
func BenchmarkDefaultPipeline_TextLine(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pipe := DefaultPipeline()
		events := makeBenchEvents(benchTextLine, 1)
		_, _ = pipe.Process(events)
	}
}

// BenchmarkDefaultPipeline_JSONBatch100 benchmarks processing 100 JSON events.
func BenchmarkDefaultPipeline_JSONBatch100(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pipe := DefaultPipeline()
		events := makeBenchEvents(benchJSONLine, 100)
		_, _ = pipe.Process(events)
	}
}

// BenchmarkTimestampNormalize benchmarks timestamp extraction alone.
func BenchmarkTimestampNormalize(b *testing.B) {
	tn := DefaultTimestampNormalizer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		events := makeBenchEvents(benchJSONLine, 1)
		_, _ = tn.Process(events)
	}
}

// BenchmarkTimestampNormalize_Text benchmarks timestamp extraction on syslog-style text.
func BenchmarkTimestampNormalize_Text(b *testing.B) {
	tn := DefaultTimestampNormalizer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		events := makeBenchEvents(benchTextLine, 1)
		_, _ = tn.Process(events)
	}
}

// BenchmarkJSONParse benchmarks JSON parsing alone.
func BenchmarkJSONParse(b *testing.B) {
	jp := &JSONParser{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		events := makeBenchEvents(benchJSONLine, 1)
		_, _ = jp.Process(events)
	}
}

// BenchmarkJSONParse_Batch100 benchmarks JSON parsing on 100 events.
func BenchmarkJSONParse_Batch100(b *testing.B) {
	jp := &JSONParser{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		events := makeBenchEvents(benchJSONLine, 100)
		_, _ = jp.Process(events)
	}
}

// BenchmarkKVParse benchmarks KV parsing alone.
func BenchmarkKVParse(b *testing.B) {
	kv := &KeyValueParser{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		events := makeBenchEvents(benchTextLine, 1)
		_, _ = kv.Process(events)
	}
}

// BenchmarkSplitRawLines benchmarks line splitting.
func BenchmarkSplitRawLines(b *testing.B) {
	// Build a blob of 100 lines
	lines := ""
	for i := 0; i < 100; i++ {
		lines += benchJSONLine + "\n"
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SplitRawLines(lines, "bench", "json")
	}
}
