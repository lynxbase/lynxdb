package pipeline

import (
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestJSONParser(t *testing.T) {
	e := event.NewEvent(time.Now(), `{"level":"ERROR","status":500,"latency":1.5,"success":true}`)
	parser := &JSONParser{}
	events, err := parser.Process([]*event.Event{e})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if events[0].GetField("level").AsString() != "ERROR" {
		t.Errorf("level: got %v", events[0].GetField("level"))
	}
	if events[0].GetField("status").AsInt() != 500 {
		t.Errorf("status: got %v", events[0].GetField("status"))
	}
	if events[0].GetField("latency").AsFloat() != 1.5 {
		t.Errorf("latency: got %v", events[0].GetField("latency"))
	}
}

func TestKeyValueParser(t *testing.T) {
	e := event.NewEvent(time.Now(), `src=web-01 level=ERROR status=500 msg="request failed"`)
	parser := &KeyValueParser{}
	events, err := parser.Process([]*event.Event{e})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if events[0].GetField("src").AsString() != "web-01" {
		t.Errorf("src: got %v", events[0].GetField("src"))
	}
	if events[0].GetField("level").AsString() != "ERROR" {
		t.Errorf("level: got %v", events[0].GetField("level"))
	}
	if events[0].GetField("msg").AsString() != "request failed" {
		t.Errorf("msg: got %v", events[0].GetField("msg"))
	}
}

func TestTimestampNormalizer(t *testing.T) {
	at := time.Date(2025, 1, 15, 8, 0, 0, 0, time.UTC)
	year := time.Now().Year()
	tests := []struct {
		raw  string
		want time.Time
	}{
		{"2025-01-15T08:00:00Z level=a", at},
		{"2025-01-15T08:00:00.1Z level=a", at.Add(100 * time.Millisecond)},
		{"2025-01-15T08:00:00.12Z level=a", at.Add(120 * time.Millisecond)},
		{"2025-01-15T08:00:00.123456789Z some log message", at.Add(123456789)},
		{"2025-01-15 08:00:00,250 INFO started", at.Add(250 * time.Millisecond)},
		{`"2025-01-15T04:00:00.337-0400" some log line`, at.Add(337 * time.Millisecond)},
		{"2025-01-15T01:00:00-0700 event data", at},
		{`" 2025-01-15T04:00:00.337-04:00",field2,field3`, at.Add(337 * time.Millisecond)},
		{`{"ts":"2025-01-15T08:00:00Z","msg":"json"}`, at},
		{"time=2025-01-15T08:00:00Z level=info", at},
		{`10.0.0.1 - - [15/Jan/2025:09:00:00 +0100] "GET / HTTP/1.1" 200 5`, at},
		{"Jan 15 08:00:00 web-01 sshd[42]: accepted", time.Date(year, 1, 15, 8, 0, 0, 0, time.UTC)},
		{"Jan  5 08:00:00 web-01 cron[7]: ran", time.Date(year, 1, 5, 8, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		e := event.NewEvent(time.Time{}, tt.raw)
		events, err := (&TimestampNormalizer{}).Process([]*event.Event{e})
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if !events[0].Time.Equal(tt.want) {
			t.Errorf("%q: time %v, want %v", tt.raw, events[0].Time, tt.want)
		}
	}
}

func TestTimestampNormalizerGivesTextWithoutATimeTheArrivalTime(t *testing.T) {
	before := time.Now()
	e := event.NewEvent(time.Time{}, "plain words")
	events, err := (&TimestampNormalizer{}).Process([]*event.Event{e})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if got := events[0].Time; got.Before(before) || got.After(time.Now()) {
		t.Fatalf("time %v, want the arrival time", got)
	}
}

func TestTimestampNormalizerKeepsATimeTheEventHas(t *testing.T) {
	at := time.Date(2020, 5, 1, 0, 0, 0, 0, time.UTC)
	e := event.NewEvent(at, "2025-01-15T08:00:00Z text")
	events, err := (&TimestampNormalizer{}).Process([]*event.Event{e})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !events[0].Time.Equal(at) {
		t.Fatalf("time %v, want %v", events[0].Time, at)
	}
}

func TestRouter(t *testing.T) {
	router := &Router{DefaultIndex: "main"}

	e1 := event.NewEvent(time.Now(), "test")
	e2 := event.NewEvent(time.Now(), "test")
	e2.Index = "security"

	events, err := router.Process([]*event.Event{e1, e2})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if events[0].Index != "main" {
		t.Errorf("e1 index: got %q, want main", events[0].Index)
	}
	if events[1].Index != "security" {
		t.Errorf("e2 index: got %q, want security (preserved)", events[1].Index)
	}
}

func TestPipeline_FullChain(t *testing.T) {
	pipe := DefaultPipeline()

	events := []*event.Event{
		func() *event.Event {
			e := event.NewEvent(time.Time{}, `{"level":"ERROR","status":500}`)
			e.Host = "web-01"

			return e
		}(),
	}

	processed, err := pipe.Process(events)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if len(processed) != 1 {
		t.Fatalf("got %d events", len(processed))
	}

	if processed[0].Index != "main" {
		t.Errorf("index: got %q, want main", processed[0].Index)
	}
	if processed[0].Time.IsZero() {
		t.Error("timestamp not set")
	}
}

func TestMetadataOnlyParser(t *testing.T) {
	raw := `{"level":"ERROR","status":500,"host":"web-01","source":"nginx","message":"timeout","ts":"2024-01-15T10:30:00Z"}`
	e := event.NewEvent(time.Now(), raw)
	parser := &MetadataOnlyParser{}
	events, err := parser.Process([]*event.Event{e})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	ev := events[0]

	// Metadata fields should be extracted.
	if ev.Host != "web-01" {
		t.Errorf("host: got %q, want web-01", ev.Host)
	}
	if ev.Source != "nginx" {
		t.Errorf("source: got %q, want nginx", ev.Source)
	}
	if ev.GetField("level").AsString() != "ERROR" {
		t.Errorf("level: got %v, want ERROR", ev.GetField("level"))
	}
	if ev.GetField("ts").AsString() != "2024-01-15T10:30:00Z" {
		t.Errorf("ts: got %v, want 2024-01-15T10:30:00Z", ev.GetField("ts"))
	}

	// Non-metadata fields should NOT be extracted.
	if !ev.GetField("status").IsNull() {
		t.Errorf("status should not be extracted in lightweight mode, got %v", ev.GetField("status"))
	}
	if !ev.GetField("message").IsNull() {
		t.Errorf("message should not be extracted in lightweight mode, got %v", ev.GetField("message"))
	}
}

func TestMetadataOnlyParser_NonJSON(t *testing.T) {
	e := event.NewEvent(time.Now(), "this is not JSON")
	parser := &MetadataOnlyParser{}
	events, err := parser.Process([]*event.Event{e})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !events[0].ParseError {
		t.Error("expected ParseError for non-JSON input")
	}
}

func TestMetadataOnlyParser_PreserveExisting(t *testing.T) {
	// If host/source are already set on the event, they should not be overwritten.
	e := event.NewEvent(time.Now(), `{"host":"json-host","source":"json-source"}`)
	e.Host = "pre-set-host"
	e.Source = "pre-set-source"
	parser := &MetadataOnlyParser{}
	events, err := parser.Process([]*event.Event{e})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if events[0].Host != "pre-set-host" {
		t.Errorf("host: got %q, want pre-set-host (should not overwrite)", events[0].Host)
	}
	if events[0].Source != "pre-set-source" {
		t.Errorf("source: got %q, want pre-set-source (should not overwrite)", events[0].Source)
	}
}

func TestLightweightPipeline(t *testing.T) {
	pipe := LightweightPipeline()

	raw := `{"level":"ERROR","status":500,"host":"web-01","message":"timeout"}`
	e := event.NewEvent(time.Time{}, raw)
	processed, err := pipe.Process([]*event.Event{e})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	ev := processed[0]

	// Pipeline should set index and timestamp.
	if ev.Index != "main" {
		t.Errorf("index: got %q, want main", ev.Index)
	}
	if ev.Time.IsZero() {
		t.Error("timestamp not set")
	}

	// Metadata fields extracted.
	if ev.Host != "web-01" {
		t.Errorf("host: got %q, want web-01", ev.Host)
	}
	if ev.GetField("level").AsString() != "ERROR" {
		t.Errorf("level: got %v, want ERROR", ev.GetField("level"))
	}

	// Non-metadata fields should NOT be extracted.
	if !ev.GetField("status").IsNull() {
		t.Errorf("status should not be extracted, got %v", ev.GetField("status"))
	}
	if !ev.GetField("message").IsNull() {
		t.Errorf("message should not be extracted, got %v", ev.GetField("message"))
	}

	// _raw should be preserved for query-time extraction.
	if ev.Raw != raw {
		t.Errorf("_raw should be preserved, got %q", ev.Raw)
	}
}
