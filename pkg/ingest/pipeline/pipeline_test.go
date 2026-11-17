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
	norm := DefaultTimestampNormalizer()

	tests := []struct {
		name string
		raw  string
	}{
		{"RFC3339", "2024-01-15T10:30:00Z some log message"},
		{"RFC3339Nano", "2024-01-15T10:30:00.123456789Z some log message"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := event.NewEvent(time.Time{}, tt.raw)
			events, err := norm.Process([]*event.Event{e})
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			if events[0].Time.IsZero() {
				t.Error("timestamp not extracted")
			}
		})
	}
}

func TestTimestampNormalizerEDGARFormat(t *testing.T) {
	norm := DefaultTimestampNormalizer()

	tests := []struct {
		name string
		raw  string
	}{
		{"minus0400_millis", `"2025-06-30T23:59:57.337-0400" some log line`},
		{"minus0700_no_millis", `2025-06-30T12:00:00-0700 event data`},
		{"quoted_with_space", `" 2025-06-30T23:59:57.337-0400",field2,field3`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := event.NewEvent(time.Time{}, tt.raw)
			events, err := norm.Process([]*event.Event{e})
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			if events[0].Time.IsZero() {
				t.Error("timestamp not extracted from EDGAR format")
			}
			// Verify the year is correct
			if events[0].Time.Year() != 2025 {
				t.Errorf("year: got %d, want 2025", events[0].Time.Year())
			}
		})
	}
}

func TestRouter(t *testing.T) {
	router := &Router{DefaultIndex: "main", PartitionCount: 4}

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

func TestRouter_Partition(t *testing.T) {
	router := &Router{DefaultIndex: "main", PartitionCount: 4}

	e := event.NewEvent(time.Now(), "test")
	e.Host = "web-01"

	p := router.Partition(e)
	if p < 0 || p >= 4 {
		t.Errorf("partition: got %d, expected 0-3", p)
	}

	// Same host should always get same partition.
	for i := 0; i < 100; i++ {
		if router.Partition(e) != p {
			t.Fatal("partition not deterministic")
		}
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
	if parser.ParseErrors.Load() != 1 {
		t.Errorf("ParseErrors: got %d, want 1", parser.ParseErrors.Load())
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
