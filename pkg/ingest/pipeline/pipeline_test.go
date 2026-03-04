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

func TestBatcher(t *testing.T) {
	batcher := NewBatcher(10)

	events := make([]*event.Event, 25)
	for i := range events {
		events[i] = event.NewEvent(time.Now(), "test")
	}

	batches := batcher.Batch(events)
	if len(batches) != 3 {
		t.Errorf("batches: got %d, want 3", len(batches))
	}
	if len(batches[0]) != 10 || len(batches[1]) != 10 || len(batches[2]) != 5 {
		t.Errorf("batch sizes: %d, %d, %d", len(batches[0]), len(batches[1]), len(batches[2]))
	}
}

func TestSyslogParser(t *testing.T) {
	e := event.NewEvent(time.Now(), `<34>Jan 15 10:30:00 web-01 sshd[12345]: Failed password for root from 10.0.0.1`)
	parser := &SyslogParser{}
	events, err := parser.Process([]*event.Event{e})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if events[0].Host != "web-01" {
		t.Errorf("host: got %q", events[0].Host)
	}
	if events[0].GetField("program").AsString() != "sshd" {
		t.Errorf("program: got %v", events[0].GetField("program"))
	}
	if events[0].GetField("pid").AsString() != "12345" {
		t.Errorf("pid: got %v", events[0].GetField("pid"))
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

func TestSplitRawLines(t *testing.T) {
	raw := "line one\nline two\n\nline three\n"
	events := SplitRawLines(raw, "test", "raw")
	if len(events) != 3 {
		t.Errorf("got %d events, want 3", len(events))
	}
}
