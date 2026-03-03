package client

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestReadNDJSON_Basic(t *testing.T) {
	input := `{"message":"line1","level":"info"}
{"message":"line2","level":"error"}
{"__meta":{"total":2,"scanned":100,"took_ms":5}}
`
	var lines []json.RawMessage

	meta, err := readNDJSON(strings.NewReader(input), func(line json.RawMessage) error {
		lines = append(lines, line)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	if meta == nil {
		t.Fatal("meta is nil")
	}
	if meta.Total != 2 {
		t.Errorf("meta.Total = %d, want 2", meta.Total)
	}
	if meta.Scanned != 100 {
		t.Errorf("meta.Scanned = %d, want 100", meta.Scanned)
	}
	if meta.TookMS != 5 {
		t.Errorf("meta.TookMS = %d, want 5", meta.TookMS)
	}
}

func TestReadNDJSON_StreamError(t *testing.T) {
	input := `{"message":"ok"}
{"__error":{"code":"STREAM_ERROR","message":"scan failed"}}
`
	var lines []json.RawMessage

	_, err := readNDJSON(strings.NewReader(input), func(line json.RawMessage) error {
		lines = append(lines, line)

		return nil
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !strings.Contains(err.Error(), "scan failed") {
		t.Errorf("error = %q, want to contain 'scan failed'", err.Error())
	}
	if len(lines) != 1 {
		t.Errorf("got %d lines before error, want 1", len(lines))
	}
}

func TestReadNDJSON_EmptyLines(t *testing.T) {
	input := `
{"message":"line1"}

{"message":"line2"}

{"__meta":{"total":2,"scanned":50,"took_ms":1}}
`
	var count int

	meta, err := readNDJSON(strings.NewReader(input), func(_ json.RawMessage) error {
		count++

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
	if meta == nil || meta.Total != 2 {
		t.Errorf("meta = %+v", meta)
	}
}

func TestReadSSE_Basic(t *testing.T) {
	input := "event: result\ndata: {\"message\":\"hello\"}\n\nevent: result\ndata: {\"message\":\"world\"}\n\nevent: catchup_done\ndata: {\"count\":2}\n\n"

	var events []SSEEvent

	err := readSSE(strings.NewReader(input), func(evt SSEEvent) error {
		events = append(events, evt)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	if events[0].Event != "result" {
		t.Errorf("events[0].Event = %q", events[0].Event)
	}
	if events[2].Event != "catchup_done" {
		t.Errorf("events[2].Event = %q", events[2].Event)
	}

	// Verify data is valid JSON.
	var msg map[string]interface{}
	if err := json.Unmarshal(events[0].Data, &msg); err != nil {
		t.Fatal(err)
	}
	if msg["message"] != "hello" {
		t.Errorf("message = %v", msg["message"])
	}
}

func TestReadSSE_Heartbeat(t *testing.T) {
	input := "event: heartbeat\ndata: {\"ts\":\"2024-01-01T00:00:00Z\"}\n\nevent: result\ndata: {\"m\":\"test\"}\n\n"

	var events []SSEEvent

	err := readSSE(strings.NewReader(input), func(evt SSEEvent) error {
		events = append(events, evt)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if events[0].Event != "heartbeat" {
		t.Errorf("events[0].Event = %q, want heartbeat", events[0].Event)
	}
}

func TestReadSSE_MultilineData(t *testing.T) {
	input := "event: progress\ndata: {\"phase\":\"scanning\",\ndata: \"percent\":50}\n\n"

	var events []SSEEvent

	err := readSSE(strings.NewReader(input), func(evt SSEEvent) error {
		events = append(events, evt)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}

	// Multi-line data should be joined with newlines.
	expected := "{\"phase\":\"scanning\",\n\"percent\":50}"
	if string(events[0].Data) != expected {
		t.Errorf("data = %q, want %q", string(events[0].Data), expected)
	}
}

func TestReadSSE_NoTrailingNewline(t *testing.T) {
	// SSE stream that ends without a final blank line — remaining event should still be flushed.
	input := "event: complete\ndata: {\"done\":true}"

	var events []SSEEvent

	err := readSSE(strings.NewReader(input), func(evt SSEEvent) error {
		events = append(events, evt)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if events[0].Event != "complete" {
		t.Errorf("Event = %q, want complete", events[0].Event)
	}
}
