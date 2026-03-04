package receiver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
)

// memSink collects events in memory for testing.
type memSink struct {
	mu     sync.Mutex
	events []*event.Event
}

func (s *memSink) Write(events []*event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, events...)

	return nil
}

func (s *memSink) Events() []*event.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]*event.Event{}, s.events...)
}

func startTestServer(t *testing.T) (*HTTPReceiver, *memSink, func()) {
	t.Helper()

	sink := &memSink{}
	pipe := pipeline.DefaultPipeline()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	recv := NewHTTPReceiver("127.0.0.1:0", pipe, sink, logger)

	ctx, cancel := context.WithCancel(context.Background())

	go recv.Start(ctx)
	recv.WaitReady()

	cleanup := func() {
		cancel()
		time.Sleep(50 * time.Millisecond)
	}

	return recv, sink, cleanup
}

func TestHTTPReceiver_PostEvents(t *testing.T) {
	recv, sink, cleanup := startTestServer(t)
	defer cleanup()

	now := float64(time.Now().Unix())
	payload := []EventPayload{
		{Time: &now, Raw: "test event 1", Host: "web-01", Source: "test", SourceType: "json", Index: "main"},
		{Time: &now, Raw: "test event 2", Host: "web-02", Source: "test", SourceType: "json", Index: "main"},
	}

	body, _ := json.Marshal(payload)
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/ingest", recv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	// Wait for processing.
	time.Sleep(50 * time.Millisecond)

	events := sink.Events()
	if len(events) != 2 {
		t.Fatalf("events: got %d, want 2", len(events))
	}
	if events[0].Raw != "test event 1" {
		t.Errorf("event[0].Raw: got %q", events[0].Raw)
	}
	if events[0].Host != "web-01" {
		t.Errorf("event[0].Host: got %q", events[0].Host)
	}
}

func TestHTTPReceiver_PostRawEvents(t *testing.T) {
	recv, sink, cleanup := startTestServer(t)
	defer cleanup()

	raw := "2024-01-01T00:00:00Z line one\n2024-01-01T00:00:01Z line two\n2024-01-01T00:00:02Z line three\n"

	req, _ := http.NewRequest("POST", fmt.Sprintf("http://%s/api/v1/ingest/raw", recv.Addr()), bytes.NewBufferString(raw))
	req.Header.Set("X-Source", "/var/log/app.log")
	req.Header.Set("X-Source-Type", "raw")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	time.Sleep(50 * time.Millisecond)

	events := sink.Events()
	if len(events) != 3 {
		t.Fatalf("events: got %d, want 3", len(events))
	}
}

func TestHTTPReceiver_HEC(t *testing.T) {
	recv, sink, cleanup := startTestServer(t)
	defer cleanup()

	now := float64(time.Now().Unix())
	hecEvents := fmt.Sprintf(
		`{"time":%f,"event":"error occurred","host":"web-01","source":"app","sourcetype":"json","index":"main"}
{"time":%f,"event":"request processed","host":"web-02","source":"app","sourcetype":"json","index":"main"}`,
		now, now)

	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/ingest/hec", recv.Addr()), "application/json", bytes.NewBufferString(hecEvents))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	time.Sleep(50 * time.Millisecond)

	events := sink.Events()
	if len(events) != 2 {
		t.Fatalf("events: got %d, want 2", len(events))
	}
	if events[0].Raw != "error occurred" {
		t.Errorf("event[0].Raw: got %q", events[0].Raw)
	}
}

func TestHTTPReceiver_Health(t *testing.T) {
	recv, _, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/health", recv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
}

func TestHTTPReceiver_InvalidJSON(t *testing.T) {
	recv, _, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/ingest", recv.Addr()), "application/json", bytes.NewBufferString("not json"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}
}
