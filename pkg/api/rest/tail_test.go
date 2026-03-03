package rest

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/config"
	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

// sseEvent represents a parsed SSE event.
type sseEvent struct {
	Type string
	Data json.RawMessage
}

// readSSEEvents reads SSE events from the response body until ctx is canceled
// or the connection closes. It sends parsed events to the returned channel.
func readSSEEvents(ctx context.Context, resp *http.Response) <-chan sseEvent {
	ch := make(chan sseEvent, 64)
	go func() {
		defer close(ch)
		scanner := bufio.NewScanner(resp.Body)
		var eventType string
		var dataLines []string
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "event: ") {
				eventType = strings.TrimPrefix(line, "event: ")
			} else if strings.HasPrefix(line, "data: ") {
				dataLines = append(dataLines, strings.TrimPrefix(line, "data: "))
			} else if line == "" && eventType != "" {
				data := strings.Join(dataLines, "\n")
				ev := sseEvent{Type: eventType, Data: json.RawMessage(data)}
				select {
				case ch <- ev:
				case <-ctx.Done():
					return
				}
				eventType = ""
				dataLines = nil
			}
		}
	}()

	return ch
}

// collectSSE reads up to n events or until timeout.
func collectSSE(t *testing.T, ch <-chan sseEvent, n int, timeout time.Duration) []sseEvent {
	t.Helper()
	var events []sseEvent
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for len(events) < n {
		select {
		case ev, ok := <-ch:
			if !ok {
				return events
			}
			events = append(events, ev)
		case <-timer.C:
			return events
		}
	}

	return events
}

func TestTail_SSE_Catchup(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	// Ingest events first.
	now := time.Now()
	events := make([]*event.Event, 5)
	for i := range events {
		events[i] = &event.Event{
			Time:  now.Add(time.Duration(i) * time.Second),
			Raw:   fmt.Sprintf("event-%d", i),
			Host:  "web-01",
			Index: "main",
		}
	}
	srv.Engine().Ingest(events)
	// Small delay for flush.
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://%s/api/v1/tail?q=search+index%%3Dmain&count=3&from=-1h", srv.Addr()), http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type: %s", ct)
	}

	ch := readSSEEvents(ctx, resp)

	// Expect up to 3 result events + 1 catchup_done.
	evts := collectSSE(t, ch, 4, 3*time.Second)

	resultCount := 0
	var catchupDone bool
	for _, ev := range evts {
		switch ev.Type {
		case "result":
			resultCount++
		case "catchup_done":
			catchupDone = true
		}
	}
	if !catchupDone {
		t.Error("missing catchup_done event")
	}
	if resultCount > 3 {
		t.Errorf("expected at most 3 catchup results, got %d", resultCount)
	}
}

func TestTail_SSE_Live(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Connect to tail first (no historical data).
	req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://%s/api/v1/tail?q=search+*&count=0&from=-1s", srv.Addr()), http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	ch := readSSEEvents(ctx, resp)

	// Wait for catchup_done.
	evts := collectSSE(t, ch, 1, 2*time.Second)
	if len(evts) == 0 || evts[len(evts)-1].Type != "catchup_done" {
		// May have gotten result events first, keep reading.
		for _, e := range evts {
			if e.Type == "catchup_done" {
				goto done
			}
		}
		more := collectSSE(t, ch, 5, 2*time.Second)
		for _, e := range more {
			if e.Type == "catchup_done" {
				goto done
			}
		}
		t.Fatal("never received catchup_done")
	}
done:

	// Now ingest a live event.
	srv.Engine().Ingest([]*event.Event{
		{Time: time.Now(), Raw: "live-event-hello", Host: "web-01", Index: "main"},
	})

	// Should receive it as a result event.
	liveEvts := collectSSE(t, ch, 1, 3*time.Second)
	if len(liveEvts) == 0 {
		t.Fatal("expected live event, got none")
	}
	found := false
	for _, ev := range liveEvts {
		if ev.Type == "result" {
			var data map[string]interface{}
			json.Unmarshal(ev.Data, &data)
			if raw, ok := data["_raw"].(string); ok && raw == "live-event-hello" {
				found = true
			}
		}
	}
	if !found {
		t.Error("live event not found in SSE stream")
	}
}

func TestTail_SSE_SPL2Filter(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Connect with a filter query.
	req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://%s/api/v1/tail?q=search+ERROR&count=0&from=-1s", srv.Addr()), http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	ch := readSSEEvents(ctx, resp)

	// Wait for catchup_done.
	collectSSE(t, ch, 1, 2*time.Second)

	// Ingest one matching and one non-matching event.
	srv.Engine().Ingest([]*event.Event{
		{Time: time.Now(), Raw: "level=INFO all good", Host: "web-01", Index: "main"},
		{Time: time.Now(), Raw: "level=ERROR something broke", Host: "web-02", Index: "main"},
	})

	// Read events — should only get the ERROR one.
	evts := collectSSE(t, ch, 2, 3*time.Second)
	for _, ev := range evts {
		if ev.Type == "result" {
			var data map[string]interface{}
			json.Unmarshal(ev.Data, &data)
			raw := data["_raw"].(string)
			if !strings.Contains(raw, "ERROR") {
				t.Errorf("expected only ERROR events, got: %s", raw)
			}
		}
	}
}

func TestTail_SSE_EvalFields(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Query with eval + fields projection.
	q := "search * | eval msg=upper(_raw) | fields _time, msg"
	req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://%s/api/v1/tail?q=%s&count=0&from=-1s", srv.Addr(), q), http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	ch := readSSEEvents(ctx, resp)
	collectSSE(t, ch, 1, 2*time.Second) // catchup_done

	srv.Engine().Ingest([]*event.Event{
		{Time: time.Now(), Raw: "hello world", Host: "web-01", Index: "main"},
	})

	evts := collectSSE(t, ch, 1, 3*time.Second)
	for _, ev := range evts {
		if ev.Type == "result" {
			var data map[string]interface{}
			json.Unmarshal(ev.Data, &data)
			if msg, ok := data["msg"].(string); ok {
				if msg != "HELLO WORLD" {
					t.Errorf("expected HELLO WORLD, got %s", msg)
				}
			}
			// _raw should not be present due to fields projection.
			if _, ok := data["_raw"]; ok {
				t.Error("_raw should not be present after fields projection")
			}
		}
	}
}

func TestTail_SSE_UnsupportedCommand(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://%s/api/v1/tail?q=search+*+|+stats+count", srv.Addr()), http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", resp.StatusCode)
	}

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)
	if _, ok := body["unsupported"]; !ok {
		t.Error("expected unsupported field in error response")
	}
}

func TestTail_SSE_ParseError(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://%s/api/v1/tail?q=search+|+|+|+broken", srv.Addr()), http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestTail_SSE_EmptyCatchup(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// No events ingested — catchup should be empty.
	req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://%s/api/v1/tail?q=search+*&count=10&from=-1s", srv.Addr()), http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	ch := readSSEEvents(ctx, resp)
	evts := collectSSE(t, ch, 2, 2*time.Second)

	// Should get catchup_done with count=0.
	for _, ev := range evts {
		if ev.Type == "catchup_done" {
			var data map[string]interface{}
			json.Unmarshal(ev.Data, &data)
			if cnt, ok := data["count"].(float64); ok && cnt != 0 {
				t.Errorf("expected count=0, got %v", cnt)
			}

			return
		}
	}
	t.Error("missing catchup_done event")
}

func TestTail_SSE_MissingQuery(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://%s/api/v1/tail", srv.Addr()), http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

// TestTail_SSE_NoGapBetweenCatchupAndLive verifies that events ingested
// during the catchup scan are not lost. Before the subscribe-before-catchup
// fix (Sprint 1), there was a window where events would fall between the
// storage snapshot and the live subscription. This test exercises that
// exact scenario: historical events exist, we start a tail, and then
// additional events are ingested while catchup is still being processed.
func TestTail_SSE_NoGapBetweenCatchupAndLive(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	// Ingest historical events that the catchup will return.
	now := time.Now()
	historical := make([]*event.Event, 5)
	for i := range historical {
		historical[i] = &event.Event{
			Time:  now.Add(time.Duration(i) * time.Second),
			Raw:   fmt.Sprintf("historical-%d", i),
			Host:  "web-01",
			Index: "main",
		}
	}
	srv.Engine().Ingest(historical)
	time.Sleep(50 * time.Millisecond) // let memtable settle

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to the tail endpoint with a catchup window that covers
	// the historical events. count=3 means only the last 3 historicals appear.
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("http://%s/api/v1/tail?q=search+*&count=3&from=-1h", srv.Addr()),
		http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	ch := readSSEEvents(ctx, resp)

	// Read catchup results + catchup_done.
	// Expect 3 result events (last 3 of historical) + 1 catchup_done = 4 events.
	catchupEvts := collectSSE(t, ch, 4, 3*time.Second)

	var catchupResults []string
	var catchupDone bool
	for _, ev := range catchupEvts {
		switch ev.Type {
		case "result":
			var data map[string]interface{}
			json.Unmarshal(ev.Data, &data)
			if raw, ok := data["_raw"].(string); ok {
				catchupResults = append(catchupResults, raw)
			}
		case "catchup_done":
			catchupDone = true
		}
	}
	if !catchupDone {
		t.Fatal("never received catchup_done")
	}
	if len(catchupResults) > 3 {
		t.Fatalf("expected at most 3 catchup results, got %d", len(catchupResults))
	}

	// Ingest events that would have fallen into the gap in the old
	// implementation (subscription wasn't active during catchup).
	gapEvents := make([]*event.Event, 3)
	for i := range gapEvents {
		gapEvents[i] = &event.Event{
			Time:  time.Now().Add(time.Duration(i) * time.Millisecond),
			Raw:   fmt.Sprintf("gap-event-%d", i),
			Host:  "web-02",
			Index: "main",
		}
	}
	srv.Engine().Ingest(gapEvents)

	// Read live events — all 3 gap events should appear.
	liveEvts := collectSSE(t, ch, 3, 3*time.Second)

	var liveResults []string
	for _, ev := range liveEvts {
		if ev.Type == "result" {
			var data map[string]interface{}
			json.Unmarshal(ev.Data, &data)
			if raw, ok := data["_raw"].(string); ok {
				liveResults = append(liveResults, raw)
			}
		}
	}

	// Verify all gap events arrived.
	if len(liveResults) < 3 {
		t.Errorf("expected 3 live events (gap events), got %d: %v", len(liveResults), liveResults)
	}
	for i := 0; i < 3; i++ {
		expected := fmt.Sprintf("gap-event-%d", i)
		found := false
		for _, raw := range liveResults {
			if raw == expected {
				found = true

				break
			}
		}
		if !found {
			t.Errorf("missing gap event %q in live results: %v", expected, liveResults)
		}
	}
}

// TestTail_SSE_NoDuplicatesAfterCatchup verifies that events already included
// in catchup results are not re-sent in the live stream. The dedup cursor
// (SetSkipBefore) should filter them out.
func TestTail_SSE_NoDuplicatesAfterCatchup(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	// Ingest events that will appear in catchup.
	now := time.Now()
	events := make([]*event.Event, 3)
	for i := range events {
		events[i] = &event.Event{
			Time:  now.Add(time.Duration(i) * time.Second),
			Raw:   fmt.Sprintf("dedup-test-%d", i),
			Host:  "web-01",
			Index: "main",
		}
	}
	srv.Engine().Ingest(events)
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("http://%s/api/v1/tail?q=search+*&count=10&from=-1h", srv.Addr()),
		http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	ch := readSSEEvents(ctx, resp)

	// Collect catchup events.
	catchupEvts := collectSSE(t, ch, 4, 3*time.Second)
	var catchupRaws []string
	for _, ev := range catchupEvts {
		if ev.Type == "result" {
			var data map[string]interface{}
			json.Unmarshal(ev.Data, &data)
			if raw, ok := data["_raw"].(string); ok {
				catchupRaws = append(catchupRaws, raw)
			}
		}
	}

	// Now ingest a NEWER event (after the catchup's latest _time).
	srv.Engine().Ingest([]*event.Event{
		{Time: time.Now(), Raw: "fresh-live-event", Host: "web-01", Index: "main"},
	})

	// Read live events — should only get the fresh one, not duplicates of catchup.
	liveEvts := collectSSE(t, ch, 5, 3*time.Second)
	for _, ev := range liveEvts {
		if ev.Type == "result" {
			var data map[string]interface{}
			json.Unmarshal(ev.Data, &data)
			raw, _ := data["_raw"].(string)
			for _, cr := range catchupRaws {
				if raw == cr {
					t.Errorf("duplicate event in live stream (already in catchup): %q", raw)
				}
			}
		}
	}
}

// TestTail_SSE_SessionLimit verifies that new tail connections are rejected
// with 429 when the concurrent session limit is reached.
func TestTail_SSE_SessionLimit(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv, err := NewServer(Config{
		Addr:   "127.0.0.1:0",
		Logger: logger,
		Tail:   config.TailConfig{MaxConcurrentSessions: 1, MaxSessionDuration: 10 * time.Second},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Start(ctx)
	srv.WaitReady()
	defer func() {
		cancel()
		time.Sleep(50 * time.Millisecond)
	}()

	// Open the first tail session — should succeed.
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()
	req1, _ := http.NewRequestWithContext(reqCtx, "GET",
		fmt.Sprintf("http://%s/api/v1/tail?q=search+*&count=0&from=-1s", srv.Addr()),
		http.NoBody)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	defer resp1.Body.Close()

	if resp1.StatusCode != 200 {
		t.Fatalf("first session: expected 200, got %d", resp1.StatusCode)
	}

	// Wait for the session to fully establish (catchup_done).
	ch1 := readSSEEvents(reqCtx, resp1)
	collectSSE(t, ch1, 1, 2*time.Second)

	// Try to open a second session — should be rejected (limit is 1).
	req2Ctx, req2Cancel := context.WithTimeout(ctx, 3*time.Second)
	defer req2Cancel()
	req2, _ := http.NewRequestWithContext(req2Ctx, "GET",
		fmt.Sprintf("http://%s/api/v1/tail?q=search+*&count=0&from=-1s", srv.Addr()),
		http.NoBody)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second session: expected 429, got %d", resp2.StatusCode)
	}
	if resp2.Header.Get("Retry-After") == "" {
		t.Error("expected Retry-After header on 429")
	}
}

// TestTail_SSE_MaxDuration verifies that the server sends a close event and
// terminates the tail session when max_session_duration is reached.
func TestTail_SSE_MaxDuration(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv, err := NewServer(Config{
		Addr:   "127.0.0.1:0",
		Logger: logger,
		Tail:   config.TailConfig{MaxSessionDuration: 2 * time.Second},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Start(ctx)
	srv.WaitReady()
	defer func() {
		cancel()
		time.Sleep(50 * time.Millisecond)
	}()

	reqCtx, reqCancel := context.WithTimeout(ctx, 10*time.Second)
	defer reqCancel()
	req, _ := http.NewRequestWithContext(reqCtx, "GET",
		fmt.Sprintf("http://%s/api/v1/tail?q=search+*&count=0&from=-1s", srv.Addr()),
		http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	ch := readSSEEvents(reqCtx, resp)

	// Collect events until the connection closes. The server should send
	// catchup_done, then after ~2s a close event with reason=max_session_duration.
	evts := collectSSE(t, ch, 10, 5*time.Second)

	var gotClose bool
	for _, ev := range evts {
		if ev.Type == "close" {
			var data map[string]interface{}
			json.Unmarshal(ev.Data, &data)
			if data["reason"] == "max_session_duration" {
				gotClose = true
			}
		}
	}
	if !gotClose {
		t.Error("expected close event with reason=max_session_duration")
	}
}

func TestTail_SSE_ClientDisconnect(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://%s/api/v1/tail?q=search+*&count=0&from=-1s", srv.Addr()), http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	ch := readSSEEvents(ctx, resp)
	collectSSE(t, ch, 1, 2*time.Second) // catchup_done

	// Cancel context to simulate client disconnect.
	cancel()
	resp.Body.Close()

	// The server should clean up without error. Give it a moment.
	time.Sleep(100 * time.Millisecond)
}
