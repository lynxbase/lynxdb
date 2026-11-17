package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// runningJobServer returns a server that always reports the job as running,
// plus a counter of how many GetJob requests it served.
func runningJobServer() (*httptest.Server, *atomic.Int64) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"type":   "job",
				"job_id": "qry_test",
				"status": "running",
			},
		})
	}))

	return srv, &calls
}

// TestPollJob_CallerDeadlineWins verifies that a caller-supplied deadline bounds
// the poll loop (it does not wait for the internal ceiling) and that the
// resulting error is a clear "did not complete" message rather than a raw
// context error.
func TestPollJob_CallerDeadlineWins(t *testing.T) {
	srv, calls := runningJobServer()
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := c.PollJob(ctx, "qry_test", nil)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected a timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "did not complete") {
		t.Errorf("expected a 'did not complete' error, got: %v", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("PollJob ran for %v; caller deadline (120ms) should have bounded it", elapsed)
	}
	if calls.Load() == 0 {
		t.Error("expected at least one GetJob call before timing out")
	}
}

// TestPollJob_NoDeadlineRespectsCancel verifies that a no-deadline caller
// context (the case where PollJob installs its internal ceiling) still stops
// promptly when the caller cancels, rather than running to the 6m ceiling.
func TestPollJob_NoDeadlineRespectsCancel(t *testing.T) {
	srv, _ := runningJobServer()
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel shortly after the loop starts; with no deadline the internal
	// ceiling (6m) would otherwise keep it running.
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := c.PollJob(ctx, "qry_test", nil)
	if err == nil {
		t.Fatal("expected an error after cancel, got nil")
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("PollJob did not stop promptly after cancel: ran %v", time.Since(start))
	}
}

func TestPollJob_ErrorStateFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"type": "job", "job_id": "qry_test", "status": "error",
				"error": map[string]interface{}{"code": "EXECUTION_ERROR", "message": "boom"},
			},
		})
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := NewClient(WithBaseURL(srv.URL)).PollJob(ctx, "qry_test", nil)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("PollJob err = %v, want the job's error", err)
	}
}
