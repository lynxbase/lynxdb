package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIngest_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q", ct)
		}

		var events []IngestEvent
		if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
			t.Fatal(err)
		}
		if len(events) != 2 {
			t.Errorf("len(events) = %d, want 2", len(events))
		}
		if events[0].Event != "log line 1" {
			t.Errorf("events[0].Event = %q, want %q", events[0].Event, "log line 1")
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"accepted": 2,
				"failed":   0,
			},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	result, err := c.IngestEvents(context.Background(), []IngestEvent{
		{
			Event:  "log line 1",
			Fields: map[string]interface{}{"level": "info"},
		},
		{
			Event:  "log line 2",
			Fields: map[string]interface{}{"level": "error"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if result.Accepted != 2 {
		t.Errorf("Accepted = %d, want 2", result.Accepted)
	}
	if result.Failed != 0 {
		t.Errorf("Failed = %d, want 0", result.Failed)
	}
}

func TestIngest_PartialFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusMultiStatus)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"accepted": 1,
				"failed":   1,
				"errors": []map[string]interface{}{
					{"index": 1, "code": "INVALID_JSON", "message": "malformed"},
				},
			},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	result, err := c.IngestEvents(context.Background(), []IngestEvent{
		{Event: "good"},
		{Event: "bad"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if result.Accepted != 1 {
		t.Errorf("Accepted = %d, want 1", result.Accepted)
	}
	if result.Failed != 1 {
		t.Errorf("Failed = %d, want 1", result.Failed)
	}
}

func TestIngest_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{
				"code":    "TOO_MANY_REQUESTS",
				"message": "rate limit exceeded",
			},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	_, err := c.IngestEvents(context.Background(), []IngestEvent{{Event: "test"}})

	if err == nil {
		t.Fatal("expected error")
	}
	if !IsRateLimited(err) {
		t.Errorf("IsRateLimited = false, err = %v", err)
	}
}

func TestIngest_RejectsGenericDocuments(t *testing.T) {
	c := NewClient(WithBaseURL("http://example.invalid"))

	_, err := c.Ingest(context.Background(), []map[string]interface{}{
		{"message": "not an ingest envelope", "level": "info"},
	})
	if err == nil {
		t.Fatal("expected validation error")
	}

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T", err)
	}
	if apiErr.Code != ErrCodeValidationError {
		t.Fatalf("Code = %q, want %q", apiErr.Code, ErrCodeValidationError)
	}
	if apiErr.Suggestion == "" {
		t.Fatal("expected non-empty suggestion")
	}
}

func TestIngestNDJSON_UsesRawEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/ingest/raw" {
			t.Fatalf("path = %s, want /api/v1/ingest/raw", r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/x-ndjson" {
			t.Fatalf("Content-Type = %q, want application/x-ndjson", ct)
		}
		if st := r.Header.Get("X-Source-Type"); st != "json" {
			t.Fatalf("X-Source-Type = %q, want json", st)
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"accepted": 2,
				"failed":   0,
			},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	result, err := c.IngestNDJSON(context.Background(), strings.NewReader("{\"a\":1}\n{\"a\":2}\n"), IngestOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Accepted != 2 {
		t.Fatalf("Accepted = %d, want 2", result.Accepted)
	}
}
