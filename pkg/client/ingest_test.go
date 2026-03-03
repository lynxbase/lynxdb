package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

		var events []map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
			t.Fatal(err)
		}
		if len(events) != 2 {
			t.Errorf("len(events) = %d, want 2", len(events))
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
	result, err := c.Ingest(context.Background(), []map[string]interface{}{
		{"message": "log line 1", "level": "info"},
		{"message": "log line 2", "level": "error"},
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
	result, err := c.Ingest(context.Background(), []map[string]interface{}{
		{"message": "good"},
		{"message": "bad"},
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
	if len(result.Errors) != 1 {
		t.Fatalf("len(Errors) = %d, want 1", len(result.Errors))
	}
	if result.Errors[0].Index != 1 {
		t.Errorf("Errors[0].Index = %d, want 1", result.Errors[0].Index)
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
	_, err := c.Ingest(context.Background(), []map[string]interface{}{{"m": "test"}})

	if err == nil {
		t.Fatal("expected error")
	}
	if !IsRateLimited(err) {
		t.Errorf("IsRateLimited = false, err = %v", err)
	}
}
