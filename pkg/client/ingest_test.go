package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIngestEvents_SendsNDJSONPerIndex(t *testing.T) {
	bodies := map[string][]map[string]interface{}{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/ingest" {
			t.Errorf("%s %s, want POST /api/v1/ingest", r.Method, r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/x-ndjson" {
			t.Errorf("Content-Type = %q", ct)
		}
		index := r.URL.Query().Get("index")
		dec := json.NewDecoder(r.Body)
		for dec.More() {
			var obj map[string]interface{}
			if err := dec.Decode(&obj); err != nil {
				t.Fatal(err)
			}
			bodies[index] = append(bodies[index], obj)
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{"accepted": len(bodies[index]), "failed": 0},
		})
	}))
	defer srv.Close()

	ts := 1700000000.5
	c := NewClient(WithBaseURL(srv.URL))
	result, err := c.IngestEvents(context.Background(), []IngestEvent{
		{Event: "log line 1", Time: &ts, Host: "web-01", Fields: map[string]interface{}{"level": "info"}},
		{Event: "log line 2", Index: "audit", Source: "app.log", Fields: map[string]interface{}{"level": "error"}},
		{Event: "log line 3", Fields: map[string]interface{}{"level": "warn"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if result.Accepted != 3 || result.Failed != 0 {
		t.Errorf("result = %+v, want 3 accepted", result)
	}
	if len(bodies[""]) != 2 || len(bodies["audit"]) != 1 {
		t.Fatalf("requests by index = %v", bodies)
	}
	first := bodies[""][0]
	want := map[string]interface{}{
		"_raw": "log line 1", "_time": "2023-11-14T22:13:20.5Z", "host": "web-01", "level": "info",
	}
	for k, v := range want {
		if first[k] != v {
			t.Errorf("first[%q] = %v, want %v", k, first[k], v)
		}
	}
	if _, ok := first["event"]; ok {
		t.Errorf("first event object has an envelope key: %v", first)
	}
	if got := bodies["audit"][0]["source"]; got != "app.log" {
		t.Errorf("audit source = %v, want app.log", got)
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

func TestIngest_SendsBodyAsIs(t *testing.T) {
	const body = "{\"message\":\"a\",\"level\":\"info\"}\n{\"message\":\"b\"}\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("index"); got != "web" {
			t.Errorf("index = %q, want web", got)
		}
		got, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != body {
			t.Errorf("body = %q, want %q", got, body)
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{"accepted": 2, "failed": 1, "expired": 3},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	result, err := c.Ingest(context.Background(), "web", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if result.Accepted != 2 || result.Failed != 1 || result.Expired != 3 {
		t.Fatalf("result = %+v, want 2 accepted, 1 failed and 3 expired", result)
	}
}

func TestIngest_InvalidIndexIsAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{"code": "VALIDATION_ERROR", "message": "bad index"},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	_, err := c.Ingest(context.Background(), ".bad", strings.NewReader("{}\n"))
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T (%v)", err, err)
	}
	if apiErr.Code != ErrCodeValidationError {
		t.Fatalf("Code = %q, want %q", apiErr.Code, ErrCodeValidationError)
	}
}
