package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestQuery_SyncEvents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}

		var req QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Q != "error" {
			t.Errorf("Q = %q, want %q", req.Q, "error")
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"type":     "events",
				"events":   []map[string]interface{}{{"message": "test"}},
				"total":    1,
				"has_more": false,
			},
			"meta": map[string]interface{}{"took_ms": 42.0, "scanned": 1000},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	result, err := c.Query(context.Background(), QueryRequest{Q: "error", From: "-1h"})
	if err != nil {
		t.Fatal(err)
	}

	if result.Type != ResultTypeEvents {
		t.Fatalf("Type = %q, want events", result.Type)
	}
	if result.Events == nil {
		t.Fatal("Events is nil")
	}
	if result.Events.Total != 1 {
		t.Errorf("Total = %d, want 1", result.Events.Total)
	}
	if len(result.Events.Events) != 1 {
		t.Fatalf("len(Events) = %d, want 1", len(result.Events.Events))
	}
	if result.Meta.TookMS != 42.0 {
		t.Errorf("TookMS = %f, want 42.0", result.Meta.TookMS)
	}
}

func TestQuery_SyncAggregate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"type":       "aggregate",
				"columns":    []string{"source", "count"},
				"rows":       [][]interface{}{{"nginx", 42}},
				"total_rows": 1,
			},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	result, err := c.QuerySync(context.Background(), "| stats count by source", "-1h", "now")
	if err != nil {
		t.Fatal(err)
	}

	if result.Type != ResultTypeAggregate {
		t.Fatalf("Type = %q, want aggregate", result.Type)
	}
	if result.Aggregate == nil {
		t.Fatal("Aggregate is nil")
	}
	if len(result.Aggregate.Columns) != 2 {
		t.Errorf("len(Columns) = %d, want 2", len(result.Aggregate.Columns))
	}
	if result.Aggregate.TotalRows != 1 {
		t.Errorf("TotalRows = %d, want 1", result.Aggregate.TotalRows)
	}
}

func TestQuery_Async(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"type":   "job",
				"job_id": "qry_abc123",
				"status": "running",
				"progress": map[string]interface{}{
					"phase":   "scanning",
					"percent": 0.0,
				},
			},
			"meta": map[string]interface{}{"query_id": "qry_abc123"},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	job, err := c.QueryAsync(context.Background(), "long query", "-24h", "now")
	if err != nil {
		t.Fatal(err)
	}

	if job.JobID != "qry_abc123" {
		t.Errorf("JobID = %q, want qry_abc123", job.JobID)
	}
	if job.Status != "running" {
		t.Errorf("Status = %q, want running", job.Status)
	}
	if job.Progress == nil {
		t.Fatal("Progress is nil")
	}
	if job.Progress.Phase != "scanning" {
		t.Errorf("Phase = %q, want scanning", job.Progress.Phase)
	}
}

func TestQuery_Timechart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"type":     "timechart",
				"interval": "5m",
				"columns":  []string{"_time", "count"},
				"rows":     [][]interface{}{{"2024-01-01T00:00:00Z", 10}},
			},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	result, err := c.QuerySync(context.Background(), "| timechart count", "-1h", "now")
	if err != nil {
		t.Fatal(err)
	}

	if result.Type != ResultTypeTimechart {
		t.Fatalf("Type = %q, want timechart", result.Type)
	}
	if result.Aggregate == nil {
		t.Fatal("Aggregate is nil")
	}
	if result.Aggregate.Interval != "5m" {
		t.Errorf("Interval = %q, want 5m", result.Aggregate.Interval)
	}
}

func TestQueryGet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}

		q := r.URL.Query().Get("q")
		if q != "error" {
			t.Errorf("q = %q, want error", q)
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"type":     "events",
				"events":   []map[string]interface{}{},
				"total":    0,
				"has_more": false,
			},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	result, err := c.QueryGet(context.Background(), "error", "-1h", "", 100)
	if err != nil {
		t.Fatal(err)
	}

	if result.Type != ResultTypeEvents {
		t.Errorf("Type = %q, want events", result.Type)
	}
}

func TestExplain(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		if q != "error | stats count" {
			t.Errorf("q = %q", q)
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"is_valid": true,
				"parsed": map[string]interface{}{
					"pipeline":       []map[string]interface{}{{"command": "search"}, {"command": "stats"}},
					"result_type":    "aggregate",
					"estimated_cost": "low",
					"uses_full_scan": false,
					"fields_read":    []string{"level"},
				},
				"errors":       []interface{}{},
				"acceleration": map[string]interface{}{"available": false},
			},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	result, err := c.Explain(context.Background(), "error | stats count")
	if err != nil {
		t.Fatal(err)
	}

	if !result.IsValid {
		t.Error("IsValid = false, want true")
	}
	if result.Parsed == nil {
		t.Fatal("Parsed is nil")
	}
	if len(result.Parsed.Pipeline) != 2 {
		t.Errorf("len(Pipeline) = %d, want 2", len(result.Parsed.Pipeline))
	}
	if result.Parsed.ResultType != "aggregate" {
		t.Errorf("ResultType = %q", result.Parsed.ResultType)
	}
}

func TestQuery_InvalidQuery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{
				"code":       "INVALID_QUERY",
				"message":    "parse error: unexpected token",
				"suggestion": "check syntax",
			},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	_, err := c.Query(context.Background(), QueryRequest{Q: "|||"})

	if err == nil {
		t.Fatal("expected error")
	}
	if !IsInvalidQuery(err) {
		t.Errorf("IsInvalidQuery = false, want true; err = %v", err)
	}
}
