package client

import (
	"context"
	"encoding/json"
	"errors"
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
		if req.Q != "search source=main error" {
			t.Errorf("Q = %q, want %q", req.Q, "search source=main error")
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"type":     "events",
				"events":   []map[string]interface{}{{"message": "test"}},
				"total":    1,
				"has_more": false,
			},
			"meta": map[string]interface{}{"took_ms": 42.0, "query_id": "qry_1", "rows": 1},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	result, err := c.Query(context.Background(), QueryRequest{Q: "search source=main error", From: "-1h"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Meta.QueryID != "qry_1" || result.Meta.Rows != 1 {
		t.Errorf("Meta = %+v, want query_id qry_1 and 1 row", result.Meta)
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
	result, err := c.QuerySync(context.Background(), "source=main | stats count() by source", "-1h", "now")
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
					"status":     "running",
					"elapsed_ms": 12,
				},
			},
			"meta": map[string]interface{}{"query_id": "qry_abc123"},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	job, err := c.QueryAsync(context.Background(), "source=main | stats count() by host", "-24h", "now")
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
	if job.Progress.Status != "running" || job.Progress.ElapsedMS != 12 {
		t.Errorf("Progress = %+v, want running after 12ms", job.Progress)
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
	result, err := c.QuerySync(context.Background(), "source=main | timechart span=5m count()", "-1h", "now")
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
		if q != "search source=main error" {
			t.Errorf("q = %q, want search source=main error", q)
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
	result, err := c.QueryGet(context.Background(), "search source=main error", "-1h", "", 100)
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
		if q != "search source=main error | stats count()" {
			t.Errorf("q = %q", q)
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"is_valid": true,
				"parsed": map[string]interface{}{
					"result_type":    "aggregate",
					"estimated_cost": "low",
					"uses_full_scan": false,
					"fields_read":    []string{"level"},
					"logical_plan":   "Aggregate()\n",
					"optimized_plan": "Aggregate()\n",
				},
				"errors": []interface{}{},
			},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	result, err := c.Explain(context.Background(), "search source=main error | stats count()")
	if err != nil {
		t.Fatal(err)
	}

	if !result.IsValid {
		t.Error("IsValid = false, want true")
	}
	if result.Parsed == nil {
		t.Fatal("Parsed is nil")
	}
	if result.Parsed.OptimizedPlan != "Aggregate()\n" {
		t.Errorf("OptimizedPlan = %q", result.Parsed.OptimizedPlan)
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
				"code":       "SYNTAX_ERROR",
				"message":    "syntax error at line 1, column 1: missing source",
				"position":   map[string]interface{}{"line": 1, "column": 1},
				"suggestion": "a query starts with source=<index>",
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
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %T, want *APIError", err)
	}
	if apiErr.Code != ErrCodeSyntaxError {
		t.Errorf("Code = %q, want %q", apiErr.Code, ErrCodeSyntaxError)
	}
	if apiErr.Suggestion != "a query starts with source=<index>" {
		t.Errorf("Suggestion = %q", apiErr.Suggestion)
	}
}
