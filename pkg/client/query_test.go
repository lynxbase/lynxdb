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
			"meta": map[string]interface{}{"took_ms": 42.0, "scanned": 1000},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	result, err := c.Query(context.Background(), QueryRequest{Q: "search source=main error", From: "-1h"})
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

func TestQuery_LintOptionAndMetadata(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Lint == nil || *req.Lint {
			t.Fatalf("Lint = %v, want pointer to false", req.Lint)
		}
		if req.LintLimit != 2 {
			t.Fatalf("LintLimit = %d, want 2", req.LintLimit)
		}
		if !req.LintFull {
			t.Fatal("LintFull = false, want true")
		}
		if req.Suggestions == nil || *req.Suggestions {
			t.Fatalf("Suggestions = %v, want false", req.Suggestions)
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"type":     "events",
				"events":   []map[string]interface{}{},
				"total":    0,
				"has_more": false,
			},
			"meta": map[string]interface{}{
				"lints": []map[string]interface{}{
					{"code": "L002", "message": "Default source", "position": 0},
				},
				"suggestions": []map[string]interface{}{
					{"text": "errors by service", "reason": "shortcut", "source_code": "L020"},
				},
				"explain": map[string]interface{}{
					"source_scope":       map[string]interface{}{"selected": []string{"main"}, "count": 1},
					"segments":           map[string]interface{}{"total": 2, "scanned": 1, "skipped": 1, "skipped_time": 1},
					"candidate_rows":     10,
					"literal_extraction": true,
					"regex_engine":       "linear",
				},
			},
		})
	}))
	defer srv.Close()

	lint := false
	suggestions := false
	c := NewClient(WithBaseURL(srv.URL))
	result, err := c.Query(context.Background(), QueryRequest{Q: "search source=main error", Lint: &lint, Suggestions: &suggestions, LintLimit: 2, LintFull: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Meta.Lints) != 1 {
		t.Fatalf("len(Meta.Lints) = %d, want 1", len(result.Meta.Lints))
	}
	if result.Meta.Lints[0].Code != "L002" {
		t.Errorf("lint code = %q, want L002", result.Meta.Lints[0].Code)
	}
	if len(result.Meta.Suggestions) != 1 {
		t.Fatalf("len(Meta.Suggestions) = %d, want 1", len(result.Meta.Suggestions))
	}
	if result.Meta.Suggestions[0].Text != "errors by service" {
		t.Errorf("suggestion text = %q, want errors by service", result.Meta.Suggestions[0].Text)
	}
	if result.Meta.Explain == nil {
		t.Fatal("Meta.Explain is nil")
	}
	if result.Meta.Explain.SourceScope == nil || result.Meta.Explain.SourceScope.Count != 1 {
		t.Fatalf("Explain.SourceScope = %#v, want count 1", result.Meta.Explain.SourceScope)
	}
	if result.Meta.Explain.Segments == nil || result.Meta.Explain.Segments.SkippedTime != 1 {
		t.Fatalf("Explain.Segments = %#v, want skipped_time 1", result.Meta.Explain.Segments)
	}
	if result.Meta.Explain.CandidateRows == nil || *result.Meta.Explain.CandidateRows != 10 {
		t.Fatalf("Explain.CandidateRows = %#v, want 10", result.Meta.Explain.CandidateRows)
	}
	if result.Meta.Explain.LiteralExtraction == nil || !*result.Meta.Explain.LiteralExtraction {
		t.Fatalf("Explain.LiteralExtraction = %#v, want true", result.Meta.Explain.LiteralExtraction)
	}
	if result.Meta.Explain.RegexEngine != "linear" {
		t.Fatalf("Explain.RegexEngine = %q, want linear", result.Meta.Explain.RegexEngine)
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
					"phase":   "scanning",
					"percent": 0.0,
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
