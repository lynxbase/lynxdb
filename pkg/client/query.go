package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// Query executes a query and returns a polymorphic result.
// For sync queries (Wait==nil): returns Events or Aggregate result with HTTP 200.
// For async queries (Wait==0): returns a JobHandle with HTTP 202.
// For hybrid queries (Wait>0): returns either based on whether the query completed in time.
func (c *Client) Query(ctx context.Context, req QueryRequest) (*QueryResult, error) {
	status, data, meta, err := c.doJSONWithStatus(ctx, http.MethodPost, "/query", req)
	if err != nil {
		return nil, err
	}

	return c.parseQueryResult(status, data, meta)
}

// QuerySync is a convenience wrapper that forces synchronous execution.
func (c *Client) QuerySync(ctx context.Context, q, from, to string) (*QueryResult, error) {
	return c.Query(ctx, QueryRequest{Q: q, From: from, To: to})
}

// QueryAsync is a convenience wrapper that forces asynchronous execution.
func (c *Client) QueryAsync(ctx context.Context, q, from, to string) (*JobHandle, error) {
	wait := float64(0)
	result, err := c.Query(ctx, QueryRequest{Q: q, From: from, To: to, Wait: &wait})
	if err != nil {
		return nil, err
	}

	if result.Job == nil {
		return nil, fmt.Errorf("lynxdb: expected async job response, got %s", result.Type)
	}

	return result.Job, nil
}

// QueryGet executes a query via GET with query parameters.
func (c *Client) QueryGet(ctx context.Context, q, from, to string, limit int) (*QueryResult, error) {
	path := fmt.Sprintf("/query?q=%s", urlEncode(q))
	if from != "" {
		path += "&from=" + urlEncode(from)
	}
	if to != "" {
		path += "&to=" + urlEncode(to)
	}
	if limit > 0 {
		path += fmt.Sprintf("&limit=%d", limit)
	}

	status, data, meta, err := c.doJSONWithStatus(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	return c.parseQueryResult(status, data, meta)
}

// Explain returns the query execution plan without running the query.
func (c *Client) Explain(ctx context.Context, q string) (*ExplainResult, error) {
	path := "/query/explain?q=" + urlEncode(q)

	var result ExplainResult
	if _, err := c.doJSON(ctx, http.MethodGet, path, nil, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

// ListJobs returns all active/recent query jobs.
func (c *Client) ListJobs(ctx context.Context) (*JobListResult, error) {
	var result JobListResult
	if _, err := c.doJSON(ctx, http.MethodGet, "/query/jobs", nil, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

// ListJobsFiltered returns jobs filtered by status.
func (c *Client) ListJobsFiltered(ctx context.Context, status string) (*JobListResult, error) {
	path := "/query/jobs"
	if status != "" {
		path += "?status=" + url.QueryEscape(status)
	}

	var result JobListResult
	if _, err := c.doJSON(ctx, http.MethodGet, path, nil, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

// parseQueryResult interprets the polymorphic query response.
func (c *Client) parseQueryResult(status int, data json.RawMessage, meta Meta) (*QueryResult, error) {
	// Async/hybrid → job handle (202).
	if status == http.StatusAccepted {
		var job JobHandle
		if err := json.Unmarshal(data, &job); err != nil {
			return nil, fmt.Errorf("lynxdb: decode job handle: %w", err)
		}

		return &QueryResult{Type: ResultTypeJob, Job: &job, Meta: meta}, nil
	}

	// Sync → determine type from data.type field.
	var typed struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &typed); err != nil {
		return nil, fmt.Errorf("lynxdb: decode result type: %w", err)
	}

	result := &QueryResult{Meta: meta}

	switch QueryResultType(typed.Type) {
	case ResultTypeEvents:
		var events EventsResult
		if err := json.Unmarshal(data, &events); err != nil {
			return nil, fmt.Errorf("lynxdb: decode events result: %w", err)
		}

		result.Type = ResultTypeEvents
		result.Events = &events
	case ResultTypeAggregate, ResultTypeTimechart:
		var agg AggregateResult
		if err := json.Unmarshal(data, &agg); err != nil {
			return nil, fmt.Errorf("lynxdb: decode aggregate result: %w", err)
		}

		result.Type = QueryResultType(typed.Type)
		result.Aggregate = &agg
	default:
		return nil, fmt.Errorf("lynxdb: unknown result type: %q", typed.Type)
	}

	return result, nil
}

// urlEncode encodes a string for use in a URL query parameter.
func urlEncode(s string) string {
	return url.QueryEscape(s)
}
