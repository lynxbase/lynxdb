package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

var structuredIngestKeys = map[string]bool{
	"event":      true,
	"time":       true,
	"source":     true,
	"sourcetype": true,
	"host":       true,
	"index":      true,
	"fields":     true,
}

// Ingest sends structured event envelopes to the server.
// Deprecated: use IngestEvents with []IngestEvent for type safety.
func (c *Client) Ingest(ctx context.Context, events []map[string]interface{}) (*IngestResult, error) {
	typed := make([]IngestEvent, 0, len(events))
	for i, raw := range events {
		ev, err := toIngestEvent(raw)
		if err != nil {
			return nil, &APIError{
				HTTPStatus: 400,
				Code:       ErrCodeValidationError,
				Message:    fmt.Sprintf("structured ingest item %d: %v", i, err),
				Suggestion: "POST /api/v1/ingest expects objects with event plus optional time/source/sourcetype/host/index/fields. Use IngestRaw for arbitrary JSON lines.",
			}
		}
		typed = append(typed, ev)
	}

	return c.IngestEvents(ctx, typed)
}

// IngestEvents sends structured event payloads to POST /api/v1/ingest.
func (c *Client) IngestEvents(ctx context.Context, events []IngestEvent) (*IngestResult, error) {
	var result IngestResult
	if _, err := c.doJSON(ctx, http.MethodPost, "/ingest", events, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

// IngestRaw sends raw log data (text/plain, application/json, etc.) to the server.
// Headers like X-Source, X-Sourcetype, X-Index are set from opts.
func (c *Client) IngestRaw(ctx context.Context, body io.Reader, opts IngestOpts) (*IngestResult, error) {
	ct := opts.ContentType
	if ct == "" {
		ct = "text/plain"
	}

	req, err := c.newRequest(ctx, http.MethodPost, c.url("/ingest/raw"), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", ct)
	if opts.Index != "" {
		req.Header.Set("X-Index", opts.Index)
	}
	if opts.Source != "" {
		req.Header.Set("X-Source", opts.Source)
	}
	if opts.Sourcetype != "" {
		req.Header.Set("X-Source-Type", opts.Sourcetype)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, c.parseAPIError(resp)
	}

	return decodeIngestResult(resp)
}

// IngestNDJSON sends newline-delimited data to the raw ingest endpoint.
// Deprecated: use IngestRaw with application/x-ndjson or ESBulk for Elasticsearch bulk payloads.
func (c *Client) IngestNDJSON(ctx context.Context, body io.Reader, opts IngestOpts) (*IngestResult, error) {
	opts.ContentType = "application/x-ndjson"
	if opts.Sourcetype == "" {
		opts.Sourcetype = "json"
	}

	return c.IngestRaw(ctx, body, opts)
}

// IngestBulk sends data in Elasticsearch bulk format.
// Deprecated: use ESBulk, which targets the preferred /api/v1/es/_bulk path.
func (c *Client) IngestBulk(ctx context.Context, body io.Reader) (*http.Response, error) {
	return c.doRaw(ctx, http.MethodPost, "/es/_bulk", body, "application/x-ndjson")
}

func decodeIngestResult(resp *http.Response) (*IngestResult, error) {
	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, err
	}

	var result IngestResult
	if err := json.Unmarshal(env.Data, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func toIngestEvent(raw map[string]interface{}) (IngestEvent, error) {
	for key := range raw {
		if !structuredIngestKeys[key] {
			return IngestEvent{}, fmt.Errorf("unsupported top-level key %q", key)
		}
	}

	msg, ok := raw["event"].(string)
	if !ok || msg == "" {
		return IngestEvent{}, fmt.Errorf("missing required string field %q", "event")
	}

	ev := IngestEvent{Event: msg}
	if v, ok := raw["time"]; ok {
		t, err := ingestTimeValue(v)
		if err != nil {
			return IngestEvent{}, err
		}
		ev.Time = &t
	}
	if v, ok := raw["source"].(string); ok {
		ev.Source = v
	}
	if v, ok := raw["sourcetype"].(string); ok {
		ev.Sourcetype = v
	}
	if v, ok := raw["host"].(string); ok {
		ev.Host = v
	}
	if v, ok := raw["index"].(string); ok {
		ev.Index = v
	}
	if v, ok := raw["fields"]; ok {
		fields, ok := v.(map[string]interface{})
		if !ok {
			return IngestEvent{}, fmt.Errorf("field %q must be an object", "fields")
		}
		ev.Fields = fields
	}

	return ev, nil
}

func ingestTimeValue(v interface{}) (float64, error) {
	switch tv := v.(type) {
	case float64:
		return tv, nil
	case int:
		return float64(tv), nil
	case int64:
		return float64(tv), nil
	default:
		return 0, fmt.Errorf("field %q must be numeric seconds since epoch", "time")
	}
}
