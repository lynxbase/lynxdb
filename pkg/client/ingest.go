package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
)

// Ingest sends structured JSON events to the server.
func (c *Client) Ingest(ctx context.Context, events []map[string]interface{}) (*IngestResult, error) {
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

// IngestNDJSON sends NDJSON data to the /ingest endpoint.
// Unlike IngestRaw (which sends to /ingest/raw for plain text),
// this sends structured NDJSON to the main ingest endpoint.
func (c *Client) IngestNDJSON(ctx context.Context, body io.Reader, opts IngestOpts) (*IngestResult, error) {
	req, err := c.newRequest(ctx, http.MethodPost, c.url("/ingest"), body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-ndjson")

	if opts.Source != "" {
		req.Header.Set("X-Source", opts.Source)
	}
	if opts.Index != "" {
		req.Header.Set("X-Index", opts.Index)
	}
	if opts.Transform != "" {
		req.Header.Set("X-Transform", opts.Transform)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, c.parseAPIError(resp)
	}

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

// IngestBulk sends data in Elasticsearch bulk format.
// The caller provides an NDJSON body. Returns the raw *http.Response
// so the caller can inspect ES-compatible response fields.
func (c *Client) IngestBulk(ctx context.Context, body io.Reader) (*http.Response, error) {
	return c.doRaw(ctx, http.MethodPost, "/ingest/bulk", body, "application/x-ndjson")
}
