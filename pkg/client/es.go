package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
)

// ESBulk sends data in Elasticsearch bulk format and returns the parsed result.
func (c *Client) ESBulk(ctx context.Context, body io.Reader) (*ESBulkResult, error) {
	resp, err := c.doRaw(ctx, http.MethodPost, "/es/_bulk", body, "application/x-ndjson")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result ESBulkResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// ESIndexDoc indexes a single document in ES-compatible format.
func (c *Client) ESIndexDoc(ctx context.Context, index string, doc io.Reader) (*http.Response, error) {
	return c.doRaw(ctx, http.MethodPost, "/es/"+url.PathEscape(index)+"/_doc", doc, "application/json")
}

// ESClusterInfo returns ES-compatible cluster info.
func (c *Client) ESClusterInfo(ctx context.Context) (*ESClusterInfoResult, error) {
	resp, err := c.doRaw(ctx, http.MethodGet, "/es/", nil, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result ESClusterInfoResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}
