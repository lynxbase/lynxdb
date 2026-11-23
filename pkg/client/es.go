package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
)

// ESBulk sends an Elasticsearch bulk request body to POST /_bulk, or to
// POST /{index}/_bulk when index is set, which names the index of the
// actions that name none.
func (c *Client) ESBulk(ctx context.Context, index string, body io.Reader) (*ESBulkResult, error) {
	path := "/_bulk"
	if index != "" {
		path = "/" + url.PathEscape(index) + "/_bulk"
	}
	resp, err := c.doRawAt(ctx, http.MethodPost, c.baseURL+path, body, "application/x-ndjson")
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

// ESClusterInfo returns the Elasticsearch cluster info of GET /.
func (c *Client) ESClusterInfo(ctx context.Context) (*ESClusterInfoResult, error) {
	resp, err := c.doRawAt(ctx, http.MethodGet, c.baseURL+"/", nil, "")
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
