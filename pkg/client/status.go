package client

import (
	"context"
	"encoding/json"
	"net/http"
)

// Status returns the unified server status.
func (c *Client) Status(ctx context.Context) (*ServerStatus, error) {
	var s ServerStatus
	if _, err := c.doJSON(ctx, http.MethodGet, "/status", nil, &s); err != nil {
		return nil, err
	}

	return &s, nil
}

// Health returns the server health check result.
// Note: /health is outside /api/v1, so we use doRaw.
// The server wraps the response in a {"data": ...} envelope via respondData,
// so we must unwrap it before decoding into HealthResult.
func (c *Client) Health(ctx context.Context) (*HealthResult, error) {
	req, err := c.newRequest(ctx, http.MethodGet, c.baseURL+"/health", nil)
	if err != nil {
		return nil, err
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

	var result HealthResult
	if err := json.Unmarshal(env.Data, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

// Stats returns detailed server statistics.
func (c *Client) Stats(ctx context.Context) (*StatsResult, error) {
	var s StatsResult
	if _, err := c.doJSON(ctx, http.MethodGet, "/stats", nil, &s); err != nil {
		return nil, err
	}

	return &s, nil
}

// CacheStats returns cache statistics.
func (c *Client) CacheStats(ctx context.Context) (CacheStatsResult, error) {
	var s CacheStatsResult
	if _, err := c.doJSON(ctx, http.MethodGet, "/cache/stats", nil, &s); err != nil {
		return nil, err
	}

	return s, nil
}

// CacheClear clears all caches.
func (c *Client) CacheClear(ctx context.Context) error {
	return c.doNoContent(ctx, http.MethodDelete, "/cache")
}

// indexesResponse wraps the indexes list from the server.
type indexesResponse struct {
	Indexes []IndexInfo `json:"indexes"`
}

// Indexes returns all configured indexes.
func (c *Client) Indexes(ctx context.Context) ([]IndexInfo, error) {
	var resp indexesResponse
	if _, err := c.doJSON(ctx, http.MethodGet, "/indexes", nil, &resp); err != nil {
		return nil, err
	}

	return resp.Indexes, nil
}

// Metrics returns storage metrics.
func (c *Client) Metrics(ctx context.Context) (MetricsResult, error) {
	var m MetricsResult
	if _, err := c.doJSON(ctx, http.MethodGet, "/metrics", nil, &m); err != nil {
		return nil, err
	}

	return m, nil
}
