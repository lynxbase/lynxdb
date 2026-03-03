package client

import (
	"context"
	"net/http"
)

// GetConfig returns the current server configuration.
func (c *Client) GetConfig(ctx context.Context) (ConfigResult, error) {
	var result ConfigResult
	if _, err := c.doJSON(ctx, http.MethodGet, "/config", nil, &result); err != nil {
		return nil, err
	}

	return result, nil
}

// PatchConfig updates server configuration fields.
func (c *Client) PatchConfig(ctx context.Context, patch ConfigPatch) (ConfigResult, error) {
	var result ConfigResult
	if _, err := c.doJSON(ctx, http.MethodPatch, "/config", patch, &result); err != nil {
		return nil, err
	}

	return result, nil
}
