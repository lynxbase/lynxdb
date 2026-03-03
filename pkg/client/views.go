package client

import (
	"context"
	"net/http"
	"net/url"
)

// viewsListResponse wraps the views list from the server.
type viewsListResponse struct {
	Views []View `json:"views"`
}

// ListViews returns all materialized views.
func (c *Client) ListViews(ctx context.Context) ([]View, error) {
	var resp viewsListResponse
	if _, err := c.doJSON(ctx, http.MethodGet, "/views", nil, &resp); err != nil {
		return nil, err
	}

	return resp.Views, nil
}

// CreateView creates a new materialized view.
func (c *Client) CreateView(ctx context.Context, input ViewInput) (*View, error) {
	var view View
	if _, err := c.doJSON(ctx, http.MethodPost, "/views", input, &view); err != nil {
		return nil, err
	}

	return &view, nil
}

// GetView returns details of a specific materialized view.
func (c *Client) GetView(ctx context.Context, name string) (*ViewDetail, error) {
	var detail ViewDetail
	if _, err := c.doJSON(ctx, http.MethodGet, "/views/"+url.PathEscape(name), nil, &detail); err != nil {
		return nil, err
	}

	return &detail, nil
}

// PatchView updates a materialized view's configuration.
func (c *Client) PatchView(ctx context.Context, name string, input ViewPatchInput) (*View, error) {
	var view View
	if _, err := c.doJSON(ctx, http.MethodPatch, "/views/"+url.PathEscape(name), input, &view); err != nil {
		return nil, err
	}

	return &view, nil
}

// DeleteView deletes a materialized view.
func (c *Client) DeleteView(ctx context.Context, name string) error {
	return c.doNoContent(ctx, http.MethodDelete, "/views/"+url.PathEscape(name))
}

// TriggerBackfill manually triggers a backfill for a materialized view.
// Returns the response status and message from the server.
func (c *Client) TriggerBackfill(ctx context.Context, name string) error {
	var resp struct {
		Status string `json:"status"`
	}
	if _, err := c.doJSON(ctx, http.MethodPost, "/views/"+url.PathEscape(name)+"/backfill", nil, &resp); err != nil {
		return err
	}

	return nil
}

// ViewBackfill returns the backfill progress for a view (SSE stream).
func (c *Client) ViewBackfill(ctx context.Context, name string, fn func(SSEEvent) error) error {
	resp, err := c.doRaw(ctx, http.MethodGet, "/views/"+url.PathEscape(name)+"/backfill", nil, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return readSSE(resp.Body, fn)
}
