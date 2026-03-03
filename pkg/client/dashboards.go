package client

import (
	"context"
	"net/http"
	"net/url"
)

// ListDashboards returns all dashboards.
// The CRUD List handler returns a raw array in the envelope data field.
func (c *Client) ListDashboards(ctx context.Context) ([]Dashboard, error) {
	var resp []Dashboard
	if _, err := c.doJSON(ctx, http.MethodGet, "/dashboards", nil, &resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// CreateDashboard creates a new dashboard.
func (c *Client) CreateDashboard(ctx context.Context, input DashboardInput) (*Dashboard, error) {
	var d Dashboard
	if _, err := c.doJSON(ctx, http.MethodPost, "/dashboards", input, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

// GetDashboard returns a specific dashboard by ID.
func (c *Client) GetDashboard(ctx context.Context, id string) (*Dashboard, error) {
	var d Dashboard
	if _, err := c.doJSON(ctx, http.MethodGet, "/dashboards/"+url.PathEscape(id), nil, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

// UpdateDashboard updates an existing dashboard.
func (c *Client) UpdateDashboard(ctx context.Context, id string, input DashboardInput) (*Dashboard, error) {
	var d Dashboard
	if _, err := c.doJSON(ctx, http.MethodPut, "/dashboards/"+url.PathEscape(id), input, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

// DeleteDashboard deletes a dashboard.
func (c *Client) DeleteDashboard(ctx context.Context, id string) error {
	return c.doNoContent(ctx, http.MethodDelete, "/dashboards/"+url.PathEscape(id))
}
