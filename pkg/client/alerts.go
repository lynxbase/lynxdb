package client

import (
	"context"
	"net/http"
	"net/url"
)

// alertsListResponse wraps the alerts list from the server.
type alertsListResponse struct {
	Alerts []Alert `json:"alerts"`
}

// ListAlerts returns all configured alerts.
func (c *Client) ListAlerts(ctx context.Context) ([]Alert, error) {
	var resp alertsListResponse
	if _, err := c.doJSON(ctx, http.MethodGet, "/alerts", nil, &resp); err != nil {
		return nil, err
	}

	return resp.Alerts, nil
}

// CreateAlert creates a new alert.
func (c *Client) CreateAlert(ctx context.Context, input AlertInput) (*Alert, error) {
	var alert Alert
	if _, err := c.doJSON(ctx, http.MethodPost, "/alerts", input, &alert); err != nil {
		return nil, err
	}

	return &alert, nil
}

// GetAlert returns a specific alert by ID.
func (c *Client) GetAlert(ctx context.Context, id string) (*Alert, error) {
	var alert Alert
	if _, err := c.doJSON(ctx, http.MethodGet, "/alerts/"+url.PathEscape(id), nil, &alert); err != nil {
		return nil, err
	}

	return &alert, nil
}

// UpdateAlert updates an existing alert.
func (c *Client) UpdateAlert(ctx context.Context, id string, input AlertInput) (*Alert, error) {
	var alert Alert
	if _, err := c.doJSON(ctx, http.MethodPut, "/alerts/"+url.PathEscape(id), input, &alert); err != nil {
		return nil, err
	}

	return &alert, nil
}

// PatchAlert partially updates an alert (e.g. enable/disable).
func (c *Client) PatchAlert(ctx context.Context, id string, patch AlertPatchInput) (*Alert, error) {
	var alert Alert
	if _, err := c.doJSON(ctx, http.MethodPatch, "/alerts/"+url.PathEscape(id), patch, &alert); err != nil {
		return nil, err
	}

	return &alert, nil
}

// DeleteAlert deletes an alert.
func (c *Client) DeleteAlert(ctx context.Context, id string) error {
	return c.doNoContent(ctx, http.MethodDelete, "/alerts/"+url.PathEscape(id))
}

// TestAlert runs a dry-run of an alert and returns what would happen.
func (c *Client) TestAlert(ctx context.Context, id string) (*AlertTestResult, error) {
	var result AlertTestResult
	if _, err := c.doJSON(ctx, http.MethodPost, "/alerts/"+url.PathEscape(id)+"/test", nil, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

// TestAlertChannels tests the notification channels of an alert.
func (c *Client) TestAlertChannels(ctx context.Context, id string) (*AlertTestResult, error) {
	var result AlertTestResult
	if _, err := c.doJSON(ctx, http.MethodPost, "/alerts/"+url.PathEscape(id)+"/test-channels", nil, &result); err != nil {
		return nil, err
	}

	return &result, nil
}
