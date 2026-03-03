package client

import (
	"context"
	"net/http"
	"net/url"
)

// ListSavedQueries returns all saved queries.
// The CRUD List handler returns a raw array in the envelope data field.
func (c *Client) ListSavedQueries(ctx context.Context) ([]SavedQuery, error) {
	var resp []SavedQuery
	if _, err := c.doJSON(ctx, http.MethodGet, "/queries", nil, &resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// CreateSavedQuery creates a new saved query.
func (c *Client) CreateSavedQuery(ctx context.Context, input SavedQueryInput) (*SavedQuery, error) {
	var sq SavedQuery
	if _, err := c.doJSON(ctx, http.MethodPost, "/queries", input, &sq); err != nil {
		return nil, err
	}

	return &sq, nil
}

// UpdateSavedQuery updates an existing saved query.
func (c *Client) UpdateSavedQuery(ctx context.Context, id string, input SavedQueryInput) (*SavedQuery, error) {
	var sq SavedQuery
	if _, err := c.doJSON(ctx, http.MethodPut, "/queries/"+url.PathEscape(id), input, &sq); err != nil {
		return nil, err
	}

	return &sq, nil
}

// DeleteSavedQuery deletes a saved query.
func (c *Client) DeleteSavedQuery(ctx context.Context, id string) error {
	return c.doNoContent(ctx, http.MethodDelete, "/queries/"+url.PathEscape(id))
}
