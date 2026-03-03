package client

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// fieldsResponse wraps the fields list from the server.
type fieldsResponse struct {
	Fields []FieldInfo `json:"fields"`
}

// Fields returns the field catalog.
func (c *Client) Fields(ctx context.Context) ([]FieldInfo, error) {
	var resp fieldsResponse
	if _, err := c.doJSON(ctx, http.MethodGet, "/fields", nil, &resp); err != nil {
		return nil, err
	}

	return resp.Fields, nil
}

// FieldsOpts configures the filtered fields listing endpoint.
type FieldsOpts struct {
	Since  string
	From   string
	To     string
	Source string
	Prefix string
}

// FieldsFiltered returns the field catalog with optional filters.
func (c *Client) FieldsFiltered(ctx context.Context, opts FieldsOpts) ([]FieldInfo, error) {
	params := url.Values{}
	if opts.Since != "" {
		params.Set("since", opts.Since)
	}
	if opts.From != "" {
		params.Set("from", opts.From)
	}
	if opts.To != "" {
		params.Set("to", opts.To)
	}
	if opts.Source != "" {
		params.Set("source", opts.Source)
	}
	if opts.Prefix != "" {
		params.Set("prefix", opts.Prefix)
	}

	path := "/fields"
	if len(params) > 0 {
		path += "?" + params.Encode()
	}

	var resp fieldsResponse
	if _, err := c.doJSON(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}

	return resp.Fields, nil
}

// FieldValuesOpts configures the FieldValues endpoint with optional filters.
type FieldValuesOpts struct {
	Limit int
	Since string
	From  string
	To    string
}

// FieldValuesFiltered returns the top values for a field with optional filters.
func (c *Client) FieldValuesFiltered(ctx context.Context, fieldName string, opts FieldValuesOpts) (*FieldValuesResult, error) {
	params := url.Values{}
	if opts.Limit > 0 {
		params.Set("limit", fmt.Sprintf("%d", opts.Limit))
	}
	if opts.Since != "" {
		params.Set("since", opts.Since)
	}
	if opts.From != "" {
		params.Set("from", opts.From)
	}
	if opts.To != "" {
		params.Set("to", opts.To)
	}

	path := "/fields/" + url.PathEscape(fieldName) + "/values"
	if len(params) > 0 {
		path += "?" + params.Encode()
	}

	var result FieldValuesResult
	meta, err := c.doJSON(ctx, http.MethodGet, path, nil, &result)
	if err != nil {
		return nil, err
	}

	result.Meta = meta

	return &result, nil
}

// FieldValues returns the top values for a specific field.
func (c *Client) FieldValues(ctx context.Context, fieldName string, limit int) (*FieldValuesResult, error) {
	path := "/fields/" + url.PathEscape(fieldName) + "/values"
	if limit > 0 {
		path += fmt.Sprintf("?limit=%d", limit)
	}

	var result FieldValuesResult
	meta, err := c.doJSON(ctx, http.MethodGet, path, nil, &result)
	if err != nil {
		return nil, err
	}

	result.Meta = meta

	return &result, nil
}

// sourcesResponse wraps the sources list from the server.
type sourcesResponse struct {
	Sources []SourceInfo `json:"sources"`
}

// Sources returns all known data sources.
func (c *Client) Sources(ctx context.Context) ([]SourceInfo, error) {
	var resp sourcesResponse
	if _, err := c.doJSON(ctx, http.MethodGet, "/sources", nil, &resp); err != nil {
		return nil, err
	}

	return resp.Sources, nil
}
