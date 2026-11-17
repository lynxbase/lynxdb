package client

import (
	"context"
	"io"
	"net/http"
)

// OTLPIngestLogs sends a JSON-encoded OTLP LogsData payload to /api/v1/otlp/v1/logs.
func (c *Client) OTLPIngestLogs(ctx context.Context, body io.Reader, contentType string) (*http.Response, error) {
	if contentType == "" {
		contentType = "application/json"
	}

	return c.doRaw(ctx, http.MethodPost, "/otlp/v1/logs", body, contentType)
}
