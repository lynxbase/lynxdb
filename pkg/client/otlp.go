package client

import (
	"context"
	"io"
	"net/http"
)

// OTLPIngestLogs sends OTLP-formatted log data to the server.
// The body should be a protobuf or JSON-encoded OTLP LogsData payload.
func (c *Client) OTLPIngestLogs(ctx context.Context, body io.Reader, contentType string) (*http.Response, error) {
	if contentType == "" {
		contentType = "application/json"
	}

	return c.doRaw(ctx, http.MethodPost, "/otlp/v1/logs", body, contentType)
}
