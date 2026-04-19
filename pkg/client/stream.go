package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const ndjsonScanBufSize = 1 << 20 // 1MB

type streamQueryRequest struct {
	Q    string `json:"q,omitempty"`
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

// QueryStream executes a streaming query via POST /query/stream.
// Each NDJSON line is delivered to fn. Returns the final stream metadata.
func (c *Client) QueryStream(ctx context.Context, req QueryRequest, fn func(json.RawMessage) error) (*StreamMeta, error) {
	body, err := marshalReader(c, streamQueryRequest{
		Q:    req.Q,
		From: req.From,
		To:   req.To,
	})
	if err != nil {
		return nil, err
	}
	resp, err := c.doRaw(ctx, http.MethodPost, "/query/stream", body, "application/json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return readNDJSON(resp.Body, fn)
}

// Tail opens a live tail SSE stream. Events are delivered to fn until the
// context is canceled or the server closes the connection.
func (c *Client) Tail(ctx context.Context, q, from string, count int, fn func(SSEEvent) error) error {
	params := url.Values{"q": {q}}
	if from != "" {
		params.Set("from", from)
	}
	if count > 0 {
		params.Set("count", fmt.Sprintf("%d", count))
	}

	resp, err := c.doRaw(ctx, http.MethodGet, "/tail?"+params.Encode(), nil, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return readSSE(resp.Body, fn)
}

// StreamJobProgress opens an SSE stream for job progress updates.
func (c *Client) StreamJobProgress(ctx context.Context, jobID string, fn func(SSEEvent) error) error {
	path := "/query/jobs/" + url.PathEscape(jobID) + "/stream"

	resp, err := c.doRaw(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return readSSE(resp.Body, fn)
}

// Histogram returns event count histogram buckets.
func (c *Client) Histogram(ctx context.Context, from, to string, buckets int, index string) (*HistogramResult, error) {
	params := url.Values{}
	if from != "" {
		params.Set("from", from)
	}
	if to != "" {
		params.Set("to", to)
	}
	if buckets > 0 {
		params.Set("buckets", fmt.Sprintf("%d", buckets))
	}
	if index != "" {
		params.Set("index", index)
	}

	path := "/histogram"
	if len(params) > 0 {
		path += "?" + params.Encode()
	}

	var result HistogramResult
	meta, err := c.doJSON(ctx, http.MethodGet, path, nil, &result)
	if err != nil {
		return nil, err
	}

	result.Meta = meta

	return &result, nil
}

// readNDJSON reads newline-delimited JSON from r, calling fn for each line
// except the final __meta line. Returns the parsed StreamMeta.
func readNDJSON(r io.Reader, fn func(json.RawMessage) error) (*StreamMeta, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, ndjsonScanBufSize), ndjsonScanBufSize)

	var meta *StreamMeta

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Fast-path: check for __meta prefix.
		if bytes.HasPrefix(line, []byte(`{"__meta":`)) {
			var wrapper struct {
				Meta StreamMeta `json:"__meta"`
			}
			if json.Unmarshal(line, &wrapper) == nil {
				meta = &wrapper.Meta
			}

			continue
		}

		// Check for __error prefix.
		if bytes.HasPrefix(line, []byte(`{"__error":`)) {
			var wrapper struct {
				Error struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"__error"`
			}
			if json.Unmarshal(line, &wrapper) == nil {
				return meta, fmt.Errorf("lynxdb: stream error: %s: %s", wrapper.Error.Code, wrapper.Error.Message)
			}

			continue
		}

		// Copy line bytes before callback (scanner reuses buffer).
		lineCopy := make(json.RawMessage, len(line))
		copy(lineCopy, line)

		if err := fn(lineCopy); err != nil {
			return meta, err
		}
	}

	if err := scanner.Err(); err != nil {
		return meta, fmt.Errorf("lynxdb: read NDJSON stream: %w", err)
	}

	return meta, nil
}

// readSSE reads Server-Sent Events from r, calling fn for each event.
func readSSE(r io.Reader, fn func(SSEEvent) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, ndjsonScanBufSize), ndjsonScanBufSize)

	var eventType string
	var dataBuf strings.Builder

	for scanner.Scan() {
		line := scanner.Text()

		if line == "" {
			// Empty line = end of event.
			if eventType != "" || dataBuf.Len() > 0 {
				data := strings.TrimSpace(dataBuf.String())
				evt := SSEEvent{
					Event: eventType,
					Data:  json.RawMessage(data),
				}

				if err := fn(evt); err != nil {
					return err
				}

				eventType = ""
				dataBuf.Reset()
			}

			continue
		}

		if strings.HasPrefix(line, "event: ") {
			eventType = strings.TrimPrefix(line, "event: ")
		} else if strings.HasPrefix(line, "data: ") {
			if dataBuf.Len() > 0 {
				dataBuf.WriteByte('\n')
			}

			dataBuf.WriteString(strings.TrimPrefix(line, "data: "))
		} else if line == "data:" {
			if dataBuf.Len() > 0 {
				dataBuf.WriteByte('\n')
			}
		}
	}

	// Flush any remaining event.
	if eventType != "" || dataBuf.Len() > 0 {
		data := strings.TrimSpace(dataBuf.String())
		evt := SSEEvent{
			Event: eventType,
			Data:  json.RawMessage(data),
		}

		if err := fn(evt); err != nil {
			return err
		}
	}

	return scanner.Err()
}

// marshalReader encodes v as JSON into a buffer from the pool and returns a reader.
// Returns an error if JSON encoding fails.
func marshalReader(c *Client, v interface{}) (io.Reader, error) {
	buf := c.getBuf()
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		return nil, fmt.Errorf("marshalReader: %w", err)
	}

	return buf, nil
}
