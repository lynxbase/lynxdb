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

// marshalReader encodes v as JSON into a buffer from the pool and returns a reader.
// Returns an error if JSON encoding fails.
func marshalReader(c *Client, v interface{}) (io.Reader, error) {
	buf := c.getBuf()
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		return nil, fmt.Errorf("marshalReader: %w", err)
	}

	return buf, nil
}
