package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"time"
)

// Ingest sends NDJSON, one JSON object per line, to POST /api/v1/ingest;
// the server appends the lines to index as they are. An empty index means
// the server's default index.
func (c *Client) Ingest(ctx context.Context, index string, ndjson io.Reader) (*IngestResult, error) {
	path := "/ingest"
	if index != "" {
		path += "?index=" + url.QueryEscape(index)
	}
	resp, err := c.doRaw(ctx, http.MethodPost, path, ndjson, "application/x-ndjson")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return decodeIngestResult(resp)
}

// IngestEvents sends events as NDJSON, one request per index: each event
// becomes an object with its text in _raw, its time in _time, its origin in
// host, source and sourcetype, and its fields.
func (c *Client) IngestEvents(ctx context.Context, events []IngestEvent) (*IngestResult, error) {
	byIndex := map[string]*bytes.Buffer{}
	var order []string
	for i, ev := range events {
		buf, ok := byIndex[ev.Index]
		if !ok {
			buf = &bytes.Buffer{}
			byIndex[ev.Index] = buf
			order = append(order, ev.Index)
		}
		line, err := json.Marshal(ingestObject(ev))
		if err != nil {
			return nil, fmt.Errorf("lynxdb: encode event %d: %w", i, err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	total := &IngestResult{}
	for _, index := range order {
		res, err := c.Ingest(ctx, index, byIndex[index])
		if err != nil {
			return nil, err
		}
		total.Accepted += res.Accepted
		total.Failed += res.Failed
	}

	return total, nil
}

// ingestObject returns the JSON object of an event.
func ingestObject(ev IngestEvent) map[string]interface{} {
	obj := make(map[string]interface{}, len(ev.Fields)+5)
	for k, v := range ev.Fields {
		obj[k] = v
	}
	if ev.Event != "" {
		obj["_raw"] = ev.Event
	}
	if ev.Time != nil {
		sec, frac := math.Modf(*ev.Time)
		obj["_time"] = time.Unix(int64(sec), int64(frac*1e9)).UTC().Format(time.RFC3339Nano)
	}
	for k, v := range map[string]string{"host": ev.Host, "source": ev.Source, "sourcetype": ev.Sourcetype} {
		if v != "" {
			obj[k] = v
		}
	}

	return obj
}

// IngestRaw sends raw log data (text/plain, application/json, etc.) to the server.
// Headers like X-Source, X-Sourcetype, X-Index are set from opts.
func (c *Client) IngestRaw(ctx context.Context, body io.Reader, opts IngestOpts) (*IngestResult, error) {
	ct := opts.ContentType
	if ct == "" {
		ct = "text/plain"
	}

	req, err := c.newRequest(ctx, http.MethodPost, c.url("/ingest/raw"), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", ct)
	if opts.Index != "" {
		req.Header.Set("X-Index", opts.Index)
	}
	if opts.Source != "" {
		req.Header.Set("X-Source", opts.Source)
	}
	if opts.Sourcetype != "" {
		req.Header.Set("X-Source-Type", opts.Sourcetype)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, c.parseAPIError(resp)
	}

	return decodeIngestResult(resp)
}

// IngestNDJSON sends newline-delimited data to the raw ingest endpoint.
// Deprecated: use Ingest, which appends JSON lines as they are.
func (c *Client) IngestNDJSON(ctx context.Context, body io.Reader, opts IngestOpts) (*IngestResult, error) {
	opts.ContentType = "application/x-ndjson"
	if opts.Sourcetype == "" {
		opts.Sourcetype = "json"
	}

	return c.IngestRaw(ctx, body, opts)
}

// IngestBulk sends data in Elasticsearch bulk format.
// Deprecated: use ESBulk, which targets the preferred /api/v1/es/_bulk path.
func (c *Client) IngestBulk(ctx context.Context, body io.Reader) (*http.Response, error) {
	return c.doRaw(ctx, http.MethodPost, "/es/_bulk", body, "application/x-ndjson")
}

func decodeIngestResult(resp *http.Response) (*IngestResult, error) {
	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, err
	}

	var result IngestResult
	if err := json.Unmarshal(env.Data, &result); err != nil {
		return nil, err
	}

	return &result, nil
}
