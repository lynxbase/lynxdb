package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

const (
	defaultBaseURL   = "http://localhost:3100"
	defaultTimeout   = 30 * time.Second
	defaultUserAgent = "lynxdb-go-client"
	apiPrefix        = "/api/v1"
)

// Client is a typed HTTP client for the LynxDB API.
type Client struct {
	baseURL    string
	httpClient *http.Client
	authToken  string
	userAgent  string
	bufPool    sync.Pool
}

// Option configures a Client.
type Option func(*Client)

// WithBaseURL sets the base URL (e.g. "http://localhost:3100").
func WithBaseURL(url string) Option {
	return func(c *Client) { c.baseURL = url }
}

// WithAuthToken sets the Bearer token for authentication.
func WithAuthToken(token string) Option {
	return func(c *Client) { c.authToken = token }
}

// WithHTTPClient sets a custom *http.Client.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.httpClient = hc }
}

// WithTimeout sets the HTTP client timeout.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.httpClient.Timeout = d }
}

// WithTLSConfig sets a custom TLS configuration for HTTPS connections.
func WithTLSConfig(tc *tls.Config) Option {
	return func(c *Client) {
		c.httpClient.Transport = &http.Transport{
			TLSClientConfig: tc,
		}
	}
}

// WithInsecureSkipVerify disables TLS certificate verification.
// Use only for testing or when --tls-skip-verify is explicitly requested.
func WithInsecureSkipVerify() Option {
	return func(c *Client) {
		c.httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, //nolint:gosec // User explicitly opted in via --tls-skip-verify.
			},
		}
	}
}

// NewClient creates a new LynxDB API client.
func NewClient(opts ...Option) *Client {
	c := &Client{
		baseURL:   defaultBaseURL,
		userAgent: defaultUserAgent,
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		bufPool: sync.Pool{
			New: func() interface{} { return new(bytes.Buffer) },
		},
	}
	for _, o := range opts {
		o(c)
	}

	return c
}

// envelope is the standard JSON response wrapper: {"data": ..., "meta": ...}.
type envelope struct {
	Data json.RawMessage `json:"data"`
	Meta Meta            `json:"meta"`
}

// errorEnvelope is the error response wrapper: {"error": ...}.
type errorEnvelope struct {
	Error json.RawMessage `json:"error"`
}

// getBuf returns a *bytes.Buffer from the pool.
func (c *Client) getBuf() *bytes.Buffer {
	buf := c.bufPool.Get().(*bytes.Buffer)
	buf.Reset()

	return buf
}

// putBuf returns a *bytes.Buffer to the pool.
func (c *Client) putBuf(buf *bytes.Buffer) {
	if buf.Cap() > 1<<20 { // don't pool buffers > 1MB
		return
	}

	c.bufPool.Put(buf)
}

// url builds the full URL for an API path.
func (c *Client) url(path string) string {
	return c.baseURL + apiPrefix + path
}

// newRequest creates an *http.Request with auth and user-agent headers.
func (c *Client) newRequest(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", c.userAgent)
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	return req, nil
}

// doJSON sends a JSON request and decodes the response envelope into dst.
// If reqBody is nil, no body is sent. If dst is nil, the data field is ignored.
func (c *Client) doJSON(ctx context.Context, method, path string, reqBody, dst interface{}) (Meta, error) {
	_, data, meta, err := c.doJSONWithStatus(ctx, method, path, reqBody)
	if err != nil {
		return meta, err
	}

	if dst != nil && len(data) > 0 {
		if err := json.Unmarshal(data, dst); err != nil {
			return meta, fmt.Errorf("lynxdb: decode response data: %w", err)
		}
	}

	return meta, nil
}

// doJSONWithStatus sends a JSON request and returns the raw status, data, and meta.
// Used for polymorphic responses where the caller inspects the HTTP status.
func (c *Client) doJSONWithStatus(ctx context.Context, method, path string, reqBody interface{}) (int, json.RawMessage, Meta, error) {
	var bodyReader io.Reader
	if reqBody != nil {
		buf := c.getBuf()
		defer c.putBuf(buf)

		if err := json.NewEncoder(buf).Encode(reqBody); err != nil {
			return 0, nil, Meta{}, fmt.Errorf("lynxdb: encode request: %w", err)
		}

		bodyReader = buf
	}

	req, err := c.newRequest(ctx, method, c.url(path), bodyReader)
	if err != nil {
		return 0, nil, Meta{}, err
	}

	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, nil, Meta{}, fmt.Errorf("lynxdb: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return resp.StatusCode, nil, Meta{}, c.parseAPIError(resp)
	}

	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return resp.StatusCode, nil, Meta{}, fmt.Errorf("lynxdb: decode response: %w", err)
	}

	return resp.StatusCode, env.Data, env.Meta, nil
}

// doRaw sends a request and returns the raw *http.Response.
// The caller is responsible for closing resp.Body.
func (c *Client) doRaw(ctx context.Context, method, path string, body io.Reader, contentType string) (*http.Response, error) {
	req, err := c.newRequest(ctx, method, c.url(path), body)
	if err != nil {
		return nil, err
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("lynxdb: %s %s: %w", method, path, err)
	}

	if resp.StatusCode >= 400 {
		apiErr := c.parseAPIError(resp)
		resp.Body.Close()

		return nil, apiErr
	}

	return resp, nil
}

// doNoContent sends a request expecting a 204 No Content (or 200) response.
func (c *Client) doNoContent(ctx context.Context, method, path string) error {
	req, err := c.newRequest(ctx, method, c.url(path), nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("lynxdb: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return c.parseAPIError(resp)
	}

	// Drain body to allow connection reuse.
	_, _ = io.Copy(io.Discard, resp.Body)

	return nil
}

// parseAPIError parses an error response body into an *APIError.
func (c *Client) parseAPIError(resp *http.Response) *APIError {
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || len(body) == 0 {
		return &APIError{
			HTTPStatus: resp.StatusCode,
			Message:    http.StatusText(resp.StatusCode),
		}
	}

	// Try structured envelope: {"error": {"code": "...", "message": "..."}}
	var env errorEnvelope
	if json.Unmarshal(body, &env) == nil && len(env.Error) > 0 {
		// Try object form.
		var structured struct {
			Code       ErrorCode `json:"code"`
			Message    string    `json:"message"`
			Suggestion string    `json:"suggestion"`
		}
		if json.Unmarshal(env.Error, &structured) == nil && structured.Message != "" {
			return &APIError{
				HTTPStatus: resp.StatusCode,
				Code:       structured.Code,
				Message:    structured.Message,
				Suggestion: structured.Suggestion,
			}
		}

		// Try string form: {"error": "some message"}
		var msg string
		if json.Unmarshal(env.Error, &msg) == nil && msg != "" {
			return &APIError{
				HTTPStatus: resp.StatusCode,
				Message:    msg,
			}
		}
	}

	// Fallback: use raw body as message.
	return &APIError{
		HTTPStatus: resp.StatusCode,
		Message:    string(body),
	}
}
