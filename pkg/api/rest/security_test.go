package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/config"
)

// startTestServerWithQueryConfig creates a test server with a custom QueryConfig.
func startTestServerWithQueryConfig(t *testing.T, qcfg config.QueryConfig) (*Server, func()) {
	t.Helper()

	// Ensure SpillDir is set to a test-local directory so that
	// CleanupOrphans does not walk the global /tmp.
	if qcfg.SpillDir == "" {
		qcfg.SpillDir = t.TempDir()
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv, err := NewServer(Config{
		Addr:   "127.0.0.1:0",
		Logger: logger,
		Query:  qcfg,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go srv.Start(ctx)
	srv.WaitReady()

	return srv, func() {
		cancel()
		time.Sleep(50 * time.Millisecond)
	}
}

// decodeErrorResponse parses the structured error envelope from a response body.
func decodeErrorResponse(t *testing.T, resp *http.Response) (code, message string) {
	t.Helper()
	var result struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	return result.Error.Code, result.Error.Message
}

// --- Query Length Validation Tests ---

func TestQueryLength_POST_Query(t *testing.T) {
	srv, cleanup := startTestServerWithQueryConfig(t, config.QueryConfig{
		MaxQueryLength: 100,
	})
	defer cleanup()

	longQuery := strings.Repeat("x", 200)
	body, _ := json.Marshal(map[string]interface{}{"q": longQuery})
	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/query", srv.Addr()),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 400, body: %s", resp.StatusCode, string(b))
	}

	code, msg := decodeErrorResponse(t, resp)
	if code != "QUERY_TOO_LARGE" {
		t.Errorf("error code: got %q, want QUERY_TOO_LARGE", code)
	}
	if !strings.Contains(msg, "200") || !strings.Contains(msg, "100") {
		t.Errorf("error message should mention actual (%d) and max (%d) lengths, got: %s", 200, 100, msg)
	}
}

func TestQueryLength_GET_Query(t *testing.T) {
	srv, cleanup := startTestServerWithQueryConfig(t, config.QueryConfig{
		MaxQueryLength: 50,
	})
	defer cleanup()

	longQuery := strings.Repeat("a", 100)
	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query?q=%s", srv.Addr(), longQuery))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	code, _ := decodeErrorResponse(t, resp)
	if code != "QUERY_TOO_LARGE" {
		t.Errorf("error code: got %q, want QUERY_TOO_LARGE", code)
	}
}

func TestQueryLength_GET_Explain(t *testing.T) {
	srv, cleanup := startTestServerWithQueryConfig(t, config.QueryConfig{
		MaxQueryLength: 50,
	})
	defer cleanup()

	longQuery := strings.Repeat("b", 100)
	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query/explain?q=%s", srv.Addr(), longQuery))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	code, _ := decodeErrorResponse(t, resp)
	if code != "QUERY_TOO_LARGE" {
		t.Errorf("error code: got %q, want QUERY_TOO_LARGE", code)
	}
}

func TestQueryLength_POST_Stream(t *testing.T) {
	srv, cleanup := startTestServerWithQueryConfig(t, config.QueryConfig{
		MaxQueryLength: 50,
	})
	defer cleanup()

	longQuery := strings.Repeat("c", 100)
	body, _ := json.Marshal(map[string]interface{}{"q": longQuery})
	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/query/stream", srv.Addr()),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	code, _ := decodeErrorResponse(t, resp)
	if code != "QUERY_TOO_LARGE" {
		t.Errorf("error code: got %q, want QUERY_TOO_LARGE", code)
	}
}

func TestQueryLength_GET_Tail(t *testing.T) {
	srv, cleanup := startTestServerWithQueryConfig(t, config.QueryConfig{
		MaxQueryLength: 50,
	})
	defer cleanup()

	longQuery := strings.Repeat("d", 100)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("http://%s/api/v1/tail?q=%s", srv.Addr(), longQuery),
		http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	code, _ := decodeErrorResponse(t, resp)
	if code != "QUERY_TOO_LARGE" {
		t.Errorf("error code: got %q, want QUERY_TOO_LARGE", code)
	}
}

func TestQueryLength_UnderLimit_Succeeds(t *testing.T) {
	srv, cleanup := startTestServerWithQueryConfig(t, config.QueryConfig{
		MaxQueryLength: 1000,
	})
	defer cleanup()

	// Query under the limit should succeed (or at least not be rejected for length).
	body, _ := json.Marshal(map[string]interface{}{
		"q": "FROM main | head 5",
	})
	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/query", srv.Addr()),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	// Should get 200 (query executes, even if no data).
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200, body: %s", resp.StatusCode, string(b))
	}
}

func TestQueryLength_Unlimited_WhenZero(t *testing.T) {
	srv, cleanup := startTestServerWithQueryConfig(t, config.QueryConfig{
		MaxQueryLength: 0, // 0 = unlimited
	})
	defer cleanup()

	// A very long query should not be rejected when MaxQueryLength is 0.
	longQuery := "FROM main | head 5" + strings.Repeat(" ", 2_000_000)
	body, _ := json.Marshal(map[string]interface{}{"q": longQuery})
	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/query", srv.Addr()),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	// Should NOT get QUERY_TOO_LARGE.
	if resp.StatusCode == http.StatusBadRequest {
		code, _ := decodeErrorResponse(t, resp)
		if code == "QUERY_TOO_LARGE" {
			t.Fatal("query should not be rejected when MaxQueryLength=0 (unlimited)")
		}
	}
}

// --- Header Validation Tests ---

func TestHeaderValidation_XSource_TooLong(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	raw := "2024-01-01T00:00:00Z test event\n"
	req, _ := http.NewRequest("POST",
		fmt.Sprintf("http://%s/api/v1/ingest/raw", srv.Addr()),
		bytes.NewBufferString(raw))
	req.Header.Set("X-Source", strings.Repeat("x", 300))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	code, msg := decodeErrorResponse(t, resp)
	if code != "VALIDATION_ERROR" {
		t.Errorf("error code: got %q, want VALIDATION_ERROR", code)
	}
	if !strings.Contains(msg, "X-Source") {
		t.Errorf("error message should mention X-Source, got: %s", msg)
	}
}

func TestHeaderValidation_XSourceType_TooLong(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	raw := "2024-01-01T00:00:00Z test event\n"
	req, _ := http.NewRequest("POST",
		fmt.Sprintf("http://%s/api/v1/ingest/raw", srv.Addr()),
		bytes.NewBufferString(raw))
	req.Header.Set("X-Source-Type", strings.Repeat("y", 300))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	code, msg := decodeErrorResponse(t, resp)
	if code != "VALIDATION_ERROR" {
		t.Errorf("error code: got %q, want VALIDATION_ERROR", code)
	}
	if !strings.Contains(msg, "X-Source-Type") {
		t.Errorf("error message should mention X-Source-Type, got: %s", msg)
	}
}

func TestHeaderValidation_XIndex_TooLong(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	raw := "2024-01-01T00:00:00Z test event\n"
	req, _ := http.NewRequest("POST",
		fmt.Sprintf("http://%s/api/v1/ingest/raw", srv.Addr()),
		bytes.NewBufferString(raw))
	req.Header.Set("X-Index", strings.Repeat("z", 300))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", resp.StatusCode)
	}

	code, msg := decodeErrorResponse(t, resp)
	if code != "VALIDATION_ERROR" {
		t.Errorf("error code: got %q, want VALIDATION_ERROR", code)
	}
	if !strings.Contains(msg, "X-Index") {
		t.Errorf("error message should mention X-Index, got: %s", msg)
	}
}

func TestHeaderValidation_XSource_AtLimit_Succeeds(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	raw := "2024-01-01T00:00:00Z test event\n"
	req, _ := http.NewRequest("POST",
		fmt.Sprintf("http://%s/api/v1/ingest/raw", srv.Addr()),
		bytes.NewBufferString(raw))
	// Exactly 256 characters — should be accepted.
	req.Header.Set("X-Source", strings.Repeat("a", 256))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200, body: %s", resp.StatusCode, string(b))
	}
}

func TestHeaderValidation_Default_Succeeds(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	raw := "2024-01-01T00:00:00Z test event\n"
	req, _ := http.NewRequest("POST",
		fmt.Sprintf("http://%s/api/v1/ingest/raw", srv.Addr()),
		bytes.NewBufferString(raw))
	// No X-Source header at all — should default to "http" and succeed.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200, body: %s", resp.StatusCode, string(b))
	}
}

// --- Token Redaction Tests ---

func TestTokenRedaction_QueryParam(t *testing.T) {
	srv, ks, rootToken, cleanup := startAuthServer(t)
	defer cleanup()
	_ = ks

	// Make a request with _token as a query parameter. After KeyAuthMiddleware
	// processes it, the _token should be stripped from r.URL.RawQuery so it
	// doesn't leak into proxy logs, Referer headers, or browser history.
	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/stats?_token=%s&extra=1", srv.Addr(), rootToken))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	// The request should succeed (authenticated via _token).
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: got %d, want 200, body: %s", resp.StatusCode, string(b))
	}
}

func TestTokenRedaction_BearerHeaderNotAffected(t *testing.T) {
	srv, _, rootToken, cleanup := startAuthServer(t)
	defer cleanup()

	// Request with Authorization header should still work (no _token in URL).
	resp, err := authReq("GET",
		fmt.Sprintf("http://%s/api/v1/stats", srv.Addr()),
		rootToken, nil)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
}

func TestTokenRedaction_MissingToken_Returns401(t *testing.T) {
	srv, _, _, cleanup := startAuthServer(t)
	defer cleanup()

	// No _token and no Authorization header — should get 401.
	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/stats?extra=1", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want 401", resp.StatusCode)
	}
}

func TestTokenRedaction_InvalidToken_Returns401(t *testing.T) {
	srv, _, _, cleanup := startAuthServer(t)
	defer cleanup()

	// Invalid _token — should get 401.
	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/stats?_token=invalid_token_value", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want 401", resp.StatusCode)
	}
}

// --- sanitizeErrorMessage Unit Tests ---

func TestSanitizeErrorMessage(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "no control chars",
			input: "invalid JSON: unexpected end of input",
			want:  "invalid JSON: unexpected end of input",
		},
		{
			name:  "preserves newline",
			input: "line one\nline two",
			want:  "line one\nline two",
		},
		{
			name:  "preserves carriage return",
			input: "line one\r\nline two",
			want:  "line one\r\nline two",
		},
		{
			name:  "preserves tab",
			input: "field:\tvalue",
			want:  "field:\tvalue",
		},
		{
			name:  "replaces null byte",
			input: "before\x00after",
			want:  "before?after",
		},
		{
			name:  "replaces bell character",
			input: "alert\x07here",
			want:  "alert?here",
		},
		{
			name:  "replaces backspace",
			input: "back\x08space",
			want:  "back?space",
		},
		{
			name:  "replaces escape",
			input: "esc\x1b[31mred\x1b[0m",
			want:  "esc?[31mred?[0m",
		},
		{
			name:  "multiple control chars",
			input: "\x01\x02\x03normal\x04\x05",
			want:  "???normal??",
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
		{
			name:  "unicode preserved",
			input: "日本語テスト",
			want:  "日本語テスト",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeErrorMessage(tt.input)
			if got != tt.want {
				t.Errorf("sanitizeErrorMessage(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// --- extractToken Unit Tests ---

func TestExtractToken(t *testing.T) {
	tests := []struct {
		name      string
		header    string
		wantKeyID string
		wantToken string
		wantOK    bool
	}{
		{
			name:      "Bearer token",
			header:    "Bearer my-secret-token",
			wantKeyID: "",
			wantToken: "my-secret-token",
			wantOK:    true,
		},
		{
			name:      "Bearer with whitespace",
			header:    "Bearer  my-secret-token ",
			wantKeyID: "",
			wantToken: "my-secret-token",
			wantOK:    true,
		},
		{
			name:   "Invalid scheme",
			header: "Digest realm=test",
			wantOK: false,
		},
		{
			name:   "Empty header",
			header: "",
			wantOK: false,
		},
		{
			name:   "ApiKey invalid base64",
			header: "ApiKey not-valid-base64!!!",
			wantOK: false,
		},
		{
			name:   "Basic missing password",
			header: "Basic " + "dXNlcjo=", // base64("user:")
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyID, token, ok := extractToken(tt.header)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok {
				if keyID != tt.wantKeyID {
					t.Errorf("keyID = %q, want %q", keyID, tt.wantKeyID)
				}
				if token != tt.wantToken {
					t.Errorf("token = %q, want %q", token, tt.wantToken)
				}
			}
		})
	}
}

// --- Token Redaction via KeyAuthMiddleware (unit-level) ---

func TestKeyAuthMiddleware_RedactsTokenFromURL(t *testing.T) {
	dir := t.TempDir()
	ks, err := auth.OpenKeyStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	root, err := ks.CreateKey("root", true)
	if err != nil {
		t.Fatal(err)
	}

	var capturedRawQuery string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedRawQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
	})

	handler := KeyAuthMiddleware(ks, inner)

	// Simulate a request with _token in query params.
	req, _ := http.NewRequest("GET", "/api/v1/stats?_token="+root.Token+"&extra=value", http.NoBody)
	rr := &fakeResponseWriter{headers: http.Header{}}
	handler.ServeHTTP(rr, req)

	if rr.code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rr.code)
	}

	// Verify _token was stripped from the URL.
	if strings.Contains(capturedRawQuery, "_token") {
		t.Errorf("_token was not redacted from URL: %q", capturedRawQuery)
	}
	// Verify other params are preserved.
	if !strings.Contains(capturedRawQuery, "extra=value") {
		t.Errorf("other query params should be preserved: %q", capturedRawQuery)
	}
}

// fakeResponseWriter is a minimal http.ResponseWriter for unit tests.
type fakeResponseWriter struct {
	headers http.Header
	body    bytes.Buffer
	code    int
}

func (f *fakeResponseWriter) Header() http.Header         { return f.headers }
func (f *fakeResponseWriter) Write(b []byte) (int, error) { return f.body.Write(b) }
func (f *fakeResponseWriter) WriteHeader(code int)        { f.code = code }
