package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewClient_Defaults(t *testing.T) {
	c := NewClient()

	if c.baseURL != defaultBaseURL {
		t.Errorf("baseURL = %q, want %q", c.baseURL, defaultBaseURL)
	}
	if c.userAgent != defaultUserAgent {
		t.Errorf("userAgent = %q, want %q", c.userAgent, defaultUserAgent)
	}
	if c.authToken != "" {
		t.Errorf("authToken = %q, want empty", c.authToken)
	}
	if c.httpClient == nil {
		t.Fatal("httpClient is nil")
	}
	if c.httpClient.Timeout != defaultTimeout {
		t.Errorf("timeout = %v, want %v", c.httpClient.Timeout, defaultTimeout)
	}
}

func TestNewClient_WithOptions(t *testing.T) {
	custom := &http.Client{Timeout: 5 * time.Second}
	c := NewClient(
		WithBaseURL("http://example.com:9000"),
		WithAuthToken("secret-token"),
		WithHTTPClient(custom),
	)

	if c.baseURL != "http://example.com:9000" {
		t.Errorf("baseURL = %q", c.baseURL)
	}
	if c.authToken != "secret-token" {
		t.Errorf("authToken = %q", c.authToken)
	}
	if c.httpClient != custom {
		t.Error("httpClient not set correctly")
	}
}

func TestNewClient_WithTimeout(t *testing.T) {
	c := NewClient(WithTimeout(10 * time.Second))

	if c.httpClient.Timeout != 10*time.Second {
		t.Errorf("timeout = %v, want 10s", c.httpClient.Timeout)
	}
}

func TestParseAPIError_Structured(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]interface{}{
				"code":       "NOT_FOUND",
				"message":    "job not found",
				"suggestion": "check job ID",
			},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	_, err := c.Status(context.Background())

	if err == nil {
		t.Fatal("expected error")
	}

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if apiErr.HTTPStatus != 404 {
		t.Errorf("HTTPStatus = %d, want 404", apiErr.HTTPStatus)
	}
	if apiErr.Code != ErrCodeNotFound {
		t.Errorf("Code = %q, want %q", apiErr.Code, ErrCodeNotFound)
	}
	if apiErr.Message != "job not found" {
		t.Errorf("Message = %q", apiErr.Message)
	}
	if apiErr.Suggestion != "check job ID" {
		t.Errorf("Suggestion = %q", apiErr.Suggestion)
	}
}

func TestParseAPIError_StringError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": "bad request body",
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	_, err := c.Status(context.Background())

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T", err)
	}
	if apiErr.Message != "bad request body" {
		t.Errorf("Message = %q, want %q", apiErr.Message, "bad request body")
	}
}

func TestParseAPIError_RawBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("plain text error"))
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	_, err := c.Status(context.Background())

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T", err)
	}
	if apiErr.Message != "plain text error" {
		t.Errorf("Message = %q", apiErr.Message)
	}
}

func TestErrorPredicates(t *testing.T) {
	tests := []struct {
		code ErrorCode
		pred func(error) bool
		name string
	}{
		{ErrCodeNotFound, IsNotFound, "IsNotFound"},
		{ErrCodeAlreadyExists, IsAlreadyExists, "IsAlreadyExists"},
		{ErrCodeAuthRequired, IsAuthRequired, "IsAuthRequired"},
		{ErrCodeTooManyRequests, IsRateLimited, "IsRateLimited"},
		{ErrCodeInvalidQuery, IsInvalidQuery, "IsInvalidQuery"},
		{ErrCodeValidationError, IsValidationError, "IsValidationError"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &APIError{HTTPStatus: 400, Code: tt.code, Message: "test"}
			if !tt.pred(err) {
				t.Errorf("%s should return true for code %s", tt.name, tt.code)
			}

			other := &APIError{HTTPStatus: 400, Code: "OTHER", Message: "test"}
			if tt.pred(other) {
				t.Errorf("%s should return false for code OTHER", tt.name)
			}
		})
	}
}

func TestAuthHeader(t *testing.T) {
	var gotAuth string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{"version": "1.0"},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL), WithAuthToken("my-token"))
	_, _ = c.Status(context.Background())

	if gotAuth != "Bearer my-token" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer my-token")
	}
}

func TestStatus_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"version":        "1.0.0",
				"uptime_seconds": 3600,
				"health":         "healthy",
				"storage":        map[string]interface{}{"used_bytes": 1024},
				"events":         map[string]interface{}{"total": 5000},
				"queries":        map[string]interface{}{"active": 2},
				"views":          map[string]interface{}{"total": 3},
			},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	status, err := c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Version != "1.0.0" {
		t.Errorf("Version = %q", status.Version)
	}
	if status.Health != "healthy" {
		t.Errorf("Health = %q", status.Health)
	}
	if status.UptimeSeconds != 3600 {
		t.Errorf("UptimeSeconds = %d", status.UptimeSeconds)
	}
}

func TestHealth_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Server wraps /health in a {"data": ...} envelope via respondData.
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{"status": "ok"},
		})
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	result, err := c.Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "ok" {
		t.Errorf("Status = %q, want %q", result.Status, "ok")
	}
}
