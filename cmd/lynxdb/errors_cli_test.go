package main

import (
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestIsConnectionError_NetOpError(t *testing.T) {
	err := &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: fmt.Errorf("connect: connection refused"),
	}

	if !isConnectionError(err) {
		t.Error("expected net.OpError to be classified as connection error")
	}
}

func TestIsConnectionError_ECONNREFUSED(t *testing.T) {
	err := fmt.Errorf("dial tcp 127.0.0.1:3100: connect: %w", syscall.ECONNREFUSED)

	if !isConnectionError(err) {
		t.Error("expected ECONNREFUSED to be classified as connection error")
	}
}

func TestIsConnectionError_StringMatch(t *testing.T) {
	tests := []struct {
		msg  string
		want bool
	}{
		{"connection refused", true},
		{"no such host", true},
		{"dial tcp 127.0.0.1:3100", true},
		{"timeout exceeded", false},
		{"invalid query syntax", false},
	}

	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			got := isConnectionError(fmt.Errorf("%s", tt.msg))
			if got != tt.want {
				t.Errorf("isConnectionError(%q) = %v, want %v", tt.msg, got, tt.want)
			}
		})
	}
}

func TestIsConnectionError_Nil(t *testing.T) {
	if isConnectionError(nil) {
		t.Error("expected nil error to return false")
	}
}

func TestIsQueryParseError_APIError(t *testing.T) {
	err := &client.APIError{
		HTTPStatus: 400,
		Code:       client.ErrCodeInvalidQuery,
		Message:    "parse error: unexpected token",
	}

	if !isQueryParseError(err) {
		t.Error("expected INVALID_QUERY APIError to be classified as query parse error")
	}
}

func TestIsQueryParseError_WrappedInQueryError(t *testing.T) {
	inner := &client.APIError{
		HTTPStatus: 400,
		Code:       client.ErrCodeInvalidQuery,
		Message:    "parse error",
	}
	qe := &queryError{inner: inner, query: "bad query"}

	if !isQueryParseError(qe) {
		t.Error("expected queryError wrapping INVALID_QUERY to be parse error")
	}
}

func TestIsQueryParseError_StringMatch(t *testing.T) {
	tests := []struct {
		msg  string
		want bool
	}{
		{"parse error at position 5", true},
		{"unknown command 'xyz'", true},
		{"syntax error near token", true},
		{"connection refused", false},
		{"internal server error", false},
	}

	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			got := isQueryParseError(fmt.Errorf("%s", tt.msg))
			if got != tt.want {
				t.Errorf("isQueryParseError(%q) = %v, want %v", tt.msg, got, tt.want)
			}
		})
	}
}

func TestIsQueryParseError_Nil(t *testing.T) {
	if isQueryParseError(nil) {
		t.Error("expected nil error to return false")
	}
}

func TestExtractFieldFromError(t *testing.T) {
	tests := []struct {
		msg  string
		want string
	}{
		{"unknown field 'status'", "status"},
		{"field 'host' not found", "host"},
		{"no such field 'src_ip'", "src_ip"},
		{"some other message", ""},
		{"field '' is empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			got := extractFieldFromError(tt.msg)
			if got != tt.want {
				t.Errorf("extractFieldFromError(%q) = %q, want %q", tt.msg, got, tt.want)
			}
		})
	}
}

func TestExtractPositionFromError_LargePosition(t *testing.T) {
	pos, _ := extractPositionFromError("unexpected token at position 9999")
	if pos != 9999 {
		t.Errorf("expected position 9999, got %d", pos)
	}
}

func TestExtractPositionFromError_EmptyMessage(t *testing.T) {
	pos, length := extractPositionFromError("")
	if pos != -1 {
		t.Errorf("expected position -1 for empty string, got %d", pos)
	}

	if length != 1 {
		t.Errorf("expected length 1 for empty string, got %d", length)
	}
}

func TestIsTimeoutError_StringMatch(t *testing.T) {
	tests := []struct {
		msg  string
		want bool
	}{
		{"context deadline exceeded", true},
		{"timeout exceeded", true},
		{"query timed out", true},
		{"Client.Timeout exceeded while awaiting headers", true},
		{"connection refused", false},
		{"parse error", false},
	}

	for _, tt := range tests {
		t.Run(tt.msg, func(t *testing.T) {
			got := isTimeoutError(fmt.Errorf("%s", tt.msg))
			if got != tt.want {
				t.Errorf("isTimeoutError(%q) = %v, want %v", tt.msg, got, tt.want)
			}
		})
	}
}

func TestIsTimeoutError_Nil(t *testing.T) {
	if isTimeoutError(nil) {
		t.Error("expected nil error to return false")
	}
}

func TestIsAuthError_APIError(t *testing.T) {
	tests := []struct {
		name string
		code client.ErrorCode
		want bool
	}{
		{"AUTH_REQUIRED", client.ErrCodeAuthRequired, true},
		{"INVALID_TOKEN", client.ErrCodeInvalidToken, true},
		{"FORBIDDEN", client.ErrCodeForbidden, true},
		{"NOT_FOUND", client.ErrCodeNotFound, false},
		{"INVALID_QUERY", client.ErrCodeInvalidQuery, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &client.APIError{
				HTTPStatus: 401,
				Code:       tt.code,
				Message:    "test",
			}
			got := isAuthError(err)
			if got != tt.want {
				t.Errorf("isAuthError(code=%s) = %v, want %v", tt.code, got, tt.want)
			}
		})
	}
}

func TestIsAuthError_Nil(t *testing.T) {
	if isAuthError(nil) {
		t.Error("expected nil error to return false")
	}
}
