package main

import (
	"errors"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestExtractPositionFromError(t *testing.T) {
	tests := []struct {
		name    string
		msg     string
		wantPos int
		wantLen int
	}{
		{
			name:    "position 0 with token",
			msg:     `spl2: unexpected command IDENT "level" at position 0`,
			wantPos: 0,
			wantLen: 5, // len("level")
		},
		{
			name:    "position 10 with token",
			msg:     `spl2: unexpected command IDENT "stauts" at position 10`,
			wantPos: 10,
			wantLen: 6, // len("stauts")
		},
		{
			name:    "no position marker",
			msg:     `spl2: some other error`,
			wantPos: -1,
			wantLen: 1,
		},
		{
			name:    "position with no token",
			msg:     `parse error at position 5`,
			wantPos: 5,
			wantLen: 1,
		},
		{
			name:    "position with quoted string token",
			msg:     `spl2: expected IDENT, got STRING "hello world" at position 3`,
			wantPos: 3,
			wantLen: 11, // len("hello world")
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pos, length := extractPositionFromError(tt.msg)
			if pos != tt.wantPos {
				t.Errorf("pos = %d, want %d", pos, tt.wantPos)
			}
			if length != tt.wantLen {
				t.Errorf("length = %d, want %d", length, tt.wantLen)
			}
		})
	}
}

func TestQueryErrorUnwrap(t *testing.T) {
	inner := &client.APIError{
		HTTPStatus: 400,
		Code:       client.ErrCodeInvalidQuery,
		Message:    "parse error",
	}
	qe := &queryError{inner: inner, query: "bad query"}

	// Error() should delegate to inner.
	if qe.Error() != inner.Error() {
		t.Errorf("Error() = %q, want %q", qe.Error(), inner.Error())
	}

	// Unwrap should preserve the APIError type.
	var apiErr *client.APIError
	if !errors.As(qe, &apiErr) {
		t.Fatal("errors.As should find *client.APIError through queryError")
	}
	if apiErr.Code != client.ErrCodeInvalidQuery {
		t.Errorf("Code = %q, want %q", apiErr.Code, client.ErrCodeInvalidQuery)
	}
}

func TestIsQueryParseError_WithQueryError(t *testing.T) {
	inner := &client.APIError{
		HTTPStatus: 400,
		Code:       client.ErrCodeInvalidQuery,
		Message:    "parse error: unexpected token",
	}
	qe := &queryError{inner: inner, query: "bad | query"}

	if !isQueryParseError(qe) {
		t.Error("isQueryParseError should return true for queryError wrapping INVALID_QUERY")
	}
}
