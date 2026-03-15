package main

import (
	"errors"
	"fmt"
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

func TestIsRequiredFlagError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "single flag",
			err:  fmt.Errorf(`required flag(s) "name" not set`),
			want: true,
		},
		{
			name: "multiple flags",
			err:  fmt.Errorf(`required flag(s) "name", "query" not set`),
			want: true,
		},
		{
			name: "unrelated error",
			err:  fmt.Errorf("connection refused"),
			want: false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "partial match - has required flag but no not set",
			err:  fmt.Errorf(`required flag(s) "name"`),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isRequiredFlagError(tt.err)
			if got != tt.want {
				t.Errorf("isRequiredFlagError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseRequiredFlags(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want []string
	}{
		{
			name: "single flag",
			err:  fmt.Errorf(`required flag(s) "name" not set`),
			want: []string{"name"},
		},
		{
			name: "two flags",
			err:  fmt.Errorf(`required flag(s) "name", "query" not set`),
			want: []string{"name", "query"},
		},
		{
			name: "four flags",
			err:  fmt.Errorf(`required flag(s) "bar1", "bar2", "foo1", "foo2" not set`),
			want: []string{"bar1", "bar2", "foo1", "foo2"},
		},
		{
			name: "nil error",
			err:  nil,
			want: nil,
		},
		{
			name: "no quotes",
			err:  fmt.Errorf("some other error"),
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseRequiredFlags(tt.err)
			if len(got) != len(tt.want) {
				t.Fatalf("parseRequiredFlags() = %v (len %d), want %v (len %d)", got, len(got), tt.want, len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("parseRequiredFlags()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
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
