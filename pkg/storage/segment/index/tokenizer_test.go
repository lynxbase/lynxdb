package index

import (
	"reflect"
	"strings"
	"testing"
)

func TestTokenize(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "simple words",
			input:    "hello world",
			expected: []string{"hello", "world"},
		},
		{
			name:     "mixed case",
			input:    "Hello World FOO",
			expected: []string{"hello", "world", "foo"},
		},
		{
			name:     "punctuation",
			input:    "error: connection refused!",
			expected: []string{"error", "connection", "refused"},
		},
		{
			name:     "IP address split",
			input:    "connection from 192.168.1.100 refused",
			expected: []string{"connection", "from", "192", "168", "1", "100", "refused"},
		},
		{
			name:     "URL-like split",
			input:    "GET /api/v1/users HTTP/1.1",
			expected: []string{"get", "api", "v1", "users", "http", "1", "1"},
		},
		{
			name:     "UUID split on minor breakers",
			input:    "request_id=550e8400-e29b-41d4-a716-446655440000",
			expected: []string{"request", "id", "550e8400", "e29b", "41d4", "a716", "446655440000"},
		},
		{
			name:     "key-value pairs",
			input:    `level=ERROR host=web-01 status=500`,
			expected: []string{"level", "error", "host", "web", "01", "status", "500"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: []string{},
		},
		{
			name:     "only whitespace",
			input:    "   \t\n  ",
			expected: []string{},
		},
		{
			name:     "numbers",
			input:    "status 200 latency 42",
			expected: []string{"status", "200", "latency", "42"},
		},
		{
			name:     "underscores in identifiers",
			input:    "field_name=some_value",
			expected: []string{"field", "name", "some", "value"},
		},
		{
			name:     "syslog-like",
			input:    "Jan 15 10:30:00 web-01 sshd[12345]: Failed password for root from 10.0.0.1",
			expected: []string{"jan", "15", "10", "30", "00", "web", "01", "sshd", "12345", "failed", "password", "for", "root", "from", "10", "0", "0", "1"},
		},
		{
			name:     "file path splitting",
			input:    "/Archives/edgar/data/1039803/tm2517090d1_tsrimg120.jpg",
			expected: []string{"archives", "edgar", "data", "1039803", "tm2517090d1", "tsrimg120", "jpg"},
		},
		{
			name:     "hyphenated number splits",
			input:    "0001829126-25-004815",
			expected: []string{"0001829126", "25", "004815"},
		},
		// Edge cases
		{
			name:     "unicode characters",
			input:    "Café über",
			expected: []string{"café", "über"},
		},
		{
			name:     "emoji splitting",
			input:    "error 🔥 fire",
			expected: []string{"error", "fire"},
		},
		{
			name:     "consecutive delimiters",
			input:    "error...warning!!!info",
			expected: []string{"error", "warning", "info"},
		},
		{
			name:     "trailing hyphen",
			input:    "web-",
			expected: []string{"web"},
		},
		{
			name:     "trailing colon",
			input:    "error:",
			expected: []string{"error"},
		},
		{
			name:     "trailing underscore",
			input:    "field_",
			expected: []string{"field"},
		},
		{
			name:     "single character tokens",
			input:    "a b c",
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "very long token",
			input:    strings.Repeat("a", 1000),
			expected: []string{strings.Repeat("a", 1000)},
		},
		{
			name:     "numbers with leading zeros",
			input:    "agent 007 status",
			expected: []string{"agent", "007", "status"},
		},
		{
			name:     "mixed CJK and latin",
			input:    "error 错误",
			expected: []string{"error", "错误"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Tokenize(tt.input)
			if len(got) == 0 && len(tt.expected) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("Tokenize(%q)\n  got:  %v\n  want: %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestTokenizeUnique(t *testing.T) {
	tokens := TokenizeUnique("error error warn error info info")
	expected := []string{"error", "warn", "info"}
	if !reflect.DeepEqual(tokens, expected) {
		t.Errorf("got %v, want %v", tokens, expected)
	}
}

func BenchmarkTokenize(b *testing.B) {
	text := `2024-01-15T10:30:00.000Z host=web-01 level=ERROR source=/var/log/app.log msg="Failed to connect to database at 192.168.1.50:5432 - connection refused" request_id=550e8400-e29b-41d4-a716-446655440000`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Tokenize(text)
	}
}
