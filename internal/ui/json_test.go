package ui

import (
	"bytes"
	"strings"
	"testing"
)

func TestColorizeJSON_NoColor(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	input := `{"name": "alice", "age": 30, "active": true, "data": null}`
	got := theme.ColorizeJSON(input)

	// With noColor, the output should be identical to the input
	// (all Render calls are no-ops).
	if got != input {
		t.Errorf("ColorizeJSON with noColor should return input unchanged.\ngot:  %q\nwant: %q", got, input)
	}
}

func TestColorizeJSON_PreservesStructure(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	input := `{"key": "value", "num": 42, "bool": false}`
	got := theme.ColorizeJSON(input)

	// Verify all tokens are present.
	for _, token := range []string{`"key"`, `"value"`, "42", "false"} {
		if !strings.Contains(got, token) {
			t.Errorf("ColorizeJSON output should contain %q, got: %q", token, got)
		}
	}
}

func TestColorizeJSON_EmptyObject(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	got := theme.ColorizeJSON("{}")
	if got != "{}" {
		t.Errorf("ColorizeJSON({}) = %q, want %q", got, "{}")
	}
}

func TestColorizeJSON_NestedJSON(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	input := `{"a": {"b": [1, 2]}}`
	got := theme.ColorizeJSON(input)

	if got != input {
		t.Errorf("ColorizeJSON nested with noColor should return input unchanged.\ngot:  %q\nwant: %q", got, input)
	}
}

func TestReadJSONString(t *testing.T) {
	tests := []struct {
		input string
		pos   int
		want  int
	}{
		{`"hello"`, 0, 7},
		{`"he\"lo"`, 0, 8},
		{`"abc`, 0, 4},    // unterminated
		{`x "y" z`, 2, 5}, // starts at 2
	}

	for _, tc := range tests {
		got := readJSONString(tc.input, tc.pos)
		if got != tc.want {
			t.Errorf("readJSONString(%q, %d) = %d, want %d", tc.input, tc.pos, got, tc.want)
		}
	}
}
