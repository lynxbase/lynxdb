package shell

import (
	"strings"
	"testing"

	"charm.land/lipgloss/v2"
)

// plainTheme returns a ShellTheme where every style is a no-op, so
// Render returns the input unchanged. This lets us test the token
// reassembly logic without ANSI codes in the output.
func plainTheme() *ShellTheme {
	s := lipgloss.NewStyle()
	return &ShellTheme{
		Command:  s,
		Keyword:  s,
		Function: s,
		String:   s,
		Number:   s,
		Operator: s,
		Pipe:     s,
		Field:    s,
	}
}

func TestHighlightSPL2_PreservesInput(t *testing.T) {
	theme := plainTheme()

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "quoted string field value",
			input: `log_type="postgres" | parse postgres(message) as pg | tail 2`,
		},
		{
			name:  "multiple quoted strings",
			input: `source="nginx" level="error" | stats count by host`,
		},
		{
			name:  "escaped quote in string",
			input: `message="hello \"world\"" | head 10`,
		},
		{
			name:  "empty string",
			input: `field="" | head 5`,
		},
		{
			name:  "no strings",
			input: `level=error | stats count by host | sort -count | head 10`,
		},
		{
			name:  "string at end",
			input: `source="nginx"`,
		},
		{
			name:  "adjacent strings",
			input: `a="one" b="two"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HighlightSPL2(tt.input, theme)
			if got != tt.input {
				t.Errorf("HighlightSPL2 changed the input\n  input: %q\n  got:   %q", tt.input, got)
			}
		})
	}
}

func TestStringRawEnd(t *testing.T) {
	tests := []struct {
		input string
		pos   int
		want  int
	}{
		{`"hello"`, 0, 7},
		{`x="postgres" |`, 2, 12},
		{`"a\"b"`, 0, 6},
		{`""`, 0, 2},
		{`"unterminated`, 0, 13},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := stringRawEnd(tt.input, tt.pos)
			if got != tt.want {
				t.Errorf("stringRawEnd(%q, %d) = %d, want %d", tt.input, tt.pos, got, tt.want)
			}
		})
	}
}

func TestHighlightSPL2_NoDoubledCharacters(t *testing.T) {
	theme := plainTheme()

	input := `log_type="postgres" | tail 2`
	got := HighlightSPL2(input, theme)

	// The old bug produced "postgress" (doubled 's') and lost the opening quote.
	if strings.Contains(got, "postgress") {
		t.Errorf("output contains doubled 's': %q", got)
	}

	if !strings.Contains(got, `"postgres"`) {
		t.Errorf("output missing quoted string \"postgres\": %q", got)
	}
}
