package spl2

import (
	"strings"
	"testing"
)

func TestSuggestFix_UnknownCommand(t *testing.T) {
	// Simulate parser error for unknown command "stauts"
	errMsg := `spl2: unexpected command IDENT "stauts" at position 10`
	hint := SuggestFix(errMsg, nil)
	if !strings.Contains(hint, "Did you mean: stats?") {
		t.Errorf("expected 'Did you mean: stats?', got: %s", hint)
	}
}

func TestSuggestFix_UnknownCommand_Where(t *testing.T) {
	errMsg := `spl2: unexpected command IDENT "wehre" at position 15`
	hint := SuggestFix(errMsg, nil)
	if !strings.Contains(hint, "Did you mean: where?") {
		t.Errorf("expected 'Did you mean: where?', got: %s", hint)
	}
}

func TestSuggestFix_UnknownCommand_NoMatch(t *testing.T) {
	errMsg := `spl2: unexpected command IDENT "zzzzzzzzz" at position 5`
	hint := SuggestFix(errMsg, nil)
	if hint == "" {
		t.Error("expected a hint listing available commands")
	}
	if !strings.Contains(hint, "Available commands:") {
		t.Errorf("expected 'Available commands:', got: %s", hint)
	}
}

func TestSuggestFix_ImplicitSearch(t *testing.T) {
	// When an unknown lowercase identifier appears at position 0, suggest "search" prefix.
	errMsg := `spl2: unexpected command IDENT "level" at position 0`
	hint := SuggestFix(errMsg, nil)
	if !strings.Contains(hint, "search level") {
		t.Errorf("expected implicit search hint with 'search level', got: %s", hint)
	}
	if !strings.Contains(hint, `"search" keyword`) {
		t.Errorf("expected mention of search keyword, got: %s", hint)
	}
}

func TestSuggestFix_ImplicitSearch_NotPosition0(t *testing.T) {
	// At non-zero position, should NOT suggest implicit search — should suggest closest command.
	errMsg := `spl2: unexpected command IDENT "level" at position 10`
	hint := SuggestFix(errMsg, nil)
	// "level" has no close match in knownCommands, so it should list available commands.
	if strings.Contains(hint, "search level") {
		t.Errorf("should not suggest implicit search at non-zero position, got: %s", hint)
	}
}

func TestSuggestFix_SPL1_Spath(t *testing.T) {
	// SPL1 users may try "| spath" — should hint to use "| json" or "| unpack_json".
	errMsg := `spl2: unexpected command IDENT "spath" at position 10`
	hint := SuggestFix(errMsg, nil)
	if !strings.Contains(hint, "spath") {
		t.Errorf("expected mention of spath, got: %s", hint)
	}
	if !strings.Contains(hint, "json") {
		t.Errorf("expected mention of json, got: %s", hint)
	}
	if !strings.Contains(hint, "unpack_json") {
		t.Errorf("expected mention of unpack_json, got: %s", hint)
	}
}

func TestSuggestFix_UnknownFunction(t *testing.T) {
	// Simulate parser error for misspelled function name in stats context.
	errMsg := `spl2: unexpected token IDENT "cunt" at position 8`
	hint := SuggestFix(errMsg, nil)
	if !strings.Contains(hint, "Did you mean: count?") {
		t.Errorf("expected 'Did you mean: count?', got: %s", hint)
	}
}

func TestSuggestFix_UnknownFunction_Avg(t *testing.T) {
	errMsg := `spl2: unexpected token IDENT "avrage" at position 8`
	hint := SuggestFix(errMsg, nil)
	if hint == "" {
		t.Error("expected a hint for 'avrage'")
	}
	if !strings.Contains(hint, "Did you mean:") {
		t.Errorf("expected fuzzy match suggestion, got: %s", hint)
	}
}

func TestSuggestFix_MissingPipe(t *testing.T) {
	// User wrote "level=error stats count" — forgot the pipe before stats.
	errMsg := `spl2: unexpected token IDENT "stats" at position 12`
	hint := SuggestFix(errMsg, nil)
	if !strings.Contains(hint, "| stats") {
		t.Errorf("expected suggestion to add pipe, got: %s", hint)
	}
	if !strings.Contains(hint, "preceded by a pipe") {
		t.Errorf("expected explanation about pipe, got: %s", hint)
	}
}

func TestSuggestFix_MissingPipe_Where(t *testing.T) {
	errMsg := `spl2: unexpected token IDENT "where" at position 5`
	hint := SuggestFix(errMsg, nil)
	if !strings.Contains(hint, "| where") {
		t.Errorf("expected suggestion to add pipe before where, got: %s", hint)
	}
}

func TestSuggestFix_QuoteMismatch_UnterminatedString(t *testing.T) {
	errMsg := `spl2: unterminated string at position 20`
	hint := SuggestFix(errMsg, nil)
	if !strings.Contains(hint, "Missing closing quote") {
		t.Errorf("expected unclosed quote hint, got: %s", hint)
	}
}

func TestSuggestFix_QuoteMismatch_MissingParen(t *testing.T) {
	errMsg := `spl2: expected ")" at position 15`
	hint := SuggestFix(errMsg, nil)
	if !strings.Contains(hint, "Missing closing parenthesis") {
		t.Errorf("expected missing paren hint, got: %s", hint)
	}
}

func TestSuggestFix_QuoteMismatch_MissingBracket(t *testing.T) {
	errMsg := `spl2: expected "]" at position 20`
	hint := SuggestFix(errMsg, nil)
	if !strings.Contains(hint, "Missing closing bracket") {
		t.Errorf("expected missing bracket hint, got: %s", hint)
	}
}

func TestSuggestFix_MissingBy(t *testing.T) {
	// User wrote "| stats count source" — forgot BY.
	errMsg := `spl2: stats: unexpected token IDENT "source" at position 15`
	hint := SuggestFix(errMsg, nil)
	if !strings.Contains(hint, "by") {
		t.Errorf("expected missing 'by' keyword hint, got: %s", hint)
	}
	if !strings.Contains(hint, "source") {
		t.Errorf("expected field name in hint, got: %s", hint)
	}
}

func TestSuggestFix_EmptyPipeline(t *testing.T) {
	// User wrote "FROM main |" — trailing pipe with nothing after.
	errMsg := `spl2: unexpected token EOF at position 12`
	hint := SuggestFix(errMsg, nil)
	if !strings.Contains(hint, "Incomplete query") {
		t.Errorf("expected incomplete query hint, got: %s", hint)
	}
	if !strings.Contains(hint, "stats count") {
		t.Errorf("expected example command in hint, got: %s", hint)
	}
}

func TestSuggestField(t *testing.T) {
	fields := []string{"status", "source", "host", "sourcetype", "_raw", "_time"}

	hint := SuggestField("stauts", fields)
	if !strings.Contains(hint, "Did you mean: status?") {
		t.Errorf("expected 'Did you mean: status?', got: %s", hint)
	}

	hint = SuggestField("sorce", fields)
	if !strings.Contains(hint, "Did you mean: source?") {
		t.Errorf("expected 'Did you mean: source?', got: %s", hint)
	}

	// Known field should return empty.
	hint = SuggestField("status", fields)
	if hint != "" {
		t.Errorf("expected empty for known field, got: %s", hint)
	}
}

func TestSuggestFunction(t *testing.T) {
	hint := SuggestFunction("cunt")
	if !strings.Contains(hint, "Did you mean: count?") {
		t.Errorf("expected 'Did you mean: count?', got: %s", hint)
	}

	hint = SuggestFunction("avrage")
	if hint == "" {
		t.Error("expected a hint for 'avrage'")
	}
}

func TestSuggestTypeMismatch(t *testing.T) {
	errMsg := "cannot compare string to int"
	hint := SuggestFix(errMsg, nil)
	if !strings.Contains(hint, "tonumber()") {
		t.Errorf("expected tonumber hint, got: %s", hint)
	}
}

func TestFormatParseError_WithCaret(t *testing.T) {
	query := `FROM main | stauts count by source`
	err := parseForTest(query)
	if err == nil {
		t.Fatal("expected parse error for misspelled command 'stauts'")
	}
	formatted := FormatParseError(err, query)
	if !strings.Contains(formatted, "^") {
		t.Errorf("expected caret in formatted error, got:\n%s", formatted)
	}
}

func TestFormatParseError_NilError(t *testing.T) {
	result := FormatParseError(nil, "")
	if result != "" {
		t.Errorf("expected empty for nil error, got: %s", result)
	}
}

func TestLevenshtein(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"a", "", 1},
		{"", "b", 1},
		{"abc", "abc", 0},
		{"abc", "abd", 1},
		{"stats", "stauts", 1},
		{"where", "wehre", 2},
		{"count", "cunt", 1},
		{"search", "serach", 2},
	}
	for _, tt := range tests {
		got := levenshtein(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("levenshtein(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

// parseForTest is a helper to trigger a parse error for testing.
func parseForTest(input string) error {
	_, err := Parse(input)

	return err
}
