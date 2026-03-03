package spl2

import (
	"reflect"
	"testing"
)

func TestExtractLiterals(t *testing.T) {
	tests := []struct {
		pattern string
		want    []string
	}{
		{"*/user_*", []string{"/user_"}},
		{"*error*timeout*", []string{"timeout", "error"}}, // timeout longer
		{"connection refused", []string{"connection refused"}},
		{"*", nil},
		{"??", nil},
		{"ab", nil},                 // too short (<3)
		{"abc", []string{"abc"}},    // exactly 3 chars
		{"err*or", []string{"err"}}, // "or" too short
		{"*abc?def*", []string{"abc", "def"}},
		{"*ab?cd*", nil},             // "ab" is 2, "cd" is 2 → both filtered
		{"hello", []string{"hello"}}, // no wildcards
		{"", nil},                    // empty pattern
		{"***", nil},                 // only wildcards
		{"*a*b*c*", nil},             // all parts < 3
		{"*long_prefix*short*", []string{"long_prefix", "short"}},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			got := ExtractLiterals(tt.pattern)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExtractLiterals(%q) = %v, want %v", tt.pattern, got, tt.want)
			}
		})
	}
}

func TestExtractLiteralBytes(t *testing.T) {
	got := ExtractLiteralBytes("*/user_*")
	if len(got) != 1 {
		t.Fatalf("expected 1 literal, got %d", len(got))
	}
	if string(got[0]) != "/user_" {
		t.Errorf("expected /user_, got %s", string(got[0]))
	}

	// Empty result
	got = ExtractLiteralBytes("*")
	if got != nil {
		t.Errorf("expected nil for *, got %v", got)
	}
}

func TestAnalyzeContainsPattern(t *testing.T) {
	tests := []struct {
		pattern         string
		caseInsensitive bool
		wantStrategy    matchStrategy
		wantLiteral     string
	}{
		{"*/user_*", false, matchContains, "/user_"},
		{"*/user_*", true, matchContains, "/user_"},
		{"error", false, matchContains, "error"}, // no wildcards in contains context = matchContains
		{"*", false, matchAll, ""},
		{"***", false, matchAll, ""},
		{"*error*timeout*", false, matchRegex, ""}, // inner wildcard
		{"*abc?def*", false, matchRegex, ""},       // ? is inner wildcard
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			plan := analyzeContainsPattern(tt.pattern, tt.caseInsensitive)
			if plan.strategy != tt.wantStrategy {
				t.Errorf("analyzeContainsPattern(%q) strategy = %d, want %d", tt.pattern, plan.strategy, tt.wantStrategy)
			}
			if plan.literal != tt.wantLiteral {
				t.Errorf("analyzeContainsPattern(%q) literal = %q, want %q", tt.pattern, plan.literal, tt.wantLiteral)
			}
		})
	}
}

func TestAnalyzePattern(t *testing.T) {
	tests := []struct {
		pattern      string
		wantStrategy matchStrategy
		wantLiteral  string
	}{
		{"error", matchExact, "error"},
		{"error*", matchPrefix, "error"},
		{"*error", matchSuffix, "error"},
		{"*error*", matchContains, "error"},
		{"*", matchAll, ""},
		{"*err*time*", matchRegex, ""},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			plan := analyzePattern(tt.pattern, false)
			if plan.strategy != tt.wantStrategy {
				t.Errorf("analyzePattern(%q) strategy = %d, want %d", tt.pattern, plan.strategy, tt.wantStrategy)
			}
			if plan.literal != tt.wantLiteral {
				t.Errorf("analyzePattern(%q) literal = %q, want %q", tt.pattern, plan.literal, tt.wantLiteral)
			}
		})
	}
}

func BenchmarkExtractLiterals(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ExtractLiterals("*/user_*")
	}
}

func BenchmarkAnalyzeContainsPattern(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = analyzeContainsPattern("*/user_*", false)
	}
}
