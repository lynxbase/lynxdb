package spl2

import (
	"regexp"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// Realistic log line (~200 bytes) for benchmarking.
const benchRawLine = `2026-02-14T14:23:01.345Z INFO api-gw connection refused to /user_service/health status=502 duration=1234 trace_id=abc123 method=GET path=/api/v1/users`

// A line that does NOT match /user_ patterns.
const benchNonMatchLine = `2026-02-14T14:23:01.345Z INFO api-gw request completed status=200 duration=45 trace_id=def456 method=GET path=/api/v1/orders`

// BenchmarkGlobMatch_Regex benchmarks the current regex-based wildcard matching.
func BenchmarkGlobMatch_Regex(b *testing.B) {
	// Simulate the matchGlobContains path: compile unanchored regex for "*/user_*"
	re := globToContainsRegex("*/user_*", false)
	raw := benchRawLine

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = re.MatchString(raw)
	}
}

// BenchmarkGlobMatch_RegexCaseInsensitive benchmarks case-insensitive regex matching.
func BenchmarkGlobMatch_RegexCaseInsensitive(b *testing.B) {
	re := globToContainsRegex("*/user_*", true)
	raw := benchRawLine

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = re.MatchString(raw)
	}
}

// BenchmarkGlobMatch_StringsContains benchmarks strings.Contains as alternative.
func BenchmarkGlobMatch_StringsContains(b *testing.B) {
	literal := "/user_"
	raw := benchRawLine

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = strings.Contains(raw, literal)
	}
}

// BenchmarkGlobMatch_StringsContainsNoMatch benchmarks strings.Contains on non-matching line.
func BenchmarkGlobMatch_StringsContainsNoMatch(b *testing.B) {
	literal := "/user_"
	raw := benchNonMatchLine

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = strings.Contains(raw, literal)
	}
}

// BenchmarkEvalKeyword_Wildcard benchmarks the full evalKeyword path with a wildcard pattern.
func BenchmarkEvalKeyword_Wildcard(b *testing.B) {
	eval := NewSearchEvaluator(&SearchKeywordExpr{
		Value:       "*/user_*",
		HasWildcard: true,
	})
	row := map[string]event.Value{
		"_raw": event.StringValue(benchRawLine),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = eval.Evaluate(row)
	}
}

// BenchmarkEvalKeyword_Literal benchmarks evalKeyword with a simple literal (no wildcard).
func BenchmarkEvalKeyword_Literal(b *testing.B) {
	eval := NewSearchEvaluator(&SearchKeywordExpr{
		Value:       "connection refused",
		HasWildcard: false,
	})
	row := map[string]event.Value{
		"_raw": event.StringValue(benchRawLine),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = eval.Evaluate(row)
	}
}

// BenchmarkEvalKeyword_NoMatch benchmarks evalKeyword when line doesn't match.
func BenchmarkEvalKeyword_NoMatch(b *testing.B) {
	eval := NewSearchEvaluator(&SearchKeywordExpr{
		Value:       "*/user_*",
		HasWildcard: true,
	})
	row := map[string]event.Value{
		"_raw": event.StringValue(benchNonMatchLine),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = eval.Evaluate(row)
	}
}

// BenchmarkMatchGlob_StarLiteralStar benchmarks the fast-path *literal* matching.
func BenchmarkMatchGlob_StarLiteralStar(b *testing.B) {
	eval := NewSearchEvaluator(&SearchKeywordExpr{Value: "*"})
	text := benchRawLine
	pattern := "*user_service*"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = eval.matchGlob(text, pattern, false)
	}
}

// BenchmarkMatchGlob_Complex benchmarks complex glob (falls through to regex).
func BenchmarkMatchGlob_Complex(b *testing.B) {
	eval := NewSearchEvaluator(&SearchKeywordExpr{Value: "*"})
	text := benchRawLine
	pattern := "*user*service*"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = eval.matchGlob(text, pattern, false)
	}
}

// BenchmarkGlobToRegex benchmarks regex compilation from glob pattern.
func BenchmarkGlobToRegex(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = GlobToRegex("*/user_*", false)
	}
}

// BenchmarkRegexCompile_Contains benchmarks raw regexp.Compile for unanchored glob.
func BenchmarkRegexCompile_Contains(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = regexp.MustCompile(`(?i).*/user_.*`)
	}
}
