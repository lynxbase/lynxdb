package spl2

import (
	"sort"
	"strings"
)

// ExtractLiterals extracts fixed substrings from a glob/wildcard pattern.
// These can be used for fast pre-filtering with strings.Contains before
// running the full regex/glob match.
//
// Examples:
//
//	"*/user_*"           → ["/user_"]
//	"*error*timeout*"    → ["timeout", "error"]  (sorted by length desc)
//	"connection refused" → ["connection refused"] (no wildcards = exact literal)
//	"*"                  → []                     (no useful literal)
//	"?"                  → []                     (single char wildcard)
//	"err*"               → ["err"]
//	"*err"               → ["err"]
//	"*abc?def*"          → ["abc", "def"]         (split on ? too)
//
// Only returns substrings >= 3 bytes (shorter ones have too many false positives).
// Returns substrings in order of descending length (longest = most selective first).
func ExtractLiterals(pattern string) []string {
	// Split on wildcard characters: * and ?
	parts := splitOnWildcards(pattern)

	var result []string
	for _, p := range parts {
		if len(p) >= 3 {
			result = append(result, p)
		}
	}

	// Sort by length descending (longest = most selective first)
	sort.Slice(result, func(i, j int) bool {
		return len(result[i]) > len(result[j])
	})

	return result
}

// ExtractLiteralBytes returns [][]byte for direct use with bytes.Contains.
// Pre-computes byte slices to avoid repeated string→[]byte conversion.
func ExtractLiteralBytes(pattern string) [][]byte {
	strs := ExtractLiterals(pattern)
	if len(strs) == 0 {
		return nil
	}
	result := make([][]byte, len(strs))
	for i, s := range strs {
		result[i] = []byte(s)
	}

	return result
}

// splitOnWildcards splits a pattern on * and ? Characters, returning
// the literal substrings between wildcards.
func splitOnWildcards(pattern string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '*' || pattern[i] == '?' {
			if i > start {
				parts = append(parts, pattern[start:i])
			}
			start = i + 1
		}
	}
	if start < len(pattern) {
		parts = append(parts, pattern[start:])
	}

	return parts
}

// matchStrategy determines the fastest matching approach for a pattern.
type matchStrategy int

const (
	matchRegex    matchStrategy = iota // fallback: compiled regex
	matchContains                      // literal: strings.Contains
	matchPrefix                        // "literal*": strings.HasPrefix
	matchSuffix                        // "*literal": strings.HasSuffix
	matchExact                         // no wildcards: ==
	matchAll                           // "*": matches everything
)

// matchPlan holds the pre-analyzed match strategy for a pattern.
type matchPlan struct {
	strategy  matchStrategy
	literal   string   // for Contains/Prefix/Suffix/Exact
	literalCI string   // lowercased literal for case-insensitive matching
	preFilter []string // for regex: literal substrings to check before regex
}

// containsWildcard returns true if the pattern contains any wildcard characters.
func containsWildcard(pattern string) bool {
	return strings.ContainsAny(pattern, "*?")
}

// analyzePattern determines the optimal match strategy for a glob pattern
// used in keyword search (substring/contains context, not anchored).
// Called once per query, result cached.
func analyzePattern(pattern string, caseInsensitive bool) matchPlan {
	// No wildcards at all → exact substring match
	if !containsWildcard(pattern) {
		plan := matchPlan{strategy: matchExact, literal: pattern}
		if caseInsensitive {
			plan.literalCI = strings.ToLower(pattern)
		}

		return plan
	}

	// Pattern is just "*" → match everything
	if pattern == "*" {
		return matchPlan{strategy: matchAll}
	}

	// Pattern is "*literal*" (no inner wildcards) → matchContains
	if len(pattern) >= 3 && pattern[0] == '*' && pattern[len(pattern)-1] == '*' {
		inner := pattern[1 : len(pattern)-1]
		if !containsWildcard(inner) {
			plan := matchPlan{strategy: matchContains, literal: inner}
			if caseInsensitive {
				plan.literalCI = strings.ToLower(inner)
			}

			return plan
		}
	}

	// Pattern is "literal*" (no other wildcards) → matchPrefix
	if pattern[len(pattern)-1] == '*' && !containsWildcard(pattern[:len(pattern)-1]) {
		literal := pattern[:len(pattern)-1]
		plan := matchPlan{strategy: matchPrefix, literal: literal}
		if caseInsensitive {
			plan.literalCI = strings.ToLower(literal)
		}

		return plan
	}

	// Pattern is "*literal" (no other wildcards) → matchSuffix
	if pattern[0] == '*' && !containsWildcard(pattern[1:]) {
		literal := pattern[1:]
		plan := matchPlan{strategy: matchSuffix, literal: literal}
		if caseInsensitive {
			plan.literalCI = strings.ToLower(literal)
		}

		return plan
	}

	// Complex pattern → extract literals for pre-filter + regex for full match
	literals := ExtractLiterals(pattern)
	if caseInsensitive {
		lowered := make([]string, len(literals))
		for i, l := range literals {
			lowered[i] = strings.ToLower(l)
		}
		literals = lowered
	}

	return matchPlan{
		strategy:  matchRegex,
		preFilter: literals,
	}
}

// analyzeContainsPattern determines the optimal match strategy for a keyword
// search pattern. Keyword search patterns are always substring matches on _raw.
//
// The key insight: in keyword search context, "*/user_*" means
// "does _raw contain a substring matching /user_*". Since outer *'s are
// implicit in keyword search, this pattern can often be simplified:
//   - "*/user_*" → the only fixed part is "/user_" → matchContains
//   - "*error*timeout*" → complex, needs regex with pre-filter
func analyzeContainsPattern(pattern string, caseInsensitive bool) matchPlan {
	// For keyword search, the pattern is already used in "contains" context.
	// Strip leading/trailing * since they're implicit in contains matching.
	stripped := pattern
	for stripped != "" && stripped[0] == '*' {
		stripped = stripped[1:]
	}
	for stripped != "" && stripped[len(stripped)-1] == '*' {
		stripped = stripped[:len(stripped)-1]
	}

	// After stripping outer *, if no wildcards remain → simple Contains
	if stripped == "" {
		return matchPlan{strategy: matchAll}
	}
	if !containsWildcard(stripped) {
		plan := matchPlan{strategy: matchContains, literal: stripped}
		if caseInsensitive {
			plan.literalCI = strings.ToLower(stripped)
		}

		return plan
	}

	// Still has inner wildcards → use regex with pre-filter
	literals := ExtractLiterals(pattern)
	if caseInsensitive {
		lowered := make([]string, len(literals))
		for i, l := range literals {
			lowered[i] = strings.ToLower(l)
		}
		literals = lowered
	}

	return matchPlan{
		strategy:  matchRegex,
		preFilter: literals,
	}
}
