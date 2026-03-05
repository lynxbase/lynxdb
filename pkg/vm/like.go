package vm

import "strings"

// MatchLike implements SQL LIKE pattern matching (case-insensitive).
// '%' matches zero or more characters, '_' matches exactly one character.
// All other characters are matched literally.
//
// Exported for use by the vectorized filter path in engine/pipeline,
// which needs to evaluate LIKE patterns without going through the VM.
func MatchLike(text, pattern string) bool {
	return matchLike(text, pattern)
}

// ClassifyLikePattern returns the structural class of a LIKE pattern
// and the literal core (if applicable). This allows callers to dispatch
// to fast-path string functions (HasPrefix, HasSuffix, Contains, ==)
// instead of running the general LIKE matcher.
//
// Returns:
//   - kind: "prefix" (%suffix removed), "suffix" (leading % removed),
//     "contains" (both % removed), "exact" (no wildcards), "general" (complex)
//   - literal: the extracted literal (lowercased, since LIKE is case-insensitive)
func ClassifyLikePattern(pattern string) (kind, literal string) {
	lower := strings.ToLower(pattern)

	// No wildcards at all → exact match.
	if !strings.ContainsAny(lower, "%_") {
		return "exact", lower
	}

	// Pure "%" → match everything.
	if lower == "%" {
		return "general", ""
	}

	hasLeading := lower[0] == '%'
	hasTrailing := lower[len(lower)-1] == '%'

	if hasLeading && hasTrailing && len(lower) >= 3 {
		inner := lower[1 : len(lower)-1]
		if !strings.ContainsAny(inner, "%_") {
			return "contains", inner
		}
	}

	if hasTrailing && !hasLeading {
		prefix := lower[:len(lower)-1]
		if !strings.ContainsAny(prefix, "%_") {
			return "prefix", prefix
		}
	}

	if hasLeading && !hasTrailing {
		suffix := lower[1:]
		if !strings.ContainsAny(suffix, "%_") {
			return "suffix", suffix
		}
	}

	return "general", lower
}
