// Package glob provides RFC-style glob pattern matching used across the
// codebase. The implementation supports *, **, ?, character classes [abc],
// negated classes [!abc], and brace alternatives {a,b,c}.
package glob

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// globRegexCache is a process-wide cache of compiled glob regexes.
// Soft-capped to avoid unbounded growth from pathological inputs;
// once full, additional patterns compile every call but are not stored.
var globRegexCache = struct {
	mu  sync.RWMutex
	m   map[string]*regexp.Regexp
	max int
}{m: make(map[string]*regexp.Regexp), max: 4096}

func compileGlobCached(key string, compile func() *regexp.Regexp) *regexp.Regexp {
	globRegexCache.mu.RLock()
	if re, ok := globRegexCache.m[key]; ok {
		globRegexCache.mu.RUnlock()

		return re
	}
	globRegexCache.mu.RUnlock()

	re := compile()
	globRegexCache.mu.Lock()
	if existing, ok := globRegexCache.m[key]; ok {
		globRegexCache.mu.Unlock()

		return existing
	}
	if len(globRegexCache.m) < globRegexCache.max {
		globRegexCache.m[key] = re
	}
	globRegexCache.mu.Unlock()

	return re
}

// ToRegex converts a glob pattern to a compiled regular expression.
// The result is anchored with ^ and $. Single * matches any character except
// /, while ** matches anything including /. When caseInsensitive is true the
// regex uses the (?i) flag.
func ToRegex(pattern string, caseInsensitive bool) *regexp.Regexp {
	return toRegex(pattern, caseInsensitive, true)
}

// Compile is like ToRegex but rejects malformed patterns instead of silently
// matching them literally or panicking: an unterminated character class
// ("[ab") or an invalid range ("[z-a]") returns an error. Use it when the
// pattern comes from user input.
func Compile(pattern string, caseInsensitive bool) (*regexp.Regexp, error) {
	if err := validateClasses(pattern); err != nil {
		return nil, err
	}
	return regexp.Compile(buildRegexString(pattern, caseInsensitive, true, false))
}

// validateClasses rejects unterminated [ character classes. Other malformed
// class contents (empty class, reversed range) surface as regexp.Compile
// errors from the translated pattern.
func validateClasses(pattern string) error {
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '\\':
			if i+1 < len(pattern) {
				i++
			}
		case '[':
			j := i + 1
			if j < len(pattern) && (pattern[j] == '!' || pattern[j] == '^') {
				j++
			}
			closed := false
			for ; j < len(pattern); j++ {
				if pattern[j] == '\\' {
					j++
					continue
				}
				if pattern[j] == ']' {
					closed = true
					break
				}
			}
			if !closed {
				return fmt.Errorf("glob: unterminated character class in %q", pattern)
			}
			i = j
		}
	}
	return nil
}

// Match reports whether text matches an RFC glob pattern.
// This is a convenience wrapper around ToRegex.
func Match(pattern, text string, caseInsensitive bool) bool {
	return ToRegex(pattern, caseInsensitive).MatchString(text)
}

// MatchCached reports whether text matches a glob pattern, using a
// process-wide regex cache keyed on (pattern, caseInsensitive).
func MatchCached(pattern, text string, caseInsensitive bool) bool {
	key := "s:" + pattern
	if caseInsensitive {
		key = "i:" + pattern
	}

	ci := caseInsensitive
	compiled := compileGlobCached(key, func() *regexp.Regexp {
		return ToRegex(pattern, ci)
	})

	return compiled.MatchString(text)
}

func toRegex(pattern string, caseInsensitive bool, anchored bool) *regexp.Regexp {
	return toRegexWithMode(pattern, caseInsensitive, anchored, false)
}

func toRegexWithMode(pattern string, caseInsensitive bool, anchored bool, wildcardMatchesSlash bool) *regexp.Regexp {
	return regexp.MustCompile(buildRegexString(pattern, caseInsensitive, anchored, wildcardMatchesSlash))
}

func buildRegexString(pattern string, caseInsensitive bool, anchored bool, wildcardMatchesSlash bool) string {
	var buf strings.Builder
	if caseInsensitive {
		buf.WriteString("(?i)")
	}
	if anchored {
		buf.WriteString("^")
	}
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		switch ch {
		case '*':
			if i+1 < len(pattern) && pattern[i+1] == '*' {
				buf.WriteString(".*")
				i++
			} else if wildcardMatchesSlash {
				buf.WriteString(".*")
			} else {
				buf.WriteString("[^/]*")
			}
		case '?':
			if wildcardMatchesSlash {
				buf.WriteByte('.')
			} else {
				buf.WriteString("[^/]")
			}
		case '[':
			next, ok := appendGlobClass(&buf, pattern, i)
			if ok {
				i = next
			} else {
				buf.WriteString(`\[`)
			}
		case '{':
			next, ok := appendGlobAlternatives(&buf, pattern, i)
			if ok {
				i = next
			} else {
				buf.WriteString(`\{`)
			}
		case '\\':
			if i+1 < len(pattern) {
				next := pattern[i+1]
				if isEscapableGlobChar(next) {
					i++
					buf.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
				} else {
					buf.WriteString(`\\`)
				}
			} else {
				buf.WriteString(`\\`)
			}
		case '.', '(', ')', ']', '}', '+', '^', '$', '|':
			buf.WriteByte('\\')
			buf.WriteByte(ch)
		default:
			buf.WriteByte(ch)
		}
	}
	if anchored {
		buf.WriteString("$")
	}

	return buf.String()
}

// ToContainsRegex converts a glob pattern to a regex without anchoring
// (substring match).
func ToContainsRegex(pattern string, caseInsensitive bool) *regexp.Regexp {
	return toRegexWithMode(pattern, caseInsensitive, false, true)
}

// LiteralPrefix returns the leading literal run of a glob pattern: the
// characters (with backslash escapes resolved) before the first
// metacharacter (*, ?, [, {). Ordered-key scans such as FST term-dictionary
// iteration use it to bound the range they must visit. Empty when the
// pattern starts with a metacharacter.
func LiteralPrefix(pattern string) string {
	var b strings.Builder
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		switch ch {
		case '*', '?', '[', '{':
			return b.String()
		case '\\':
			if i+1 < len(pattern) {
				i++
				b.WriteByte(pattern[i])
				continue
			}
			b.WriteByte(ch)
		default:
			b.WriteByte(ch)
		}
	}
	return b.String()
}

func isEscapableGlobChar(ch byte) bool {
	switch ch {
	case '*', '?', '[', ']', '{', '}', '\\':
		return true
	default:
		return false
	}
}

func appendGlobClass(buf *strings.Builder, pattern string, start int) (int, bool) {
	i := start + 1
	if i >= len(pattern) {
		return start, false
	}

	var class strings.Builder
	class.WriteByte('[')
	switch pattern[i] {
	case '!':
		class.WriteByte('^')
		i++
	case '^':
		class.WriteByte('\\')
		class.WriteByte('^')
		i++
	}

	for ; i < len(pattern); i++ {
		ch := pattern[i]
		if ch == ']' {
			class.WriteByte(']')
			buf.WriteString(class.String())

			return i, true
		}
		if ch == '\\' {
			if i+1 >= len(pattern) {
				class.WriteString(`\\`)
				continue
			}
			i++
			ch = pattern[i]
		}
		class.WriteByte(ch)
	}

	return start, false
}

func appendGlobAlternatives(buf *strings.Builder, pattern string, start int) (int, bool) {
	i := start + 1
	var parts []string
	var part strings.Builder

	for ; i < len(pattern); i++ {
		ch := pattern[i]
		switch ch {
		case '\\':
			if i+1 < len(pattern) {
				i++
				part.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
			} else {
				part.WriteString(`\\`)
			}
		case ',':
			parts = append(parts, part.String())
			part.Reset()
		case '}':
			parts = append(parts, part.String())
			if len(parts) < 2 {
				return start, false
			}
			buf.WriteString("(?:")
			for j, alt := range parts {
				if j > 0 {
					buf.WriteByte('|')
				}
				buf.WriteString(alt)
			}
			buf.WriteByte(')')

			return i, true
		default:
			part.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
		}
	}

	return start, false
}
