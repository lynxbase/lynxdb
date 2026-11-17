// Package glob provides RFC-style glob pattern matching used across the
// codebase. The implementation supports *, **, ?, character classes [abc],
// negated classes [!abc], and brace alternatives {a,b,c}.
package glob

import (
	"fmt"
	"regexp"
	"strings"
)

// Compile converts a glob pattern to a regular expression anchored with ^ and
// $, in which * matches anything but / and ** matches anything; an
// unterminated character class ("[ab") or an invalid range ("[z-a]") returns
// an error.
func Compile(pattern string, caseInsensitive bool) (*regexp.Regexp, error) {
	if err := validateClasses(pattern); err != nil {
		return nil, err
	}
	return regexp.Compile(buildRegexString(pattern, caseInsensitive))
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

func buildRegexString(pattern string, caseInsensitive bool) string {
	var buf strings.Builder
	if caseInsensitive {
		buf.WriteString("(?i)")
	}
	buf.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		ch := pattern[i]
		switch ch {
		case '*':
			if i+1 < len(pattern) && pattern[i+1] == '*' {
				buf.WriteString(".*")
				i++
			} else {
				buf.WriteString("[^/]*")
			}
		case '?':
			buf.WriteString("[^/]")
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
	buf.WriteString("$")

	return buf.String()
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
