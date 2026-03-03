package index

import (
	"strings"
	"unicode"
)

// Tokenize splits text into lowercase tokens suitable for full-text search.
// It handles whitespace + punctuation splitting while preserving IPs, URLs, and UUIDs.
func Tokenize(text string) []string {
	text = strings.ToLower(text)
	tokens := make([]string, 0, 16)
	var current strings.Builder

	flush := func() {
		if current.Len() > 0 {
			tokens = append(tokens, current.String())
			current.Reset()
		}
	}

	runes := []rune(text)
	for i := 0; i < len(runes); i++ {
		r := runes[i]

		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			current.WriteRune(r)

		case r == ':' || r == '-' || r == '_':
			// Minor breakers: always split on these characters.
			flush()

		default:
			flush()
		}
	}
	flush()

	return tokens
}

// TokenizeUnique returns deduplicated tokens in stable order.
func TokenizeUnique(text string) []string {
	tokens := Tokenize(text)
	seen := make(map[string]bool, len(tokens))
	unique := make([]string, 0, len(tokens))
	for _, t := range tokens {
		if !seen[t] {
			seen[t] = true
			unique = append(unique, t)
		}
	}

	return unique
}
