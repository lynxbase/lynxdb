package ui

import (
	"strings"
)

// ColorizeJSON applies syntax highlighting to a JSON string using the theme's
// JSON styles. Extracted from query_tui.go for reuse across the CLI.
func (t *Theme) ColorizeJSON(s string) string {
	var b strings.Builder

	i := 0
	for i < len(s) {
		ch := s[i]

		switch {
		case ch == '{' || ch == '}' || ch == '[' || ch == ']':
			b.WriteString(t.JSONBrace.Render(string(ch)))
			i++
		case ch == ':':
			b.WriteString(t.JSONPunct.Render(": "))
			i++
			if i < len(s) && s[i] == ' ' {
				i++
			}
		case ch == ',':
			b.WriteString(t.JSONPunct.Render(","))
			i++
		case ch == '"':
			end := readJSONString(s, i)
			str := s[i:end]
			rest := strings.TrimLeft(s[end:], " \t\n\r")
			if rest != "" && rest[0] == ':' {
				b.WriteString(t.JSONKey.Render(str))
			} else {
				b.WriteString(t.JSONStr.Render(str))
			}
			i = end
		case ch == 't' || ch == 'f':
			if strings.HasPrefix(s[i:], "true") {
				b.WriteString(t.JSONBool.Render("true"))
				i += 4
			} else if strings.HasPrefix(s[i:], "false") {
				b.WriteString(t.JSONBool.Render("false"))
				i += 5
			} else {
				b.WriteByte(ch)
				i++
			}
		case ch == 'n' && strings.HasPrefix(s[i:], "null"):
			b.WriteString(t.JSONNull.Render("null"))
			i += 4
		case (ch >= '0' && ch <= '9') || ch == '-':
			j := i + 1
			for j < len(s) && isNumChar(s[j]) {
				j++
			}
			b.WriteString(t.JSONNum.Render(s[i:j]))
			i = j
		default:
			b.WriteByte(ch)
			i++
		}
	}

	return b.String()
}

// readJSONString scans from pos (which must point to an opening quote)
// to the matching closing quote, handling backslash escapes.
func readJSONString(s string, pos int) int {
	i := pos + 1
	for i < len(s) {
		if s[i] == '\\' {
			i += 2

			continue
		}
		if s[i] == '"' {
			return i + 1
		}
		i++
	}

	return len(s)
}

// isNumChar reports whether ch is valid inside a JSON number literal.
func isNumChar(ch byte) bool {
	return ch == '.' || ch == 'e' || ch == 'E' || ch == '+' || ch == '-' || (ch >= '0' && ch <= '9')
}
