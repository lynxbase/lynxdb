package unpack

import (
	"github.com/lynxbase/lynxdb/pkg/event"
)

// LogfmtParser extracts fields from logfmt-style input (key=value key2="quoted value").
// Hand-written byte scanner for zero-allocation on the hot path (no regex, no strings.Split).
type LogfmtParser struct{}

// Name returns the parser format name.
func (p *LogfmtParser) Name() string { return "logfmt" }

// Parse scans input for key=value pairs and calls emit for each.
// Supports: unquoted values, double-quoted values, and empty values (key=).
func (p *LogfmtParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	i := 0
	n := len(input)

	for i < n {
		// Skip whitespace.
		for i < n && (input[i] == ' ' || input[i] == '\t') {
			i++
		}
		if i >= n {
			break
		}

		// Read key: scan until '=' or whitespace.
		keyStart := i
		for i < n && input[i] != '=' && input[i] != ' ' && input[i] != '\t' {
			i++
		}
		if i >= n || input[i] != '=' {
			// No '=' found — skip this token.
			for i < n && input[i] != ' ' && input[i] != '\t' {
				i++
			}

			continue
		}

		key := input[keyStart:i]
		i++ // consume '='

		if key == "" {
			continue
		}

		// Read value.
		var val string
		if i < n && input[i] == '"' {
			// Quoted value: scan until closing quote.
			i++ // skip opening quote
			valStart := i
			for i < n && input[i] != '"' {
				if input[i] == '\\' && i+1 < n {
					i++ // skip escaped char
				}
				i++
			}
			val = input[valStart:i]
			if i < n {
				i++ // skip closing quote
			}
		} else {
			// Unquoted value: scan until whitespace.
			valStart := i
			for i < n && input[i] != ' ' && input[i] != '\t' {
				i++
			}
			val = input[valStart:i]
		}

		if !emit(key, InferValue(val)) {
			return nil // short-circuit
		}
	}

	return nil
}
