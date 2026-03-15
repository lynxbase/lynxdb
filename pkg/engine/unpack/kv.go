package unpack

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// KVParser extracts key=value pairs from log lines with configurable delimiters.
// Default: space-delimited pairs with '=' assignment and '"' quoting.
//
// Example (default):
//
//	level=error msg="connection refused" duration_ms=3.14
//
// Example (custom delim=",", assign=":", quote="'"):
//
//	level:error,msg:'connection refused',duration_ms:3.14
type KVParser struct {
	Delim  byte // pair delimiter (default: ' ')
	Assign byte // key=value separator (default: '=')
	Quote  byte // value quote character (default: '"')
}

// Name returns the parser format name.
func (p *KVParser) Name() string { return "kv" }

// DeclareFields declares the fields produced by the KV parser.
func (p *KVParser) DeclareFields() FieldDeclaration {
	return FieldDeclaration{Dynamic: true}
}

// defaults fills in zero-value config with sensible defaults.
func (p *KVParser) defaults() (delim, assign, quote byte) {
	delim = p.Delim
	if delim == 0 {
		delim = ' '
	}
	assign = p.Assign
	if assign == 0 {
		assign = '='
	}
	quote = p.Quote
	if quote == 0 {
		quote = '"'
	}

	return delim, assign, quote
}

// Parse extracts key=value pairs from the input string.
func (p *KVParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return nil
	}

	delim, assign, quote := p.defaults()

	i := 0
	for i < len(s) {
		// Skip delimiters
		for i < len(s) && s[i] == delim {
			i++
		}
		if i >= len(s) {
			break
		}

		// Find the assignment character
		keyStart := i
		for i < len(s) && s[i] != assign && s[i] != delim {
			i++
		}
		if i >= len(s) || s[i] != assign {
			// No assignment found — skip this token
			for i < len(s) && s[i] != delim {
				i++
			}

			continue
		}

		key := s[keyStart:i]
		i++ // skip assign character

		if i >= len(s) {
			// Key with no value — emit empty string
			if !emit(key, event.StringValue("")) {
				return nil
			}

			break
		}

		// Parse value — may be quoted
		var val string
		if s[i] == quote {
			i++ // skip opening quote
			valStart := i
			for i < len(s) && s[i] != quote {
				if s[i] == '\\' && i+1 < len(s) {
					i++ // skip escaped char
				}
				i++
			}
			val = s[valStart:i]
			if i < len(s) {
				i++ // skip closing quote
			}
		} else {
			valStart := i
			for i < len(s) && s[i] != delim {
				i++
			}
			val = s[valStart:i]
		}

		if !emit(key, InferValue(val)) {
			return nil
		}
	}

	return nil
}
