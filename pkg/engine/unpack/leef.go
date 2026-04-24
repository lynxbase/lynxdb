package unpack

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// LEEFParser extracts fields from IBM LEEF (Log Event Extended Format) lines.
// LEEF is structurally similar to CEF but uses a different header layout and
// supports two versions:
//
//   - LEEF 1.0: tab-delimited extensions (default)
//   - LEEF 2.0: custom delimiter specified in a 6th pipe-delimited header field
//
// Format:
//
//	LEEF:version|vendor|product|version|event_id|extensions
//	LEEF:2.0|vendor|product|version|event_id|0x09|extensions  (LEEF 2.0 with custom delim)
//
// Example:
//
//	LEEF:1.0|IBM|QRadar|7.0|PortScan|src=10.0.0.1	dst=192.168.1.1	proto=TCP
type LEEFParser struct{}

// Name returns the parser format name.
func (p *LEEFParser) Name() string { return "leef" }

// DeclareFields declares the fields produced by the LEEF parser.
func (p *LEEFParser) DeclareFields() FieldDeclaration {
	return FieldDeclaration{
		Known:   []string{"leef_version", "device_vendor", "device_product", "device_version", "event_id"},
		Dynamic: true,
	}
}

// Parse extracts fields from a LEEF log line.
func (p *LEEFParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return nil
	}

	// Must start with "LEEF:" (case-insensitive).
	if len(s) < 5 || strings.ToUpper(s[:5]) != "LEEF:" {
		return nil
	}
	s = s[5:]

	headerNames := []string{
		"leef_version", "device_vendor", "device_product", "device_version", "event_id",
	}

	for idx := 0; idx < 5; idx++ {
		pipeIdx := strings.IndexByte(s, '|')
		if pipeIdx < 0 {
			if idx == 4 {
				// Last header field — rest is the value, no extensions.
				if !emit(headerNames[idx], event.StringValue(s)) {
					return nil
				}
				s = ""

				break
			}

			return nil // Malformed: not enough pipe delimiters.
		}
		field := s[:pipeIdx]
		s = s[pipeIdx+1:]

		if idx == 0 {
			// Version — try numeric inference.
			if !emit(headerNames[idx], InferValue(field)) {
				return nil
			}
		} else {
			if !emit(headerNames[idx], event.StringValue(field)) {
				return nil
			}
		}
	}

	if len(s) == 0 {
		return nil
	}

	// Determine extension delimiter.
	// LEEF 2.0 has a 6th pipe-delimited field specifying the delimiter.
	// Common values: "0x09" (tab), "^" (caret), or a literal character.
	delim := "\t" // LEEF 1.0 default

	// Check if there's a custom delimiter field (LEEF 2.0).
	// The first character tells us: if it's a control code like "0x09" or a
	// single printable char that isn't '=' (which would be a key), it's a delimiter.
	if len(s) > 0 {
		// Check for "0xNN|extensions" pattern (hex-encoded delimiter).
		if len(s) >= 4 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
			pipeIdx := strings.IndexByte(s, '|')
			if pipeIdx > 0 && pipeIdx <= 6 {
				delimSpec := s[:pipeIdx]
				s = s[pipeIdx+1:]
				delim = decodeLEEFDelim(delimSpec)
			}
		} else if len(s) >= 2 && s[1] == '|' && s[0] != '=' {
			// Single character delimiter like "^|extensions"
			delim = string(s[0])
			s = s[2:]
		}
	}

	// Parse extension key=value pairs.
	parseLEEFExtension(s, delim, emit)

	return nil
}

// decodeLEEFDelim decodes a LEEF delimiter specification.
// Handles "0x09" (tab), "0x0a" (newline), or returns raw string.
func decodeLEEFDelim(spec string) string {
	switch strings.ToLower(spec) {
	case "0x09":
		return "\t"
	case "0x0a":
		return "\n"
	case "0x0d":
		return "\r"
	case "0x20":
		return " "
	default:
		if len(spec) == 1 {
			return spec
		}

		return "\t" // fallback to tab
	}
}

// parseLEEFExtension parses LEEF extension key=value pairs separated by delim.
func parseLEEFExtension(s, delim string, emit func(key string, val event.Value) bool) {
	pairs := strings.Split(s, delim)
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		eqIdx := strings.IndexByte(pair, '=')
		if eqIdx < 0 {
			continue
		}
		key := strings.TrimSpace(pair[:eqIdx])
		val := strings.TrimSpace(pair[eqIdx+1:])
		if key == "" {
			continue
		}
		if !emit(key, InferValue(val)) {
			return
		}
	}
}
