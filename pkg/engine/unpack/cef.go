package unpack

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// CEFParser extracts fields from Common Event Format (CEF) log lines.
// Format: CEF:version|device_vendor|device_product|device_version|signature_id|name|severity|extension
// Pipe characters within the first 7 header fields are escaped as \|.
// The extension section contains key=value pairs separated by spaces.
//
// Example:
//
//	CEF:0|SecurityVendor|Firewall|1.0|100|Connection Refused|5|src=10.0.0.1 dst=192.168.1.1 dpt=80
type CEFParser struct{}

// Name returns the parser format name.
func (p *CEFParser) Name() string { return "cef" }

// Parse extracts fields from a CEF log line.
func (p *CEFParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return nil
	}

	// Must start with "CEF:" (case-insensitive).
	if len(s) < 4 || strings.ToUpper(s[:4]) != "CEF:" {
		return nil
	}
	s = s[4:]

	// Split header into 7 pipe-delimited fields, handling \| escapes.
	// Fields: version, device_vendor, device_product, device_version,
	//         signature_id, name, severity
	headerNames := []string{
		"cef_version", "device_vendor", "device_product", "device_version",
		"signature_id", "name", "severity",
	}

	for idx := 0; idx < 7; idx++ {
		field, rest, found := scanCEFField(s)
		if idx < 6 && !found {
			// First 6 fields require a pipe delimiter. Last field (severity)
			// is followed by pipe or end of header.
			return nil
		}

		val := unescapeCEFPipe(field)
		// Try numeric inference for version and severity
		if idx == 0 || idx == 6 {
			if !emit(headerNames[idx], InferValue(val)) {
				return nil
			}
		} else {
			if !emit(headerNames[idx], event.StringValue(val)) {
				return nil
			}
		}

		if found {
			s = rest
		} else {
			s = ""
		}
	}

	// Parse extension: key=value pairs separated by spaces.
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return nil
	}

	parseCEFExtension(s, emit)

	return nil
}

// scanCEFField reads a pipe-delimited field, handling \| escapes.
// Returns the raw field content, the remaining string after the pipe, and
// whether a pipe delimiter was found.
func scanCEFField(s string) (field, rest string, found bool) {
	var b strings.Builder
	i := 0
	for i < len(s) {
		if s[i] == '\\' && i+1 < len(s) && s[i+1] == '|' {
			b.WriteString(s[:i])
			b.WriteByte('|')
			s = s[i+2:]
			i = 0

			continue
		}
		if s[i] == '|' {
			if b.Len() > 0 {
				b.WriteString(s[:i])

				return b.String(), s[i+1:], true
			}

			return s[:i], s[i+1:], true
		}
		i++
	}
	if b.Len() > 0 {
		b.WriteString(s)

		return b.String(), "", false
	}

	return s, "", false
}

// unescapeCEFPipe replaces \| with | and \\ with \ in a CEF field value.
func unescapeCEFPipe(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}
	r := strings.NewReplacer(`\|`, `|`, `\\`, `\`)

	return r.Replace(s)
}

// parseCEFExtension parses the CEF extension section (key=value pairs).
// Values may contain spaces if they are the last value before the next key=.
func parseCEFExtension(s string, emit func(key string, val event.Value) bool) {
	for len(s) > 0 {
		// Find the next key=
		eqIdx := strings.IndexByte(s, '=')
		if eqIdx < 0 {
			break
		}

		key := strings.TrimSpace(s[:eqIdx])
		s = s[eqIdx+1:]

		// Value extends until the next " key=" pattern or end of string.
		// We look for the next " word=" pattern.
		val := ""
		nextKeyIdx := findNextCEFKey(s)
		if nextKeyIdx >= 0 {
			val = s[:nextKeyIdx]
			s = s[nextKeyIdx:]
		} else {
			val = s
			s = ""
		}

		val = strings.TrimSpace(val)
		if key != "" {
			if !emit(key, InferValue(val)) {
				return
			}
		}
	}
}

// findNextCEFKey finds the position of the next " key=" pattern in the extension.
// Returns -1 if not found.
func findNextCEFKey(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' {
			// Look ahead for "word="
			rest := s[i+1:]
			eqIdx := strings.IndexByte(rest, '=')
			if eqIdx > 0 {
				// Check that the part before = is a valid key (no spaces)
				potentialKey := rest[:eqIdx]
				if !strings.ContainsRune(potentialKey, ' ') && len(potentialKey) > 0 {
					return i + 1
				}
			}
		}
	}

	return -1
}
