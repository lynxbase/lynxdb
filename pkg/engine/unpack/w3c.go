package unpack

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// W3CParser extracts fields from W3C Extended Log Format lines.
// This format is used by IIS, some CDNs, and other web servers.
//
// Field names are defined by a #Fields: directive that precedes the log data.
// The parser can either receive the fields header through NewW3CParser or
// parse it from a #Fields: line embedded in the input.
//
// W3C field names containing hyphens are normalized to underscores:
// e.g., "cs-uri-stem" becomes "cs_uri_stem".
//
// Example header:
//
//	#Fields: date time s-ip cs-method cs-uri-stem sc-status
//
// Example data line:
//
//	2026-02-14 14:52:01 10.0.0.1 GET /api/health 200
type W3CParser struct {
	fields []string // pre-configured field names (normalized)
}

// NewW3CParser creates a W3CParser with pre-configured field names from a
// #Fields: directive. The header string should be the value after "#Fields: ".
// Field names are normalized: hyphens are replaced with underscores.
func NewW3CParser(header string) *W3CParser {
	header = strings.TrimSpace(header)
	if strings.HasPrefix(header, "#Fields:") {
		header = strings.TrimSpace(header[8:])
	}

	var fields []string
	for _, f := range strings.Fields(header) {
		fields = append(fields, normalizeW3CField(f))
	}

	return &W3CParser{fields: fields}
}

// Name returns the parser format name.
func (p *W3CParser) Name() string { return "w3c" }

// DeclareFields declares the fields produced by this W3C parser instance.
func (p *W3CParser) DeclareFields() FieldDeclaration {
	if len(p.fields) == 0 {
		return FieldDeclaration{Dynamic: true}
	}
	names := make([]string, len(p.fields))
	copy(names, p.fields)
	return FieldDeclaration{Known: names}
}

// Parse extracts fields from a W3C Extended Log Format line.
// Comment lines starting with '#' are used to update the field list.
// Data lines are split by whitespace and matched positionally to field names.
func (p *W3CParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return nil
	}

	// Handle directive lines.
	if s[0] == '#' {
		if strings.HasPrefix(s, "#Fields:") {
			header := strings.TrimSpace(s[8:])
			p.fields = nil
			for _, f := range strings.Fields(header) {
				p.fields = append(p.fields, normalizeW3CField(f))
			}
		}
		// Skip all comment/directive lines (no data to emit).
		return nil
	}

	// No fields defined — cannot parse.
	if len(p.fields) == 0 {
		return nil
	}

	// Split data line by whitespace.
	values := strings.Fields(s)

	for i, fieldName := range p.fields {
		if i >= len(values) {
			break
		}
		val := values[i]
		// W3C uses "-" for missing values.
		if val == "-" {
			continue
		}
		if !emit(fieldName, InferValue(val)) {
			return nil
		}
	}

	return nil
}

// normalizeW3CField replaces hyphens with underscores in W3C field names.
// This makes field names valid identifiers for SPL2 queries.
func normalizeW3CField(name string) string {
	return strings.ReplaceAll(name, "-", "_")
}
