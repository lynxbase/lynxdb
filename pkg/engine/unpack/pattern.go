package unpack

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// PatternParser extracts fields using a user-defined extraction pattern.
// Patterns use %{name} placeholders that compile to named regex capture groups.
//
// Supported type hints:
//
//	%{name}           → (\S+)     non-whitespace (default)
//	%{name:int}       → (-?\d+)  integer
//	%{name:float}     → (-?\d+\.?\d*)  float
//	%{name:timestamp} → (.+?)    lazy match (for timestamps with spaces)
//	%{name:rest}      → (.*)     greedy match (rest of line)
//
// Literal text between placeholders is escaped with regexp.QuoteMeta.
//
// Example:
//
//	"%{name} [%{timestamp:timestamp}] %{status:int}"
//	matches: "web-01 [2026-02-14 14:52:01] 200"
//	emits:   name="web-01", timestamp="2026-02-14 14:52:01", status=200
type PatternParser struct {
	re     *regexp.Regexp
	fields []patternField
}

// patternField describes a named capture group and its type coercion.
type patternField struct {
	name     string
	coercion string // "", "int", "float", "timestamp", "rest"
}

// NewPatternParser compiles a pattern string into a PatternParser.
// Returns an error if the pattern is malformed or the resulting regex is invalid.
func NewPatternParser(pattern string) (*PatternParser, error) {
	if pattern == "" {
		return nil, fmt.Errorf("unpack_pattern: empty pattern")
	}

	var fields []patternField
	var regexBuf strings.Builder

	i := 0
	for i < len(pattern) {
		// Look for %{ placeholder start.
		pctIdx := strings.Index(pattern[i:], "%{")
		if pctIdx < 0 {
			// No more placeholders — escape remaining literal text.
			regexBuf.WriteString(regexp.QuoteMeta(pattern[i:]))

			break
		}

		// Escape literal text before placeholder.
		if pctIdx > 0 {
			regexBuf.WriteString(regexp.QuoteMeta(pattern[i : i+pctIdx]))
		}
		i += pctIdx + 2 // skip "%{"

		// Find closing "}".
		closeIdx := strings.IndexByte(pattern[i:], '}')
		if closeIdx < 0 {
			return nil, fmt.Errorf("unpack_pattern: unclosed %%{ at position %d", i-2)
		}
		spec := pattern[i : i+closeIdx]
		i += closeIdx + 1 // skip "}"

		// Parse "name" or "name:type".
		name := spec
		coercion := ""
		if colonIdx := strings.IndexByte(spec, ':'); colonIdx >= 0 {
			name = spec[:colonIdx]
			coercion = spec[colonIdx+1:]
		}

		if name == "" {
			return nil, fmt.Errorf("unpack_pattern: empty field name in %%{} at position %d", i)
		}

		// Build capture group based on coercion type.
		var groupPattern string
		switch coercion {
		case "", "string":
			groupPattern = `\S+`
		case "int":
			groupPattern = `-?\d+`
		case "float":
			groupPattern = `-?\d+\.?\d*`
		case "timestamp":
			groupPattern = `.+?`
		case "rest":
			groupPattern = `.*`
		default:
			return nil, fmt.Errorf("unpack_pattern: unknown type %q for field %q", coercion, name)
		}

		regexBuf.WriteString("(?P<")
		regexBuf.WriteString(name)
		regexBuf.WriteString(">")
		regexBuf.WriteString(groupPattern)
		regexBuf.WriteString(")")

		fields = append(fields, patternField{name: name, coercion: coercion})
	}

	re, err := regexp.Compile(regexBuf.String())
	if err != nil {
		return nil, fmt.Errorf("unpack_pattern: invalid compiled regex: %w", err)
	}

	return &PatternParser{re: re, fields: fields}, nil
}

// Name returns the parser format name.
func (p *PatternParser) Name() string { return "pattern" }

// DeclareFields declares the fields produced by this pattern parser instance.
func (p *PatternParser) DeclareFields() FieldDeclaration {
	names := make([]string, len(p.fields))
	for i, f := range p.fields {
		names[i] = f.name
	}
	return FieldDeclaration{Known: names}
}

// Parse applies the compiled pattern to input and emits captured fields.
func (p *PatternParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	matches := p.re.FindStringSubmatch(input)
	if matches == nil {
		return nil // no match — skip silently (schema-on-read)
	}

	// Map submatch indices to named groups.
	names := p.re.SubexpNames()
	for i := 1; i < len(matches); i++ {
		name := names[i]
		if name == "" {
			continue
		}
		raw := matches[i]

		val := p.coerceValue(name, raw)
		if !emit(name, val) {
			return nil
		}
	}

	return nil
}

// coerceValue converts a raw string match to the appropriate type based on
// the field's declared coercion. Falls back to string on conversion failure.
func (p *PatternParser) coerceValue(name, raw string) event.Value {
	for _, f := range p.fields {
		if f.name != name {
			continue
		}
		switch f.coercion {
		case "int":
			if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
				return event.IntValue(n)
			}
		case "float":
			if f64, err := strconv.ParseFloat(raw, 64); err == nil {
				return event.FloatValue(f64)
			}
		}
		// "timestamp", "rest", "string", "" → return as string.
		return event.StringValue(raw)
	}

	return event.StringValue(raw)
}
