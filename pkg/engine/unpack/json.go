package unpack

import (
	"encoding/json"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// JSONParser extracts fields from JSON input with dot-notation flattening for
// nested objects. Arrays are preserved as JSON string values. Native JSON types
// (bool, int, float, null) are preserved without string roundtrip.
type JSONParser struct{}

// Name returns the parser format name.
func (p *JSONParser) Name() string { return "json" }

// Parse decodes a JSON object from input and emits flattened key-value pairs.
// Nested objects are flattened with dot-notation (e.g., "request.method").
// Arrays and nested arrays are emitted as JSON string values.
func (p *JSONParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	s := strings.TrimSpace(input)
	if len(s) == 0 || s[0] != '{' {
		return nil // not a JSON object — skip silently
	}

	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()

	var raw map[string]json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		return nil // malformed JSON — skip silently (schema-on-read)
	}

	return flattenJSON("", raw, emit)
}

// flattenJSON recursively walks a JSON object and emits leaf values with
// dot-notation keys. Returns early if emit returns false.
func flattenJSON(prefix string, obj map[string]json.RawMessage, emit func(string, event.Value) bool) error {
	for key, raw := range obj {
		fullKey := key
		if prefix != "" {
			fullKey = prefix + "." + key
		}

		val, isObj := decodeJSONValue(raw)
		if isObj != nil {
			// Nested object: recurse with dot-notation prefix.
			if err := flattenJSON(fullKey, isObj, emit); err != nil {
				return err
			}

			continue
		}

		if !emit(fullKey, val) {
			return nil // short-circuit
		}
	}

	return nil
}

// decodeJSONValue decodes a json.RawMessage into a typed event.Value.
// For objects, it returns (zero, nestedMap) so the caller can recurse.
// For arrays, objects, and null, it returns the appropriate value.
func decodeJSONValue(raw json.RawMessage) (event.Value, map[string]json.RawMessage) {
	s := strings.TrimSpace(string(raw))
	if len(s) == 0 {
		return event.NullValue(), nil
	}

	switch s[0] {
	case '{':
		// Try to decode as nested object for flattening.
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(raw, &nested); err == nil {
			return event.Value{}, nested
		}
		// Malformed nested JSON — emit as string.
		return event.StringValue(s), nil

	case '[':
		// Arrays are emitted as JSON string values (no column explosion).
		return event.StringValue(s), nil

	case '"':
		// String value.
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return event.StringValue(s), nil
		}

		return event.StringValue(str), nil

	case 't', 'f':
		// Boolean.
		if s == "true" {
			return event.BoolValue(true), nil
		}
		if s == "false" {
			return event.BoolValue(false), nil
		}

		return event.StringValue(s), nil

	case 'n':
		// Null.
		if s == "null" {
			return event.NullValue(), nil
		}

		return event.StringValue(s), nil

	default:
		// Number: try integer first, then float.
		var num json.Number
		if err := json.Unmarshal(raw, &num); err == nil {
			if n, err := num.Int64(); err == nil {
				return event.IntValue(n), nil
			}
			if f, err := num.Float64(); err == nil {
				return event.FloatValue(f), nil
			}
		}

		return event.StringValue(s), nil
	}
}
