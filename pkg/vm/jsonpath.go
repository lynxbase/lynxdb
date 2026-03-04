package vm

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// jsonExtractByPath walks a JSON string using a dot-separated path and returns
// the value at that path. Uses encoding/json.Decoder for streaming traversal
// without full unmarshal.
//
// Examples:
//
//	jsonExtractByPath(`{"user":{"id":42}}`, "user.id")  → IntValue(42)
//	jsonExtractByPath(`{"a":{"b":"c"}}`, "a.b")         → StringValue("c")
//	jsonExtractByPath(`{"a":[1,2]}`, "a")               → StringValue("[1,2]")
//	jsonExtractByPath(`{"x":1}`, "y")                   → NullValue()
//
// JsonExtractByPath extracts a value from a JSON string using a dot-separated
// path with optional bracket notation for array indexing.
func JsonExtractByPath(input string, path string) event.Value {
	return jsonExtractByPath(input, path)
}

func jsonExtractByPath(input string, path string) event.Value {
	s := strings.TrimSpace(input)
	if len(s) == 0 || s[0] != '{' {
		return event.NullValue()
	}

	parts := strings.Split(path, ".")
	if len(parts) == 0 {
		return event.NullValue()
	}

	return walkJSONPath([]byte(s), parts)
}

// arrayAccess holds the parsed result of a path segment that may contain
// bracket notation for array indexing: "items[0]", "items[-1]", "items[*]".
type arrayAccess struct {
	key      string // part before '[', e.g. "items"
	index    int    // numeric index (0-based, can be negative)
	wildcard bool   // true if [*]
	hasIndex bool   // true if segment contains [...]
}

// parseArrayAccess parses a path segment for optional array index notation.
// Examples:
//
//	"items"    → {key:"items", hasIndex:false}
//	"items[0]" → {key:"items", index:0, hasIndex:true}
//	"items[-1]" → {key:"items", index:-1, hasIndex:true}
//	"items[*]" → {key:"items", wildcard:true, hasIndex:true}
func parseArrayAccess(segment string) arrayAccess {
	bracketIdx := strings.IndexByte(segment, '[')
	if bracketIdx < 0 {
		return arrayAccess{key: segment}
	}

	key := segment[:bracketIdx]
	inner := segment[bracketIdx+1:]
	// Strip trailing ']'
	if len(inner) > 0 && inner[len(inner)-1] == ']' {
		inner = inner[:len(inner)-1]
	}

	if inner == "*" {
		return arrayAccess{key: key, wildcard: true, hasIndex: true}
	}

	idx, err := strconv.Atoi(inner)
	if err != nil {
		// Malformed index — treat as plain key lookup.
		return arrayAccess{key: segment}
	}

	return arrayAccess{key: key, index: idx, hasIndex: true}
}

// resolveArrayIndex applies a (possibly negative) index to a JSON array.
// Returns the raw JSON element at the resolved position, or nil if out of bounds.
func resolveArrayIndex(data []byte, index int) []byte {
	var arr []json.RawMessage
	if err := json.Unmarshal(data, &arr); err != nil {
		return nil
	}

	if index < 0 {
		index = len(arr) + index
	}
	if index < 0 || index >= len(arr) {
		return nil
	}

	return []byte(arr[index])
}

// walkJSONPathRaw navigates through nested JSON objects following the path segments
// and returns the raw JSON bytes at the target location. Returns nil if the path
// doesn't exist or if any intermediate segment is not an object.
//
// Supports array indexing via bracket notation: "items[0]", "items[-1]", "items[*]".
// Wildcard [*] fans out to all array elements and collects results as a JSON array.
func walkJSONPathRaw(data []byte, path []string) []byte {
	current := data

	for i, segment := range path {
		aa := parseArrayAccess(segment)

		// Decode current level as a map of raw messages.
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(current, &obj); err != nil {
			return nil
		}

		raw, ok := obj[aa.key]
		if !ok {
			return nil
		}

		if !aa.hasIndex {
			// Plain object key lookup — no array indexing.
			current = []byte(raw)
			continue
		}

		// Wildcard: fan out to all array elements, recursively apply remaining path.
		if aa.wildcard {
			var arr []json.RawMessage
			if err := json.Unmarshal(raw, &arr); err != nil {
				return nil
			}

			remaining := path[i+1:]
			var results []json.RawMessage
			for _, elem := range arr {
				if len(remaining) == 0 {
					results = append(results, elem)
				} else {
					sub := walkJSONPathRaw([]byte(elem), remaining)
					if sub != nil {
						results = append(results, json.RawMessage(sub))
					}
				}
			}

			out, err := json.Marshal(results)
			if err != nil {
				return nil
			}
			return out
		}

		// Positional index: [N] or [-N].
		elem := resolveArrayIndex(raw, aa.index)
		if elem == nil {
			return nil
		}
		current = elem
	}

	return current
}

// walkJSONPath navigates through nested JSON objects following the path segments.
func walkJSONPath(data []byte, path []string) event.Value {
	raw := walkJSONPathRaw(data, path)
	if raw == nil {
		return event.NullValue()
	}

	return jsonFragmentToValue(raw)
}

// jsonFragmentToValue converts a raw JSON fragment (already extracted by path
// navigation) into a typed event.Value. Preserves native JSON types:
// null → NullValue, bool → BoolValue, integer → IntValue, float → FloatValue,
// string → StringValue, object/array → StringValue (JSON representation).
func jsonFragmentToValue(raw []byte) event.Value {
	s := strings.TrimSpace(string(raw))
	if len(s) == 0 {
		return event.NullValue()
	}

	switch s[0] {
	case 'n':
		if s == "null" {
			return event.NullValue()
		}

		return event.StringValue(s)

	case 't':
		if s == "true" {
			return event.BoolValue(true)
		}

		return event.StringValue(s)

	case 'f':
		if s == "false" {
			return event.BoolValue(false)
		}

		return event.StringValue(s)

	case '"':
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return event.StringValue(s)
		}

		return event.StringValue(str)

	case '{', '[':
		// Objects and arrays are returned as their JSON string representation.
		return event.StringValue(s)

	default:
		// Number: try integer first, then float.
		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()

		var num json.Number
		if err := dec.Decode(&num); err == nil {
			if n, err := num.Int64(); err == nil {
				return event.IntValue(n)
			}
			if f, err := num.Float64(); err == nil {
				return event.FloatValue(f)
			}
		}

		return event.StringValue(s)
	}
}

// jsonKeys navigates to the given path (or root if empty) in the input JSON,
// and returns a JSON array of the object's keys. Returns null if the target
// is not a JSON object.
func jsonKeys(input, path string) event.Value {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return event.NullValue()
	}

	var data []byte
	if path == "" {
		data = []byte(s)
	} else {
		if s[0] != '{' {
			return event.NullValue()
		}
		parts := strings.Split(path, ".")
		val := walkJSONPath([]byte(s), parts)
		if val.IsNull() {
			return event.NullValue()
		}
		data = []byte(val.String())
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(data, &obj); err != nil {
		return event.NullValue()
	}

	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}

	result, err := json.Marshal(keys)
	if err != nil {
		return event.NullValue()
	}

	return event.StringValue(string(result))
}

// jsonArrayLength navigates to the given path (or root if empty) in the input
// JSON and returns the length of the JSON array at that location. Returns null
// if the target is not a JSON array.
func jsonArrayLength(input, path string) event.Value {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return event.NullValue()
	}

	var data []byte
	if path == "" {
		data = []byte(s)
	} else {
		if s[0] != '{' {
			return event.NullValue()
		}
		parts := strings.Split(path, ".")
		val := walkJSONPath([]byte(s), parts)
		if val.IsNull() {
			return event.NullValue()
		}
		data = []byte(val.String())
	}

	var arr []json.RawMessage
	if err := json.Unmarshal(data, &arr); err != nil {
		return event.NullValue()
	}

	return event.IntValue(int64(len(arr)))
}

// jsonIsValid checks if the input string is valid JSON.
func jsonIsValid(input string) bool {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return false
	}

	return json.Valid([]byte(s))
}

// jsonType navigates to the given path (or root if empty) in the input JSON
// and returns the JSON type as a string: "string", "number", "boolean",
// "array", "object", or "null". Returns null for non-JSON input or missing path.
func jsonType(input, path string) event.Value {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return event.NullValue()
	}

	// Get the raw JSON fragment — use walkJSONPathRaw to preserve the raw bytes
	// (including quotes around strings and literal "null") so we can inspect the type.
	var raw []byte
	if path == "" {
		raw = []byte(s)
	} else {
		if s[0] != '{' {
			return event.NullValue()
		}
		parts := strings.Split(path, ".")
		raw = walkJSONPathRaw([]byte(s), parts)
		if raw == nil {
			return event.NullValue()
		}
	}

	trimmed := strings.TrimSpace(string(raw))
	if len(trimmed) == 0 {
		return event.NullValue()
	}

	switch trimmed[0] {
	case '{':
		return event.StringValue("object")
	case '[':
		return event.StringValue("array")
	case '"':
		return event.StringValue("string")
	case 't', 'f':
		if trimmed == "true" || trimmed == "false" {
			return event.StringValue("boolean")
		}

		return event.NullValue()
	case 'n':
		if trimmed == "null" {
			return event.StringValue("null")
		}

		return event.NullValue()
	default:
		if (trimmed[0] >= '0' && trimmed[0] <= '9') || trimmed[0] == '-' {
			return event.StringValue("number")
		}

		return event.NullValue()
	}
}

// jsonSet sets or adds a value at the given dot-path in a JSON string.
// Creates intermediate objects as needed. Returns null if input is not valid JSON.
func jsonSet(input, path, value string) event.Value {
	s := strings.TrimSpace(input)
	if len(s) == 0 || s[0] != '{' {
		return event.NullValue()
	}

	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(s), &obj); err != nil {
		return event.NullValue()
	}

	parts := strings.Split(path, ".")
	if len(parts) == 0 {
		return event.NullValue()
	}

	var parsed interface{}
	if err := json.Unmarshal([]byte(value), &parsed); err != nil {
		// If unmarshal fails, treat as a bare string.
		parsed = value
	}

	current := obj
	for i := 0; i < len(parts)-1; i++ {
		next, ok := current[parts[i]]
		if !ok {
			// Create intermediate object.
			newMap := make(map[string]interface{})
			current[parts[i]] = newMap
			current = newMap
		} else if nextMap, isMap := next.(map[string]interface{}); isMap {
			current = nextMap
		} else {
			// Path segment exists but is not an object — overwrite with new object.
			newMap := make(map[string]interface{})
			current[parts[i]] = newMap
			current = newMap
		}
	}

	current[parts[len(parts)-1]] = parsed

	result, err := json.Marshal(obj)
	if err != nil {
		return event.NullValue()
	}

	return event.StringValue(string(result))
}

// jsonRemove removes the key at the given dot-path from a JSON string.
// Returns the original JSON if path doesn't exist. Returns null if input
// is not valid JSON.
func jsonRemove(input, path string) event.Value {
	s := strings.TrimSpace(input)
	if len(s) == 0 || s[0] != '{' {
		return event.NullValue()
	}

	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(s), &obj); err != nil {
		return event.NullValue()
	}

	parts := strings.Split(path, ".")
	if len(parts) == 0 {
		return event.NullValue()
	}

	// Walk to the parent of the leaf key.
	current := obj
	for i := 0; i < len(parts)-1; i++ {
		next, ok := current[parts[i]]
		if !ok {
			// Path doesn't exist — return original.
			return event.StringValue(s)
		}
		nextMap, isMap := next.(map[string]interface{})
		if !isMap {
			// Path segment is not an object — return original.
			return event.StringValue(s)
		}
		current = nextMap
	}

	leaf := parts[len(parts)-1]
	if _, exists := current[leaf]; !exists {
		// Key doesn't exist — return original.
		return event.StringValue(s)
	}
	delete(current, leaf)

	result, err := json.Marshal(obj)
	if err != nil {
		return event.NullValue()
	}

	return event.StringValue(string(result))
}

// jsonMerge shallow-merges two JSON objects. Keys from b overwrite a on conflict.
// Returns null if either input is not a JSON object.
func jsonMerge(a, b string) event.Value {
	sa := strings.TrimSpace(a)
	sb := strings.TrimSpace(b)
	if len(sa) == 0 || sa[0] != '{' || len(sb) == 0 || sb[0] != '{' {
		return event.NullValue()
	}

	var objA, objB map[string]interface{}
	if err := json.Unmarshal([]byte(sa), &objA); err != nil {
		return event.NullValue()
	}
	if err := json.Unmarshal([]byte(sb), &objB); err != nil {
		return event.NullValue()
	}

	for k, v := range objB {
		objA[k] = v
	}

	result, err := json.Marshal(objA)
	if err != nil {
		return event.NullValue()
	}

	return event.StringValue(string(result))
}
