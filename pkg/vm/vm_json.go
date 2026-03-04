package vm

import (
	"encoding/json"
	"strconv"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// jsonExtractValue implements the OpJsonExtract opcode.
// Pops two values from the stack: path (top), field (below).
// If field is null or not a string, pushes null.
// If path is null or not a string, pushes null.
// Otherwise, extracts the value at the given dot-path from the JSON field.
func jsonExtractValue(field, path event.Value) event.Value {
	if field.IsNull() || path.IsNull() {
		return event.NullValue()
	}

	return jsonExtractByPath(field.String(), path.String())
}

// jsonValidValue implements the OpJsonValid opcode.
// Pops one value from the stack and pushes a bool indicating whether it's valid JSON.
// If the value is null, pushes false.
func jsonValidValue(field event.Value) event.Value {
	if field.IsNull() {
		return event.BoolValue(false)
	}

	return event.BoolValue(jsonIsValid(field.String()))
}

// jsonKeysValue implements the OpJsonKeys opcode.
// Pops two values from the stack: path (top), field (below).
// Returns a JSON array of the object's keys at the given path.
func jsonKeysValue(field, path event.Value) event.Value {
	if field.IsNull() {
		return event.NullValue()
	}

	p := ""
	if !path.IsNull() {
		p = path.String()
	}

	return jsonKeys(field.String(), p)
}

// jsonArrayLenValue implements the OpJsonArrayLen opcode.
// Pops two values from the stack: path (top), field (below).
// Returns the length of the JSON array at the given path.
func jsonArrayLenValue(field, path event.Value) event.Value {
	if field.IsNull() {
		return event.NullValue()
	}

	p := ""
	if !path.IsNull() {
		p = path.String()
	}

	return jsonArrayLength(field.String(), p)
}

// jsonObjectValue implements the OpJsonObject opcode.
// Takes N values from the stack (pairs of key, value) and builds a JSON object.
// N must be even — odd N drops the last unpaired value.
func jsonObjectValue(values []event.Value) event.Value {
	m := make(map[string]interface{}, len(values)/2)
	for i := 0; i+1 < len(values); i += 2 {
		key := values[i].String()
		m[key] = eventValueToInterface(values[i+1])
	}

	data, err := json.Marshal(m)
	if err != nil {
		return event.NullValue()
	}

	return event.StringValue(string(data))
}

// jsonArrayValue implements the OpJsonArray opcode.
// Takes N values from the stack and builds a JSON array.
func jsonArrayValue(values []event.Value) event.Value {
	arr := make([]interface{}, len(values))
	for i, v := range values {
		arr[i] = eventValueToInterface(v)
	}

	data, err := json.Marshal(arr)
	if err != nil {
		return event.NullValue()
	}

	return event.StringValue(string(data))
}

// jsonTypeValue implements the OpJsonType opcode.
// Pops two values from the stack: path (top), field (below).
// Returns the JSON type at the given path as a string.
func jsonTypeValue(field, path event.Value) event.Value {
	if field.IsNull() {
		return event.NullValue()
	}

	p := ""
	if !path.IsNull() {
		p = path.String()
	}

	return jsonType(field.String(), p)
}

// jsonSetValue implements the OpJsonSet opcode.
// Pops three values: value (top), path, field (bottom).
// Sets the value at the given dot-path in the JSON field.
func jsonSetValue(field, path, value event.Value) event.Value {
	if field.IsNull() || path.IsNull() {
		return event.NullValue()
	}

	// Convert the event.Value to its JSON representation for embedding.
	valIface := eventValueToInterface(value)
	valJSON, err := json.Marshal(valIface)
	if err != nil {
		return event.NullValue()
	}

	return jsonSet(field.String(), path.String(), string(valJSON))
}

// jsonRemoveValue implements the OpJsonRemove opcode.
// Pops two values from the stack: path (top), field (below).
// Removes the key at the given dot-path from the JSON field.
func jsonRemoveValue(field, path event.Value) event.Value {
	if field.IsNull() || path.IsNull() {
		return event.NullValue()
	}

	return jsonRemove(field.String(), path.String())
}

// jsonMergeValue implements the OpJsonMerge opcode.
// Pops two values from the stack: json2 (top), json1 (below).
// Shallow-merges two JSON objects; json2 keys overwrite json1 on conflict.
func jsonMergeValue(a, b event.Value) event.Value {
	if a.IsNull() || b.IsNull() {
		return event.NullValue()
	}

	return jsonMerge(a.String(), b.String())
}

// eventValueToInterface converts an event.Value to a Go interface{} suitable
// for JSON marshaling. Preserves native types: int→int64, float→float64,
// bool→bool, null→nil, string→string (with numeric detection for string-typed
// numbers that should serialize without quotes).
func eventValueToInterface(v event.Value) interface{} {
	switch v.Type() {
	case event.FieldTypeNull:
		return nil
	case event.FieldTypeBool:
		return v.AsBool()
	case event.FieldTypeInt:
		return v.AsInt()
	case event.FieldTypeFloat:
		return v.AsFloat()
	case event.FieldTypeString:
		s := v.AsString()
		// Try to preserve numeric types for string-encoded numbers
		// so json_object("age", "30") produces {"age":30} not {"age":"30"}.
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}

		return s
	default:
		return v.String()
	}
}
