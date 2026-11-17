package event

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ErrTypeMismatch is returned when a Value accessor is called with the wrong type.
var ErrTypeMismatch = errors.New("value type mismatch")

// FieldType represents the type of a field value.
type FieldType uint8

const (
	FieldTypeNull FieldType = iota
	FieldTypeString
	FieldTypeInt
	FieldTypeFloat
	FieldTypeBool
	FieldTypeTimestamp
	// The tag value is persisted in spill and segment encodings: constants
	// are never renumbered and new ones are appended.
	FieldTypeDuration
	FieldTypeArray
	FieldTypeObject
)

func (ft FieldType) String() string {
	switch ft {
	case FieldTypeNull:
		return "null"
	case FieldTypeString:
		return "string"
	case FieldTypeInt:
		return "int"
	case FieldTypeFloat:
		return "float"
	case FieldTypeBool:
		return "bool"
	case FieldTypeTimestamp:
		return "timestamp"
	case FieldTypeDuration:
		return "duration"
	case FieldTypeArray:
		return "array"
	case FieldTypeObject:
		return "object"
	default:
		return fmt.Sprintf("unknown(%d)", ft)
	}
}

// Value is a tagged union representing a field value.
//
// num holds ints, bools (0/1), timestamps (UnixNano), durations (nanoseconds),
// and float bits (math.Float64bits) - one shared 8-byte slot keyed by typ.
// Arrays and objects live behind the single ref pointer. The struct is 40
// bytes.
type Value struct {
	typ FieldType
	str string
	num int64
	ref *refPayload
}

// refPayload holds the heap-allocated part of array and object values.
// Exactly one of arr/obj is set, matching the Value's FieldType.
type refPayload struct {
	arr []Value
	obj map[string]Value
}

// NullValue returns a null Value.
func NullValue() Value {
	return Value{typ: FieldTypeNull}
}

// StringValue returns a string Value.
func StringValue(s string) Value {
	return Value{typ: FieldTypeString, str: s}
}

// IntValue returns an int Value.
func IntValue(n int64) Value {
	return Value{typ: FieldTypeInt, num: n}
}

// FloatValue returns a float Value.
func FloatValue(f float64) Value {
	return Value{typ: FieldTypeFloat, num: int64(math.Float64bits(f))}
}

// BoolValue returns a bool Value.
func BoolValue(b bool) Value {
	if b {
		return Value{typ: FieldTypeBool, num: 1}
	}

	return Value{typ: FieldTypeBool, num: 0}
}

// TimestampValue returns a timestamp Value (stored as UnixNano).
func TimestampValue(t time.Time) Value {
	return Value{typ: FieldTypeTimestamp, num: t.UnixNano()}
}

// DurationValue returns a duration Value (stored as nanoseconds).
func DurationValue(d time.Duration) Value {
	return Value{typ: FieldTypeDuration, num: int64(d)}
}

// ArrayValue returns an array Value. The slice is retained, not copied.
func ArrayValue(elems []Value) Value {
	return Value{typ: FieldTypeArray, ref: &refPayload{arr: elems}}
}

// ObjectValue returns an object Value. The map is retained, not copied.
func ObjectValue(fields map[string]Value) Value {
	return Value{typ: FieldTypeObject, ref: &refPayload{obj: fields}}
}

// Type returns the FieldType of this Value.
func (v Value) Type() FieldType { return v.typ }

// IsNull returns true if the value is null.
func (v Value) IsNull() bool { return v.typ == FieldTypeNull }

// AsString returns the string value or the zero value if not a string.
// This method no longer panics on type mismatch; it logs a warning and returns "".
// Prefer AsStringE or TryAsString for explicit error handling.
func (v Value) AsString() string {
	if v.typ != FieldTypeString {
		slog.Warn("AsString called on non-string value", "type", v.typ.String())

		return ""
	}

	return v.str
}

// AsStringE returns the string value or an error if not a string.
func (v Value) AsStringE() (string, error) {
	if v.typ != FieldTypeString {
		return "", fmt.Errorf("%w: got %s, want string", ErrTypeMismatch, v.typ)
	}

	return v.str, nil
}

// AsInt returns the int value or the zero value if not an int.
// This method no longer panics on type mismatch; it logs a warning and returns 0.
// Prefer AsIntE or TryAsInt for explicit error handling.
func (v Value) AsInt() int64 {
	if v.typ != FieldTypeInt {
		slog.Warn("AsInt called on non-int value", "type", v.typ.String())

		return 0
	}

	return v.num
}

// AsIntE returns the int value or an error if not an int.
func (v Value) AsIntE() (int64, error) {
	if v.typ != FieldTypeInt {
		return 0, fmt.Errorf("%w: got %s, want int", ErrTypeMismatch, v.typ)
	}

	return v.num, nil
}

// AsFloat returns the float value or the zero value if not a float.
// This method no longer panics on type mismatch; it logs a warning and returns 0.
// Prefer AsFloatE or TryAsFloat for explicit error handling.
func (v Value) AsFloat() float64 {
	if v.typ != FieldTypeFloat {
		slog.Warn("AsFloat called on non-float value", "type", v.typ.String())

		return 0
	}

	return math.Float64frombits(uint64(v.num))
}

// AsFloatE returns the float value or an error if not a float.
func (v Value) AsFloatE() (float64, error) {
	if v.typ != FieldTypeFloat {
		return 0, fmt.Errorf("%w: got %s, want float", ErrTypeMismatch, v.typ)
	}

	return math.Float64frombits(uint64(v.num)), nil
}

// AsBool returns the bool value or the zero value if not a bool.
// This method no longer panics on type mismatch; it logs a warning and returns false.
// Prefer AsBoolE or TryAsBool for explicit error handling.
func (v Value) AsBool() bool {
	if v.typ != FieldTypeBool {
		slog.Warn("AsBool called on non-bool value", "type", v.typ.String())

		return false
	}

	return v.num != 0
}

// AsBoolE returns the bool value or an error if not a bool.
func (v Value) AsBoolE() (bool, error) {
	if v.typ != FieldTypeBool {
		return false, fmt.Errorf("%w: got %s, want bool", ErrTypeMismatch, v.typ)
	}

	return v.num != 0, nil
}

// AsTimestamp returns the timestamp value or the zero value if not a timestamp.
// This method no longer panics on type mismatch; it logs a warning and returns time.Time{}.
// Prefer AsTimestampE or TryAsTimestamp for explicit error handling.
func (v Value) AsTimestamp() time.Time {
	if v.typ != FieldTypeTimestamp {
		slog.Warn("AsTimestamp called on non-timestamp value", "type", v.typ.String())

		return time.Time{}
	}

	return time.Unix(0, v.num)
}

// AsTimestampE returns the timestamp value or an error if not a timestamp.
func (v Value) AsTimestampE() (time.Time, error) {
	if v.typ != FieldTypeTimestamp {
		return time.Time{}, fmt.Errorf("%w: got %s, want timestamp", ErrTypeMismatch, v.typ)
	}

	return time.Unix(0, v.num), nil
}

// AsDuration returns the duration value or the zero value if not a duration.
// This method does not panic on type mismatch; it logs a warning and returns 0.
// Prefer AsDurationE or TryAsDuration for explicit error handling.
func (v Value) AsDuration() time.Duration {
	if v.typ != FieldTypeDuration {
		slog.Warn("AsDuration called on non-duration value", "type", v.typ.String())

		return 0
	}

	return time.Duration(v.num)
}

// AsDurationE returns the duration value or an error if not a duration.
func (v Value) AsDurationE() (time.Duration, error) {
	if v.typ != FieldTypeDuration {
		return 0, fmt.Errorf("%w: got %s, want duration", ErrTypeMismatch, v.typ)
	}

	return time.Duration(v.num), nil
}

// AsArray returns the array elements or nil if not an array.
// This method does not panic on type mismatch; it logs a warning and returns nil.
// Prefer AsArrayE or TryAsArray for explicit error handling.
func (v Value) AsArray() []Value {
	if v.typ != FieldTypeArray {
		slog.Warn("AsArray called on non-array value", "type", v.typ.String())

		return nil
	}

	return v.ref.arr
}

// AsArrayE returns the array elements or an error if not an array.
func (v Value) AsArrayE() ([]Value, error) {
	if v.typ != FieldTypeArray {
		return nil, fmt.Errorf("%w: got %s, want array", ErrTypeMismatch, v.typ)
	}

	return v.ref.arr, nil
}

// AsObject returns the object fields or nil if not an object.
// This method does not panic on type mismatch; it logs a warning and returns nil.
// Prefer AsObjectE or TryAsObject for explicit error handling.
func (v Value) AsObject() map[string]Value {
	if v.typ != FieldTypeObject {
		slog.Warn("AsObject called on non-object value", "type", v.typ.String())

		return nil
	}

	return v.ref.obj
}

// AsObjectE returns the object fields or an error if not an object.
func (v Value) AsObjectE() (map[string]Value, error) {
	if v.typ != FieldTypeObject {
		return nil, fmt.Errorf("%w: got %s, want object", ErrTypeMismatch, v.typ)
	}

	return v.ref.obj, nil
}

// TryAsString returns the string value and true, or zero value and false if not a string.
func (v Value) TryAsString() (string, bool) {
	if v.typ != FieldTypeString {
		return "", false
	}

	return v.str, true
}

// TryAsInt returns the int value and true, or zero value and false if not an int.
func (v Value) TryAsInt() (int64, bool) {
	if v.typ != FieldTypeInt {
		return 0, false
	}

	return v.num, true
}

// TryAsFloat returns the float value and true, or zero value and false if not a float.
func (v Value) TryAsFloat() (float64, bool) {
	if v.typ != FieldTypeFloat {
		return 0, false
	}

	return math.Float64frombits(uint64(v.num)), true
}

// TryAsBool returns the bool value and true, or zero value and false if not a bool.
func (v Value) TryAsBool() (bool, bool) {
	if v.typ != FieldTypeBool {
		return false, false
	}

	return v.num != 0, true
}

// TryAsTimestamp returns the timestamp value and true, or zero value and false if not a timestamp.
func (v Value) TryAsTimestamp() (time.Time, bool) {
	if v.typ != FieldTypeTimestamp {
		return time.Time{}, false
	}

	return time.Unix(0, v.num), true
}

// TryAsDuration returns the duration value and true, or zero value and false if not a duration.
func (v Value) TryAsDuration() (time.Duration, bool) {
	if v.typ != FieldTypeDuration {
		return 0, false
	}

	return time.Duration(v.num), true
}

// TryAsArray returns the array elements and true, or nil and false if not an array.
func (v Value) TryAsArray() ([]Value, bool) {
	if v.typ != FieldTypeArray {
		return nil, false
	}

	return v.ref.arr, true
}

// TryAsObject returns the object fields and true, or nil and false if not an object.
func (v Value) TryAsObject() (map[string]Value, bool) {
	if v.typ != FieldTypeObject {
		return nil, false
	}

	return v.ref.obj, true
}

// Interface returns the value as a plain Go interface{}.
func (v Value) Interface() interface{} {
	switch v.typ {
	case FieldTypeString:
		return v.str
	case FieldTypeInt:
		return v.num
	case FieldTypeFloat:
		return math.Float64frombits(uint64(v.num))
	case FieldTypeBool:
		return v.num != 0
	case FieldTypeTimestamp:
		return time.Unix(0, v.num).UTC()
	case FieldTypeDuration:
		return time.Duration(v.num)
	case FieldTypeArray:
		out := make([]interface{}, len(v.ref.arr))
		for i, e := range v.ref.arr {
			out[i] = e.Interface()
		}

		return out
	case FieldTypeObject:
		out := make(map[string]interface{}, len(v.ref.obj))
		for k, e := range v.ref.obj {
			out[k] = e.Interface()
		}

		return out
	default:
		return nil
	}
}

// ValueFromInterface converts a Go interface{} value back to a typed Value.
func ValueFromInterface(v interface{}) Value {
	switch val := v.(type) {
	case string:
		return StringValue(val)
	case int64:
		return IntValue(val)
	case int:
		return IntValue(int64(val))
	case float64:
		return FloatValue(val)
	case bool:
		return BoolValue(val)
	case time.Time:
		return TimestampValue(val)
	case time.Duration:
		return DurationValue(val)
	case []Value:
		return ArrayValue(val)
	case map[string]Value:
		return ObjectValue(val)
	case []interface{}:
		elems := make([]Value, len(val))
		for i, e := range val {
			elems[i] = ValueFromInterface(e)
		}

		return ArrayValue(elems)
	case map[string]interface{}:
		fields := make(map[string]Value, len(val))
		for k, e := range val {
			fields[k] = ValueFromInterface(e)
		}

		return ObjectValue(fields)
	case Value:
		return val
	case nil:
		return NullValue()
	default:
		return StringValue(fmt.Sprint(v))
	}
}

// String returns a human-readable representation of the value.
func (v Value) String() string {
	switch v.typ {
	case FieldTypeNull:
		return "<null>"
	case FieldTypeString:
		return v.str
	case FieldTypeInt:
		return strconv.FormatInt(v.num, 10)
	case FieldTypeFloat:
		return strconv.FormatFloat(math.Float64frombits(uint64(v.num)), 'g', -1, 64)
	case FieldTypeBool:
		if v.num != 0 {
			return "true"
		}

		return "false"
	case FieldTypeTimestamp:
		return time.Unix(0, v.num).UTC().Format(time.RFC3339Nano)
	case FieldTypeDuration:
		return time.Duration(v.num).String()
	case FieldTypeArray, FieldTypeObject:
		var sb strings.Builder
		v.appendJSONLike(&sb)

		return sb.String()
	default:
		return "<unknown>"
	}
}

// MemSize returns an approximation of the in-memory byte cost of this Value.
// Scalar types (null, int, float, bool, timestamp, duration) return a fixed
// size. Strings add the string payload length. Arrays and objects recurse to
// include the refPayload overhead and all nested elements.
//
// The scalar path is O(1) and allocation-free. Used by memory accounting to
// charge operators for the full deep size of array/object payloads.
func (v Value) MemSize() int {
	const (
		// valueBase is sizeof(Value): typ(1) + padding(7) + str header(16) + num(8) + ref pointer(8) = 40.
		valueBase = 40
		// refPayloadBase is sizeof(refPayload): slice header(24) + map header(8) = 32.
		refPayloadBase = 32
		// mapEntryOverhead is the approximate per-entry overhead of a Go map: bucket + key header.
		mapEntryOverhead = 56
	)

	switch v.typ {
	case FieldTypeString:
		return valueBase + len(v.str)
	case FieldTypeArray:
		if v.ref == nil {
			return valueBase
		}
		size := valueBase + refPayloadBase + len(v.ref.arr)*valueBase
		for _, e := range v.ref.arr {
			// Only add extra cost for variable-size elements (avoid double-counting valueBase).
			extra := e.MemSize() - valueBase
			if extra > 0 {
				size += extra
			}
		}

		return size
	case FieldTypeObject:
		if v.ref == nil {
			return valueBase
		}
		size := valueBase + refPayloadBase
		for k, e := range v.ref.obj {
			size += mapEntryOverhead + len(k)
			size += e.MemSize()
		}

		return size
	default:
		return valueBase
	}
}

// appendJSONLike renders the value in compact JSON-like form. Object keys are
// sorted so the rendering is deterministic: group and dedup keys render
// through String, and equal objects must produce equal strings.
func (v Value) appendJSONLike(sb *strings.Builder) {
	switch v.typ {
	case FieldTypeNull:
		sb.WriteString("null")
	case FieldTypeString:
		sb.WriteString(strconv.Quote(v.str))
	case FieldTypeTimestamp, FieldTypeDuration:
		sb.WriteString(strconv.Quote(v.String()))
	case FieldTypeArray:
		sb.WriteByte('[')
		for i, e := range v.ref.arr {
			if i > 0 {
				sb.WriteByte(',')
			}
			e.appendJSONLike(sb)
		}
		sb.WriteByte(']')
	case FieldTypeObject:
		keys := make([]string, 0, len(v.ref.obj))
		for k := range v.ref.obj {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		sb.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(strconv.Quote(k))
			sb.WriteByte(':')
			e := v.ref.obj[k]
			e.appendJSONLike(sb)
		}
		sb.WriteByte('}')
	default:
		sb.WriteString(v.String())
	}
}
