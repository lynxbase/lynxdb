package event

import (
	"errors"
	"fmt"
	"strconv"
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
	default:
		return fmt.Sprintf("unknown(%d)", ft)
	}
}

// Value is a tagged union representing a field value.
type Value struct {
	typ FieldType
	str string
	num int64
	flt float64
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
	return Value{typ: FieldTypeFloat, flt: f}
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

// Type returns the FieldType of this Value.
func (v Value) Type() FieldType { return v.typ }

// IsNull returns true if the value is null.
func (v Value) IsNull() bool { return v.typ == FieldTypeNull }

// AsString returns the string value or panics if not a string.
//
// Deprecated: Use AsStringE or TryAsString in new code. This method panics on
// type mismatch and should only be called when the type is statically guaranteed.
func (v Value) AsString() string {
	s, err := v.AsStringE()
	if err != nil {
		panic(err.Error())
	}

	return s
}

// AsStringE returns the string value or an error if not a string.
func (v Value) AsStringE() (string, error) {
	if v.typ != FieldTypeString {
		return "", fmt.Errorf("%w: got %s, want string", ErrTypeMismatch, v.typ)
	}

	return v.str, nil
}

// AsInt returns the int value or panics if not an int.
//
// Deprecated: Use AsIntE or TryAsInt in new code. This method panics on
// type mismatch and should only be called when the type is statically guaranteed.
func (v Value) AsInt() int64 {
	n, err := v.AsIntE()
	if err != nil {
		panic(err.Error())
	}

	return n
}

// AsIntE returns the int value or an error if not an int.
func (v Value) AsIntE() (int64, error) {
	if v.typ != FieldTypeInt {
		return 0, fmt.Errorf("%w: got %s, want int", ErrTypeMismatch, v.typ)
	}

	return v.num, nil
}

// AsFloat returns the float value or panics if not a float.
//
// Deprecated: Use AsFloatE or TryAsFloat in new code. This method panics on
// type mismatch and should only be called when the type is statically guaranteed.
func (v Value) AsFloat() float64 {
	f, err := v.AsFloatE()
	if err != nil {
		panic(err.Error())
	}

	return f
}

// AsFloatE returns the float value or an error if not a float.
func (v Value) AsFloatE() (float64, error) {
	if v.typ != FieldTypeFloat {
		return 0, fmt.Errorf("%w: got %s, want float", ErrTypeMismatch, v.typ)
	}

	return v.flt, nil
}

// AsBool returns the bool value or panics if not a bool.
//
// Deprecated: Use AsBoolE or TryAsBool in new code. This method panics on
// type mismatch and should only be called when the type is statically guaranteed.
func (v Value) AsBool() bool {
	b, err := v.AsBoolE()
	if err != nil {
		panic(err.Error())
	}

	return b
}

// AsBoolE returns the bool value or an error if not a bool.
func (v Value) AsBoolE() (bool, error) {
	if v.typ != FieldTypeBool {
		return false, fmt.Errorf("%w: got %s, want bool", ErrTypeMismatch, v.typ)
	}

	return v.num != 0, nil
}

// AsTimestamp returns the timestamp value or panics if not a timestamp.
//
// Deprecated: Use AsTimestampE or TryAsTimestamp in new code. This method panics on
// type mismatch and should only be called when the type is statically guaranteed.
func (v Value) AsTimestamp() time.Time {
	t, err := v.AsTimestampE()
	if err != nil {
		panic(err.Error())
	}

	return t
}

// AsTimestampE returns the timestamp value or an error if not a timestamp.
func (v Value) AsTimestampE() (time.Time, error) {
	if v.typ != FieldTypeTimestamp {
		return time.Time{}, fmt.Errorf("%w: got %s, want timestamp", ErrTypeMismatch, v.typ)
	}

	return time.Unix(0, v.num), nil
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

	return v.flt, true
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

// Interface returns the value as a plain Go interface{}.
func (v Value) Interface() interface{} {
	switch v.typ {
	case FieldTypeString:
		return v.str
	case FieldTypeInt:
		return v.num
	case FieldTypeFloat:
		return v.flt
	case FieldTypeBool:
		return v.num != 0
	case FieldTypeTimestamp:
		return time.Unix(0, v.num)
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
		return strconv.FormatFloat(v.flt, 'g', -1, 64)
	case FieldTypeBool:
		if v.num != 0 {
			return "true"
		}

		return "false"
	case FieldTypeTimestamp:
		return time.Unix(0, v.num).UTC().Format(time.RFC3339Nano)
	default:
		return "<unknown>"
	}
}
