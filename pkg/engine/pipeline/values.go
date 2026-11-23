package pipeline

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
	"unsafe"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// ExecValue returns a value of a row as one of the columnar executor's: a
// time or a duration as its nanoseconds, an array or an object as a
// variant whose bytes it appends to buf. The bytes of a string are those of
// the row's value.
func ExecValue(v event.Value, buf []byte) (exec.Value, []byte) {
	switch v.Type() {
	case event.FieldTypeBool:
		return exec.Value{Kind: vec.KindBool, Bool: v.AsBool()}, buf
	case event.FieldTypeInt:
		return exec.Value{Kind: vec.KindInt, Int: v.AsInt()}, buf
	case event.FieldTypeFloat:
		return exec.Value{Kind: vec.KindFloat, Float: v.AsFloat()}, buf
	case event.FieldTypeString:
		s := v.AsString()

		return exec.Value{Kind: vec.KindString, Bytes: unsafe.Slice(unsafe.StringData(s), len(s))}, buf
	case event.FieldTypeTimestamp:
		return exec.Value{Kind: vec.KindInt, Int: v.AsTimestamp().UnixNano()}, buf
	case event.FieldTypeDuration:
		return exec.Value{Kind: vec.KindInt, Int: int64(v.AsDuration())}, buf
	case event.FieldTypeArray, event.FieldTypeObject:
		start := len(buf)
		buf = appendVariant(buf, v)

		return exec.Value{Kind: vec.KindVariant, Bytes: buf[start:]}, buf
	}

	return exec.Value{Kind: vec.KindNull}, buf
}

// appendVariant appends a value of a row in the variant encoding.
func appendVariant(dst []byte, v event.Value) []byte {
	switch v.Type() {
	case event.FieldTypeBool:
		return variant.AppendBool(dst, v.AsBool())
	case event.FieldTypeInt:
		return variant.AppendInt(dst, v.AsInt())
	case event.FieldTypeFloat:
		return variant.AppendFloat(dst, v.AsFloat())
	case event.FieldTypeString:
		return variant.AppendString(dst, []byte(v.AsString()))
	case event.FieldTypeTimestamp:
		return variant.AppendInt(dst, v.AsTimestamp().UnixNano())
	case event.FieldTypeDuration:
		return variant.AppendInt(dst, int64(v.AsDuration()))
	case event.FieldTypeArray:
		dst, mark := variant.BeginArray(dst)
		for _, e := range v.AsArray() {
			dst = appendVariant(dst, e)
		}

		return variant.End(dst, mark)
	case event.FieldTypeObject:
		dst, mark := variant.BeginObject(dst)
		for k, e := range v.AsObject() {
			dst = appendVariant(variant.AppendKey(dst, []byte(k)), e)
		}

		return variant.End(dst, mark)
	}

	return variant.AppendNull(dst)
}

// RowValue returns a value of the columnar executor as one of a row, with
// bytes of its own: an integer of a time or a duration as that, when kind,
// the IR kind of the value, says it is one, and a variant as the array or
// the object it holds.
func RowValue(v exec.Value, kind ir.Kind) event.Value {
	switch v.Kind {
	case vec.KindBool:
		return event.BoolValue(v.Bool)
	case vec.KindInt:
		switch kind {
		case ir.KindTimestamp:
			return event.TimestampValue(time.Unix(0, v.Int).UTC())
		case ir.KindDuration:
			return event.DurationValue(time.Duration(v.Int))
		}

		return event.IntValue(v.Int)
	case vec.KindFloat:
		return event.FloatValue(v.Float)
	case vec.KindString:
		return event.StringValue(string(v.Bytes))
	case vec.KindVariant:
		text, err := variant.AppendJSON(nil, v.Bytes)
		if err != nil {
			return event.NullValue()
		}
		dec := json.NewDecoder(bytes.NewReader(text))
		dec.UseNumber()
		var decoded any
		if dec.Decode(&decoded) != nil {
			return event.NullValue()
		}

		return decodedValue(decoded)
	}

	return event.NullValue()
}

// decodedValue turns a decoded JSON value into a value of a row.
func decodedValue(v any) event.Value {
	switch v := v.(type) {
	case bool:
		return event.BoolValue(v)
	case string:
		return event.StringValue(v)
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return event.IntValue(n)
		}
		if f, err := v.Float64(); err == nil {
			return event.FloatValue(f)
		}

		return event.StringValue(v.String())
	case []any:
		elems := make([]event.Value, len(v))
		for i, e := range v {
			elems[i] = decodedValue(e)
		}

		return event.ArrayValue(elems)
	case map[string]any:
		fields := make(map[string]event.Value, len(v))
		for k, e := range v {
			fields[k] = decodedValue(e)
		}

		return event.ObjectValue(fields)
	}

	return event.NullValue()
}

// fields gives an expression the fields of a row. A dotted name that the
// row has no field of is the member of an object that a field holds.
type fields struct {
	row map[string]event.Value
	buf []byte
}

func (f *fields) reset(row map[string]event.Value) { f.row, f.buf = row, f.buf[:0] }

func (f *fields) Field(path string) exec.Value {
	v, ok := f.row[path]
	if !ok {
		if v, ok = member(f.row, path); !ok {
			return exec.Value{}
		}
	}
	var x exec.Value
	x, f.buf = ExecValue(v, f.buf)

	return x
}

func (f *fields) Values(fn func(exec.Value) bool) {
	for _, v := range f.row {
		// Times are not searched as numbers.
		if t := v.Type(); t == event.FieldTypeTimestamp || t == event.FieldTypeDuration {
			continue
		}
		var x exec.Value
		x, f.buf = ExecValue(v, f.buf)
		if !fn(x) {
			return
		}
	}
}

// member returns the member at path of an object that a field of the row
// holds, the field whose name is the longest start of path.
func member(row map[string]event.Value, path string) (event.Value, bool) {
	for at := strings.LastIndexByte(path, '.'); at > 0; at = strings.LastIndexByte(path[:at], '.') {
		v, ok := row[path[:at]]
		if !ok {
			continue
		}
		for _, name := range strings.Split(path[at+1:], ".") {
			if v.Type() != event.FieldTypeObject {
				return event.Value{}, false
			}
			if v, ok = v.AsObject()[name]; !ok {
				return event.Value{}, false
			}
		}

		return v, true
	}

	return event.Value{}, false
}
