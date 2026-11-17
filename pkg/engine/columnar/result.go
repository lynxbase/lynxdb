package columnar

import (
	"sort"
	"strconv"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// Len returns the number of rows.
func (r *Result) Len() int {
	if r.table != nil {
		return r.table.Rows
	}

	return len(r.rows)
}

// Maps returns the rows as maps of their fields; a row has no entry for a
// field it does not hold.
func (r *Result) Maps() []map[string]event.Value {
	if r.rows == nil && r.table != nil {
		r.rows = rowsOf(r.table, r.schema)
	}
	if r.rows == nil {
		r.rows = []map[string]event.Value{}
	}

	return r.rows
}

// AppendJSON appends row i as a JSON object: the columns the plan fixes in
// their order, any others in the order of their names, a time as text in
// the layout of RFC 3339, and no member for a field the row does not hold.
func (r *Result) AppendJSON(dst []byte, i int) []byte {
	dst = append(dst, '{')
	first := len(dst)
	if r.table != nil {
		for c := range r.table.Columns {
			name := r.table.Columns[c].Name
			col, listed := r.schema.Lookup(name)
			v := r.table.Value(i, c)
			if v.Kind == exec.KindAny && !listed {
				continue
			}
			if len(dst) > first {
				dst = append(dst, ',')
			}
			dst = append(variant.AppendJSONString(dst, []byte(name)), ':')
			switch at := name == ir.TimeColumn || (listed && col.Type.Kind == ir.KindTimestamp); {
			case v.IsNull():
				dst = append(dst, "null"...)
			case at && v.Kind == vec.KindInt:
				dst = appendTime(dst, v.Int)
			case v.Kind == vec.KindBool:
				dst = strconv.AppendBool(dst, v.Bool)
			case v.Kind == vec.KindInt:
				dst = strconv.AppendInt(dst, v.Int, 10)
			case v.Kind == vec.KindFloat:
				dst = variant.AppendJSONFloat(dst, v.Float)
			case v.Kind == vec.KindString:
				dst = variant.AppendJSONString(dst, v.Bytes)
			default:
				if text, err := variant.AppendJSON(dst, v.Bytes); err == nil {
					dst = text
				} else {
					dst = append(dst, "null"...)
				}
			}
		}

		return append(dst, '}')
	}
	row := r.rows[i]
	names := r.Columns
	if names == nil {
		names = make([]string, 0, len(row))
		for name := range row {
			names = append(names, name)
		}
		sort.Strings(names)
	}
	for _, name := range names {
		v, ok := row[name]
		if !ok {
			continue
		}
		if len(dst) > first {
			dst = append(dst, ',')
		}
		dst = appendValue(append(variant.AppendJSONString(dst, []byte(name)), ':'), v)
	}

	return append(dst, '}')
}

func appendTime(dst []byte, nanos int64) []byte {
	return append(time.Unix(0, nanos).UTC().AppendFormat(append(dst, '"'), time.RFC3339Nano), '"')
}

// appendValue appends a value of a row as JSON; a duration is its
// nanoseconds.
func appendValue(dst []byte, v event.Value) []byte {
	switch v.Type() {
	case event.FieldTypeBool:
		return strconv.AppendBool(dst, v.AsBool())
	case event.FieldTypeInt:
		return strconv.AppendInt(dst, v.AsInt(), 10)
	case event.FieldTypeFloat:
		return variant.AppendJSONFloat(dst, v.AsFloat())
	case event.FieldTypeString:
		return variant.AppendJSONString(dst, []byte(v.AsString()))
	case event.FieldTypeTimestamp:
		return appendTime(dst, v.AsTimestamp().UnixNano())
	case event.FieldTypeDuration:
		return strconv.AppendInt(dst, int64(v.AsDuration()), 10)
	case event.FieldTypeArray:
		dst = append(dst, '[')
		for i, e := range v.AsArray() {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendValue(dst, e)
		}

		return append(dst, ']')
	case event.FieldTypeObject:
		fields := v.AsObject()
		names := make([]string, 0, len(fields))
		for name := range fields {
			names = append(names, name)
		}
		sort.Strings(names)
		dst = append(dst, '{')
		for i, name := range names {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendValue(append(variant.AppendJSONString(dst, []byte(name)), ':'), fields[name])
		}

		return append(dst, '}')
	}

	return append(dst, "null"...)
}
