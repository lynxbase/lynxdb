// Package ndjson writes events as newline-delimited JSON objects, the form
// in which the database takes them.
package ndjson

import (
	"encoding/json"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// AppendEvent writes an event as one JSON object: its time, text and
// origin under _time, _raw, host, source and sourcetype, then its fields.
func AppendEvent(dst []byte, ev *event.Event) []byte {
	dst = append(dst, '{')
	n := 0
	key := func(k string) {
		if n > 0 {
			dst = append(dst, ',')
		}
		n++
		dst = appendString(dst, k)
		dst = append(dst, ':')
	}
	written := map[string]bool{}
	if !ev.Time.IsZero() {
		key("_time")
		dst = appendString(dst, ev.Time.UTC().Format(time.RFC3339Nano))
		written["_time"] = true
	}
	for _, f := range [...]struct{ k, v string }{{"_raw", ev.Raw}, {"host", ev.Host}, {"source", ev.Source}, {"sourcetype", ev.SourceType}} {
		if f.v != "" {
			key(f.k)
			dst = appendString(dst, f.v)
			written[f.k] = true
		}
	}
	names := make([]string, 0, len(ev.Fields))
	for k, v := range ev.Fields {
		if !v.IsNull() && !written[k] {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	for _, k := range names {
		key(k)
		dst = appendValue(dst, ev.Fields[k])
	}

	return append(dst, '}')
}

func appendValue(dst []byte, v event.Value) []byte {
	switch v.Type() {
	case event.FieldTypeString:
		return appendString(dst, v.AsString())
	case event.FieldTypeInt:
		return strconv.AppendInt(dst, v.AsInt(), 10)
	case event.FieldTypeFloat:
		b, err := json.Marshal(v.AsFloat())
		if err != nil {
			return append(dst, "null"...)
		}

		return append(dst, b...)
	case event.FieldTypeBool:
		return strconv.AppendBool(dst, v.AsBool())
	case event.FieldTypeTimestamp:
		return appendString(dst, v.AsTimestamp().UTC().Format(time.RFC3339Nano))
	}
	b, err := json.Marshal(v.Interface())
	if err != nil {
		return appendString(dst, v.String())
	}

	return append(dst, b...)
}

const hexDigits = "0123456789abcdef"

// appendString writes s as a JSON string.
func appendString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' && c < utf8.RuneSelf {
			i++

			continue
		}
		if c < utf8.RuneSelf {
			dst = append(dst, s[start:i]...)
			switch c {
			case '"', '\\':
				dst = append(dst, '\\', c)
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			}
			i++
			start = i

			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, `�`...)
			i += size
			start = i

			continue
		}
		i += size
	}

	return append(append(dst, s[start:]...), '"')
}
