package irfunc

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// kind is the type of a value as the IR rules see it: the executor holds
// times and durations as integers, which the IR types of the arguments tell
// apart.
type kind uint8

const (
	kNull kind = iota
	kStr
	kInt
	kFloat
	kBool
	kTime
	kDur
	kArray
	kObject
)

// val is one value of an IR function. n holds an integer, a boolean as 0 or
// 1, a time as nanoseconds since the epoch or a duration in nanoseconds; s
// holds the text of a string or the variant encoding of an array or object.
type val struct {
	k kind
	n int64
	f float64
	s string
}

var null = val{}

func intVal(n int64) val          { return val{k: kInt, n: n} }
func floatVal(f float64) val      { return val{k: kFloat, f: f} }
func strVal(s string) val         { return val{k: kStr, s: s} }
func timeVal(t time.Time) val     { return val{k: kTime, n: t.UnixNano()} }
func durVal(d time.Duration) val  { return val{k: kDur, n: int64(d)} }
func boolVal(b bool) val          { return val{k: kBool, n: b2i(b)} }
func (v val) isNull() bool        { return v.k == kNull }
func (v val) time() time.Time     { return time.Unix(0, v.n).UTC() }
func (v val) dur() time.Duration  { return time.Duration(v.n) }
func (v val) isNum() bool         { return v.k == kInt || v.k == kFloat }
func bytesString(b []byte) string { return unsafe.String(unsafe.SliceData(b), len(b)) }
func stringBytes(s string) []byte { return unsafe.Slice(unsafe.StringData(s), len(s)) }

func b2i(b bool) int64 {
	if b {
		return 1
	}

	return 0
}

// argKind is what the IR type of an argument says about its integers.
type argKind uint8

const (
	argPlain argKind = iota
	argTime
	argDur
)

// in converts an argument of the executor to a value.
func in(v exec.Value, k argKind) val {
	switch v.Kind {
	case vec.KindBool:
		return boolVal(v.Bool)
	case vec.KindInt:
		switch k {
		case argTime:
			return val{k: kTime, n: v.Int}
		case argDur:
			return val{k: kDur, n: v.Int}
		}
		return intVal(v.Int)
	case vec.KindFloat:
		return floatVal(v.Float)
	case vec.KindString:
		return strVal(bytesString(v.Bytes))
	case vec.KindVariant:
		return fromVariant(v.Bytes)
	}

	return null
}

// fromVariant reads a variant as the row engine does: through its JSON text.
func fromVariant(b []byte) val {
	if len(b) > 0 {
		switch b[0] {
		case 0x06:
			return val{k: kArray, s: bytesString(b)}
		case 0x07:
			return val{k: kObject, s: bytesString(b)}
		}
	}
	text, err := variant.AppendJSON(nil, b)
	if err != nil {
		return null
	}
	dec := json.NewDecoder(bytes.NewReader(text))
	dec.UseNumber()
	var x any
	if dec.Decode(&x) != nil {
		return null
	}

	return decodedVal(x)
}

// decodedVal converts a decoded JSON scalar as the row engine reads a
// variant.
func decodedVal(x any) val {
	switch x := x.(type) {
	case bool:
		return boolVal(x)
	case string:
		return strVal(x)
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return intVal(n)
		}
		if f, err := x.Float64(); err == nil {
			return floatVal(f)
		}
		return strVal(x.String())
	}

	return null
}

// out converts a result to a value of the executor with its bytes in buf.
func out(v val, buf []byte) (exec.Value, []byte) {
	switch v.k {
	case kBool:
		return exec.Value{Kind: vec.KindBool, Bool: v.n != 0}, buf
	case kInt, kTime, kDur:
		return exec.Value{Kind: vec.KindInt, Int: v.n}, buf
	case kFloat:
		return exec.Value{Kind: vec.KindFloat, Float: v.f}, buf
	case kStr:
		start := len(buf)
		buf = append(buf, v.s...)

		return exec.Value{Kind: vec.KindString, Bytes: buf[start:]}, buf
	case kArray, kObject:
		start := len(buf)
		buf = append(buf, v.s...)

		return exec.Value{Kind: vec.KindVariant, Bytes: buf[start:]}, buf
	}

	return exec.Value{Kind: vec.KindNull}, buf
}

// numFloat interprets a value as a float.
func numFloat(v val) (float64, bool) {
	switch v.k {
	case kInt:
		return float64(v.n), true
	case kFloat:
		return v.f, true
	case kStr:
		f, err := strconv.ParseFloat(strings.TrimSpace(v.s), 64)
		return f, err == nil
	case kBool:
		return float64(v.n), true
	}

	return 0, false
}

// numInt interprets a value as an integer, truncating a float.
func numInt(v val) (int64, bool) {
	switch v.k {
	case kInt:
		return v.n, true
	case kFloat:
		if math.IsNaN(v.f) || math.IsInf(v.f, 0) {
			return 0, false
		}
		return toInt(v.f), true
	case kStr:
		s := strings.TrimSpace(v.s)
		if n, ok := parseIntText(s); ok {
			return n, true
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			return toInt(f), true
		}
	case kBool:
		return v.n, true
	}

	return 0, false
}

// toInt converts a float to an integer, saturating beyond int64 and giving
// 0 for NaN, so that the result does not depend on the processor.
func toInt(f float64) int64 {
	switch {
	case f != f:
		return 0
	case f >= 1<<63:
		return math.MaxInt64
	case f < -(1 << 63):
		return math.MinInt64
	}

	return int64(f)
}

// parseIntText is strconv.ParseInt in base 10 without allocating an error
// for text that is not an integer.
func parseIntText(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	digits := s
	if digits[0] == '+' || digits[0] == '-' {
		digits = digits[1:]
	}
	if digits == "" {
		return 0, false
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)

	return n, err == nil
}

// parseNumber parses decimal text to an int when it has no fraction or
// exponent and to a float otherwise.
func parseNumber(s string) (val, bool) {
	s = strings.TrimSpace(s)
	if n, ok := parseIntText(s); ok {
		return intVal(n), true
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return floatVal(f), true
	}

	return null, false
}

// timestampLayout is the text form of times converted to strings.
const timestampLayout = "2006-01-02 15:04:05.999999999"

// appendFloat writes a float as the IR's string form of a float.
func appendFloat(dst []byte, f float64) []byte {
	start := len(dst)
	dst = strconv.AppendFloat(dst, f, 'g', -1, 64)
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return dst
	}
	if bytes.IndexByte(dst[start:], 'e') >= 0 || bytes.IndexByte(dst[start:], '.') >= 0 {
		return dst
	}

	return append(dst, ".0"...)
}

// appendPad2 writes n as fmt's %02d does.
func appendPad2(dst []byte, n int64) []byte {
	if n >= 0 && n < 10 {
		dst = append(dst, '0')
	}

	return strconv.AppendInt(dst, n, 10)
}

// appendTwo writes n with a leading zero below ten, negative numbers too.
func appendTwo(dst []byte, n int64) []byte {
	if n < 10 {
		dst = append(dst, '0')
	}

	return strconv.AppendInt(dst, n, 10)
}

// appendTimeOfDay writes a duration as HH:mm:ss[.fraction].
func appendTimeOfDay(dst []byte, d time.Duration) []byte {
	if d < 0 {
		dst = append(dst, '-')
		d = -d
	}
	h := int64(d / time.Hour)
	dst = appendTwo(dst, h)
	dst = append(dst, ':')
	dst = appendTwo(dst, int64(d%time.Hour/time.Minute))
	dst = append(dst, ':')
	dst = appendTwo(dst, int64(d%time.Minute/time.Second))
	if ns := int64(d % time.Second); ns > 0 {
		var digits [10]byte
		frac := strconv.AppendInt(digits[:0], ns+1e9, 10)[1:]
		for len(frac) > 0 && frac[len(frac)-1] == '0' {
			frac = frac[:len(frac)-1]
		}
		dst = append(dst, '.')
		dst = append(dst, frac...)
	}

	return dst
}

// appendText writes the canonical text of a value that is not null.
func appendText(dst []byte, v val) []byte {
	switch v.k {
	case kStr:
		return append(dst, v.s...)
	case kInt:
		return strconv.AppendInt(dst, v.n, 10)
	case kFloat:
		return appendFloat(dst, v.f)
	case kBool:
		return strconv.AppendBool(dst, v.n != 0)
	case kTime:
		return v.time().AppendFormat(dst, timestampLayout)
	case kDur:
		return appendTimeOfDay(dst, v.dur())
	case kArray, kObject:
		b, err := json.Marshal(tree(v))
		if err != nil {
			return dst
		}
		return append(dst, b...)
	}

	return dst
}

// timestampInputLayouts are the text forms a string may give a time in.
var timestampInputLayouts = []string{
	time.RFC3339Nano, "2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04", "2006-01-02", "01/02/2006:15:04:05", "02/Jan/2006:15:04:05 -0700",
}

// timeOf interprets a value as an instant: a time, or a string in one of
// the accepted layouts.
func timeOf(v val) (time.Time, bool) {
	switch v.k {
	case kTime:
		return v.time(), true
	case kStr:
		s := strings.TrimSpace(v.s)
		if len(s) < 10 {
			return time.Time{}, false
		}
		for _, l := range timestampInputLayouts {
			if t, err := time.Parse(l, s); err == nil {
				return t.UTC(), true
			}
		}
	}

	return time.Time{}, false
}

// timeOfDay interprets a value as a time of day: a duration, the clock part
// of a time, or an HH:mm[:ss[.fraction]] string.
func timeOfDay(v val) (time.Duration, bool) {
	switch v.k {
	case kDur:
		return v.dur(), true
	case kTime:
		t := v.time()
		return t.Sub(dayStart(t)), true
	case kStr:
		s := strings.TrimSpace(v.s)
		for _, l := range []string{"15:04:05.999999999", "15:04:05", "15:04"} {
			if t, err := time.Parse(l, s); err == nil {
				return time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute +
					time.Duration(t.Second())*time.Second + time.Duration(t.Nanosecond()), true
			}
		}
		if t, ok := timeOf(v); ok {
			return t.Sub(dayStart(t)), true
		}
	}

	return 0, false
}

func dayStart(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// tree decodes an array or an object into the values the row engine holds:
// []any and map[string]any with int64, float64, string, bool and nil leaves.
func tree(v val) any {
	text, err := variant.AppendJSON(nil, stringBytes(v.s))
	if err != nil {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(text))
	dec.UseNumber()
	var x any
	if dec.Decode(&x) != nil {
		return nil
	}

	return plain(x)
}

// plain replaces the json.Number leaves of a decoded value by int64,
// float64 or string, as the row engine reads them.
func plain(x any) any {
	switch t := x.(type) {
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return n
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return t.String()
	case []any:
		for i, e := range t {
			t[i] = plain(e)
		}
	case map[string]any:
		for k, e := range t {
			t[k] = plain(e)
		}
	}

	return x
}
