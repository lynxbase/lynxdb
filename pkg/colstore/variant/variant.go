// Package variant is a compact binary encoding for JSON values that have no
// column of their own, such as arrays.
//
// A value starts with a tag byte. Containers carry their byte length, so a
// reader can step over one without decoding it.
package variant

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"unicode/utf8"
)

const (
	tagNull   = 0x00
	tagFalse  = 0x01
	tagTrue   = 0x02
	tagInt    = 0x03 // zigzag uvarint
	tagFloat  = 0x04 // 8 bytes little-endian
	tagString = 0x05 // uvarint length, bytes
	tagArray  = 0x06 // uint32 byte length, elements
	tagObject = 0x07 // uint32 byte length, then (uvarint key length, key, value) pairs
)

// ErrCorrupt reports an encoding that cannot be decoded.
var ErrCorrupt = errors.New("variant: corrupt value")

// AppendNull appends a null.
func AppendNull(dst []byte) []byte { return append(dst, tagNull) }

// AppendBool appends a boolean.
func AppendBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, tagTrue)
	}

	return append(dst, tagFalse)
}

// AppendInt appends an integer.
func AppendInt(dst []byte, v int64) []byte {
	dst = append(dst, tagInt)

	return binary.AppendUvarint(dst, uint64(v<<1)^uint64(v>>63))
}

// AppendFloat appends a float.
func AppendFloat(dst []byte, v float64) []byte {
	dst = append(dst, tagFloat)

	return binary.LittleEndian.AppendUint64(dst, math.Float64bits(v))
}

// AppendString appends a string.
func AppendString(dst, s []byte) []byte {
	dst = append(dst, tagString)
	dst = binary.AppendUvarint(dst, uint64(len(s)))

	return append(dst, s...)
}

// BeginArray opens an array and returns the mark to pass to End.
func BeginArray(dst []byte) ([]byte, int) {
	dst = append(dst, tagArray, 0, 0, 0, 0)

	return dst, len(dst)
}

// BeginObject opens an object and returns the mark to pass to End.
func BeginObject(dst []byte) ([]byte, int) {
	dst = append(dst, tagObject, 0, 0, 0, 0)

	return dst, len(dst)
}

// AppendKey appends an object key; the member value must follow.
func AppendKey(dst, key []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(key)))

	return append(dst, key...)
}

// End closes the container opened at mark.
func End(dst []byte, mark int) []byte {
	binary.LittleEndian.PutUint32(dst[mark-4:], uint32(len(dst)-mark))

	return dst
}

// AppendJSON appends the JSON text of the value at the start of v.
func AppendJSON(dst, v []byte) ([]byte, error) {
	dst, _, err := appendJSON(dst, v, 0)

	return dst, err
}

const maxDepth = 512

func appendJSON(dst, v []byte, depth int) (out, rest []byte, err error) {
	if len(v) == 0 || depth > maxDepth {
		return dst, nil, ErrCorrupt
	}
	tag, v := v[0], v[1:]
	switch tag {
	case tagNull:
		return append(dst, "null"...), v, nil
	case tagFalse:
		return append(dst, "false"...), v, nil
	case tagTrue:
		return append(dst, "true"...), v, nil
	case tagInt:
		u, n := binary.Uvarint(v)
		if n <= 0 {
			return dst, nil, ErrCorrupt
		}

		return strconv.AppendInt(dst, int64(u>>1)^-int64(u&1), 10), v[n:], nil
	case tagFloat:
		if len(v) < 8 {
			return dst, nil, ErrCorrupt
		}

		return AppendJSONFloat(dst, math.Float64frombits(binary.LittleEndian.Uint64(v))), v[8:], nil
	case tagString:
		u, n := binary.Uvarint(v)
		if n <= 0 || u > uint64(len(v)-n) {
			return dst, nil, ErrCorrupt
		}
		end := n + int(u)

		return AppendJSONString(dst, v[n:end]), v[end:], nil
	case tagArray, tagObject:
		if len(v) < 4 {
			return dst, nil, ErrCorrupt
		}
		size := int(binary.LittleEndian.Uint32(v))
		if size > len(v)-4 {
			return dst, nil, ErrCorrupt
		}
		body, rest := v[4:4+size], v[4+size:]
		if tag == tagArray {
			dst = append(dst, '[')
			for i := 0; len(body) > 0; i++ {
				if i > 0 {
					dst = append(dst, ',')
				}
				if dst, body, err = appendJSON(dst, body, depth+1); err != nil {
					return dst, nil, err
				}
			}

			return append(dst, ']'), rest, nil
		}
		dst = append(dst, '{')
		for i := 0; len(body) > 0; i++ {
			if i > 0 {
				dst = append(dst, ',')
			}
			u, n := binary.Uvarint(body)
			if n <= 0 || u > uint64(len(body)-n) {
				return dst, nil, ErrCorrupt
			}
			dst = AppendJSONString(dst, body[n:n+int(u)])
			dst = append(dst, ':')
			if dst, body, err = appendJSON(dst, body[n+int(u):], depth+1); err != nil {
				return dst, nil, err
			}
		}

		return append(dst, '}'), rest, nil
	}

	return dst, nil, ErrCorrupt
}

// AppendJSONFloat appends f as a JSON number, or null when JSON has no
// representation for it.
func AppendJSONFloat(dst []byte, f float64) []byte {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return append(dst, "null"...)
	}
	abs := math.Abs(f)
	format := byte('f')
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}

	return strconv.AppendFloat(dst, f, format, -1, 64)
}

const hex = "0123456789abcdef"

// AppendJSONString appends s as a quoted JSON string.
func AppendJSONString(dst, s []byte) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' && c < utf8.RuneSelf {
			i++

			continue
		}
		if c >= utf8.RuneSelf {
			r, size := utf8.DecodeRune(s[i:])
			if r != utf8.RuneError || size != 1 {
				i += size

				continue
			}
			dst = append(dst, s[start:i]...)
			dst = append(dst, `�`...)
			i++
			start = i

			continue
		}
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
			dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xF])
		}
		i++
		start = i
	}
	dst = append(dst, s[start:]...)

	return append(dst, '"')
}
