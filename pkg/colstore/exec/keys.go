package exec

import (
	"encoding/binary"
	"math"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// A group key is encoded as one tagged value per key expression: the tag
// alone for a null, which a missing key is too, or a boolean, eight
// little-endian bytes for an integer or a float, and the bytes of a string
// or variant, preceded by their length as a varint unless the value is the
// last of the key.
const (
	tagNull byte = iota
	tagFalse
	tagTrue
	tagInt
	tagFloat
	tagString
	tagVariant
	tagCount
)

var tagSeeds = func() (seeds [tagCount]uint64) {
	for i := range seeds {
		seeds[i] = vec.Mix64(uint64(i) + 0x9e3779b97f4a7c15)
	}

	return seeds
}()

// keyHash hashes an encoded key so that a key of one string hashes its bytes
// in place.
func keyHash(key []byte) uint64 { return vec.Hash(key[1:]) ^ tagSeeds[key[0]] }

func bytesTag(kind vec.Kind) byte {
	if kind == vec.KindVariant {
		return tagVariant
	}

	return tagString
}

func appendKeyBytes(dst []byte, tag byte, v []byte, last bool) []byte {
	dst = append(dst, tag)
	if !last {
		dst = binary.AppendUvarint(dst, uint64(len(v)))
	}

	return append(dst, v...)
}

func appendKeyInt(dst []byte, v int64) []byte {
	return binary.LittleEndian.AppendUint64(append(dst, tagInt), uint64(v))
}

// appendKeyFloat appends a float, as the integer it equals when it equals
// one: a number is one key whichever kind it is stored as.
func appendKeyFloat(dst []byte, v float64) []byte {
	if i, ok := whole(v); ok {
		return appendKeyInt(dst, i)
	}

	return binary.LittleEndian.AppendUint64(append(dst, tagFloat), math.Float64bits(v))
}

// whole returns the integer that v equals, if it equals one.
func whole(v float64) (int64, bool) {
	if v >= -1<<63 && v < 1<<63 {
		if i := int64(v); float64(i) == v {
			return i, true
		}
	}

	return 0, false
}

func appendKeyBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, tagTrue)
	}

	return append(dst, tagFalse)
}

// decodeKey appends the values of an encoded key to cols, one per key
// expression.
func decodeKey(key []byte, cols []colVec) {
	for i := range cols {
		c := &cols[i]
		tag := key[0]
		key = key[1:]
		switch tag {
		case tagFalse:
			c.appendInt(vec.KindBool, 0)
		case tagTrue:
			c.appendInt(vec.KindBool, 1)
		case tagInt:
			c.appendInt(vec.KindInt, int64(binary.LittleEndian.Uint64(key)))
			key = key[8:]
		case tagFloat:
			c.appendFloat(math.Float64frombits(binary.LittleEndian.Uint64(key)))
			key = key[8:]
		case tagString, tagVariant:
			kind := vec.KindString
			if tag == tagVariant {
				kind = vec.KindVariant
			}
			size := len(key)
			if i != len(cols)-1 {
				n, used := binary.Uvarint(key)
				key, size = key[used:], int(n)
			}
			c.appendBytes(kind, key[:size])
			key = key[size:]
		default:
			c.appendNull()
		}
	}
}

// keyValue returns value part of an encoded key of parts values; its bytes
// alias the key.
func keyValue(key []byte, part, parts int) Value {
	for i := 0; ; i++ {
		tag := key[0]
		key = key[1:]
		var v Value
		size := 0
		switch tag {
		case tagFalse, tagTrue:
			v = Value{Kind: vec.KindBool, Bool: tag == tagTrue}
		case tagInt:
			v, size = Value{Kind: vec.KindInt, Int: int64(binary.LittleEndian.Uint64(key))}, 8
		case tagFloat:
			v, size = Value{Kind: vec.KindFloat, Float: math.Float64frombits(binary.LittleEndian.Uint64(key))}, 8
		case tagString, tagVariant:
			size = len(key)
			if i != parts-1 {
				n, used := binary.Uvarint(key)
				key, size = key[used:], int(n)
			}
			v = Value{Kind: vec.KindString, Bytes: key[:size]}
			if tag == tagVariant {
				v.Kind = vec.KindVariant
			}
		}
		if i == part {
			return v
		}
		key = key[size:]
	}
}
