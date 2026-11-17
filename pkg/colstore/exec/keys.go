package exec

import (
	"encoding/binary"
	"math"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// A group key is encoded as one tagged value per key expression: the tag
// alone for a missing key or a boolean, eight little-endian bytes for an
// integer or a float, and the bytes of a string or variant, preceded by
// their length as a varint unless the value is the last of the key.
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

func appendKeyFloat(dst []byte, v float64) []byte {
	return binary.LittleEndian.AppendUint64(append(dst, tagFloat), math.Float64bits(v))
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
