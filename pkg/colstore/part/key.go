package part

import (
	"bytes"
	"encoding/binary"
	"math"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// A sort key tuple is encoded so that comparing the bytes of two tuples
// orders them the way Merge orders rows: per path a kind byte (0 for a
// missing value), then the value. Strings escape 0x00 as 0x00 0xFF and end
// with 0x00 0x00, so one value can never run into the next.

// AppendKeyString appends the encoding of a string value of a sort key.
func AppendKeyString(dst, s []byte) []byte {
	return appendKeyBytes(append(dst, byte(vec.KindString)), s)
}

// AppendKeyInt appends the encoding of an integer value of a sort key.
func AppendKeyInt(dst []byte, v int64) []byte {
	return binary.BigEndian.AppendUint64(append(dst, byte(vec.KindInt)), uint64(v)^(1<<63))
}

func appendKeyBytes(dst, s []byte) []byte {
	for {
		i := bytes.IndexByte(s, 0)
		if i < 0 {
			break
		}
		dst = append(append(dst, s[:i]...), 0x00, 0xFF)
		s = s[i+1:]
	}

	return append(append(dst, s...), 0x00, 0x00)
}

// appendKeyValue appends the encoding of value i of column c.
func appendKeyValue(dst []byte, c *shred.Column, i int) []byte {
	dst = append(dst, byte(c.Kind))
	switch c.Kind {
	case vec.KindBool:
		return append(dst, c.Bools[i])
	case vec.KindInt:
		// Flipping the sign bit makes negative values sort first as bytes.
		return binary.BigEndian.AppendUint64(dst, uint64(c.Ints[i])^(1<<63))
	case vec.KindFloat:
		f := c.Floats[i]
		var u uint64
		switch {
		case math.IsNaN(f):
		case f == 0:
			u = 1 << 63
		case f > 0:
			u = math.Float64bits(f) | 1<<63
		default:
			u = ^math.Float64bits(f)
		}

		return binary.BigEndian.AppendUint64(dst, u)
	case vec.KindString, vec.KindVariant:
		return appendKeyBytes(dst, c.Bytes(i))
	}

	return dst
}

// appendFirstKey appends the encoded sort key of the first row of b.
func appendFirstKey(dst []byte, b *shred.Batch, key []string) []byte {
	for _, path := range key {
		found := false
		for kind := vec.KindNull; kind <= vec.KindVariant && !found; kind++ {
			if c := b.Lookup(path, kind); c != nil && c.Count > 0 && c.Row(0) == 0 {
				dst = appendKeyValue(dst, c, 0)
				found = true
			}
		}
		if !found {
			dst = append(dst, 0)
		}
	}

	return dst
}

// BlocksWithKeyPrefix returns the range of blocks, first to end, that can
// hold rows whose encoded sort key starts with prefix; a part that is not
// ordered returns all of its blocks.
func (p *Part) BlocksWithKeyPrefix(prefix []byte) (first, end int) {
	n := p.NumBlocks()
	if len(p.firstKeys) != n {
		return 0, n
	}
	// The block before the first one starting at or after the prefix can
	// end with matching rows.
	first = sort.Search(n, func(i int) bool { return bytes.Compare(p.firstKeys[i], prefix) >= 0 })
	if first > 0 {
		first--
	}
	end = sort.Search(n, func(i int) bool {
		k := p.firstKeys[i]

		return bytes.Compare(k, prefix) > 0 && !bytes.HasPrefix(k, prefix)
	})

	return first, max(first, end)
}
