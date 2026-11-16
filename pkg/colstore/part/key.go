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
		return binary.BigEndian.AppendUint64(dst, keyFloat(c.Floats[i]))
	case vec.KindString, vec.KindVariant:
		return appendKeyBytes(dst, c.Bytes(i))
	}

	return dst
}

// keyFloat maps a float to bits that order like the float, with NaN first
// and the two zeros equal.
func keyFloat(f float64) uint64 {
	switch {
	case math.IsNaN(f):
		return 0
	case f == 0:
		return 1 << 63
	case f > 0:
		return math.Float64bits(f) | 1<<63
	}

	return ^math.Float64bits(f)
}

// appendDecodedKey appends the encoding of row of a decoded column, or of a
// missing value when c is nil.
func appendDecodedKey(dst []byte, c *vec.Column, row int) []byte {
	if c == nil {
		return append(dst, 0)
	}
	dst = append(dst, byte(c.Kind))
	switch c.Kind {
	case vec.KindBool:
		if c.Bool(row) {
			return append(dst, 1)
		}

		return append(dst, 0)
	case vec.KindInt:
		return binary.BigEndian.AppendUint64(dst, uint64(c.Int(row))^(1<<63))
	case vec.KindFloat:
		return binary.BigEndian.AppendUint64(dst, keyFloat(c.Float(row)))
	case vec.KindString, vec.KindVariant:
		return appendKeyBytes(dst, c.Bytes(row))
	}

	return dst
}

// SplitKeys returns up to n-1 ascending encoded sort keys that cut the rows
// of ordered parts into ranges of similar size.
func SplitKeys(srcs []*Part, n int) [][]byte {
	type blockKey struct {
		key  []byte
		rows int
	}
	var blocks []blockKey
	total := 0
	for _, p := range srcs {
		for i, key := range p.firstKeys {
			rows, _, _ := p.BlockStats(i)
			blocks = append(blocks, blockKey{key: key, rows: rows})
			total += rows
		}
	}
	sort.Slice(blocks, func(i, j int) bool { return bytes.Compare(blocks[i].key, blocks[j].key) < 0 })
	var cuts [][]byte
	seen := 0
	for _, b := range blocks {
		if len(cuts) == n-1 {
			break
		}
		// A cut at the smallest key would leave an empty first range.
		if next := (len(cuts) + 1) * total / n; seen >= next && bytes.Compare(b.key, blocks[0].key) > 0 &&
			(len(cuts) == 0 || bytes.Compare(b.key, cuts[len(cuts)-1]) > 0) {
			cuts = append(cuts, b.key)
		}
		seen += b.rows
	}

	return cuts
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
