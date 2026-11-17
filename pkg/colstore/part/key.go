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

// AppendRowKey appends the encoded sort key of a row of b.
func AppendRowKey(dst []byte, b *shred.Batch, key []string, row int) []byte {
	for _, path := range key {
		found := false
		for kind := vec.KindNull; kind <= vec.KindVariant && !found; kind++ {
			c := b.Lookup(path, kind)
			if c == nil || c.Count == 0 {
				continue
			}
			if i, ok := c.ValueAt(row); ok {
				dst = appendKeyValue(dst, c, i)
				found = true
			}
		}
		if !found {
			dst = append(dst, 0)
		}
	}

	return dst
}

// appendEdgeKey appends the encoded sort key of the first row of b, or of
// its last row.
func appendEdgeKey(dst []byte, b *shred.Batch, key []string, last bool) []byte {
	for _, path := range key {
		found := false
		for kind := vec.KindNull; kind <= vec.KindVariant && !found; kind++ {
			c := b.Lookup(path, kind)
			if c == nil || c.Count == 0 {
				continue
			}
			value, row := 0, 0
			if last {
				value, row = c.Count-1, b.Rows-1
			}
			if c.Row(value) == row {
				dst = appendKeyValue(dst, c, value)
				found = true
			}
		}
		if !found {
			dst = append(dst, 0)
		}
	}

	return dst
}

// KeyRange returns the encoded sort keys of the first and the last row of
// an ordered part, and nil for a part that is unordered or empty.
func (p *Part) KeyRange() (first, last []byte) {
	if len(p.firstKeys) == 0 {
		return nil, nil
	}

	return p.firstKeys[0], p.lastKey
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

// FirstKey returns the encoded sort key of the first row of block i of an
// ordered part.
func (p *Part) FirstKey(i int) []byte { return p.firstKeys[i] }

// keyColumnSize returns the length of the first column of an encoded sort
// key.
func keyColumnSize(key []byte) int {
	switch vec.Kind(key[0]) {
	case vec.KindBool:
		return min(2, len(key))
	case vec.KindInt, vec.KindFloat:
		return min(9, len(key))
	case vec.KindString, vec.KindVariant:
		for i := 1; i+1 < len(key); i++ {
			if key[i] != 0 {
				continue
			}
			if key[i+1] == 0 {
				return i + 2
			}
			i++
		}

		return len(key)
	}

	return 1
}

// SharedKeyColumns returns how many leading columns two encoded sort keys
// have in common.
func SharedKeyColumns(a, b []byte) int {
	n := 0
	for len(a) > 0 {
		size := keyColumnSize(a)
		if size > len(b) || !bytes.Equal(a[:size], b[:size]) {
			break
		}
		a, b = a[size:], b[size:]
		n++
	}

	return n
}

// KeyPrefix returns the first columns columns of an encoded sort key.
func KeyPrefix(key []byte, columns int) []byte {
	end := 0
	for ; columns > 0 && end < len(key); columns-- {
		end += keyColumnSize(key[end:])
	}

	return key[:end]
}

// columnsWithin returns how many leading columns of key end in its first n
// bytes.
func columnsWithin(key []byte, n int) int {
	columns, end := 0, 0
	for end < len(key) {
		if end += keyColumnSize(key[end:]); end > n {
			break
		}
		columns++
	}

	return columns
}

// KeyKind returns the kind of the value an encoded sort key holds at column,
// zero when it holds none.
func KeyKind(key []byte, column int) vec.Kind {
	key = key[len(KeyPrefix(key, column)):]
	if len(key) == 0 {
		return 0
	}

	return vec.Kind(key[0])
}

// KeyString appends to dst the string that an encoded sort key holds at
// column, and reports false when the key holds no string there.
func KeyString(dst, key []byte, column int) ([]byte, bool) {
	key = key[len(KeyPrefix(key, column)):]
	if len(key) == 0 || vec.Kind(key[0]) != vec.KindString {
		return dst, false
	}
	body := key[1 : keyColumnSize(key)-2]
	for {
		i := bytes.IndexByte(body, 0)
		if i < 0 {
			return append(dst, body...), true
		}
		// 0x00 0xFF stands for a zero byte.
		dst = append(dst, body[:i+1]...)
		body = body[i+2:]
	}
}

// startsRun reports whether the part's index proves that no row before
// block i has a key starting with the first n bytes of the block's first
// key.
func (p *Part) startsRun(i, n int) bool {
	return i == 0 || (len(p.keyStats) != 0 && p.KeyShared(i) < columnsWithin(p.firstKeys[i], n))
}

// BlockBelowKey reports whether the part's index proves that every row of
// block i has an encoded sort key below key.
func (p *Part) BlockBelowKey(i int, key []byte) bool {
	if i+1 == len(p.firstKeys) {
		return bytes.Compare(p.lastKey, key) < 0
	}
	next := p.firstKeys[i+1]
	if bytes.Compare(next, key) < 0 {
		return true
	}
	// Rows before a block that starts with key are below key unless they
	// share every column of key with the block's first row.
	return bytes.HasPrefix(next, key) && len(p.keyStats) != 0 && p.KeyShared(i+1) < columnsWithin(next, len(key))
}

// BlocksInKeyRange returns the range of blocks, first to end, of an ordered
// part that can hold rows whose encoded sort key is at least lo and below hi.
func (p *Part) BlocksInKeyRange(lo, hi []byte) (first, end int) {
	n := len(p.firstKeys)
	if n == 0 || bytes.Compare(p.lastKey, lo) < 0 {
		return 0, 0
	}
	// The block before the first one starting at or after lo can end with
	// rows of the range, unless the index proves that it does not.
	first = sort.Search(n, func(i int) bool { return bytes.Compare(p.firstKeys[i], lo) >= 0 })
	if first > 0 && (first == n || !bytes.HasPrefix(p.firstKeys[first], lo) || !p.startsRun(first, len(lo))) {
		first--
	}
	end = sort.Search(n, func(i int) bool { return bytes.Compare(p.firstKeys[i], hi) >= 0 })

	return first, max(first, end)
}

// SplitKeysWithPrefix returns up to n-1 ascending encoded sort keys starting
// with prefix that cut the rows of ordered parts holding that prefix into
// ranges of similar size. The keys hold columns key columns, so that rows
// equal in those columns stay in one range.
func SplitKeysWithPrefix(srcs []*Part, n int, prefix []byte, columns int) [][]byte {
	type blockKey struct {
		key  []byte
		rows int
	}
	var blocks []blockKey
	total := 0
	for _, p := range srcs {
		if len(p.firstKeys) == 0 {
			continue
		}
		first, end := p.BlocksWithKeyPrefix(prefix)
		for i := first; i < end; i++ {
			rows, _, _ := p.BlockStats(i)
			blocks = append(blocks, blockKey{key: KeyPrefix(p.firstKeys[i], columns), rows: rows})
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
		if next := (len(cuts) + 1) * total / n; seen >= next && bytes.HasPrefix(b.key, prefix) &&
			(len(cuts) == 0 || bytes.Compare(b.key, cuts[len(cuts)-1]) > 0) {
			cuts = append(cuts, b.key)
		}
		seen += b.rows
	}

	return cuts
}

// BlockKeys holds the decoded sort key columns of one block of an ordered
// part.
type BlockKeys struct {
	cols []vec.Column
	// byPath lists, for each sort key path, the columns holding it, one per
	// value kind.
	byPath [][]int
	rows   int
}

// Load decodes the sort key columns of b.
func (k *BlockKeys) Load(b *Block) error {
	key := b.part.sortKey
	for len(k.byPath) < len(key) {
		k.byPath = append(k.byPath, nil)
	}
	k.byPath = k.byPath[:len(key)]
	k.rows = b.Rows
	n := 0
	for i, path := range key {
		k.byPath[i] = k.byPath[i][:0]
		refs := b.Path(b.part.ids[path])
		for j := range refs {
			if n == len(k.cols) {
				k.cols = append(k.cols, vec.Column{})
			}
			if err := b.Decode(&refs[j], &k.cols[n]); err != nil {
				return err
			}
			k.byPath[i] = append(k.byPath[i], n)
			n++
		}
	}

	return nil
}

// Column returns the column holding values of kind at sort key path i, valid
// until the next Load, or nil when the block has none.
func (k *BlockKeys) Column(i int, kind vec.Kind) *vec.Column {
	for _, j := range k.byPath[i] {
		if k.cols[j].Kind == kind {
			return &k.cols[j]
		}
	}

	return nil
}

// AppendKey appends the encoded sort key of row.
func (k *BlockKeys) AppendKey(dst []byte, row int) []byte {
	for _, cols := range k.byPath {
		var c *vec.Column
		for _, j := range cols {
			if k.cols[j].IsValid(row) {
				c = &k.cols[j]

				break
			}
		}
		dst = appendDecodedKey(dst, c, row)
	}

	return dst
}

// Search returns the first row whose encoded sort key is not below bound,
// building keys in scratch so that goroutines can search one block at once.
func (k *BlockKeys) Search(bound []byte, scratch *[]byte) int {
	buf := *scratch
	row := sort.Search(k.rows, func(row int) bool {
		buf = k.AppendKey(buf[:0], row)

		return bytes.Compare(buf, bound) >= 0
	})
	*scratch = buf

	return row
}

// Retained returns the bytes of the buffers kept for reuse.
func (k *BlockKeys) Retained() int {
	n := 0
	for i := range k.cols {
		n += k.cols[i].Retained()
	}

	return n
}
