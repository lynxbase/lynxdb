package exec

import (
	"slices"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// Stats describes what a query read and held.
type Stats struct {
	// Blocks is the number of blocks opened; the skipped ones were ruled out
	// before that by sort key, token filter or time range.
	Blocks         int64
	SkippedByKey   int64
	SkippedByToken int64
	SkippedByTime  int64
	// SkippedByStats counts opened blocks that the filter rejected from
	// column statistics and constants, without decoding a column.
	SkippedByStats int64
	// BlocksMatched and RowsMatched count what passed the filter.
	BlocksMatched int64
	RowsMatched   int64
	// HeldBytes is the memory the query held at its end: arenas, logs and
	// reusable buffers.
	HeldBytes int64
}

// Column is one column of a result.
type Column struct {
	Name string
	// Kind is the kind of every value, or KindAny when rows differ in kind
	// or all are null; Kinds then holds the kind of each row, null rows as
	// KindAny.
	Kind  vec.Kind
	Kinds []vec.Kind
	// Values holds the rows in the flat layout; a row uses the buffer of its
	// own kind.
	Values vec.Column
}

// Result is the rows a query produced.
type Result struct {
	Columns []Column
	Rows    int
	Stats   Stats
}

// Value returns the value at a row and column.
func (r *Result) Value(row, col int) Value {
	c := &r.Columns[col]
	kind := c.Kind
	if c.Kinds != nil {
		kind = c.Kinds[row]
	}
	if !c.Values.IsValid(row) {
		return Value{}
	}
	switch kind {
	case vec.KindBool:
		return Value{Kind: kind, Bool: c.Values.Bools.Get(row)}
	case vec.KindInt:
		return Value{Kind: kind, Int: c.Values.Ints[row]}
	case vec.KindFloat:
		return Value{Kind: kind, Float: c.Values.Floats[row]}
	case vec.KindString, vec.KindVariant:
		return Value{Kind: kind, Bytes: c.Values.Data[c.Values.Offsets[row]:c.Values.Offsets[row+1]]}
	}

	return Value{}
}

// Row appends the values of a row to dst.
func (r *Result) Row(row int, dst []Value) []Value {
	for col := range r.Columns {
		dst = append(dst, r.Value(row, col))
	}

	return dst
}

// kindMixed marks a colVec whose rows differ in kind.
const kindMixed vec.Kind = 0xFF

// colVec is a column being built, one value per row, of any kinds.
type colVec struct {
	// kinds holds the kind of each row, KindAny for a null.
	kinds []vec.Kind
	// ints holds integers and booleans; like floats and ends it is either
	// unused or as long as kinds.
	ints   []int64
	floats []float64
	// ends[i] is where the bytes of row i end in data.
	ends []uint32
	data []byte
	// kind is the kind of every row, KindAny while there is none, or
	// kindMixed.
	kind vec.Kind
}

func (c *colVec) reset() {
	c.kinds, c.ints, c.floats, c.ends, c.data = c.kinds[:0], c.ints[:0], c.floats[:0], c.ends[:0], c.data[:0]
	c.kind = KindAny
}

func (c *colVec) len() int { return len(c.kinds) }

func (c *colVec) held() int {
	return cap(c.kinds) + 8*cap(c.ints) + 8*cap(c.floats) + 4*cap(c.ends) + cap(c.data)
}

// push adds a row of kind and keeps the buffers in use aligned.
func (c *colVec) push(kind vec.Kind) {
	switch {
	case len(c.kinds) == 0:
		c.kind = kind
	case c.kind != kind:
		c.kind = kindMixed
	}
	c.kinds = append(c.kinds, kind)
	if len(c.ints) != 0 {
		c.ints = append(c.ints, 0)
	}
	if len(c.floats) != 0 {
		c.floats = append(c.floats, 0)
	}
	if len(c.ends) != 0 {
		c.ends = append(c.ends, uint32(len(c.data)))
	}
}

func (c *colVec) appendNull() { c.push(KindAny) }

func (c *colVec) appendInt(kind vec.Kind, v int64) {
	c.push(kind)
	n := len(c.kinds)
	if len(c.ints) != n {
		c.ints = append(c.ints, make([]int64, n-len(c.ints))...)
	}
	c.ints[n-1] = v
}

func (c *colVec) appendFloat(v float64) {
	c.push(vec.KindFloat)
	n := len(c.kinds)
	if len(c.floats) != n {
		c.floats = append(c.floats, make([]float64, n-len(c.floats))...)
	}
	c.floats[n-1] = v
}

func (c *colVec) appendBytes(kind vec.Kind, v []byte) {
	c.push(kind)
	n := len(c.kinds)
	if len(c.ends) != n {
		c.ends = append(c.ends, make([]uint32, n-len(c.ends))...)
	}
	c.data = append(c.data, v...)
	c.ends[n-1] = uint32(len(c.data))
}

func (c *colVec) appendValue(v Value) {
	switch v.Kind {
	case vec.KindBool:
		b := int64(0)
		if v.Bool {
			b = 1
		}
		c.appendInt(vec.KindBool, b)
	case vec.KindInt:
		c.appendInt(vec.KindInt, v.Int)
	case vec.KindFloat:
		c.appendFloat(v.Float)
	case vec.KindString, vec.KindVariant:
		c.appendBytes(v.Kind, v.Bytes)
	default:
		c.appendNull()
	}
}

// bulkInts makes the column n integers and returns them to be filled.
func (c *colVec) bulkInts(n int) []int64 {
	c.reset()
	c.kinds = sized(c.kinds, n)
	for i := range c.kinds {
		c.kinds[i] = vec.KindInt
	}
	c.ints = sized(c.ints, n)
	c.kind = vec.KindInt

	return c.ints
}

// bulkFloats makes the column n floats and returns them to be filled.
func (c *colVec) bulkFloats(n int) []float64 {
	c.reset()
	c.kinds = sized(c.kinds, n)
	for i := range c.kinds {
		c.kinds[i] = vec.KindFloat
	}
	c.floats = sized(c.floats, n)
	c.kind = vec.KindFloat

	return c.floats
}

// truncate keeps the first n rows.
func (c *colVec) truncate(n int) {
	c.kinds = c.kinds[:n]
	if len(c.ints) != 0 {
		c.ints = c.ints[:n]
	}
	if len(c.floats) != 0 {
		c.floats = c.floats[:n]
	}
	if len(c.ends) != 0 {
		c.ends = c.ends[:n]
	}
}

// setNull turns row i into a null.
func (c *colVec) setNull(i int) {
	c.kinds[i] = KindAny
	c.kind = kindMixed
}

func (c *colVec) bytes(i int) []byte {
	start := uint32(0)
	if i > 0 {
		start = c.ends[i-1]
	}

	return c.data[start:c.ends[i]]
}

// value returns row i; its bytes alias the column.
func (c *colVec) value(i int) Value {
	switch kind := c.kinds[i]; kind {
	case vec.KindBool:
		return Value{Kind: kind, Bool: c.ints[i] != 0}
	case vec.KindInt:
		return Value{Kind: kind, Int: c.ints[i]}
	case vec.KindFloat:
		return Value{Kind: kind, Float: c.floats[i]}
	case vec.KindString, vec.KindVariant:
		return Value{Kind: kind, Bytes: c.bytes(i)}
	}

	return Value{}
}

// appendFrom adds row i of src.
func (c *colVec) appendFrom(src *colVec, i int) {
	switch kind := src.kinds[i]; kind {
	case vec.KindBool, vec.KindInt:
		c.appendInt(kind, src.ints[i])
	case vec.KindFloat:
		c.appendFloat(src.floats[i])
	case vec.KindString, vec.KindVariant:
		c.appendBytes(kind, src.bytes(i))
	default:
		c.appendNull()
	}
}

// appendAll adds every row of src.
func (c *colVec) appendAll(src *colVec) {
	n := src.len()
	if n == 0 {
		return
	}
	if c.len() == 0 && len(c.ints) == 0 && len(c.floats) == 0 && len(c.ends) == 0 {
		c.kinds = append(c.kinds[:0], src.kinds...)
		c.ints = append(c.ints[:0], src.ints...)
		c.floats = append(c.floats[:0], src.floats...)
		c.ends = append(c.ends[:0], src.ends...)
		c.data = append(c.data[:0], src.data...)
		c.kind = src.kind

		return
	}
	for i := 0; i < n; i++ {
		c.appendFrom(src, i)
	}
}

// column turns the built rows into a result column that owns its memory.
func (c *colVec) column(name string) Column {
	n := c.len()
	out := Column{Name: name, Kind: c.kind}
	v := &out.Values
	v.Rows, v.Layout = n, vec.Flat
	nulls, bools := false, false
	seen := KindAny
	for _, k := range c.kinds {
		switch {
		case k == KindAny:
			nulls = true
		case seen == KindAny:
			seen = k
		case seen != k:
			seen = kindMixed
		}
		bools = bools || k == vec.KindBool
	}
	out.Kind = seen
	if seen == kindMixed || seen == KindAny {
		out.Kind = KindAny
		out.Kinds = slices.Clone(c.kinds)
	}
	v.Kind = out.Kind
	if nulls {
		v.Valid = make(vec.Bitmap, vec.BitmapWords(n))
		for i, k := range c.kinds {
			if k != KindAny {
				v.Valid.Set(i)
			}
		}
	}
	if len(c.ints) != 0 {
		v.Ints = append([]int64(nil), c.ints...)
	}
	if bools {
		v.Bools = make(vec.Bitmap, vec.BitmapWords(n))
		for i, k := range c.kinds {
			if k == vec.KindBool && c.ints[i] != 0 {
				v.Bools.Set(i)
			}
		}
	}
	if len(c.floats) != 0 {
		v.Floats = append([]float64(nil), c.floats...)
	}
	if len(c.ends) != 0 {
		v.Offsets = make([]uint32, n+1)
		copy(v.Offsets[1:], c.ends)
		v.Data = append([]byte(nil), c.data...)
	}

	return out
}
