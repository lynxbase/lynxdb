package vec

// Column is one decoded column of one block.
//
// Flat strings use Offsets[i]..Offsets[i+1] into Data for row i. Dict and
// Const strings use the same pair as the dictionary, addressed by code.
// Rows whose Valid bit is clear hold an unspecified value.
type Column struct {
	Kind   Kind
	Layout Layout
	Rows   int
	Valid  Bitmap // nil when every row has a value

	Ints   []int64
	Floats []float64
	Bools  Bitmap

	Offsets []uint32
	Data    []byte
	Codes8  []uint8  // Dict with at most 256 entries; empty otherwise
	Codes16 []uint16 // Dict with more entries; empty otherwise

	validBuf Bitmap
}

// UseValid makes Valid a cleared bitmap of rows bits, reusing its memory.
func (c *Column) UseValid(rows int) Bitmap {
	n := BitmapWords(rows)
	if cap(c.validBuf) < n {
		c.validBuf = make(Bitmap, n)
	}
	c.validBuf = c.validBuf[:n]
	clear(c.validBuf)
	c.Valid = c.validBuf

	return c.Valid
}

// IsValid reports whether row i holds a value.
func (c *Column) IsValid(i int) bool { return c.Valid == nil || c.Valid.Get(i) }

// DictLen returns the number of dictionary entries of a Dict or Const column.
func (c *Column) DictLen() int {
	if len(c.Offsets) == 0 {
		return 0
	}

	return len(c.Offsets) - 1
}

// DictValue returns dictionary entry code.
func (c *Column) DictValue(code int) []byte {
	return c.Data[c.Offsets[code]:c.Offsets[code+1]]
}

// Code returns the dictionary code of row i of a Dict column.
func (c *Column) Code(i int) int {
	if len(c.Codes8) != 0 {
		return int(c.Codes8[i])
	}

	return int(c.Codes16[i])
}

// Bytes returns the string or variant payload of row i.
func (c *Column) Bytes(i int) []byte {
	switch c.Layout {
	case Dict:
		return c.DictValue(c.Code(i))
	case Const:
		return c.DictValue(0)
	}

	return c.Data[c.Offsets[i]:c.Offsets[i+1]]
}

// Int returns the integer of row i.
func (c *Column) Int(i int) int64 {
	if c.Layout == Const {
		return c.Ints[0]
	}

	return c.Ints[i]
}

// Float returns the float of row i.
func (c *Column) Float(i int) float64 {
	if c.Layout == Const {
		return c.Floats[0]
	}

	return c.Floats[i]
}

// Bool returns the boolean of row i.
func (c *Column) Bool(i int) bool {
	if c.Layout == Const {
		return c.Bools.Get(0)
	}

	return c.Bools.Get(i)
}
