package vec

// Column is one decoded column of one block: Flat strings use
// Offsets[i]..Offsets[i+1] into Data for row i, Dict and Const strings use
// the same pair as a dictionary addressed by code, and rows whose Valid bit
// is clear hold an unspecified value.
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

// Retained returns the bytes of the buffers the column keeps for reuse.
func (c *Column) Retained() int {
	return 8*(cap(c.Ints)+cap(c.Floats)+cap(c.Bools)+cap(c.validBuf)) +
		4*cap(c.Offsets) + cap(c.Data) + cap(c.Codes8) + 2*cap(c.Codes16)
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

// DictFind returns the code of want in the sorted dictionary of a Dict or
// Const column.
func (c *Column) DictFind(want []byte) (code int, ok bool) {
	lo, hi := 0, c.DictLen()
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if string(c.DictValue(mid)) < string(want) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo, lo < c.DictLen() && string(c.DictValue(lo)) == string(want)
}

// MaskEqBytes writes into mask which rows of a string column equal want,
// never selecting a row without a value.
func MaskEqBytes(mask []uint8, c *Column, want []byte) {
	mask = mask[:c.Rows]
	switch c.Layout {
	case Const:
		fill := uint8(0)
		if string(c.DictValue(0)) == string(want) {
			fill = 0xFF
		}
		for i := range mask {
			mask[i] = fill
		}
	case Dict:
		code, ok := c.DictFind(want)
		switch {
		case !ok:
			clear(mask)

			return
		case len(c.Codes8) != 0:
			MaskEq8(mask, c.Codes8, uint8(code))
		default:
			MaskEq16(mask, c.Codes16, uint16(code))
		}
	default:
		for i := range mask {
			var m uint8
			if string(c.Data[c.Offsets[i]:c.Offsets[i+1]]) == string(want) {
				m = 0xFF
			}
			mask[i] = m
		}
	}
	if c.Valid != nil {
		MaskAndBitmap(mask, c.Valid)
	}
}
