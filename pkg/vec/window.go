package vec

// Window is a view of consecutive rows of a column: its values are the
// column's own, so it is valid only while the column is, and its presence
// bits are a copy.
type Window struct {
	Column

	valid, bools Bitmap
}

// Set makes w the rows from to to of c; Valid is nil when all of them hold a
// value.
func (w *Window) Set(c *Column, from, to int) {
	n := to - from
	w.Column = Column{Kind: c.Kind, Layout: c.Layout, Rows: n}
	if c.Valid != nil && c.Valid.Rank(to)-c.Valid.Rank(from) != n {
		w.valid = copyBits(w.valid, c.Valid, from, n)
		w.Valid = w.valid
	}
	switch c.Layout {
	case Const:
		w.Ints, w.Floats, w.Bools, w.Offsets, w.Data = c.Ints, c.Floats, c.Bools, c.Offsets, c.Data
	case Dict:
		w.Offsets, w.Data = c.Offsets, c.Data
		if len(c.Codes8) != 0 {
			w.Codes8 = c.Codes8[from:to]
		} else {
			w.Codes16 = c.Codes16[from:to]
		}
	default:
		switch c.Kind {
		case KindInt:
			w.Ints = c.Ints[from:to]
		case KindFloat:
			w.Floats = c.Floats[from:to]
		case KindBool:
			w.bools = copyBits(w.bools, c.Bools, from, n)
			w.Bools = w.bools
		case KindString, KindVariant:
			w.Offsets, w.Data = c.Offsets[from:to+1], c.Data
		}
	}
}

// Drop ends the view, so that the window keeps none of the column's memory.
func (w *Window) Drop() { w.Column = Column{} }

// Retained returns the bytes of the buffers the window keeps for reuse.
func (w *Window) Retained() int { return 8 * (cap(w.valid) + cap(w.bools)) }

// copyBits returns dst holding the n bits of src that start at bit from,
// with the rest of its last word clear.
func copyBits(dst, src Bitmap, from, n int) Bitmap {
	words := BitmapWords(n)
	if cap(dst) < words {
		dst = make(Bitmap, words)
	}
	dst = dst[:words]
	first, shift := from>>6, uint(from)&63
	for i := range dst {
		v := src[first+i] >> shift
		if shift != 0 && first+i+1 < len(src) {
			v |= src[first+i+1] << (64 - shift)
		}
		dst[i] = v
	}
	if rem := uint(n) & 63; rem != 0 {
		dst[words-1] &= 1<<rem - 1
	}

	return dst
}
