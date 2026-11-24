package shred

import (
	"slices"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// Col returns the column holding values of kind at path, creating it when
// the batch has none.
func (b *Batch) Col(path string, kind vec.Kind) *Column {
	pc := b.pathCols(path)
	c := pc.byKind[kind]
	if c == nil {
		c = &Column{Path: path, Kind: kind, lastRow: -1}
		pc.byKind[kind] = c
		b.cols = append(b.cols, c)
	}

	return c
}

// Lookup returns the column holding values of kind at path, or nil.
func (b *Batch) Lookup(path string, kind vec.Kind) *Column {
	if pc := b.paths[path]; pc != nil {
		return pc.byKind[kind]
	}

	return nil
}

// AppendRows completes the rows whose values were appended through columns,
// one per timestamp, and accounts size bytes of input to them.
func (b *Batch) AppendRows(times []int64, size int) {
	b.Times = append(b.Times, times...)
	b.Rows += len(times)
	b.Bytes += size
}

// placeRun records that the next n appended values belong to rows row to
// row+n-1.
func (c *Column) placeRun(row, n int) {
	if !c.Sparse && c.Count != row {
		c.Sparse = true
		c.RowIDs = c.RowIDs[:0]
		for i := 0; i < c.Count; i++ {
			c.RowIDs = append(c.RowIDs, uint32(i))
		}
	}
	if c.Sparse {
		for i := 0; i < n; i++ {
			c.RowIDs = append(c.RowIDs, uint32(row+i))
		}
	}
	c.Count += n
	c.lastRow = int32(row + n - 1)
}

// AppendNull records a null at row, which must be above every row the
// column already holds; the same holds for the other Append methods.
func (c *Column) AppendNull(row int) { c.placeRun(row, 1) }

// AppendPlaces records that rows row to row+n-1 hold a value each without
// taking the values, which the caller keeps beside the column.
func (c *Column) AppendPlaces(row, n int) { c.placeRun(row, n) }

// AppendBool appends a boolean at row.
func (c *Column) AppendBool(row int, v bool) {
	c.placeRun(row, 1)
	bit := uint8(0)
	if v {
		bit = 1
	}
	c.Bools = append(c.Bools, bit)
}

// AppendInt appends an integer at row.
func (c *Column) AppendInt(row int, v int64) {
	c.placeRun(row, 1)
	c.Ints = append(c.Ints, v)
}

// AppendInts appends integers for the consecutive rows starting at row.
func (c *Column) AppendInts(row int, vals []int64) {
	c.placeRun(row, len(vals))
	c.Ints = append(c.Ints, vals...)
}

// AppendFloat appends a float at row.
func (c *Column) AppendFloat(row int, v float64) {
	c.placeRun(row, 1)
	c.Floats = append(c.Floats, v)
}

// AppendFloats appends floats for the consecutive rows starting at row.
func (c *Column) AppendFloats(row int, vals []float64) {
	c.placeRun(row, len(vals))
	c.Floats = append(c.Floats, vals...)
}

// AppendBytes appends a string or variant payload at row.
func (c *Column) AppendBytes(row int, v []byte) {
	c.placeRun(row, 1)
	c.Data = append(c.Data, v...)
	c.Ends = append(c.Ends, uint32(len(c.Data)))
}

// AppendByteRun appends payloads for the consecutive rows starting at row;
// payload i spans offsets[i] to offsets[i+1] of data.
func (c *Column) AppendByteRun(row int, data []byte, offsets []uint32) {
	n := len(offsets) - 1
	if n <= 0 {
		return
	}
	c.placeRun(row, n)
	shift := uint32(len(c.Data)) - offsets[0]
	c.Data = append(c.Data, data[offsets[0]:offsets[n]]...)
	for _, end := range offsets[1:] {
		c.Ends = append(c.Ends, end+shift)
	}
}

// ReserveRun appends payloads of width bytes for the n consecutive rows
// starting at row and returns their bytes, end to end, for the caller to
// fill.
func (c *Column) ReserveRun(row, n, width int) []byte {
	c.placeRun(row, n)
	start := len(c.Data)
	c.Data = slices.Grow(c.Data, n*width)[:start+n*width]
	for end := start + width; end <= start+n*width; end += width {
		c.Ends = append(c.Ends, uint32(end))
	}

	return c.Data[start:]
}

// CopyRows appends rows from to to-1 of b to dst.
func (b *Batch) CopyRows(dst *Batch, from, to int) {
	base := dst.Rows
	for _, c := range b.cols {
		if c.Count == 0 {
			continue
		}
		// first and end bound the values whose rows fall in the range.
		first, end := min(from, c.Count), min(to, c.Count)
		if c.Sparse {
			first = lowerBound(c.RowIDs, uint32(from))
			end = lowerBound(c.RowIDs, uint32(to))
		}
		if first == end {
			continue
		}
		out := dst.Col(c.Path, c.Kind)
		if c.Sparse {
			for _, row := range c.RowIDs[first:end] {
				out.placeRun(base+int(row)-from, 1)
			}
		} else {
			out.placeRun(base+first-from, end-first)
		}
		switch c.Kind {
		case vec.KindBool:
			out.Bools = append(out.Bools, c.Bools[first:end]...)
		case vec.KindInt:
			out.Ints = append(out.Ints, c.Ints[first:end]...)
		case vec.KindFloat:
			out.Floats = append(out.Floats, c.Floats[first:end]...)
		case vec.KindString, vec.KindVariant:
			start := c.start(first)
			shift := uint32(len(out.Data)) - start
			out.Data = append(out.Data, c.Data[start:c.Ends[end-1]]...)
			for _, e := range c.Ends[first:end] {
				out.Ends = append(out.Ends, e+shift)
			}
		}
	}
	size := 0
	if b.Rows > 0 {
		size = int(int64(b.Bytes) * int64(to-from) / int64(b.Rows))
	}
	dst.AppendRows(b.Times[from:to], size)
}

// lowerBound returns the position of the first element not below v in an
// ascending slice.
func lowerBound(s []uint32, v uint32) int {
	lo, hi := 0, len(s)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s[mid] < v {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}
