package shred

import "github.com/lynxbase/lynxdb/pkg/vec"

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
