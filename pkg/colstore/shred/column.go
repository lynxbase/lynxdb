// Package shred parses JSON documents once, straight into typed column
// builders keyed by flattened path and value kind.
package shred

import "github.com/lynxbase/lynxdb/pkg/vec"

// Column accumulates the values of one path that share a kind.
//
// Value i belongs to row i unless Sparse is set, in which case it belongs to
// row RowIDs[i]. Strings and variants live in Data, value i ending at Ends[i].
type Column struct {
	Path   string
	Kind   vec.Kind
	Count  int
	Sparse bool
	RowIDs []uint32

	Ints   []int64
	Floats []float64
	Bools  []uint8
	Ends   []uint32
	Data   []byte

	lastRow int32
}

// place records that the next appended value belongs to row.
func (c *Column) place(row int) {
	if c.lastRow == int32(row) {
		// A key repeated inside one document keeps its last value.
		c.pop()
	}
	if c.Sparse {
		c.RowIDs = append(c.RowIDs, uint32(row))
	} else if c.Count != row {
		c.Sparse = true
		c.RowIDs = c.RowIDs[:0]
		for i := 0; i < c.Count; i++ {
			c.RowIDs = append(c.RowIDs, uint32(i))
		}
		c.RowIDs = append(c.RowIDs, uint32(row))
	}
	c.lastRow = int32(row)
	c.Count++
}

func (c *Column) pop() {
	c.Count--
	n := c.Count
	switch c.Kind {
	case vec.KindInt:
		c.Ints = c.Ints[:n]
	case vec.KindFloat:
		c.Floats = c.Floats[:n]
	case vec.KindBool:
		c.Bools = c.Bools[:n]
	case vec.KindString, vec.KindVariant:
		c.Ends = c.Ends[:n]
		c.Data = c.Data[:c.start(n)]
	}
	if c.Sparse {
		c.RowIDs = c.RowIDs[:n]
	}
	c.lastRow = -1
}

func (c *Column) start(i int) uint32 {
	if i == 0 {
		return 0
	}

	return c.Ends[i-1]
}

// Bytes returns the payload of string or variant value i.
func (c *Column) Bytes(i int) []byte { return c.Data[c.start(i):c.Ends[i]] }

// Row returns the row that value i belongs to.
func (c *Column) Row(i int) int {
	if c.Sparse {
		return int(c.RowIDs[i])
	}

	return i
}

func (c *Column) reset() {
	c.Count = 0
	c.Sparse = false
	c.RowIDs = c.RowIDs[:0]
	c.Ints = c.Ints[:0]
	c.Floats = c.Floats[:0]
	c.Bools = c.Bools[:0]
	c.Ends = c.Ends[:0]
	c.Data = c.Data[:0]
	c.lastRow = -1
}

func (c *Column) retained() int {
	return cap(c.RowIDs)*4 + cap(c.Ints)*8 + cap(c.Floats)*8 + cap(c.Bools) + cap(c.Ends)*4 + cap(c.Data)
}
