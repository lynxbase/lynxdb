package vec

// SetPresence makes w tell which of the rows from to to of c hold a value,
// for a column whose values were not decoded; Valid is nil when all of them
// do.
func (w *Window) SetPresence(c *Column, from, to int) {
	n := to - from
	w.Column = Column{Kind: c.Kind, Layout: Flat, Rows: n}
	if c.Valid != nil && c.Valid.Rank(to)-c.Valid.Rank(from) != n {
		w.valid = copyBits(w.valid, c.Valid, from, n)
		w.Valid = w.valid
	}
}
