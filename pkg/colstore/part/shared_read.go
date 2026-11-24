package part

import (
	"slices"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// sharedAhead makes a block ask for its dictionary whole when its strings,
// each on a page of its own at worst, may lie on more than one page in this
// many of the dictionary's.
const sharedAhead = 4

// Shared reports whether the block stores the column as codes of the part
// dictionary of its path, which Part.Dictionary returns. Decode turns the
// codes into strings; DecodeShared returns them as they are.
func (c *ColumnRef) Shared() bool { return !c.bad && c.enc == encShared }

// DecodeShared reads a column that the block stores as codes of its part
// dictionary without reading a string: it returns in codes, whose memory it
// reuses, the code of every value in the order of the rows, and fills dst
// with which rows hold a value, as DecodePresence does. Equal codes of one
// part are equal strings, the codes order as the bytes of their strings do,
// and all are below the Len of the dictionary, so that values can be
// counted, grouped and compared by code inside a part; codes of different
// parts have nothing in common.
func (b *Block) DecodeShared(ref *ColumnRef, dst *vec.Column, codes []uint32) ([]uint32, error) {
	if !ref.Shared() {
		return codes, ErrCorrupt
	}
	d, ok := b.part.Dictionary(ref.PathID)
	chunk := b.data[ref.off : ref.off+ref.length]
	if !ok || !b.checked(ref, chunk) {
		return codes, ErrCorrupt
	}
	dst.Kind, dst.Rows, dst.Layout, dst.Valid = ref.Kind, b.Rows, vec.Flat, nil
	if ref.flags&flagSparse != 0 {
		var err error
		if chunk, err = b.readPresence(chunk, ref.Count, dst); err != nil {
			return codes, err
		}
	}
	if codes, ok = readSharedCodes(codes, chunk, ref.Count, uint64(d.codes)); !ok {
		return codes, ErrCorrupt
	}

	return codes, nil
}

// decodeShared fills dst with the strings of a shared column, whose codes
// chunk holds.
func (b *Block) decodeShared(ref *ColumnRef, chunk []byte, sparse bool, rows int, dst *vec.Column) error {
	d, ok := b.part.Dictionary(ref.PathID)
	if !ok {
		return ErrCorrupt
	}
	codes, ok := readSharedCodes(b.d.codes32, chunk, ref.Count, uint64(d.codes))
	b.d.codes32 = codes
	if !ok {
		return ErrCorrupt
	}
	if uint64(ref.Count)*dictPage*sharedAhead >= d.dataLen {
		d.ahead()
	}

	return d.fill(dst, codes, sparse, rows)
}

// fill makes dst hold the strings of codes: one for every row of rows, or
// with sparse set one for every row that dst.Valid marks.
func (d *Dictionary) fill(dst *vec.Column, codes []uint32, sparse bool, rows int) error {
	if need := len(codes) * d.reader.stride; cap(dst.Data) < need {
		dst.Data = make([]byte, 0, need)
	}
	data := dst.Data[:0]
	dst.Offsets = grow(dst.Offsets, rows+1)
	offsets := dst.Offsets
	// The string of the code before lies from from to to.
	prev, from, to := ^uint32(0), 0, 0
	next := 0
	for r := 0; r < rows; r++ {
		offsets[r] = uint32(len(data))
		if sparse && !dst.Valid.Get(r) {
			continue
		}
		code := codes[next]
		next++
		start := len(data)
		if code == prev {
			data = append(data, data[from:to]...)
		} else {
			var err error
			if data, err = d.Value(data, code); err != nil {
				return err
			}
		}
		prev, from, to = code, start, len(data)
	}
	if len(data) > maxSectionSize {
		return ErrCorrupt
	}
	offsets[rows] = uint32(len(data))
	dst.Data, dst.Layout = data, vec.Flat

	return nil
}

// DecodeRows is Decode for the rows listed, in any order: the other rows of
// dst hold an unspecified value. Of a shared column it reads the strings of
// those rows and no others, so that a few rows cost a few pages of the
// dictionary and not most of them.
func (b *Block) DecodeRows(ref *ColumnRef, rows []uint32, dst *vec.Column) error {
	if !ref.Shared() {
		return b.Decode(ref, dst)
	}
	d, _ := b.part.Dictionary(ref.PathID)
	codes, err := b.DecodeShared(ref, dst, b.d.codes32)
	b.d.codes32 = codes
	if err != nil {
		return err
	}
	dst.Codes8, dst.Codes16 = dst.Codes8[:0], dst.Codes16[:0]
	picked := append(b.pos[:0], rows...)
	slices.Sort(picked)
	b.pos = picked
	data := dst.Data[:0]
	dst.Offsets = grow(dst.Offsets, b.Rows+1)
	offsets := dst.Offsets
	at := 0
	for _, row := range picked {
		r := int(row)
		if r >= b.Rows || r < at {
			continue
		}
		for ; at <= r; at++ {
			offsets[at] = uint32(len(data))
		}
		if !dst.IsValid(r) {
			continue
		}
		value := r
		if dst.Valid != nil {
			value = dst.Valid.Rank(r)
		}
		if data, err = d.Value(data, codes[value]); err != nil {
			return err
		}
	}
	for ; at <= b.Rows; at++ {
		offsets[at] = uint32(len(data))
	}
	dst.Data = data

	return nil
}

// findShared is FindEqual for a shared column: it looks want up in the
// dictionary and the code it has there in the codes of the block.
func (b *Block) findShared(ref *ColumnRef, want []byte, dst []uint32) ([]uint32, bool, error) {
	d, ok := b.part.Dictionary(ref.PathID)
	if !ok {
		return dst, false, ErrCorrupt
	}
	code, found, err := d.Find(want)
	if err != nil || !found {
		return dst, err == nil, err
	}
	codes, err := b.DecodeShared(ref, &b.present, b.d.codes32)
	b.d.codes32 = codes
	if err != nil {
		return dst, false, err
	}
	from := len(dst)
	for i, c := range codes {
		if c == code {
			dst = append(dst, uint32(i))
		}
	}
	if b.present.Valid == nil || len(dst) == from {
		return dst, true, nil
	}
	// The values found are numbered among the rows that hold one.
	rows := b.positions(b.present.Valid)
	for i, v := range dst[from:] {
		dst[from+i] = rows[v]
	}

	return dst, true, nil
}
