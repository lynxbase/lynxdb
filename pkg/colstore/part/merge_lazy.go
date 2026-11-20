package part

import (
	"encoding/binary"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// lazyColumn gives the values of a column of a merge cursor's block as the
// merge writes them, read from the chunk where the part lies instead of
// decoded whole into a buffer of the block's size.
type lazyColumn struct {
	packed  packedReader
	scratch packScratch
}

// put appends values at to at+n-1 of the column to oc, at rows from row on.
func (l *lazyColumn) put(oc *shred.Column, at, row, n int) {
	l.packed.put(oc.ReserveRun(row, n, l.packed.stride), at, at+n)
}

// openLazy readies column j of the cursor's block to be read lazily and
// reports whether its chunk is one that is; its column then holds only the
// presence of its values.
func (c *mergeCursor) openLazy(j int) (bool, error) {
	ref := &c.refs[j]
	if ref.Kind != vec.KindString || ref.enc != encFlat || ref.bad {
		return false, nil
	}
	chunk := c.blk.data[ref.off : ref.off+ref.length]
	body, rawLen, stride, ok := packedBody(chunk, ref.flags&flagSparse != 0)
	if !ok {
		return false, nil
	}
	col := c.cols[j]
	col.release()
	if err := c.blk.DecodePresence(ref, &col.Column); err != nil {
		return false, err
	}
	if c.lazyUsed == len(c.lazyBufs) {
		c.lazyBufs = append(c.lazyBufs, new(lazyColumn))
	}
	l := c.lazyBufs[c.lazyUsed]
	c.lazyUsed++
	if err := l.packed.open(body, rawLen, &l.scratch); err != nil {
		return false, err
	}
	if l.packed.count != ref.Count || l.packed.stride != stride {
		return false, ErrCorrupt
	}
	c.lazy[j] = l

	return true, nil
}

// packedBody returns the packed section of a flat chunk of strings of one
// length, its size once unpacked and that length, and false for a chunk of
// any other strings.
func packedBody(chunk []byte, sparse bool) (body []byte, rawLen, stride int, ok bool) {
	if sparse {
		if len(chunk) == 0 {
			return nil, 0, 0, false
		}
		if chunk, _, ok = skipBlob(chunk[1:]); !ok {
			return nil, 0, 0, false
		}
	}
	// The lengths of the values, one for them all.
	if len(chunk) == 0 || chunk[0] != intConst {
		return nil, 0, 0, false
	}
	length, n := binary.Uvarint(chunk[1:])
	if n <= 0 {
		return nil, 0, 0, false
	}
	chunk = chunk[1+n:]
	head, n := binary.Uvarint(chunk)
	if n <= 0 || head&3 != blobPacked {
		return nil, 0, 0, false
	}
	size := head >> 2
	raw, m := binary.Uvarint(chunk[n:])
	if m <= 0 || size > uint64(len(chunk)-n-m) || raw > maxSectionSize {
		return nil, 0, 0, false
	}

	return chunk[n+m : n+m+int(size)], int(raw), int(unzigzag(length)), true
}
