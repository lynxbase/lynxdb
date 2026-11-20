package part

import (
	"encoding/binary"
	"math"
	"math/bits"
	"slices"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// An anchor is a string column of a block whose values spell numbers, which
// the timed columns of the block may follow whatever form the anchor is
// stored in: a follower keeps the anchor's shape, and reading the follower
// reads the anchor's values and the numbers they spell.

// anchorShape is how the values of an anchor spell their numbers, and the
// bits below the number that a follower follows.
type anchorShape struct {
	form             uint8
	stride, from, to int
	prefix, suffix   []byte
	alphabet         []byte
	radixBits        uint
	headDigits       int
	frac             int
	zone             []byte
	shift            uint
	digit            [256]uint8
}

// take makes a the shape that s found, its bytes copied, followed shift
// bits above the low end of its head numbers.
func (a *anchorShape) take(s *timedScratch, shift uint) {
	a.form, a.stride, a.from, a.to = s.form, s.stride, s.from, s.to
	a.radixBits, a.headDigits, a.frac, a.shift = s.radixBits, s.headDigits, s.frac, shift
	a.prefix, a.suffix, a.alphabet, a.zone = a.prefix[:0], a.suffix[:0], a.alphabet[:0], a.zone[:0]
	if s.form == formStamp {
		a.zone = append(a.zone, s.zone...)
	} else {
		a.prefix = append(a.prefix, s.template[:s.from]...)
		a.suffix = append(a.suffix, s.template[s.to:s.stride]...)
		a.alphabet = append(a.alphabet, s.alphabet...)
	}
	a.digits()
}

func (a *anchorShape) digits() {
	for i := range a.digit {
		a.digit[i] = 0xFF
	}
	for d, b := range a.alphabet {
		a.digit[b] = uint8(d)
	}
}

// appendTo appends the shape as a follower keeps it.
func (a *anchorShape) appendTo(dst []byte) []byte {
	dst = append(dst, a.form)
	if a.form == formStamp {
		dst = append(dst, byte(a.frac), byte(len(a.zone)))
		dst = append(dst, a.zone...)
	} else {
		dst = binary.AppendUvarint(dst, uint64(a.stride))
		dst = binary.AppendUvarint(dst, uint64(a.from))
		dst = binary.AppendUvarint(dst, uint64(a.to))
		dst = append(dst, a.prefix...)
		dst = append(dst, a.suffix...)
		dst = append(dst, byte(len(a.alphabet)))
		dst = append(dst, a.alphabet...)
		dst = append(dst, byte(a.headDigits))
	}

	return append(dst, byte(a.shift))
}

// readAnchorShape reads into a the shape that appendTo wrote and returns
// its bytes, nil when they are not one.
func readAnchorShape(r *reader, a *anchorShape) []byte {
	start := r.buf
	form := r.bytes(1)
	if r.bad || form[0] > formStamp {
		return nil
	}
	a.form, a.prefix, a.suffix, a.alphabet, a.zone = form[0], nil, nil, nil, nil
	a.radixBits, a.headDigits, a.frac = 0, 0, 0
	if a.form == formStamp {
		layout := r.bytes(2)
		if r.bad || layout[0] > 9 || int(layout[1]) > maxStampZone {
			return nil
		}
		a.frac, a.zone = int(layout[0]), r.bytes(uint64(layout[1]))
		if r.bad || !stampsZone(a.zone) {
			return nil
		}
		a.stride = stampDate + len(a.zone)
		if a.frac > 0 {
			a.stride += 1 + a.frac
		}
		a.from, a.to = 0, a.stride
	} else {
		a.stride, a.from, a.to = int(r.uvarint()), int(r.uvarint()), int(r.uvarint())
		if r.bad || a.stride < 2 || a.stride > 255 || a.from >= a.to || a.to > a.stride {
			return nil
		}
		a.prefix, a.suffix = r.bytes(uint64(a.from)), r.bytes(uint64(a.stride-a.to))
		size := r.bytes(1)
		if r.bad {
			return nil
		}
		a.alphabet = r.bytes(uint64(size[0]))
		head := r.bytes(1)
		if r.bad {
			return nil
		}
		maxHead := 19
		switch n := len(a.alphabet); {
		case n == 10:
		case n >= 2 && n <= 64 && n&(n-1) == 0:
			a.radixBits = uint(bits.TrailingZeros(uint(n)))
			maxHead = 64/int(a.radixBits) + 1
		default:
			return nil
		}
		if a.headDigits = int(head[0]); a.headDigits < 1 || a.headDigits > min(a.to-a.from, maxHead) {
			return nil
		}
		a.digits()
	}
	shift := r.bytes(1)
	if r.bad || shift[0] > 63 {
		return nil
	}
	a.shift = uint(shift[0])

	return start[:len(start)-len(r.buf)]
}

// number returns the number that v spells in the shape, shifted, and false
// when v does not have the shape.
func (a *anchorShape) number(v []byte) (int64, bool) {
	if a.form == formStamp {
		x, ok := parseStamp(v, a.frac, a.zone)

		return int64(uint64(x) >> a.shift), ok
	}
	if len(v) != a.stride || string(v[:a.from]) != string(a.prefix) || string(v[a.to:]) != string(a.suffix) {
		return 0, false
	}
	head := v[a.from : a.from+a.headDigits]
	num, bad := uint64(0), uint8(0)
	if a.radixBits == 0 {
		for _, b := range head {
			d := a.digit[b]
			bad |= d
			num = num*10 + uint64(d)
		}
	} else {
		radix := a.radixBits & 7
		for _, b := range head {
			d := a.digit[b]
			bad |= d
			num = num<<radix | uint64(d)
		}
	}
	for _, b := range v[a.from+a.headDigits : a.to] {
		bad |= a.digit[b]
	}
	// A byte outside the alphabet has the digit 0xFF, and digits are below
	// 64; a leading digit may not carry the number past 64 bits.
	if over := int(a.radixBits)*a.headDigits - 64; bad&0x80 != 0 || a.radixBits != 0 && over > 0 && int(a.digit[head[0]])>>(int(a.radixBits)-over) != 0 {
		return 0, false
	}

	return int64(num >> a.shift), true
}

// timedRef is a column of the block being written whose values spell
// numbers, which its timed columns may follow: its shape, the sample that
// chooses who follows whom, and, once a column follows it, the number of
// its value in every row that holds one of the shape.
type timedRef struct {
	path   string
	col    *shred.Column
	shape  anchorShape
	k      int
	sample [timedSample]uint64
	rows   [timedSample]int
	// root is the bits a value of the sample takes followed by nothing but
	// the row time, and parent the column it follows, followNone for none.
	root   int
	parent int
	vals   []int64
	valid  []bool
	filled bool
}

// maxAnchors is the most columns of a block among which every way of
// following is weighed; beyond it a column follows one before it.
const maxAnchors = 5

// findAnchors makes the string columns of the block whose values spell
// numbers ones that its timed columns may follow, and chooses for each
// whom it follows, if anyone, by the fewest bits of them all.
func (w *Writer) findAnchors(cols []*shred.Column, times []int64) {
	s := &w.timedBuf
	refs := w.timedRefs[:0]
	for _, c := range cols {
		if c.Kind != vec.KindString || c.Count < minTimed {
			continue
		}
		k, ok := s.sampleShape(c)
		if !ok {
			continue
		}
		for j := 0; j < k; j++ {
			s.sampleRefs[j] = times[s.sampleRows[j]]
		}
		for sh := range s.lowCost {
			s.lowCost[sh] = -1
		}
		s.tried = s.tried[:0]
		_, shift, timeBits := s.sampleRelation(k, false)
		clear(s.sampleRefs[:k])
		_, _, noneBits := s.sampleRelation(k, true)
		// A follower follows the number above a clock or a counter in the
		// low bits, which no relation to the row times finds when those are
		// far; without one, the number that follows them best.
		if low := s.lowStructure(k); low > 0 {
			shift = low
		}
		if len(refs) == cap(refs) {
			refs = append(refs, timedRef{})
		} else {
			refs = refs[:len(refs)+1]
		}
		r := &refs[len(refs)-1]
		r.path, r.col, r.k, r.parent, r.filled = c.Path, c, k, followNone, false
		r.root = min(timeBits, noneBits, s.headPacked)
		copy(r.sample[:k], s.sample[:k])
		copy(r.rows[:k], s.sampleRows[:k])
		r.shape.take(s, shift)
	}
	w.timedRefs = refs
	m := len(refs)
	if m < 2 {
		return
	}
	cost := grow(w.anchorCost, m*m)
	for i := range refs {
		// The bits of a follower's low bits do not depend on whom it
		// follows, nor do the shifts tried without a reference.
		for sh := range s.lowCost {
			s.lowCost[sh] = -1
		}
		s.tried = s.tried[:0]
		for j := range refs {
			cost[i*m+j] = 1 << 30
			if i != j {
				cost[i*m+j] = s.followCost(&refs[i], &refs[j])
			}
		}
	}
	w.anchorCost = cost
	chooseParents(refs, cost)
	for i := range refs {
		if p := refs[i].parent; p >= 0 {
			refs[p].fill(len(times))
		}
	}
}

// lowStructure returns the most low bits of the distinct ones among the
// first k head numbers of the sample that hold an identifier of few values,
// as a clock does: bits in which the numbers take far fewer distinct values
// than they could and than there are numbers, and that the bit below them
// hardly adds to; zero when there are none.
func (s *timedScratch) lowStructure(k int) uint {
	heads := append(s.heldHeads[:0], s.sample[:k]...)
	slices.Sort(heads)
	heads = slices.Compact(heads)
	distinct := func(sh uint) int {
		tmp := s.lows[:len(heads)]
		for j, n := range heads {
			tmp[j] = int64(n & (1<<sh - 1))
		}
		slices.Sort(tmp)

		return len(slices.Compact(tmp))
	}
	above := distinct(32)
	for sh := uint(31); sh > 0; sh-- {
		d := distinct(sh)
		if 4*above <= 3*len(heads) && 8*above <= 1<<(sh+1) && 8*above <= 9*d {
			return sh + 1
		}
		above = d
	}

	return 0
}

// followCost returns the bits that a value of f's sample takes following a,
// an exception's for those that a holds no number for.
func (s *timedScratch) followCost(f, a *timedRef) int {
	held := 0
	for j := 0; j < f.k; j++ {
		if v, ok := valueAt(a.col, f.rows[j]); ok {
			if n, ok := a.shape.number(v); ok {
				s.sample[held], s.sampleRefs[held] = f.sample[j], n
				held++
			}
		}
	}
	if held < f.k-f.k/maxIrregularShare {
		return 1 << 30
	}
	_, _, b := s.sampleRelation(held, false)

	return b + (f.k-held)*exceptionBits/f.k
}

// chooseParents sets whom every column of refs follows, by the fewest bits
// over them all that cost gives, column by column against each other, so
// that no column follows itself however far and none follows more than
// maxTimedDepth columns one through another.
func chooseParents(refs []timedRef, cost []int) {
	m := len(refs)
	if m > maxAnchors {
		// links counts the columns that each one follows one through
		// another.
		links := make([]int, m)
		for i := range refs {
			best, c := followNone, refs[i].root
			for j := 0; j < i; j++ {
				if links[j] < maxTimedDepth && cost[i*m+j] < c {
					best, c = j, cost[i*m+j]
				}
			}
			refs[i].parent = best
			if best >= 0 {
				links[i] = links[best] + 1
			}
		}

		return
	}
	var parent, best [maxAnchors]int
	bestTotal := math.MaxInt
	var try func(i, total int)
	try = func(i, total int) {
		if total >= bestTotal {
			return
		}
		if i == m {
			if bounded(parent[:m]) {
				bestTotal = total
				best = parent
			}

			return
		}
		parent[i] = followNone
		try(i+1, total+refs[i].root)
		for j := 0; j < m; j++ {
			if j != i && cost[i*m+j] < 1<<30 {
				parent[i] = j
				try(i+1, total+cost[i*m+j])
			}
		}
	}
	try(0, 0)
	for i := range refs {
		refs[i].parent = best[i]
	}
}

// bounded reports whether following parents from any column ends within
// maxTimedDepth of them.
func bounded(parent []int) bool {
	for i := range parent {
		at := i
		for links := 0; parent[at] >= 0; links++ {
			if links == maxTimedDepth {
				return false
			}
			at = parent[at]
		}
	}

	return true
}

// fill reads the number of the anchor's value in each of the rows.
func (r *timedRef) fill(rows int) {
	if r.filled {
		return
	}
	r.vals, r.valid = grow(r.vals, rows), grow(r.valid, rows)
	clear(r.valid)
	c := r.col
	for i := 0; i < c.Count; i++ {
		if n, ok := r.shape.number(c.Bytes(i)); ok {
			row := c.Row(i)
			r.vals[row], r.valid[row] = n, true
		}
	}
	r.filled = true
}

// valueAt returns the value of c in row, and false when it holds none.
func valueAt(c *shred.Column, row int) ([]byte, bool) {
	if !c.Sparse {
		if row >= c.Count {
			return nil, false
		}

		return c.Bytes(row), true
	}
	lo, hi := 0, len(c.RowIDs)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if int(c.RowIDs[mid]) < row {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == len(c.RowIDs) || int(c.RowIDs[lo]) != row {
		return nil, false
	}

	return c.Bytes(lo), true
}

// anchorSpace holds what decoding the columns of a block takes beyond the
// columns: the scratch of decoding, and the numbers of the anchors that the
// block's owner has read with the scratch and the column that reading one
// takes at each depth of following, which stay where they are however deep
// a read goes.
type anchorSpace struct {
	d       decodeScratch
	owner   *Block
	anchors []anchorRead
	scratch [maxTimedDepth + 1]decodeScratch
	cols    [maxTimedDepth + 1]vec.Column
}

// anchorSpace returns the space the block reads anchors with.
func (b *Block) anchorSpace() *anchorSpace {
	if b.space != nil {
		return b.space
	}

	return &b.ownSpace
}

// share makes the block decode with the scratch and space of s, which other
// blocks that decode one at a time share.
func (b *Block) share(s *anchorSpace) {
	b.space, b.d = s, &s.d
}

// anchorRead holds the numbers by row of an anchor of the block, read for a
// follower and kept for the others.
type anchorRead struct {
	pathID uint32
	shape  []byte
	vals   []int64
	valid  vec.Bitmap
}

// anchorNumbers returns the numbers by row of the anchor with the path id
// that a follower at depth reads in the shape whose bytes raw are, reading
// them the first time with scratch of their own.
func (b *Block) anchorNumbers(pathID uint32, shape *anchorShape, raw []byte, depth int) (*anchorRead, error) {
	space := b.anchorSpace()
	if space.owner != b {
		space.owner, space.anchors = b, space.anchors[:0]
	}
	for i := range space.anchors {
		if a := &space.anchors[i]; a.pathID == pathID && string(a.shape) == string(raw) {
			return a, nil
		}
	}
	ref, ok := b.Column(pathID, vec.KindString)
	if !ok || ref.bad || depth > maxTimedDepth {
		return nil, ErrCorrupt
	}
	col := &space.cols[depth]
	// The shape may lie in the scratch that reading the anchor swaps out.
	want := *shape
	shape = &want
	held, outer := b.d, b.depth
	b.d, b.depth = &space.scratch[depth], depth
	// A timed anchor of the shape gives its numbers as they are read; any
	// other is read as strings, whose numbers are parsed.
	v := &b.d.timed
	timed := false
	var err error
	if ref.enc == encTimed {
		if err = b.readTimed(ref, v, depth); err == nil {
			if timed = shape.matches(v); timed {
				err = v.resolve()
			}
		}
	}
	if err == nil && !timed {
		err = b.decode(ref, col, false)
	}
	var a *anchorRead
	if err == nil {
		if len(space.anchors) == cap(space.anchors) {
			space.anchors = append(space.anchors, anchorRead{})
		} else {
			space.anchors = space.anchors[:len(space.anchors)+1]
		}
		a = &space.anchors[len(space.anchors)-1]
		a.pathID, a.shape = pathID, raw
		a.vals = grow(a.vals, b.Rows)
		a.valid = grow(a.valid, vec.BitmapWords(b.Rows))
		clear(a.valid)
		if timed {
			a.fromTimed(v, shape, ref.Count)
		} else {
			for row := 0; row < b.Rows; row++ {
				if col.IsValid(row) {
					if n, ok := shape.number(col.Bytes(row)); ok {
						a.vals[row] = n
						a.valid.Set(row)
					}
				}
			}
		}
	}
	b.d, b.depth = held, outer
	if err != nil {
		return nil, err
	}

	return a, nil
}

// fromTimed fills a from the count values of a timed chunk read into v
// whose shape is shape: the numbers that the chunk holds, those that its
// values kept as strings spell, and those of the values before runs again.
func (a *anchorRead) fromTimed(v *timedValues, shape *anchorShape, count int) {
	mask := uint64(1)<<v.shift - 1
	j, run, left, next := 0, 0, 0, 0
	last, ok := int64(0), false
	for i := 0; i < count; i++ {
		row := i
		if v.allRows != nil {
			row = int(v.allRows[i])
		}
		if left == 0 && run < len(v.runStart) && int(v.runStart[run]) == i {
			left = int(v.runLen[run])
			run++
		}
		if left > 0 {
			left--
		} else {
			if next < len(v.excIndex) && int(v.excIndex[next]) == j {
				last, ok = shape.number(v.excValue[next])
				next++
			} else {
				head := uint64(v.parts[j]) << v.shift
				if v.shift > 0 {
					head |= uint64(v.low[j]) & mask
				}
				last, ok = int64(head>>shape.shift), true
			}
			j++
		}
		if ok {
			a.vals[row] = last
			a.valid.Set(row)
		}
	}
}

// matches reports whether the values of the timed chunk read into v have
// the shape.
func (a *anchorShape) matches(v *timedValues) bool {
	if a.form != v.form {
		return false
	}
	if a.form == formStamp {
		return a.frac == v.frac && string(a.zone) == string(v.zone)
	}

	return a.stride == v.stride && a.from == v.from && a.to == v.to && a.headDigits == v.headDigits &&
		string(a.prefix) == string(v.prefix) && string(a.suffix) == string(v.suffix) && string(a.alphabet) == string(v.alphabet)
}
