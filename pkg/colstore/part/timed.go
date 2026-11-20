package part

import (
	"encoding/binary"
	"math/bits"
	"slices"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// A timed chunk holds strings most of which have one length, share their
// bytes outside a span, and spell inside it a number in an alphabet of a
// power of two of symbols or of ten, digit order being byte order, whose
// leading digits follow a reference of their row: the head number, the
// span's first digits, shifted right by some bits, less the reference in a
// unit of a power of ten, takes few bits. Such are keys that begin with
// their time and end with a counter or noise. The reference is nothing, the
// time of the row in nanoseconds, or the number that the row's value of
// another column of the block spells, whatever form that column is stored
// in, named in the chunk with the shape its values have; the head numbers
// of a block are read as integers whatever their reference, which packed
// digits cannot, and with exceptions, which they lack. The chunk keeps
// the differences and the shifted-out bits as integer sections, the digits
// past the head as more, and every other string as it is, as an exception;
// a value that fits but whose difference lies far from the others' keeps
// its difference among the exceptions instead.
// Decoding it reads the time column of its block and the column it
// follows: the integer sections of a timed one, the strings of any other,
// whose numbers it parses. The strings may also be
// stamps, times in one RFC 3339 layout, whose head number is their time;
// see stamps.go.
//
//	form        byte, formDigits or formStamp; a stamp's chunk then holds
//	            the fraction digits and the zone, a byte for its length and
//	            its bytes, in place of all up to the unit
//	stride      uvarint, the length of the strings
//	from, to    uvarint, the span
//	template    the from bytes before the span and the stride-to after it
//	alphabet    count byte, then the symbols in digit order
//	head        byte, the digits of the head number
//	unit, shift bytes, the unit's power of ten and the bits shifted out
//	reference   byte, refNone, refTime or refColumn, then for a column
//	            the uvarint length and the path of the column followed and
//	            the shape of its values: the form, then the stride, span,
//	            template, alphabet and head digits of digits or the
//	            fraction digits and zone of stamps, as above, and a byte of
//	            the bits below the number followed
//	runs        uvarint count, then for each run of values equal to the
//	            one before them the uvarint of the values from the end of
//	            the last run to its start times four plus its length less
//	            one, three for four or more, and for those the uvarint of
//	            the length less four; the values that begin no run are the
//	            ones that the exceptions and the sections below hold
//	exceptions  uvarint count, then for each the uvarint number of values
//	            since the last and either the uvarint length plus one and
//	            the bytes of the string, or a zero and the zigzag uvarint
//	            of a far difference
//	differences integer section, one a value, an exception's that of the
//	            value before it
//	low         when shift is set, the shifted-out bits: a byte, lowPlain or
//	            lowDict, then their frames, or the integer section of their
//	            distinct values in order and the frames of their numbers
//	tail        an integer section for every tailDigits digits past the head
const (
	// minTimed is the fewest values that a timed chunk is tried for.
	minTimed = 64
	// timedSample is the number of values the shape is judged on.
	timedSample = 64
	// maxExceptionShare bounds the values whose differences lie far from
	// the others', one in this many, beyond which the rest stay in their
	// frames.
	maxExceptionShare = 16
	// rareShare is the share, one in this many values, under which a byte
	// at a position or in the digits is taken for an exception's.
	rareShare = 16
	// maxIrregularShare bounds the values that do not fit the shape or lack
	// a reference, one in this many, which are exceptions whatever they
	// cost.
	maxIrregularShare = 4
	// minTimedGain is the bits a value is to save over its digits for the
	// form to be worth its tables.
	minTimedGain = 2
	// maxTimedDepth bounds the columns a timed column follows one through
	// another: the writer makes no longer chain, and the reader refuses one.
	maxTimedDepth = 8
)

// The forms of the shifted-out bits of a timed chunk.
const (
	lowPlain = 0
	lowDict  = 1
	// maxLowDict is the most distinct shifted-out values that a dictionary
	// is made of, and maxLowDictBits the most bits shifted out for one.
	maxLowDict     = 256
	maxLowDictBits = 16
)

// The forms of the strings of a timed chunk.
const (
	formDigits = 0
	formStamp  = 1
)

// The references of a timed chunk.
const (
	refNone   = 0
	refTime   = 1
	refColumn = 2
)

// The candidates of follow for no reference and for the row time; others
// number the columns of the block that may be followed.
const (
	followNone = -2
	followTime = -1
)

// timedShape is the shape that the values of a timed chunk share.
type timedShape struct {
	form             uint8
	stride, from, to int
	alphabet         []byte
	// radixBits is the bits of a digit, zero for decimal digits.
	radixBits  uint
	headDigits int
	// frac and zone are the layout of stamps.
	frac  int
	zone  []byte
	unit  uint8
	shift uint
}

// tailDigits returns the digits past the head that one integer section of
// the tail holds.
func (h *timedShape) tailDigits() int {
	if h.radixBits == 0 {
		return 18
	}

	return 63 / int(h.radixBits)
}

// timedScratch holds what encoding a timed chunk reuses between columns.
type timedScratch struct {
	timedShape
	template   []byte
	digit      [256]uint8
	misses     []int32
	sample     [timedSample]uint64
	sampleRefs [timedSample]int64
	trial      [timedSample]int64
	unitRefs   [timedSample]int64
	lows       [timedSample]int64
	picked     [timedSample]int
	heldHeads  [timedSample]uint64
	sampleRows [timedSample]int
	heads      []uint64
	regular    []bool
	refs       []int64
	refOK      []bool
	diffs, low []int64
	tail       []int64
	exceptions []int32
	far        []bool
	farDiffs   []int64
	lowDict    []int64
	lowCodes   []int64
	lowCode    []int32
	lowCost    [64]int8
	tried      []uint
	// self is the column's place among the anchors, -1 when it is none, and
	// allowed the anchor it may follow, followNone for none.
	self, allowed int
	// runs holds the first value and the length of every run of values
	// equal to the one before them, and distinctCol the values that begin
	// no run, which values is when there are runs and the column otherwise.
	runs        []int32
	distinctCol shred.Column
	values      *shred.Column
	// best, unit, shift and cost are what the sample found: the reference,
	// followNone, followTime or a column that may be followed, and the
	// bits a value takes against it.
	best      int
	cost      int
	refKind   uint8
	reference string
	// forced makes only the one reference the codec may take.
	forced bool
	only   int
	// reason tells why the last column was not timed; lowDictSize and
	// sections tell of the last one that was: the bytes of its head, its
	// differences, its low bits and its tail.
	reason      string
	lowDictSize int
	sections    [4]int
	// headPacked is the bits that packing the head digits would take, by
	// the ranges of their digits.
	headPacked   int
	irregular    int
	refIrregular int
}

// unitDiv returns t in units of 10^power of its own.
func unitDiv(t int64, power uint8) int64 {
	switch power {
	case 3:
		return t / 1e3
	case 6:
		return t / 1e6
	case 9:
		return t / 1e9
	}

	return t
}

var timedUnits = [...]uint8{0, 3, 6, 9}

// appendTimed appends the column, the rows of whose batch have times, as a
// timed chunk and reports whether it did; when it did not, w.block is
// unchanged.
func (w *Writer) appendTimed(c *shred.Column, times []int64) bool {
	s := &w.timedBuf
	n := c.Count
	s.reason, s.lowDictSize, s.sections = "", 0, [4]int{}
	if n < minTimed {
		s.reason = "few values"

		return false
	}
	s.self, s.allowed = -1, followNone
	for i := range w.timedRefs {
		if w.timedRefs[i].col == c {
			s.self, s.allowed = i, w.timedRefs[i].parent
		}
	}
	all := n
	c = s.distinct(c)
	s.values, n = c, c.Count
	if !s.promising(c, times, w.timedRefs) {
		return false
	}
	if s.form == formStamp {
		s.scanStamps(c)
	} else {
		s.scan(c)
	}
	if s.irregular > n/maxIrregularShare {
		s.reason = "too many values off the shape"

		return false
	}
	// The reference that the sample found of the fewest bits.
	best := s.best
	s.refs, s.refOK = grow(s.refs, n), grow(s.refOK, n)
	if !s.follow(c, times, w.timedRefs, best) {
		s.reason = "too many values without the reference"

		return false
	}
	s.refKind, s.reference = refNone, ""
	switch {
	case best == followTime:
		s.refKind = refTime
	case best >= 0:
		s.refKind, s.reference = refColumn, w.timedRefs[best].path
	}
	s.diffs, s.low = grow(s.diffs, n), grow(s.low, n)
	shift, unit := s.shift&63, s.unit
	mask := uint64(1)<<shift - 1
	for i, head := range s.heads[:n] {
		s.diffs[i] = int64(head>>shift) - unitDiv(s.refs[i], unit)
		s.low[i] = int64(head & mask)
	}
	s.pickExceptions(n)
	if len(s.exceptions) == n {
		s.reason = "all values exceptions"

		return false
	}

	start := len(w.block)
	w.block = append(w.block, s.form)
	if s.form == formStamp {
		w.block = append(w.block, byte(s.frac), byte(len(s.zone)))
		w.block = append(w.block, s.zone...)
		w.block = append(w.block, s.unit, byte(s.shift))
	} else {
		w.block = binary.AppendUvarint(w.block, uint64(s.stride))
		w.block = binary.AppendUvarint(w.block, uint64(s.from))
		w.block = binary.AppendUvarint(w.block, uint64(s.to))
		w.block = append(w.block, s.template[:s.from]...)
		w.block = append(w.block, s.template[s.to:]...)
		w.block = append(w.block, byte(len(s.alphabet)))
		w.block = append(w.block, s.alphabet...)
		w.block = append(w.block, byte(s.headDigits), s.unit, byte(s.shift))
	}
	w.block = append(w.block, s.refKind)
	if s.refKind == refColumn {
		w.block = binary.AppendUvarint(w.block, uint64(len(s.reference)))
		w.block = append(w.block, s.reference...)
		w.block = w.timedRefs[best].shape.appendTo(w.block)
	}
	w.block = binary.AppendUvarint(w.block, uint64(len(s.runs)/2))
	for k, end := 0, 0; k < len(s.runs); k += 2 {
		at, length := int(s.runs[k]), int(s.runs[k+1])
		short := min(length-1, 3)
		w.block = binary.AppendUvarint(w.block, uint64(at-end)<<2|uint64(short))
		if short == 3 {
			w.block = binary.AppendUvarint(w.block, uint64(length-4))
		}
		end = at + length
	}
	w.block = binary.AppendUvarint(w.block, uint64(len(s.exceptions)))
	prev := -1
	for k, e := range s.exceptions {
		w.block = binary.AppendUvarint(w.block, uint64(int(e)-prev-1))
		prev = int(e)
		if s.far[k] {
			w.block = append(w.block, 0)
			w.block = binary.AppendUvarint(w.block, zigzag(s.farDiffs[k]))

			continue
		}
		v := c.Bytes(int(e))
		w.block = binary.AppendUvarint(w.block, uint64(len(v))+1)
		w.block = append(w.block, v...)
	}
	s.sections[0] = len(w.block) - start
	w.block = appendInts(w.block, s.diffs[:n], &w.blobs, &w.e)
	s.sections[1] = len(w.block) - start - s.sections[0]
	if s.shift > 0 {
		w.block = s.appendLow(w.block, n, &w.blobs, &w.e)
	}
	s.sections[2] = len(w.block) - start - s.sections[0] - s.sections[1]
	s.tail = grow(s.tail, n)
	for at := s.from + s.headDigits; s.form == formDigits && at < s.to; at += s.tailDigits() {
		end := min(at+s.tailDigits(), s.to)
		for i := 0; i < n; i++ {
			s.tail[i] = 0
			if s.regular[i] {
				s.tail[i] = int64(s.number(c.Bytes(i)[at:end]))
			}
		}
		w.block = appendInts(w.block, s.tail[:n], &w.blobs, &w.e)
	}
	if len(w.block)-start >= s.stride*all {
		w.block = w.block[:start]
		s.reason = "larger than the strings"

		return false
	}

	return true
}

// runShare is the share of values, one in this many, that must repeat the
// one before them for runs to be kept apart from the other values.
const runShare = 64

// distinct lists in s.runs the runs of the column's values equal to the one
// before them and returns the column of the values that begin none, with
// their rows, or c itself when few values repeat.
func (s *timedScratch) distinct(c *shred.Column) *shred.Column {
	s.runs = s.runs[:0]
	n, repeats := c.Count, 0
	for i := 1; i < n; i++ {
		if string(c.Bytes(i)) == string(c.Bytes(i-1)) {
			repeats++
		}
	}
	if repeats == 0 || repeats*runShare < n {
		return c
	}
	d := &s.distinctCol
	d.Path, d.Kind, d.Count, d.Sparse = c.Path, c.Kind, 0, true
	d.Data, d.Ends, d.RowIDs = d.Data[:0], d.Ends[:0], d.RowIDs[:0]
	for i := 0; i < n; {
		v := c.Bytes(i)
		d.Data = append(d.Data, v...)
		d.Ends = append(d.Ends, uint32(len(d.Data)))
		d.RowIDs = append(d.RowIDs, uint32(c.Row(i)))
		d.Count++
		j := i + 1
		for j < n && string(c.Bytes(j)) == string(v) {
			j++
		}
		if j > i+1 {
			s.runs = append(s.runs, int32(i+1), int32(j-i-1))
		}
		i = j
	}

	return d
}

// appendLow appends the shifted-out bits of the n values: as numbers in a
// dictionary of their distinct values when there are few, and as they are
// otherwise. Neither goes through the general codec, which costs more time
// than it saves on bits that are a counter, a clock or noise.
func (s *timedScratch) appendLow(dst []byte, n int, bc *blobCodec, e *encodeScratch) []byte {
	if s.shift > maxLowDictBits {
		dst = append(dst, lowPlain, intFrames)

		return appendFrames(dst, s.low[:n], false)
	}
	// The number of a value is its place among the distinct ones.
	codes := grow(s.lowCode, 1<<s.shift)
	clear(codes)
	for _, v := range s.low[:n] {
		codes[v] = 1
	}
	s.lowDict = s.lowDict[:0]
	for v, seen := range codes {
		if seen != 0 {
			codes[v] = int32(len(s.lowDict))
			s.lowDict = append(s.lowDict, int64(v))
		}
	}
	s.lowCode = codes
	if len(s.lowDict) > maxLowDict || len(s.lowDict) == 1 {
		dst = append(dst, lowPlain, intFrames)

		return appendFrames(dst, s.low[:n], false)
	}
	s.lowDictSize = len(s.lowDict)
	dst = append(dst, lowDict)
	dst = binary.AppendUvarint(dst, uint64(len(s.lowDict)))
	dst = appendInts(dst, s.lowDict, bc, e)
	s.lowCodes = grow(s.lowCodes, n)
	for i, v := range s.low[:n] {
		s.lowCodes[i] = int64(codes[v])
	}
	dst = append(dst, intFrames)

	return appendFrames(dst, s.lowCodes[:n], false)
}

// follow fills s.refs with the reference of every value, nothing for k of
// followNone, the row time for followTime and the values of refs[k]
// otherwise, and reports whether the values without one, which become
// exceptions, are few.
func (s *timedScratch) follow(c *shred.Column, times []int64, refs []timedRef, k int) bool {
	n := c.Count
	s.refIrregular = 0
	switch k {
	case followNone:
		clear(s.refs[:n])
		for i := range n {
			s.refOK[i] = true
		}
	case followTime:
		for i := range n {
			s.refs[i], s.refOK[i] = times[c.Row(i)], true
		}
	default:
		vals, valid := refs[k].vals, refs[k].valid
		for i := range n {
			row := c.Row(i)
			s.refs[i], s.refOK[i] = vals[row], valid[row]
			if !valid[row] && s.regular[i] {
				s.refIrregular++
			}
		}
	}

	return s.irregular+s.refIrregular <= n/maxIrregularShare
}

// pickExceptions lists the values of the n that the chunk keeps as they
// are: those that do not fit the shape or lack a reference, and those whose
// differences lie far from the sample's while they are few. Each takes the
// difference and the low bits of the value before it, so that it widens no
// frame.
func (s *timedScratch) pickExceptions(n int) {
	lo, hi := s.window(n)
	s.exceptions, s.far, s.farDiffs = s.exceptions[:0], s.far[:0], s.farDiffs[:0]
	for i := 0; i < n; i++ {
		switch fits := s.regular[i] && s.refOK[i]; {
		case !fits:
			s.exceptions, s.far, s.farDiffs = append(s.exceptions, int32(i)), append(s.far, false), append(s.farDiffs, 0)
		case s.diffs[i] < lo || s.diffs[i] > hi:
			s.exceptions, s.far, s.farDiffs = append(s.exceptions, int32(i)), append(s.far, true), append(s.farDiffs, s.diffs[i])
		}
	}
	if len(s.exceptions) == 0 {
		return
	}
	// The values before the first one kept take its difference and low
	// bits; a far value keeps its low bits.
	first := -1
	for i := 0; i < n && first < 0; i++ {
		if s.regular[i] && s.refOK[i] && s.diffs[i] >= lo && s.diffs[i] <= hi {
			first = i
		}
	}
	if first < 0 {
		return
	}
	prevDiff, prevLow := s.diffs[first], s.low[first]
	for i, next := 0, 0; i < n; i++ {
		if next < len(s.exceptions) && int(s.exceptions[next]) == i {
			s.diffs[i] = prevDiff
			if !s.far[next] {
				s.low[i] = prevLow
			}
			next++

			continue
		}
		prevDiff, prevLow = s.diffs[i], s.low[i]
	}
}

// farBits is about what a far difference of bits bits costs among the
// exceptions: its place, its tag and its bytes.
func farBits(bits int) int { return 8 * (2 + (bits+7)/7) }

// promising reports whether an even sample of the column's values, whose
// times are those of their batch's rows, has a shape and follows nothing,
// the times or the anchor of refs it may closely enough for the whole
// column to be looked at.
func (s *timedScratch) promising(c *shred.Column, times []int64, refs []timedRef) bool {
	k, ok := s.sampleShape(c)
	if !ok {
		return false
	}
	s.relate(k, times, refs)
	switch {
	case s.cost >= 1<<30:
		s.reason = "no reference in the sample"

		return false
	case s.cost+minTimedGain > s.headPacked:
		s.reason = "no reference saves bits over packing in the sample"

		return false
	}

	return true
}

// sampleShape finds the shape of an even sample of the column's values, as
// digits or else as stamps, and the head numbers of the k of them that have
// it, the first k of the sample.
func (s *timedScratch) sampleShape(c *shred.Column) (int, bool) {
	if k, ok := s.sampleDigits(c); ok {
		return k, true
	}
	reason := s.reason
	if k, ok := s.sampleStamps(c); ok {
		return k, true
	}
	if s.reason == "" {
		s.reason = reason
	}

	return 0, false
}

// sampleDigits is sampleShape for digits.
func (s *timedScratch) sampleDigits(c *shred.Column) (int, bool) {
	n := c.Count
	for j := range s.picked {
		s.picked[j] = j * (n - 1) / (timedSample - 1)
	}
	if s.form = formDigits; !s.shape(c, s.picked[:]) {
		if s.reason == "" {
			s.reason = "no shape in the sample"
		}

		return 0, false
	}
	k := 0
	for _, i := range s.picked {
		if h, ok := s.head(c.Bytes(i)); ok {
			s.sample[k], s.sampleRows[k] = h, c.Row(i)
			k++
		}
	}
	if k < timedSample/2 {
		s.reason = "few sample values fit the shape"

		return 0, false
	}

	return k, true
}

// relate finds the relation to nothing, then to the times and to every
// column of refs that may be followed, of the first k values of the sample
// that fit the shape, and keeps the one of the fewest bits.
func (s *timedScratch) relate(k int, times []int64, refs []timedRef) {
	for sh := range s.lowCost {
		s.lowCost[sh] = -1
	}
	s.tried = s.tried[:0]
	keep := func(k int, unit uint8, shift uint, bits int) {
		if bits < s.cost && (!s.forced || s.only == k) {
			s.best, s.unit, s.shift, s.cost = k, unit, shift, bits
		}
	}
	s.cost = 1 << 30
	for j := 0; j < k; j++ {
		s.sampleRefs[j] = times[s.sampleRows[j]]
	}
	u, sh, b := s.sampleRelation(k, false)
	keep(followTime, u, sh, b)
	clear(s.sampleRefs[:k])
	u, sh, b = s.sampleRelation(k, true)
	keep(followNone, u, sh, b)
	// A column is followed by the values of the sample whose rows it holds
	// a value in, the others becoming exceptions; the sample is put back
	// in order after.
	copy(s.heldHeads[:k], s.sample[:k])
	for r := range refs {
		if r == s.self || !refs[r].filled || (s.forced && r != s.only) || (!s.forced && r != s.allowed) {
			continue
		}
		held := 0
		for j := 0; j < k; j++ {
			if row := s.sampleRows[j]; refs[r].valid[row] {
				s.sample[held], s.sampleRefs[held] = s.heldHeads[j], refs[r].vals[row]
				held++
			}
		}
		if held < k-k/maxIrregularShare {
			continue
		}
		if held < k {
			for sh := range s.lowCost {
				s.lowCost[sh] = -1
			}
		}
		u, sh, b = s.sampleRelation(held, false)
		keep(r, u, sh, b+(k-held)*exceptionBits/k)
		if held < k {
			for sh := range s.lowCost {
				s.lowCost[sh] = -1
			}
		}
	}
	copy(s.sample[:k], s.heldHeads[:k])
}

// sampleStep returns the step between the values of an even sample of n.
func sampleStep(n int) int { return max(n/timedSample, 1) }

// shape finds the length, the span, the template and the alphabet of the
// column's values, or of those that idx numbers when it is set, and reports
// whether they suit a timed chunk.
func (s *timedScratch) shape(c *shred.Column, idx []int) bool {
	n := c.Count
	if idx != nil {
		n = len(idx)
	}
	value := func(j int) []byte {
		if idx != nil {
			return c.Bytes(idx[j])
		}

		return c.Bytes(j)
	}
	var lens [timedSample]int
	for k := range lens {
		lens[k] = len(c.Bytes(k * (c.Count - 1) / (timedSample - 1)))
	}
	slices.Sort(lens[:])
	best, run := 0, 0
	for k := 0; k < len(lens); {
		j := k
		for j < len(lens) && lens[j] == lens[k] {
			j++
		}
		if j-k > run {
			best, run = lens[k], j-k
		}
		k = j
	}
	if run < timedSample-timedSample/maxIrregularShare || best < 2 || best > 255 {
		s.reason = "lengths differ in the sample"

		return false
	}
	s.stride = best
	// The template holds at every position the byte that most of a sample
	// of the values have there; the span runs from the first position to
	// the last where more than the exceptions differ from it.
	s.template = grow(s.template, best)
	var votes [timedSample]byte
	for k := 0; k < best; k++ {
		m := 0
		for j := 0; j < timedSample; j++ {
			if v := c.Bytes(j * (c.Count - 1) / (timedSample - 1)); len(v) == best {
				votes[m] = v[k]
				m++
			}
		}
		s.template[k] = majority(votes[:m])
	}
	s.misses = grow(s.misses, best)
	misses := s.misses
	clear(misses)
	for j := 0; j < n; j++ {
		if v := value(j); len(v) == best {
			for k := range v {
				if v[k] != s.template[k] {
					misses[k]++
				}
			}
		}
	}
	s.from, s.to = 0, best
	for s.from < best && int(misses[s.from]) <= n/rareShare {
		s.from++
	}
	for s.to > s.from && int(misses[s.to-1]) <= n/rareShare {
		s.to--
	}
	if s.from == s.to {
		s.reason = "values alike in the sample"

		return false
	}
	// The alphabet is every byte that the spans of many values hold; a
	// value with a byte that few hold is an exception.
	var holders [256]int32
	var seen [256]int32
	for j := 0; j < n; j++ {
		if v := value(j); len(v) == best {
			for _, b := range v[s.from:s.to] {
				if seen[b] != int32(j+1) {
					seen[b] = int32(j + 1)
					holders[b]++
				}
			}
		}
	}
	s.alphabet = s.alphabet[:0]
	for b, k := range holders {
		if k > 0 && int(k)*rareShare >= n {
			s.alphabet = append(s.alphabet, byte(b))
		}
	}
	switch size := len(s.alphabet); {
	case size == 10:
		s.radixBits = 0
	case size >= 2 && size <= 64 && size&(size-1) == 0:
		s.radixBits = uint(bits.TrailingZeros(uint(size)))
	default:
		s.reason = "alphabet of the sample neither ten nor a power of two"

		return false
	}
	for i := range s.digit {
		s.digit[i] = 0xFF
	}
	for d, b := range s.alphabet {
		s.digit[b] = uint8(d)
	}
	// The number begins with the digits that all values share, so that the
	// head numbers of columns of one kind are alike and one may follow
	// another.
	for s.from > 0 && s.digit[s.template[s.from-1]] != 0xFF {
		s.from--
	}
	digits := s.to - s.from
	if s.radixBits == 0 {
		s.headDigits = min(digits, 19)
	} else {
		s.headDigits = min(digits, 64/int(s.radixBits))
		// One digit more fits when the leading digit leaves room for it;
		// the values whose leading digit does not are exceptions.
		if spare := 64 - int(s.radixBits)*s.headDigits; s.headDigits < digits && spare > 0 && int(s.digit[s.template[s.from]])>>spare == 0 {
			s.headDigits++
		}
	}
	// What packing the head digits would take, by the range of the digits
	// at each position.
	var lo, hi [64]uint8
	for k := range lo {
		lo[k] = 0xFF
	}
	head := min(s.headDigits, len(lo))
	for j := 0; j < n; j++ {
		if v := value(j); len(v) == best {
			for k, b := range v[s.from : s.from+head] {
				if d := s.digit[b]; d != 0xFF {
					lo[k], hi[k] = min(lo[k], d), max(hi[k], d)
				}
			}
		}
	}
	s.headPacked = 0
	for k := 0; k < head; k++ {
		if hi[k] > lo[k] {
			s.headPacked += bits.Len8(hi[k] - lo[k])
		}
	}

	return true
}

// scan reads the head number of every value of the column that fits the
// shape, which the sample found.
func (s *timedScratch) scan(c *shred.Column) {
	n := c.Count
	s.heads, s.regular = grow(s.heads, n), grow(s.regular, n)
	s.irregular = 0
	over := int(s.radixBits)*s.headDigits - 64
	radix := s.radixBits & 7
	from, to, headEnd := s.from, s.to, s.from+s.headDigits
	prefix, suffix := s.template[:from], s.template[to:s.stride]
	digit := &s.digit
	for i := 0; i < n; i++ {
		v := c.Bytes(i)
		if len(v) != s.stride || (from > 0 && string(v[:from]) != string(prefix)) || (to < len(v) && string(v[to:]) != string(suffix)) {
			s.heads[i], s.regular[i] = 0, false
			s.irregular++

			continue
		}
		num, bad := uint64(0), uint8(0)
		if radix == 0 {
			for _, b := range v[from:headEnd] {
				d := digit[b]
				bad |= d
				num = num*10 + uint64(d)
			}
		} else {
			for _, b := range v[from:headEnd] {
				d := digit[b]
				bad |= d
				num = num<<radix | uint64(d)
			}
		}
		for _, b := range v[headEnd:to] {
			bad |= digit[b]
		}
		// A byte outside the alphabet has the digit 0xFF, and digits are
		// below 64.
		if bad&0x80 != 0 || (over > 0 && int(digit[v[from]])>>(int(radix)-over) != 0) {
			s.heads[i], s.regular[i] = 0, false
			s.irregular++

			continue
		}
		s.heads[i], s.regular[i] = num, true
	}
}

// majority returns the byte that most of votes hold, which it sorts.
func majority(votes []byte) byte {
	slices.Sort(votes)
	best, run := byte(0), 0
	for k := 0; k < len(votes); {
		j := k
		for j < len(votes) && votes[j] == votes[k] {
			j++
		}
		if j-k > run {
			best, run = votes[k], j-k
		}
		k = j
	}

	return best
}

// head returns the head number of v, and false when v does not fit the
// shape.
func (s *timedScratch) head(v []byte) (uint64, bool) {
	if len(v) != s.stride || string(v[:s.from]) != string(s.template[:s.from]) || string(v[s.to:]) != string(s.template[s.to:]) {
		return 0, false
	}
	for _, b := range v[s.from:s.to] {
		if s.digit[b] == 0xFF {
			return 0, false
		}
	}
	digits := v[s.from : s.from+s.headDigits]
	if over := int(s.radixBits)*s.headDigits - 64; over > 0 && int(s.digit[digits[0]])>>(int(s.radixBits)-over) != 0 {
		return 0, false
	}

	return s.number(digits), true
}

// number returns the number that digits, all of the alphabet, spell.
func (s *timedScratch) number(digits []byte) uint64 {
	n := uint64(0)
	if s.radixBits == 0 {
		for _, b := range digits {
			n = n*10 + uint64(s.digit[b])
		}

		return n
	}
	for _, b := range digits {
		n = n<<s.radixBits | uint64(s.digit[b])
	}

	return n
}

// sampleRelation returns the unit and the shift that make the differences
// of the first k values of the sample from their references take the
// fewest bits, with those bits and the shifted-out bits', per value. Without
// references, when none is set, it tries no shift and those that the times
// were tried with.
func (s *timedScratch) sampleRelation(k int, none bool) (unit uint8, shift uint, cost int) {
	cost = 1 << 30
	if k == 0 {
		return 0, 0, cost
	}
	if none {
		for _, sh := range append(s.tried, 0) {
			for j := 0; j < k; j++ {
				s.trial[j] = int64(s.sample[j] >> sh)
			}
			if b := bits.Len64(trimmedSpread(s.trial[:k])) + s.lowBits(k, sh); b < cost {
				shift, cost = sh, b
			}
		}

		return 0, shift, cost
	}
	top := bits.Len64(slices.Max(s.sample[:k]))
	for _, u := range timedUnits {
		for j := 0; j < k; j++ {
			s.unitRefs[j] = unitDiv(s.sampleRefs[j], u)
		}
		// The time in the number is about as wide as the reference.
		near := top - bits.Len64(uint64(max(slices.Max(s.unitRefs[:k]), 0)))
		last := -1
		for d := -3; d <= 3; d++ {
			sh := min(max(near+d, 0), 63)
			if sh == last {
				continue
			}
			last = sh
			for j := 0; j < k; j++ {
				s.trial[j] = int64(s.sample[j]>>uint(sh)) - s.unitRefs[j]
			}
			if !slices.Contains(s.tried, uint(sh)) {
				s.tried = append(s.tried, uint(sh))
			}
			if b := bits.Len64(trimmedSpread(s.trial[:k])) + s.lowBits(k, uint(sh)); b < cost {
				unit, shift, cost = u, uint(sh), b
			}
		}
	}

	return unit, shift, cost
}

// lowBits returns the bits per value that the low shift bits of the first k
// values of the sample take: those of their distinct values when most
// repeat, shift otherwise.
func (s *timedScratch) lowBits(k int, shift uint) int {
	if shift == 0 {
		return 0
	}
	if s.lowCost[shift] >= 0 {
		return int(s.lowCost[shift])
	}
	mask := uint64(1)<<shift - 1
	tmp := s.lows[:k]
	for j, n := range s.sample[:k] {
		tmp[j] = int64(n & mask)
	}
	slices.Sort(tmp)
	distinct := len(slices.Compact(tmp))
	b := int(shift)
	if 2*distinct <= k {
		b = min(bits.Len(uint(distinct)), int(shift))
	}
	s.lowCost[shift] = int8(b)

	return b
}

// trimmed is the number of the lowest and of the highest values of a
// sample that a spread leaves out, as exceptions may lie there.
const trimmed = timedSample / rareShare

// trimmedSpread returns the spread of vals but for the trimmed lowest and
// the trimmed highest.
func trimmedSpread(vals []int64) uint64 {
	if len(vals) <= 4*trimmed {
		return uint64(slices.Max(vals)) - uint64(slices.Min(vals))
	}
	var lo, hi [trimmed + 1]int64
	for i := range lo {
		lo[i], hi[i] = 1<<63-1, -1<<63
	}
	for _, v := range vals {
		if v < lo[trimmed] {
			i := trimmed
			for ; i > 0 && lo[i-1] > v; i-- {
				lo[i] = lo[i-1]
			}
			lo[i] = v
		}
		if v > hi[trimmed] {
			i := trimmed
			for ; i > 0 && hi[i-1] < v; i-- {
				hi[i] = hi[i-1]
			}
			hi[i] = v
		}
	}

	return uint64(hi[trimmed]) - uint64(lo[trimmed])
}

// exceptionBits is about what an exception costs over a value kept in the
// frames: its bytes, its place and its slot in the frames.
const exceptionBits = 8 * 16

// window returns the bounds outside which a difference is cheaper kept as
// an exception: around the median of a sample of the n values, the width
// whose values cost the fewest bits, a value inside it costing the bits of
// the width and one outside it an exception's.
func (s *timedScratch) window(n int) (lo, hi int64) {
	k := 0
	for i := 0; i < n && k < timedSample; i += sampleStep(n) {
		if s.regular[i] && s.refOK[i] {
			s.trial[k] = s.diffs[i]
			k++
		}
	}
	if k == 0 {
		return -1 << 63, 1<<63 - 1
	}
	vals := s.trial[:k]
	slices.Sort(vals)
	mid := vals[k/2]
	var far [65]int
	regular, refOK := s.regular[:n], s.refOK[:n]
	for i, d := range s.diffs[:n] {
		if regular[i] && refOK[i] {
			// The distance from mid without a branch, as its sign is a coin.
			d -= mid
			sign := d >> 63
			far[bits.Len64(uint64(d^sign-sign))]++
		}
	}
	// A width of 2^w bits holds the values whose distance has fewer than w
	// bits.
	best, cost, outside, outCost := 64, 64*n, 0, 0
	for w := 63; w >= 0; w-- {
		outside += far[w+1]
		outCost += far[w+1] * farBits(w+1)
		if outside > n/maxExceptionShare {
			break
		}
		if c := (n-outside)*(w+1) + outCost; c < cost {
			best, cost = w, c
		}
	}
	if best >= 62 {
		return -1 << 63, 1<<63 - 1
	}
	reach := int64(1)<<best - 1

	return mid - reach, mid + reach
}

// timedValues is a timed chunk read but for its strings.
type timedValues struct {
	timedShape
	prefix, suffix []byte
	refKind        uint8
	reference      []byte
	// refShape is how the values of the column referred to spell numbers,
	// and refRaw its bytes.
	refShape anchorShape
	refRaw   []byte
	words    wordSpeller
	speller  stampSpeller
	// parts holds the shifted head number of every value, a nonsense one
	// for an exception.
	parts, low []int64
	lowDict    []int64
	tails      [][]int64
	excIndex   []int32
	excValue   [][]byte
	// farIndex and farDiff hold the values whose differences are far, and
	// those differences.
	farIndex []int32
	farDiff  []int64
	// lowCoded tells that low holds numbers in lowDict; refs, refValid and
	// rows are the references of the rows and the rows of the values,
	// until resolve adds them.
	lowCoded bool
	refs     []int64
	refValid vec.Bitmap
	rows     []uint32
	// runStart and runLen hold the first value and the length of every run
	// of values equal to the one before them, kept the values that begin
	// none, which the sections hold, and keptSize their bytes; allRows holds
	// the row of every value of a sparse column, and keptRows the rows of
	// the kept values when there are runs.
	runStart, runLen []int32
	kept             int
	keptSize         uint64
	allRows          []uint32
	keptRows         []uint32
	size             uint64
	next             int
}

// timedFollowed is a timed column of a block that another follows: the
// shifted head number of its value in every row that holds one that is not
// an exception.
// timedChunk returns the chunk of the timed column ref after its presence.
func (b *Block) timedChunk(ref *ColumnRef) ([]byte, bool) {
	chunk := b.data[ref.off : ref.off+ref.length]
	if ref.flags&flagSparse == 0 {
		return chunk, true
	}
	if len(chunk) == 0 {
		return nil, false
	}
	chunk, _, ok := skipBlob(chunk[1:])

	return chunk, ok
}

// readTimedHead reads the shape, the reference and the exceptions of a
// timed chunk of count values into v, the exceptions only when keep is
// set, and returns what follows them.
func readTimedHead(chunk []byte, count int, v *timedValues, keep bool) ([]byte, bool) {
	r := reader{buf: chunk}
	form := r.bytes(1)
	if r.bad || form[0] > formStamp {
		return nil, false
	}
	if v.form = form[0]; v.form == formStamp {
		return readStampHead(&r, count, v, keep)
	}
	v.stride, v.from, v.to = int(r.uvarint()), int(r.uvarint()), int(r.uvarint())
	if r.bad || v.stride < 2 || v.stride > 255 || v.from >= v.to || v.to > v.stride {
		return nil, false
	}
	v.prefix, v.suffix = r.bytes(uint64(v.from)), r.bytes(uint64(v.stride-v.to))
	size := r.bytes(1)
	if r.bad {
		return nil, false
	}
	v.alphabet = r.bytes(uint64(size[0]))
	params := r.bytes(3)
	if r.bad {
		return nil, false
	}
	v.headDigits, v.unit, v.shift = int(params[0]), params[1], uint(params[2])

	return v.readRest(&r, count, keep)
}

// readStampHead is readTimedHead for a chunk of stamps, past the form.
func readStampHead(r *reader, count int, v *timedValues, keep bool) ([]byte, bool) {
	layout := r.bytes(2)
	if r.bad || layout[0] > 9 || int(layout[1]) > maxStampZone {
		return nil, false
	}
	v.frac, v.zone = int(layout[0]), r.bytes(uint64(layout[1]))
	params := r.bytes(2)
	if r.bad || !stampsZone(v.zone) {
		return nil, false
	}
	v.stride = stampDate + len(v.zone)
	if v.frac > 0 {
		v.stride += 1 + v.frac
	}
	v.from, v.to, v.prefix, v.suffix, v.alphabet, v.radixBits, v.headDigits = 0, v.stride, nil, nil, nil, 0, 0
	v.unit, v.shift = params[0], uint(params[1])
	v.speller = stampSpeller{frac: v.frac, zone: v.zone}

	return v.readRest(r, count, keep)
}

// readRest reads the reference and the exceptions of a timed chunk, after
// its shape.
func (v *timedValues) readRest(r *reader, count int, keep bool) ([]byte, bool) {
	kind := r.bytes(1)
	if r.bad || kind[0] > refColumn {
		return nil, false
	}
	v.refKind, v.reference, v.refRaw = kind[0], nil, nil
	if v.refKind == refColumn {
		v.reference = r.bytes(r.uvarint())
		if v.refRaw = readAnchorShape(r, &v.refShape); v.refRaw == nil {
			return nil, false
		}
	}
	runs := r.uvarint()
	if r.bad || runs > uint64(count) {
		return nil, false
	}
	v.runStart, v.runLen = v.runStart[:0], v.runLen[:0]
	repeats := 0
	for k, end := 0, 0; k < int(runs); k++ {
		x := r.uvarint()
		length := uint64(x&3) + 1
		if length == 4 {
			length += r.uvarint()
		}
		at := uint64(end) + x>>2
		if r.bad || x>>2 == 0 || length > uint64(count) || at+length > uint64(count) {
			return nil, false
		}
		v.runStart, v.runLen = append(v.runStart, int32(at)), append(v.runLen, int32(length))
		end = int(at + length)
		repeats += int(length)
	}
	v.kept = count - repeats
	count = v.kept
	exceptions := r.uvarint()
	if r.bad || exceptions > uint64(count) {
		return nil, false
	}
	if v.shift > 63 || (v.unit != 0 && v.unit != 3 && v.unit != 6 && v.unit != 9) {
		return nil, false
	}
	if v.form == formDigits {
		switch n := len(v.alphabet); {
		case n == 10:
			v.radixBits = 0
		case n >= 2 && n <= 64 && n&(n-1) == 0:
			v.radixBits = uint(bits.TrailingZeros(uint(n)))
		default:
			return nil, false
		}
		maxHead := 19
		if v.radixBits != 0 {
			maxHead = 64/int(v.radixBits) + 1
		}
		if v.headDigits < 1 || v.headDigits > min(v.to-v.from, maxHead) {
			return nil, false
		}
		if v.radixBits != 0 && !v.words.set(v.alphabet, v.radixBits) {
			return nil, false
		}
	}
	v.excIndex, v.excValue, v.next = v.excIndex[:0], v.excValue[:0], 0
	v.farIndex, v.farDiff = v.farIndex[:0], v.farDiff[:0]
	// A run repeats the value before it, whose number among the kept ones
	// is that of its first value less the runs before it; one that repeats
	// a string takes the string's bytes, the others the stride's.
	strings, size := uint64(0), uint64(0)
	repeated, run, before := int64(repeats)*int64(v.stride), 0, 0
	for i, at := 0, -1; i < int(exceptions); i++ {
		at += int(r.uvarint()) + 1
		if tag := r.uvarint(); tag == 0 {
			if diff := unzigzag(r.uvarint()); keep {
				v.farIndex, v.farDiff = append(v.farIndex, int32(at)), append(v.farDiff, diff)
			}
		} else {
			value := r.bytes(tag - 1)
			if keep {
				v.excIndex, v.excValue = append(v.excIndex, int32(at)), append(v.excValue, value)
			}
			strings++
			size += uint64(len(value))
			for ; run < len(v.runStart) && int(v.runStart[run])-1-before <= at; run++ {
				if int(v.runStart[run])-1-before == at {
					repeated += int64(v.runLen[run]) * int64(len(value)-v.stride)
				}
				before += int(v.runLen[run])
			}
		}
		if r.bad || at >= count {
			return nil, false
		}
	}
	v.keptSize = size + (uint64(count)-strings)*uint64(v.stride)
	v.size = v.keptSize + uint64(repeated)

	return r.buf, !r.bad && repeated >= 0 && v.size <= maxSectionSize
}

// readTimed reads the timed column ref into v, following depth columns
// already.
func (b *Block) readTimed(ref *ColumnRef, v *timedValues, depth int) error {
	chunk, ok := b.timedChunk(ref)
	if !ok || !b.checked(ref, b.data[ref.off:ref.off+ref.length]) {
		return ErrCorrupt
	}
	rest, ok := readTimedHead(chunk, ref.Count, v, true)
	if !ok {
		return ErrCorrupt
	}
	count := v.kept
	var err error
	if v.parts, rest, err = readInts(v.parts, rest, count, &b.d); err != nil {
		return err
	}
	for k, at := range v.farIndex {
		v.parts[at] = v.farDiff[k]
	}
	v.lowCoded = false
	if v.shift > 0 {
		if rest, err = v.readLow(rest, count, &b.d); err != nil {
			return err
		}
	}
	tails := 0
	for at := v.from + v.headDigits; v.form == formDigits && at < v.to; at += v.tailDigits() {
		if tails == len(v.tails) {
			v.tails = append(v.tails, nil)
		}
		if v.tails[tails], rest, err = readInts(v.tails[tails], rest, count, &b.d); err != nil {
			return err
		}
		tails++
	}
	v.tails = v.tails[:tails]
	if len(rest) != 0 {
		return ErrCorrupt
	}

	// The references of the values, those of their rows, which resolve or
	// the spelling adds.
	v.refs, v.refValid, v.rows, v.allRows = nil, nil, nil, nil
	switch v.refKind {
	case refNone:
	case refTime:
		if v.refs, err = b.rowTimes(); err != nil {
			return err
		}
	default:
		id, ok := b.part.PathID(string(v.reference))
		if !ok || depth >= maxTimedDepth {
			return ErrCorrupt
		}
		anchor, err := b.anchorNumbers(id, &v.refShape, v.refRaw, depth+1)
		if err != nil {
			return err
		}
		v.refs, v.refValid = anchor.vals, anchor.valid
	}
	if ref.flags&flagSparse != 0 {
		if v.allRows, err = b.valueRows(ref); err != nil {
			return err
		}
	}
	v.rows = v.allRows
	if len(v.runStart) > 0 {
		v.rows = v.keptRowsOf(ref.Count)
	}

	return nil
}

// keptRowsOf returns the rows of the kept values of the count.
func (v *timedValues) keptRowsOf(count int) []uint32 {
	v.keptRows = grow(v.keptRows, v.kept)[:0]
	for i, run := 0, 0; i < count; i++ {
		if run < len(v.runStart) && int(v.runStart[run]) == i {
			i += int(v.runLen[run]) - 1
			run++

			continue
		}
		row := uint32(i)
		if v.allRows != nil {
			row = v.allRows[i]
		}
		v.keptRows = append(v.keptRows, row)
	}

	return v.keptRows
}

// resolve turns the differences of v into the shifted head numbers and its
// numbers of shifted-out bits into the bits.
func (v *timedValues) resolve() error {
	if v.lowCoded {
		for i, code := range v.low {
			if uint64(code) >= uint64(len(v.lowDict)) {
				return ErrCorrupt
			}
			v.low[i] = v.lowDict[code]
		}
		v.lowCoded = false
	}
	if v.refs == nil {
		return nil
	}
	for i, next := 0, 0; i < len(v.parts); i++ {
		if next < len(v.excIndex) && int(v.excIndex[next]) == i {
			next++

			continue
		}
		row := i
		if v.rows != nil {
			row = int(v.rows[i])
		}
		if v.refValid != nil && !v.refValid.Get(row) {
			return ErrCorrupt
		}
		v.parts[i] += unitDiv(v.refs[row], v.unit)
	}
	v.refs = nil

	return nil
}

// readLow reads the shifted-out bits of count values from the start of src
// and returns what follows them.
func (v *timedValues) readLow(src []byte, count int, d *decodeScratch) ([]byte, error) {
	if len(src) == 0 || src[0] > lowDict {
		return nil, ErrCorrupt
	}
	form, src := src[0], src[1:]
	if form == lowPlain {
		var err error
		v.low, src, err = readInts(v.low, src, count, d)

		return src, err
	}
	size, n := binary.Uvarint(src)
	if n <= 0 || size == 0 || size > maxLowDict {
		return nil, ErrCorrupt
	}
	var err error
	if v.lowDict, src, err = readInts(v.lowDict, src[n:], int(size), d); err != nil {
		return nil, err
	}
	if v.low, src, err = readInts(v.low, src, count, d); err != nil {
		return nil, err
	}
	v.lowCoded = true

	return src, nil
}

// valueRows returns the row of every value of the column ref, which has
// gaps.
func (b *Block) valueRows(ref *ColumnRef) ([]uint32, error) {
	chunk := b.data[ref.off : ref.off+ref.length]
	if _, err := b.readPresence(chunk, ref.Count, &b.present); err != nil {
		return nil, err
	}
	rows := b.positions(b.present.Valid)
	if len(rows) != ref.Count {
		return nil, ErrCorrupt
	}

	return rows, nil
}

// rowTimes returns the time of every row of the block, decoding its time
// column the first time.
func (b *Block) rowTimes() ([]int64, error) {
	if b.timesRead {
		return b.times.Ints, nil
	}
	ref, ok := b.Column(0, vec.KindInt)
	if !ok || ref.bad || ref.enc == encTime || ref.flags&flagSparse != 0 {
		return nil, ErrCorrupt
	}
	if err := b.decode(ref, &b.times, false); err != nil {
		return nil, err
	}
	if b.times.Layout == vec.Const {
		t := b.times.Ints[0]
		b.times.Ints = grow(b.times.Ints, b.Rows)
		for i := range b.times.Ints {
			b.times.Ints[i] = t
		}
		b.times.Layout = vec.Flat
	}
	if len(b.times.Ints) != b.Rows {
		return nil, ErrCorrupt
	}
	b.timesRead = true

	return b.times.Ints, nil
}

// timedDataSize returns the bytes that the values of the timed column ref
// take, or 0 when the chunk does not say.
func (b *Block) timedDataSize(ref *ColumnRef) uint32 {
	chunk, ok := b.timedChunk(ref)
	if !ok {
		return 0
	}
	v := &b.d.timed
	if _, ok := readTimedHead(chunk, ref.Count, v, false); !ok {
		return 0
	}

	return uint32(v.size)
}

// put writes value i at the start of dst and returns its length.
func (v *timedValues) put(dst []byte, i int) int {
	if v.next < len(v.excIndex) && int(v.excIndex[v.next]) == i {
		n := copy(dst, v.excValue[v.next])
		v.next++

		return n
	}
	out := dst[:v.stride]
	num := uint64(v.parts[i]) << v.shift
	if v.shift > 0 {
		num |= uint64(v.low[i]) & (1<<v.shift - 1)
	}
	if v.form == formStamp {
		v.speller.put(out, int64(num))

		return v.stride
	}
	copy(out[:v.from], v.prefix)
	copy(out[v.to:], v.suffix)
	v.spell(out[v.from:v.from+v.headDigits], num)
	if len(v.tails) > 0 {
		per := v.tailDigits()
		for k, tail := range v.tails {
			from := v.from + v.headDigits + k*per
			v.spell(out[from:min(from+per, v.to)], uint64(tail[i]))
		}
	}

	return v.stride
}

// spell writes num into out in the chunk's alphabet, its last digit last.
func (v *timedValues) spell(out []byte, num uint64) {
	if v.radixBits != 0 {
		v.words.spell(out, num)

		return
	}
	for j := len(out) - 1; j >= 0; j-- {
		out[j] = v.alphabet[num%10]
		num /= 10
	}
}

// wordSpeller spells numbers in an alphabet of a power of two, in byte
// order, eight digits a word: it spreads the digits one a byte and adds to
// each the letter its run of consecutive letters begins with.
type wordSpeller struct {
	radix uint
	// eight holds the bits of eight digits, m2 and m1 those of the low two
	// digits of every four and the low digit of every two.
	eight, m2, m1 uint64
	first         uint64
	// runs holds the first two runs after the first letter's, empty ones
	// when there are fewer, and more any others.
	runs [2]letterRun
	more []letterRun
}

// letterRun is a run of the alphabet after its first: digits from the run
// on are step letters further from the first letter than their own number.
type letterRun struct{ over, step uint64 }

const (
	everyByte = 0x0101010101010101
	byteHighs = 0x8080808080808080
)

// set readies s for alphabet, whose letters ascend, and reports whether
// they do.
func (s *wordSpeller) set(alphabet []byte, radix uint) bool {
	s.radix, s.runs, s.more = radix, [2]letterRun{}, s.more[:0]
	s.eight = 1<<(8*radix) - 1
	s.m2 = (1<<(2*radix) - 1) * (1<<32 | 1)
	s.m1 = (1<<radix - 1) * (1<<48 | 1<<32 | 1<<16 | 1)
	s.first = uint64(alphabet[0]) * everyByte
	for d := 1; d < len(alphabet); d++ {
		gap := int(alphabet[d]) - int(alphabet[d-1])
		if gap <= 0 {
			return false
		}
		if gap == 1 {
			continue
		}
		r := letterRun{over: uint64(0x80-d) * everyByte, step: uint64(gap - 1)}
		switch {
		case s.runs[0].step == 0:
			s.runs[0] = r
		case s.runs[1].step == 0:
			s.runs[1] = r
		default:
			s.more = append(s.more, r)
		}
	}

	return true
}

// word spells the eight digits of x, the first at the lowest address.
func (s *wordSpeller) word(x uint64) uint64 {
	x = s.spread(x)
	out := s.letters(x)
	for _, r := range s.more {
		out += (x + r.over) & byteHighs >> 7 * r.step
	}

	return out
}

// spread puts the eight digits of x one a byte, the first at the lowest
// address.
func (s *wordSpeller) spread(x uint64) uint64 {
	b := s.radix & 7
	x = x&(1<<(4*b)-1) | x>>(4*b)<<32
	x = x&s.m2 | x>>(2*b)&s.m2<<16
	x = x&s.m1 | x>>b&s.m1<<8

	return bits.ReverseBytes64(x)
}

// letters turns the digits spread over the bytes of x into letters of the
// first three runs.
func (s *wordSpeller) letters(x uint64) uint64 {
	// A byte's high bit tells whether its digit is in a run or after it.
	return x + s.first + (x+s.runs[0].over)&byteHighs>>7*s.runs[0].step +
		(x+s.runs[1].over)&byteHighs>>7*s.runs[1].step
}

// spell writes num into out, its last digit last.
func (s *wordSpeller) spell(out []byte, num uint64) {
	b := s.radix & 7
	var word [8]byte
	for at := 0; at < len(out); at += 8 {
		x := num << (uint(at+8-len(out)) * b & 63)
		if rest := len(out) - at - 8; rest >= 0 {
			x = num >> (uint(rest) * b & 63)
		}
		binary.LittleEndian.PutUint64(word[:], s.word(x&s.eight))
		copy(out[at:], word[:])
	}
}

// decodeTimed fills dst from the timed column ref, laid out over rows
// slots: one for every row, whether dst.Valid marks it or not, when sparse
// is set, and one for every value otherwise.
func (b *Block) decodeTimed(ref *ColumnRef, sparse bool, rows int, dst *vec.Column) error {
	v := &b.d.timed
	if err := b.readTimed(ref, v, b.depth); err != nil {
		return err
	}
	dst.Offsets = grow(dst.Offsets, rows+1)
	if !sparse && len(v.runStart) == 0 {
		off, err := v.putValues(dst)
		if err != nil {
			return err
		}
		if off != int(v.size) {
			return ErrCorrupt
		}

		return nil
	}
	// The kept values one after another, laid out over the slots with the
	// runs that repeat them.
	kv := &b.d.kept
	kv.Offsets = grow(kv.Offsets, v.kept+1)
	koff, err := v.putValues(kv)
	if err != nil {
		return err
	}
	if uint64(koff) != v.keptSize {
		return ErrCorrupt
	}
	dst.Data = grow(dst.Data, int(v.size))
	count := ref.Count
	off, i, j, run, left := 0, 0, 0, 0, 0
	for slot := 0; slot < rows; slot++ {
		dst.Offsets[slot] = uint32(off)
		if sparse && !dst.Valid.Get(slot) {
			continue
		}
		if i == count {
			return ErrCorrupt
		}
		if left == 0 && run < len(v.runStart) && int(v.runStart[run]) == i {
			left = int(v.runLen[run])
			run++
		}
		k := j
		if left > 0 {
			k, left = j-1, left-1
		} else {
			j++
		}
		if k < 0 || k >= v.kept {
			return ErrCorrupt
		}
		off += copy(dst.Data[off:], kv.Data[kv.Offsets[k]:kv.Offsets[k+1]])
		i++
	}
	dst.Offsets[rows] = uint32(off)
	dst.Data = dst.Data[:off]
	if i != count || j != v.kept || off != int(v.size) {
		return ErrCorrupt
	}

	return nil
}

// putValues writes the kept values of the chunk one after another into dst,
// their offsets into dst.Offsets, which has room for them, and returns their
// bytes.
func (v *timedValues) putValues(dst *vec.Column) (int, error) {
	n := v.kept
	if v.form == formDigits && len(v.tails) == 0 && v.radixBits != 0 && v.headDigits <= 16 {
		// The fast path writes 16 bytes a value, past the value's own end.
		dst.Data = grow(dst.Data, int(v.keptSize)+16)
		off, err := v.putAll(dst, n)
		if err != nil {
			return 0, err
		}
		dst.Data = dst.Data[:off]
		dst.Offsets[n] = uint32(off)

		return off, nil
	}
	if err := v.resolve(); err != nil {
		return 0, err
	}
	dst.Data = grow(dst.Data, int(v.keptSize))
	off := 0
	for i := 0; i < n; i++ {
		dst.Offsets[i] = uint32(off)
		off += v.put(dst.Data[off:], i)
	}
	dst.Offsets[n] = uint32(off)

	return off, nil
}

// putAll writes the count values of a chunk of digits whose span is all
// head, of at most 16 digits of a power of two, one after another into dst,
// and returns their bytes. It adds the references and looks the shifted-out
// bits up as it goes, and writes the digits a word at a time, past the end
// of each value into where the next one goes; dst.Data has 16 bytes of room
// past the values.
func (v *timedValues) putAll(dst *vec.Column, count int) (int, error) {
	stride, from, to, digits := v.stride, v.from, v.to, v.headDigits
	shift, lowMask := v.shift&63, uint64(1)<<(v.shift&63)-1
	words := v.words
	b, many := words.radix&7, len(words.more) > 0
	// The first eight digits are a word, and any after them another.
	right, left := uint(max(digits-8, 0))*b&63, uint(max(8-digits, 0))*b&63
	second := uint(max(16-digits, 0)) * b & 63
	parts, low, dict := v.parts[:count], v.low, v.lowDict
	if shift == 0 {
		low = nil
	}
	coded := v.lowCoded
	excIndex, excValue := v.excIndex, v.excValue
	prefix, suffix := v.prefix, v.suffix
	refs, valid, rows, unit := v.refs, v.refValid, v.rows, v.unit
	data, offsets := dst.Data, dst.Offsets[:len(parts)]
	off, exc, nextExc := 0, 0, -1
	if len(excIndex) > 0 {
		nextExc = int(excIndex[0])
	}
	for i, part := range parts {
		offsets[i] = uint32(off)
		if i == nextExc {
			off += copy(data[off:], excValue[exc])
			exc++
			nextExc = -1
			if exc < len(excIndex) {
				nextExc = int(excIndex[exc])
			}

			continue
		}
		if refs != nil {
			row := i
			if rows != nil {
				row = int(rows[i])
			}
			if valid != nil && !valid.Get(row) {
				return 0, ErrCorrupt
			}
			part += unitDiv(refs[row], unit)
		}
		num := uint64(part) << shift
		if low != nil {
			bits := uint64(low[i])
			if coded {
				if bits >= uint64(len(dict)) {
					return 0, ErrCorrupt
				}
				bits = uint64(dict[bits])
			}
			num |= bits & lowMask
		}
		if from > 0 {
			copy(data[off:off+from], prefix)
		}
		if many {
			binary.LittleEndian.PutUint64(data[off+from:], words.word(num>>right<<left))
			if digits > 8 {
				binary.LittleEndian.PutUint64(data[off+from+8:], words.word(num<<second&words.eight))
			}
		} else {
			binary.LittleEndian.PutUint64(data[off+from:], words.letters(words.spread(num>>right<<left)))
			if digits > 8 {
				binary.LittleEndian.PutUint64(data[off+from+8:], words.letters(words.spread(num<<second&words.eight)))
			}
		}
		if to < stride {
			copy(data[off+to:off+stride], suffix)
		}
		off += stride
	}

	return off, nil
}
