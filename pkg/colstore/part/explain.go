package part

import (
	"encoding/binary"
	"io"
	"math/bits"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// ColumnExplain sums, over a sample of the blocks of parts, what the string
// column of one path takes in every form the writer has, what it took as
// stored, and why the writer chose as it did. It holds no values.
type ColumnExplain struct {
	Path   string
	Blocks int
	Values uint64
	// Repeats counts the values equal to the one before them.
	Repeats uint64
	// Stored holds by encoding what the blocks take as they are stored,
	// presence left out; Forms what they take in each form, presence left
	// out, the timed form as the writer would choose it and against each
	// reference alone; Smallest how often each form is the smallest.
	Stored   map[string]*FormSum
	Forms    map[string]*FormSum
	Smallest map[string]int
	// Timed tells of the timed form as the writer would choose it.
	Timed TimedExplain
	// Lost counts, by reason, the blocks whose stored form is not their
	// smallest.
	Lost map[string]int
}

// FormSum sums the blocks, values and bytes of one form.
type FormSum struct {
	Blocks int
	Values uint64
	Bytes  int64
}

// TimedExplain sums the shapes and sections of timed chunks.
type TimedExplain struct {
	// Rejected counts the blocks that the codec declined, by reason.
	Rejected map[string]int
	// Form, Reference, Unit, Shift, DifferenceBits and LowDict count the
	// blocks by their form, reference, unit's power of ten, bits shifted
	// out, bits of the spread of their differences and distinct
	// shifted-out values, zero when those are stored as they are.
	Form, Reference                      map[string]int
	Unit, Shift, DifferenceBits, LowDict map[int]int
	// Strings and Far count the exceptions kept as strings and as far
	// differences, and StringBytes and FarBytes their bytes.
	Strings, Far, StringBytes, FarBytes int64
	// Sections holds the bytes of the heads, differences, low bits and
	// tails of the chunks.
	Sections map[string]int64
}

func (e *ColumnExplain) init() {
	if e.Stored == nil {
		e.Stored, e.Forms, e.Smallest, e.Lost = map[string]*FormSum{}, map[string]*FormSum{}, map[string]int{}, map[string]int{}
		t := &e.Timed
		t.Rejected, t.Form, t.Reference = map[string]int{}, map[string]int{}, map[string]int{}
		t.Unit, t.Shift, t.DifferenceBits, t.LowDict = map[int]int{}, map[int]int{}, map[int]int{}, map[int]int{}
		t.Sections = map[string]int64{}
	}
}

func (e *ColumnExplain) add(sums map[string]*FormSum, form string, values int, bytes int) {
	s := sums[form]
	if s == nil {
		s = &FormSum{}
		sums[form] = s
	}
	s.Blocks++
	s.Values += uint64(values)
	s.Bytes += int64(bytes)
}

// ExplainColumn adds to e the blocks of the part that pick says to look at,
// for the string column of e.Path; pick is called once per block of the
// part in order.
func (p *Part) ExplainColumn(e *ColumnExplain, pick func() bool) error {
	e.init()
	id, ok := p.PathID(e.Path)
	if !ok {
		for i := 0; i < p.NumBlocks(); i++ {
			pick()
		}

		return nil
	}
	w := NewWriter(io.Discard, Options{})
	defer w.Close()
	var b Block
	var col, other vec.Column
	for i := 0; i < p.NumBlocks(); i++ {
		if !pick() {
			continue
		}
		if err := p.OpenBlock(i, &b); err != nil {
			return err
		}
		ref, ok := b.Column(id, vec.KindString)
		if !ok {
			continue
		}
		times, err := b.rowTimes()
		if err != nil {
			return err
		}
		w.times = times
		// The timed columns before this one, as the writer would have them
		// to follow.
		w.timedRefs = w.timedRefs[:0]
		refs := b.Columns()
		for j := range refs {
			r := &refs[j]
			if r.PathID >= id || r.Kind != vec.KindString || r.enc != encTimed {
				continue
			}
			if err := b.DecodeValues(r, &other); err != nil {
				return err
			}
			var oc shred.Column
			if err := b.toShred(r, &other, &oc); err != nil {
				return err
			}
			w.block = w.block[:0]
			if w.appendTimed(&oc, times) {
				w.addTimedRef(&oc, p.paths[r.PathID], b.Rows)
			}
		}
		if err := b.DecodeValues(ref, &col); err != nil {
			return err
		}
		var c shred.Column
		if err := b.toShred(ref, &col, &c); err != nil {
			return err
		}
		e.explainBlock(w, &b, ref, &c, times)
	}

	return nil
}

// toShred makes c the column of the block's rows that ref holds, whose
// values col holds one after another.
func (b *Block) toShred(ref *ColumnRef, col *vec.Column, c *shred.Column) error {
	c.Path, c.Kind, c.Count = "", vec.KindString, ref.Count
	c.Data, c.Ends = col.Data, c.Ends[:0]
	for i := 0; i < ref.Count; i++ {
		c.Ends = append(c.Ends, col.Offsets[i+1])
	}
	c.Sparse, c.RowIDs = ref.flags&flagSparse != 0, c.RowIDs[:0]
	if c.Sparse {
		rows, err := b.valueRows(ref)
		if err != nil {
			return err
		}
		c.RowIDs = append(c.RowIDs, rows...)
	}

	return nil
}

// storedBytes returns the bytes of the chunk of ref but for its presence.
func (b *Block) storedBytes(ref *ColumnRef) int {
	chunk := b.data[ref.off : ref.off+ref.length]
	if ref.flags&flagSparse == 0 || len(chunk) == 0 {
		return len(chunk)
	}
	if rest, _, ok := skipBlob(chunk[1:]); ok {
		return len(rest)
	}

	return len(chunk)
}

// explainBlock adds one block's column c, stored as ref, to e.
func (e *ColumnExplain) explainBlock(w *Writer, b *Block, ref *ColumnRef, c *shred.Column, times []int64) {
	n := c.Count
	e.Blocks++
	e.Values += uint64(n)
	for i := 1; i < n; i++ {
		if string(c.Bytes(i)) == string(c.Bytes(i-1)) {
			e.Repeats++
		}
	}
	stored := encodingName(ref.enc)
	e.add(e.Stored, stored, n, b.storedBytes(ref))

	sizes := map[string]int{}
	try := func(form string, fn func() bool) {
		w.block = w.block[:0]
		if fn() {
			sizes[form] = len(w.block)
			e.add(e.Forms, form, n, len(w.block))
		}
	}
	distinct, dictOK := w.dict.build(c, false)
	if dictOK && distinct > 1 {
		try("dict", func() bool { return w.encodeDict(c, distinct) })
	}
	stride := len(c.Bytes(0))
	for i := 1; i < n && stride > 0; i++ {
		if int(c.Ends[i]-c.Ends[i-1]) != stride {
			stride = 0
		}
	}
	if stride > 0 {
		if distinct, ok := w.dict.build(c, true); ok {
			try("dict.wide", func() bool { return w.encodeDict(c, distinct) })
		}
		try("flat.packed", func() bool { w.appendFlat(0, c, stride); return true })
	}
	try("flat", func() bool { w.appendFlat(0, c, 0); return true })

	s := &w.timedBuf
	t := &e.Timed
	s.forced = false
	try("timed", func() bool { return w.appendTimed(c, times) })
	if _, ok := sizes["timed"]; ok {
		t.Form[[...]string{formDigits: "digits", formStamp: "stamp"}[s.form]]++
		t.Reference[referenceName(s.best, w.timedRefs)]++
		t.Unit[int(s.unit)]++
		t.Shift[int(s.shift)]++
		lo, hi := int64(1<<63-1), int64(-1<<63)
		for _, d := range s.diffs[:n] {
			lo, hi = min(lo, d), max(hi, d)
		}
		t.DifferenceBits[bits.Len64(uint64(hi)-uint64(lo))]++
		t.LowDict[s.lowDictSize]++
		for k, at := range s.exceptions {
			if s.far[k] {
				t.Far++
				t.FarBytes += int64(1 + uvarintLen(zigzag(s.farDiffs[k])))
			} else {
				t.Strings++
				t.StringBytes += int64(uvarintLen(uint64(len(c.Bytes(int(at))))+1) + len(c.Bytes(int(at))))
			}
		}
		for k, name := range [...]string{"head", "differences", "low", "tail"} {
			size := s.sections[k]
			if k == 3 {
				size = sizes["timed"] - s.sections[0] - s.sections[1] - s.sections[2]
			}
			t.Sections[name] += int64(size)
		}
	} else {
		t.Rejected[s.reason]++
	}
	// The timed form against each reference alone.
	s.forced = true
	for k := followNone; k < len(w.timedRefs); k++ {
		s.only = k
		try("timed."+referenceName(k, w.timedRefs), func() bool { return w.appendTimed(c, times) })
	}
	s.forced = false

	best, smallest := "", 1<<62
	for form, size := range sizes {
		if size < smallest || size == smallest && form < best {
			best, smallest = form, size
		}
	}
	if best == "" {
		return
	}
	e.Smallest[best]++
	if storedSize := b.storedBytes(ref); storedSize > smallest {
		e.Lost[lostReason(stored, best, sizes, s.reason, dictOK, stride > 0 && !w.dict.fewRepeats(c))]++
	}
}

// lostReason tells why a block was stored in a larger form than best.
func lostReason(stored, best string, sizes map[string]int, reason string, dictOK, repeats bool) string {
	_, timed := sizes["timed"]
	toTimed := strings.HasPrefix(best, "timed")
	switch {
	case stored == "dict" && dictOK:
		return "a dictionary is taken before other forms are tried"
	case stored == "dict" && repeats:
		return "a wide dictionary won the last trial"
	case stored == "flat" && toTimed && !timed:
		return "timed form declined: " + reason
	case stored == "flat" && toTimed && best != "timed":
		return "the timed form chose another reference than " + best[len("timed."):]
	case stored == "flat" && toTimed && repeats:
		return "values repeat, so they are not packed, and the trial state kept them flat"
	case stored == "flat" && toTimed:
		return "the trial state kept them flat"
	case stored == "flat" && best == "flat.packed":
		return "values repeat, so they are not packed"
	case stored == "timed":
		return "the trial state kept them timed"
	case stored == best || strings.HasPrefix(best, stored+"."):
		return "the same form came out larger as stored, its level or packing chosen by trials"
	}

	return stored + " where " + best + " is smaller"
}

func encodingName(enc uint8) string {
	if int(enc) < len(encodingNames) {
		return encodingNames[enc]
	}

	return "invalid"
}

func referenceName(k int, refs []timedRef) string {
	switch {
	case k == followNone:
		return "none"
	case k == followTime:
		return "time"
	case k >= 0 && k < len(refs):
		return refs[k].path
	}

	return "unknown"
}

func uvarintLen(v uint64) int {
	var buf [binary.MaxVarintLen64]byte

	return binary.PutUvarint(buf[:], v)
}
