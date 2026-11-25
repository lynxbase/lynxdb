package exec

import (
	"slices"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
)

// verdicts holds what one predicate on a string gives for the strings of
// one part dictionary: two bits a code, the lower set once the string of
// the code was tested and the upper when it passed. The workers of a query
// share it and set the bits with atomic operations, both bits of a code at
// once, so that a string is tested by the worker that first meets its code
// and by no other row of the part.
type verdicts struct {
	bits []uint64
	// packed is set when the layout of the strings that the dictionary
	// packs tells that none of them passes, and none when it keeps no
	// others, so that none of its strings does.
	packed, none bool
}

// verdictKey names the predicate and the dictionary of a verdicts.
type verdictKey struct {
	p *pred
	d *part.Dictionary
}

// patterned reports whether p tests a string for a pattern.
func (p *pred) patterned() bool { return p.kind == predAny || p.op == OpMatch }

// verdictsOf returns what predicate p gives for the strings of d, which the
// first worker to ask makes.
func (w *worker) verdictsOf(p *pred, d *part.Dictionary) *verdicts {
	key := verdictKey{p, d}
	if v := w.judged[key]; v != nil {
		return v
	}
	x := w.x
	x.verdictMu.Lock()
	defer x.verdictMu.Unlock()
	v := x.verdicts[key]
	if v == nil {
		v = &verdicts{}
		if p.patterned() {
			v.packed = !d.PackedMayHold(&p.pat)
			v.none = v.packed && !d.Odd()
		}
		if !v.none {
			// The code that follows the last stands for no string, which
			// passes nothing.
			none := d.Len()
			v.bits = bitWords.Get(none>>5 + 1)
			clear(v.bits)
			v.bits[none>>5] = 1 << (2 * (none & 31))
		}
		if x.verdicts == nil {
			x.verdicts = map[verdictKey]*verdicts{}
		}
		x.verdicts[key] = v
	}
	if w.judged == nil {
		w.judged = map[verdictKey]*verdicts{}
	}
	w.judged[key] = v

	return v
}

// passed reports whether the string of code, which was tested, passed.
func (v *verdicts) passed(code uint32) uint8 {
	return uint8(atomic.LoadUint64(&v.bits[code>>5]) >> (2*(code&31) + 1) & 1)
}

// judge tests the strings of those of codes that no worker has tested for
// predicate p, each once and in the order of the dictionary.
func (w *worker) judge(p *pred, d *part.Dictionary, v *verdicts, codes []uint32) error {
	fresh := w.fresh[:0]
	for _, code := range codes {
		if atomic.LoadUint64(&v.bits[code>>5])>>(2*(code&31))&1 == 0 {
			fresh = append(fresh, code)
		}
	}
	w.fresh = fresh
	if len(fresh) == 0 {
		return nil
	}
	slices.Sort(fresh)
	fresh = slices.Compact(fresh)
	if v.packed {
		// The strings kept as they are, which their codes tell from the
		// others, are the ones that may pass.
		odd, err := d.AppendOdd(w.odd[:0], fresh)
		if w.odd = odd; err != nil {
			return err
		}
		next := 0
		for _, code := range fresh {
			if next < len(odd) && odd[next] == code {
				next++

				continue
			}
			atomic.OrUint64(&v.bits[code>>5], 1<<(2*(code&31)))
		}
		if fresh = odd; len(fresh) == 0 {
			return nil
		}
	}
	d.Ahead(len(fresh))
	c := &w.spelled
	c.Offsets, c.Data = c.Offsets[:0], c.Data[:0]
	var err error
	for _, code := range fresh {
		c.Offsets = append(c.Offsets, uint32(len(c.Data)))
		if c.Data, err = d.Value(c.Data, code); err != nil {
			return err
		}
	}
	c.Offsets = append(c.Offsets, uint32(len(c.Data)))
	set := func(i int, pass uint64) {
		code := fresh[i]
		atomic.OrUint64(&v.bits[code>>5], (1|pass<<1)<<(2*(code&31)))
	}
	if !p.patterned() {
		for i := range fresh {
			pass := uint64(0)
			if p.test(c.Data[c.Offsets[i]:c.Offsets[i+1]]) {
				pass = 1
			}
			set(i, pass)
		}

		return nil
	}
	// The strings are searched as one run of bytes, and those found pass.
	w.idx2 = p.pat.AppendMatches(w.idx2[:0], c.Offsets, c.Data)
	next := 0
	for _, i := range w.idx2 {
		for ; next < int(i); next++ {
			set(next, 0)
		}
		set(next, 1)
		next++
	}
	for ; next < len(fresh); next++ {
		set(next, 0)
	}

	return nil
}

// evalCodes selects the rows whose string at the path of p, which the open
// block stores as codes of dictionary d, passes p: by what p gives for the
// string of each code, which is tested once for the part.
func (w *worker) evalCodes(p *pred, d *part.Dictionary, ref *part.ColumnRef, depth int) (verdict, error) {
	v := w.verdictsOf(p, d)
	if v.none {
		w.ruledOut++

		return none, nil
	}
	codes, _, err := w.rowCodes(p.path, ref, d)
	if err != nil {
		return none, err
	}
	codes = codes[:w.rows]
	if err := w.judge(p, d, v, codes); err != nil {
		return none, err
	}
	w.judgedColumns++
	m := w.mask(depth)
	hit := uint8(0)
	for r, code := range codes {
		pass := v.passed(code)
		m[r] = -pass
		hit |= pass
	}
	if hit == 0 {
		return none, nil
	}

	return some, nil
}

// anyCodes marks in m the rows whose string in the column of ref, which
// the open block stores as codes of a part dictionary, holds the pattern of
// p, and reports whether there is such a row.
func (w *worker) anyCodes(p *pred, ref *part.ColumnRef, m []uint8) (bool, error) {
	d, ok := w.p.Dictionary(ref.PathID)
	if !ok {
		return false, part.ErrCorrupt
	}
	v := w.verdictsOf(p, d)
	if v.none {
		w.ruledOut++

		return false, nil
	}
	c := &w.anyCol
	var err error
	if w.packed, err = w.b.DecodeShared(ref, c, w.packed); err != nil {
		return false, err
	}
	w.decoded = true
	w.searched++
	w.judgedColumns++
	codes := w.packed
	if err := w.judge(p, d, v, codes); err != nil {
		return false, err
	}
	hit := false
	if c.Valid == nil {
		for r, code := range codes {
			if v.passed(code) != 0 {
				m[r], hit = 0xFF, true
			}
		}

		return hit, nil
	}
	w.anyRows = c.Valid.AppendSet(w.anyRows[:0])
	for i, code := range codes {
		if v.passed(code) != 0 {
			m[w.anyRows[i]], hit = 0xFF, true
		}
	}

	return hit, nil
}

// releaseVerdicts returns the memory of what predicates gave for the
// strings of dictionaries.
func (x *execution) releaseVerdicts() {
	for _, v := range x.verdicts {
		bitWords.Put(v.bits)
	}
	x.verdicts = nil
}
