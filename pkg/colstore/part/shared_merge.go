package part

import (
	"bytes"
	"math"
	"math/bits"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// pathShare is what the blocks that a merge reads hold as strings at one
// path: the chunks and values of the blocks that store the strings
// themselves, and the values of those that store codes.
type pathShare struct {
	path                              string
	ownBytes, ownValues, sharedValues uint64
}

// sharedBuf holds a column of a merge cursor's block that the output
// shares: for every value, the code that the plan gives it and the number
// of its string in source src of the plan.
type sharedBuf struct {
	plan          *sharedPlan
	src           int32
	codes, locals []uint32
}

// share returns what the merge knows of the path with number id in the
// part of c.
func (m *merger) share(c *mergeCursor, id uint32) *pathShare {
	if at := c.shareOf[id]; at >= 0 {
		return &m.shares[at]
	}
	path := c.part.paths[id]
	at, ok := m.shareIdx[path]
	if !ok {
		if m.shareIdx == nil {
			m.shareIdx = map[string]int32{}
		}
		at = int32(len(m.shares))
		m.shareIdx[path] = at
		m.shares = append(m.shares, pathShare{path: path})
	}
	c.shareOf[id] = at

	return &m.shares[at]
}

// planShared decides, before the merge writes a block, which string columns
// its part shares, and readies their dictionary: the strings of the
// dictionaries of the parts it reads, merged in order without a look at the
// rows, and with them the strings of the blocks that store their own, which
// it reads for that. A column that no part shares yet is shared when its
// values repeat enough across the blocks of the range for the dictionary and
// the codes to take one part in sharedGain less than its chunks do; one
// that every part shares stays shared for as long as that takes no more.
func (m *merger) planShared() error {
	if !m.w.packs(vec.KindString) {
		return nil
	}
	m.shares = m.shares[:0]
	clear(m.shareIdx)
	for _, c := range m.cursors {
		if c.done {
			continue
		}
		p := c.part
		c.shareOf, c.sharedVals = grow(c.shareOf, len(p.paths)), grow(c.sharedVals, len(p.paths))
		c.plans, c.planSrc = grow(c.plans, len(p.paths)), grow(c.planSrc, len(p.paths))
		clear(c.sharedVals)
		clear(c.plans)
		for i := range c.shareOf {
			c.shareOf[i] = -1
		}
		c.firstBlock, c.endBlock = c.block, p.NumBlocks()
		if m.hi != nil {
			c.endBlock = c.firstBlock + sort.Search(c.endBlock-c.firstBlock, func(i int) bool { return bytes.Compare(p.firstKeys[c.firstBlock+i], m.hi) >= 0 })
		}
		for b := c.firstBlock; b < c.endBlock; b++ {
			if err := p.OpenBlock(b, &m.last); err != nil {
				return err
			}
			refs := m.last.Columns()
			for j := range refs {
				ref := &refs[j]
				if ref.Kind != vec.KindString || ref.bad || ref.Count == 0 {
					continue
				}
				sh := m.share(c, ref.PathID)
				if ref.enc == encShared {
					sh.sharedValues += uint64(ref.Count)
					c.sharedVals[ref.PathID] += uint64(ref.Count)
				} else {
					sh.ownBytes += uint64(ref.length)
					sh.ownValues += uint64(ref.Count)
				}
			}
		}
	}
	for i := range m.shares {
		sh := &m.shares[i]
		if sh.path == TimePath || m.w.isKey(sh.path) {
			continue
		}
		// Chunks of a few bytes a value are timed or of few values, which
		// codes do not beat.
		if sh.sharedValues == 0 && (sh.ownValues < minSharedValues || sh.ownBytes < minFlatBytes || sh.ownBytes < minFlatValue*sh.ownValues) {
			continue
		}
		if err := m.planPath(sh); err != nil {
			return err
		}
	}

	return nil
}

// planPath plans the column at the path of sh, unless sharing it is not
// worth it.
func (m *merger) planPath(sh *pathShare) error {
	sp := sharedPlans.Get()
	sp.path, sp.merged = sh.path, true
	for _, c := range m.cursors {
		if c.done {
			continue
		}
		if id, ok := c.part.PathID(sh.path); ok && c.sharedVals[id] > 0 {
			d, ok := c.part.Dictionary(id)
			if !ok {
				sp.release()

				return ErrCorrupt
			}
			sp.addSource(c.part, d, c.sharedVals[id])
		}
	}
	ok := true
	var err error
	if sh.ownValues > 0 {
		ok, err = m.gather(sp, sh)
	}
	if ok && err == nil {
		ok, err = sp.number()
	}
	if err != nil || !ok || !sp.worth(sh) {
		sp.release()

		return err
	}
	m.w.setPlan(sp)
	for _, c := range m.cursors {
		if c.done {
			continue
		}
		if id, ok := c.part.PathID(sh.path); ok {
			c.plans[id], c.planSrc[id] = sp, sp.source(c.part)
		}
	}

	return nil
}

// gather reads the strings that the blocks of the range store themselves at
// the path of sh into the set of sp, and reports whether it did: not when
// the first of them hardly repeat in a column that no part shares yet, nor
// when the merges under way hold all the memory they may for such sets.
func (m *merger) gather(sp *sharedPlan, sh *pathShare) (bool, error) {
	set := valueSets.Get()
	sp.set = set
	introduce := len(sp.srcs) == 0
	col := &m.shareCol
	for pass := 0; pass < 2; pass++ {
		for _, c := range m.cursors {
			if c.done {
				continue
			}
			id, ok := c.part.PathID(sh.path)
			if !ok {
				continue
			}
			for b := c.firstBlock; b < c.endBlock; b++ {
				// One block in shareSample first, from all over the range.
				if (b%shareSample == 0) != (pass == 0) {
					continue
				}
				if err := c.part.OpenBlock(b, &m.last); err != nil {
					return false, err
				}
				ref, ok := m.last.Column(id, vec.KindString)
				if !ok || ref.enc == encShared || ref.Count == 0 {
					continue
				}
				if err := m.last.DecodeValues(ref, col); err != nil {
					return false, err
				}
				if !sp.take(c.src, b, col, ref.Count) {
					return false, nil
				}
				if grown := int64(set.retained()+4*cap(sp.numbers)) - sp.reserved; grown >= setStep {
					if !reserve(grown) {
						return false, nil
					}
					sp.reserved += grown
				}
			}
		}
		if pass == 0 && introduce && set.added >= sharedTrialValues &&
			((set.added-uint64(set.len()))*shareRepeats < set.added || set.len() <= maxCounted) {
			return false, nil
		}
	}
	if introduce && uint64(set.len())*sharedGain > sh.ownValues*(sharedGain-1) {
		return false, nil
	}
	set.seal()

	return true, nil
}

// worth reports whether the dictionary of the plan and the codes of the
// values that sh counts take less than the chunks that the blocks would
// hold on their own, by one part in sharedGain when some of the blocks do
// hold such chunks. The strings that the part will hold are not known
// before its rows are: of a dictionary that the range reads a share of, it
// takes as many as that share of the values would hold if each string had
// as many values as any other, which is more than a skewed column holds.
func (sp *sharedPlan) worth(sh *pathShare) bool {
	live, flat := 0.0, float64(sh.ownBytes)
	if sp.set != nil {
		live = float64(sp.set.len())
	}
	for i := range sp.srcs {
		s := &sp.srcs[i]
		share := min(1, float64(s.values)/float64(max(s.dict.values, 1)))
		repeats := float64(s.dict.values) / float64(s.dict.stored)
		live += float64(s.dict.stored) * (1 - math.Pow(1-share, repeats))
		flat += float64(s.dict.flat) * share
	}
	codes := float64(sp.codes)
	live = max(min(live, codes), 1)
	values := sh.ownValues + sh.sharedValues
	others, otherBytes := float64(sp.stats.others)*live/codes, float64(sp.stats.otherBytes)*live/codes
	shared := float64(sp.layout.size(uint64(live), uint64(others), uint64(otherBytes), uint64(sp.codes))) +
		float64(values)*float64(bits.Len(uint(sp.codes-1)))/8 + 4*float64(values/defaultBlockRows+1)
	sp.planned, sp.flat = values, uint64(flat)
	if sh.ownValues == 0 {
		return shared <= flat
	}

	return shared*sharedGain <= flat*(sharedGain-1)
}

// decodeShared reads column j of the cursor's block, which the output
// shares with plan sp, as the codes of that plan: from the codes of the
// block where it stores codes, without reading a string, and otherwise from
// its strings. The column then holds only the presence of its values.
func (c *mergeCursor) decodeShared(j int, sp *sharedPlan) (*sharedBuf, error) {
	ref, col := &c.refs[j], c.cols[j]
	for len(c.sbufs) <= j {
		c.sbufs = append(c.sbufs, sharedBuf{})
	}
	sb := &c.sbufs[j]
	sb.plan = sp
	if ref.enc == encShared {
		col.release()
		var err error
		if sb.codes, err = c.blk.DecodeShared(ref, &col.Column, sb.codes); err != nil {
			return nil, err
		}
		sb.locals = grow(sb.locals, ref.Count)
		if sb.src = c.planSrc[ref.PathID]; sb.src < 0 {
			return nil, ErrCorrupt
		}

		return sb, sp.fromDict(sb.src, sb.codes, sb.locals)
	}
	// The strings of the block were read into the set of the plan, which
	// kept the number of each.
	at, ok := sp.numbersAt[uint64(c.src)<<32|uint64(c.block)]
	if !ok || at+ref.Count > len(sp.numbers) {
		return nil, errShared
	}
	col.release()
	if err := c.blk.DecodePresence(ref, &col.Column); err != nil {
		return nil, err
	}
	sb.codes, sb.locals, sb.src = grow(sb.codes, ref.Count), grow(sb.locals, ref.Count), int32(len(sp.srcs))
	sp.fromNumbers(sp.numbers[at:at+ref.Count], sb.codes, sb.locals)

	return sb, nil
}

// take adds the count values of col, the strings of block b of the part at
// place src among those of the merge, decoded by value, to the set of the
// plan and notes the number of each; it reports whether the set had room.
func (sp *sharedPlan) take(src, b int, col *vec.Column, count int) bool {
	if sp.numbersAt == nil {
		sp.numbersAt = map[uint64]int{}
	}
	sp.numbersAt[uint64(src)<<32|uint64(b)] = len(sp.numbers)
	set := sp.set
	if col.Layout == vec.Flat {
		for i := 0; i < count; i++ {
			number, ok := set.number(col.Data[col.Offsets[i]:col.Offsets[i+1]])
			if !ok {
				return false
			}
			sp.numbers = append(sp.numbers, number)
		}

		return true
	}
	// The entries of a dictionary are all the strings there are: each is
	// looked up once, and the values take the numbers of their entries.
	entries := col.DictLen()
	at := len(sp.numbers)
	for e := 0; e < entries; e++ {
		number, ok := set.number(col.DictValue(e))
		if !ok {
			return false
		}
		sp.numbers = append(sp.numbers, number)
	}
	for i := 0; i < count; i++ {
		code := 0
		if col.Layout == vec.Dict {
			code = col.Code(i)
		}
		sp.numbers = append(sp.numbers, sp.numbers[at+code])
	}
	sp.numbers = sp.numbers[:at+copy(sp.numbers[at:], sp.numbers[at+entries:])]
	set.added += uint64(count - entries)

	return true
}
