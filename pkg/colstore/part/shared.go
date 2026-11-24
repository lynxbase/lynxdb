package part

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"math/bits"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// A string column whose values repeat across the blocks of a part is shared:
// its blocks store, in place of the strings, their codes in the dictionary
// of the part, which stores each string once. A block of such a column
// holds, after the presence of a sparse one, the layout of its codes, the
// lowest of them, and every code above that one.
const (
	sharedPacked = 0 // the bits of a code, then every code in that many bits
	sharedRuns   = 1 // the code and the length of every run of equal codes
)

const (
	// minSharedValues is the least values of a column in a part for which
	// a dictionary is thought of.
	minSharedValues = 4096
	// sharedTrialValues is the least values of the first block of a column
	// that a flush weighs the forms on.
	sharedTrialValues = 1024
	// sharedGain makes a column shared when that takes at most all but one
	// part in this many of what the forms of single blocks take: less is
	// within what the sizes are known to before the part is written, and
	// not worth reading a dictionary beside every block.
	sharedGain = 8
	// shareSample is one in how many runs of shareRun values is looked at
	// first; a column of which fewer than one in shareRepeats of those
	// repeats an earlier one is left alone.
	shareSample  = 16
	shareRun     = 64
	shareRepeats = 64
	// shareSkip is the number of parts a writer writes before it thinks
	// again of sharing a column that it found not worth it.
	shareSkip = 16
	// minFlatValue is the bytes a value that a column takes in the forms of
	// single blocks from which a merge thinks of sharing it: the codes
	// alone take two or three.
	minFlatValue = 4
	// minFlatBytes is the least such bytes of a column in the parts of a
	// merge for the same.
	minFlatBytes = 64 << 10
	// setStep is the growth of a set of strings between two requests for
	// memory.
	setStep = 1 << 20
	// setAhead is how many times the strings of the values looked at first
	// a set makes room for at once.
	setAhead = 4
	// sharedSeen is the number of codes of a block remembered so that the
	// tokens of a string are taken once.
	sharedSeen = 1024
)

// errShared reports codes of a shared column that do not match its rows.
var errShared = errors.New("part: a shared column was given other codes than it has values")

// sharedPlan is a column of the part being written that is shared: where
// its strings come from, the codes they get and which of them blocks used.
type sharedPlan struct {
	path string
	// merged tells that a merge gives the codes of every block; otherwise
	// the writer finds them in set. tentative is set until the first block
	// of the column has been weighed.
	merged, tentative bool
	codes             int
	used              vec.Bitmap
	stats             entryStats
	layout            packLayout
	// planned is the number of values that the plan was made for and flat
	// what they take in the forms of single blocks; values counts those
	// written as codes.
	planned, flat, values uint64
	// set holds the strings of the blocks that store them as they are, and
	// setCodes the code of each by its place when the plan has other
	// sources.
	set      *valueSet
	setCodes codeMap
	srcs     []planSource
	// reserved is the memory of set and numbers taken from the budget.
	reserved int64
	// numbers holds, for the blocks that a merge read into set, the number
	// in set of every value, block after block, and numbersAt where those
	// of a block begin: a merge then writes the codes of such a block
	// without reading its strings a second time. starved is set when the
	// merges under way left no memory for the set.
	numbers   []uint32
	numbersAt map[blockKey]int
	starved   bool
	// block and local hold, for the values of the block being gathered, the
	// code and the number of the string in its source, which for the set is
	// the number it got there, and from the sources of runs of them.
	block, local []uint32
	from         []fromRun
	seen         [sharedSeen]uint32
	heap         []int32
	walk         []planWalk
	last         []byte
}

// blockKey names a block of a part being merged.
type blockKey struct {
	part  *Part
	block int
}

// planSource is the dictionary of a part being merged, with the code that
// the plan gives each of its stored strings that the merge needs.
type planSource struct {
	part  *Part
	dict  *Dictionary
	codes codeMap
	// need marks the codes of the dictionary that the blocks to be merged
	// hold; nil when the merge reads every block of the part and so needs
	// every string of it. needBuf is its memory, the plan's own: the plan
	// outlives the merge that found the codes.
	need, needBuf vec.Bitmap
	// values counts the values that the blocks to be merged hold as codes
	// of the dictionary.
	values uint64
}

// codeMap holds the code that a plan gives each string of one of its
// sources, by the number of the string in the source; the codes never
// decrease with the numbers. It keeps them as they are, or a few bits of
// each, where a string that the merge does not need has the code of the one
// before it.
type codeMap struct {
	few    bool
	dense  []uint32
	wide   monotone
	filled int
	last   uint32
}

// fewCodes is the most strings of the sources of one plan whose codes are
// kept as they are, 4 MiB of them, which are read at once; the bits kept of
// the others are searched at every read.
const fewCodes = 1 << 20

// reset readies the map for the codes, below bound, of n strings: as they
// are when few is set.
func (m *codeMap) reset(n int, bound uint64, few bool) {
	m.few, m.filled, m.last = few, 0, 0
	if m.few {
		m.dense = grow(m.dense, n)

		return
	}
	m.wide.reset(n, bound)
}

// put notes the code of string i, which follows every string put before.
func (m *codeMap) put(i int, code uint32) {
	if m.few {
		m.dense[i] = code

		return
	}
	for ; m.filled < i; m.filled++ {
		m.wide.append(m.last)
	}
	m.wide.append(code)
	m.filled, m.last = i+1, code
}

// seal ends the puts of a source of n strings.
func (m *codeMap) seal(n int) {
	for ; !m.few && m.filled < n; m.filled++ {
		m.wide.append(m.last)
	}
}

func (m *codeMap) get(i int) uint32 {
	if m.few {
		return m.dense[i]
	}

	return m.wide.get(i)
}

func (m *codeMap) retained() int { return 4*cap(m.dense) + m.wide.retained() }

// fromRun tells that the next n values of a block come from source src,
// the set when src is the number of dictionaries.
type fromRun struct{ src, n int32 }

// planWalk walks the strings of one source of a plan that the merge needs,
// in order: pos is the number of the string it stands on in the source, at
// the code of that string in a dictionary of which the merge needs a part,
// and val the string, which buf holds when the source is a dictionary and
// the set itself otherwise.
type planWalk struct {
	pos, at, n int
	val, buf   []byte
	code       uint32
}

// maxPlanBytes bounds the buffers that a pooled plan keeps.
const maxPlanBytes = 8 << 20

var sharedPlans = mem.Pool[sharedPlan]{Reset: func(sp *sharedPlan) bool {
	retained := 8*cap(sp.used) + 4*(cap(sp.block)+cap(sp.local)) + sp.setCodes.retained()
	srcs := sp.srcs[:cap(sp.srcs)]
	for i := range srcs {
		srcs[i].part, srcs[i].dict, srcs[i].need = nil, nil, nil
		retained += srcs[i].codes.retained() + 8*cap(srcs[i].needBuf)
	}
	for i := range sp.walk {
		sp.walk[i].val = nil
		retained += cap(sp.walk[i].buf)
	}
	sp.srcs, sp.set = sp.srcs[:0], nil
	sp.merged, sp.tentative, sp.starved, sp.codes, sp.planned, sp.flat, sp.values = false, false, false, 0, 0, 0, 0
	sp.block, sp.local, sp.from = sp.block[:0], sp.local[:0], sp.from[:0]
	sp.numbers = sp.numbers[:0]
	clear(sp.numbersAt)

	return retained+4*cap(sp.numbers) <= maxPlanBytes
}}

// release gives back what the plan holds beyond its own buffers.
func (sp *sharedPlan) release() {
	if sp.set != nil {
		valueSets.Put(sp.set)
		sp.set = nil
	}
	if sp.reserved > 0 {
		setHeld.Add(-sp.reserved)
		sp.reserved = 0
	}
	sharedPlans.Put(sp)
}

// source returns the number of the dictionary of p among the sources.
func (sp *sharedPlan) source(p *Part) int32 {
	for i := range sp.srcs {
		if sp.srcs[i].part == p {
			return int32(i)
		}
	}

	return -1
}

// addSource makes the dictionary d of p a source of the plan: values of
// the values to be merged are its codes, those that need marks, or any of
// them when need is nil.
func (sp *sharedPlan) addSource(p *Part, d *Dictionary, values uint64, need vec.Bitmap) {
	if len(sp.srcs) < cap(sp.srcs) {
		sp.srcs = sp.srcs[:len(sp.srcs)+1]
	} else {
		sp.srcs = append(sp.srcs, planSource{})
	}
	s := &sp.srcs[len(sp.srcs)-1]
	s.part, s.dict, s.values, s.need = p, d, values, nil
	if need != nil {
		s.needBuf = append(s.needBuf[:0], need...)
		s.need = s.needBuf
	}
}

// sources returns the number of sources, the set among them.
func (sp *sharedPlan) sources() int {
	if sp.set != nil {
		return len(sp.srcs) + 1
	}

	return len(sp.srcs)
}

// entries returns the number of strings of source i.
func (sp *sharedPlan) entries(i int) int {
	if i < len(sp.srcs) {
		return sp.srcs[i].dict.stored
	}

	return sp.set.len()
}

// codesOf returns the codes of the strings of source i.
func (sp *sharedPlan) codesOf(i int) *codeMap {
	if i < len(sp.srcs) {
		return &sp.srcs[i].codes
	}

	return &sp.setCodes
}

// load makes the string at the place that walk i stands on its value.
func (sp *sharedPlan) load(i int) error {
	wk := &sp.walk[i]
	if i == len(sp.srcs) {
		wk.val = sp.set.value(wk.pos)

		return nil
	}
	var err error
	wk.buf, err = sp.srcs[i].dict.entry(wk.buf[:0], wk.pos)
	wk.val = wk.buf

	return err
}

// step moves walk i to the next string of its source that the merge needs
// and reports whether there is one.
func (sp *sharedPlan) step(i int) (bool, error) {
	wk := &sp.walk[i]
	if i == len(sp.srcs) || sp.srcs[i].need == nil {
		wk.pos++

		return wk.pos < wk.n, nil
	}
	s := &sp.srcs[i]
	if wk.at = nextSet(s.need, wk.at+1); wk.at < 0 {
		return false, nil
	}
	var err error
	wk.pos, err = s.dict.index(uint32(wk.at))

	return err == nil, err
}

// nextSet returns the first bit of bm at or after from that is set, or -1.
func nextSet(bm vec.Bitmap, from int) int {
	for w := from >> 6; w < len(bm); w++ {
		word := bm[w]
		if w == from>>6 {
			word &= ^uint64(0) << (uint(from) & 63)
		}
		if word != 0 {
			return w<<6 + bits.TrailingZeros64(word)
		}
	}

	return -1
}

// sift restores the heap of walks below position at: ordered by the codes
// of the strings they stand on with byCode set, and by the strings
// otherwise.
func (sp *sharedPlan) sift(at int, byCode bool) {
	h := sp.heap
	less := func(a, b int32) bool {
		if byCode {
			return sp.walk[a].code < sp.walk[b].code
		}

		return bytes.Compare(sp.walk[a].val, sp.walk[b].val) < 0
	}
	for {
		least, l := at, 2*at+1
		if l < len(h) && less(h[l], h[least]) {
			least = l
		}
		if r := l + 1; r < len(h) && less(h[r], h[least]) {
			least = r
		}
		if least == at {
			return
		}
		h[at], h[least] = h[least], h[at]
		at = least
	}
}

// startWalks puts a walk on the first string of every source that the merge
// needs, with its code when coded is set and the string itself otherwise,
// and makes a heap of them.
func (sp *sharedPlan) startWalks(coded bool) error {
	k := sp.sources()
	for len(sp.walk) < k {
		sp.walk = append(sp.walk, planWalk{})
	}
	sp.heap = sp.heap[:0]
	for i := 0; i < k; i++ {
		sp.walk[i].pos, sp.walk[i].at, sp.walk[i].n = -1, -1, sp.entries(i)
		ok, err := sp.advance(i, coded)
		if err != nil {
			return err
		}
		if ok {
			sp.heap = append(sp.heap, int32(i))
		}
	}
	for at := len(sp.heap)/2 - 1; at >= 0; at-- {
		sp.sift(at, coded)
	}

	return nil
}

// advance moves walk i on and readies what orders it: the code of its
// string with coded set and the string otherwise.
func (sp *sharedPlan) advance(i int, coded bool) (bool, error) {
	ok, err := sp.step(i)
	if !ok || err != nil {
		return false, err
	}
	if coded {
		sp.walk[i].code = sp.codesOf(i).get(sp.walk[i].pos)

		return true, nil
	}

	return true, sp.load(i)
}

// pop moves the walk at the top of the heap on, or takes it off the heap
// when its source has no string left.
func (sp *sharedPlan) pop(coded bool) error {
	ok, err := sp.advance(int(sp.heap[0]), coded)
	if err != nil {
		return err
	}
	if !ok {
		sp.heap[0] = sp.heap[len(sp.heap)-1]
		sp.heap = sp.heap[:len(sp.heap)-1]
	}
	sp.sift(0, coded)

	return nil
}

// stride returns the length that most strings of the sources have.
func (sp *sharedPlan) stride() int {
	var weight [maxDictStride + 1]uint64
	for i := range sp.srcs {
		d := sp.srcs[i].dict
		weight[d.reader.stride] += uint64(d.stored - d.exceptions)
	}
	if sp.set != nil {
		length, count := sp.set.stride()
		weight[length] += uint64(count)
	}
	best := 0
	for length, n := range weight {
		if n > weight[best] {
			best = length
		}
	}

	return best
}

// number merges the strings of the sources that the merge needs in the order
// of their bytes and gives each distinct one the next code, which it notes
// for every source that holds the string. It reports whether the strings
// pack.
func (sp *sharedPlan) number() (bool, error) {
	k := sp.sources()
	bound := uint64(0)
	for i := 0; i < k; i++ {
		bound += uint64(sp.entries(i))
	}
	if bound == 0 || bound > math.MaxUint32 {
		return false, nil
	}
	sp.stats.reset(sp.stride())
	if len(sp.srcs) == 0 {
		// The set alone: the code of a string is its place. What packing
		// takes does not depend on the order of the strings, and they lie
		// side by side in the order they came in.
		for number := 0; number < sp.set.len(); number++ {
			sp.stats.add(sp.set.numbered(uint32(number)))
		}
		sp.codes = sp.set.len()
	} else {
		few := fewCodes
		for i := 0; i < k; i++ {
			n := sp.entries(i)
			sp.codesOf(i).reset(n, bound, n <= few)
			if n <= few {
				few -= n
			}
		}
		if err := sp.startWalks(false); err != nil {
			return false, err
		}
		code := -1
		for len(sp.heap) > 0 {
			i := int(sp.heap[0])
			wk := &sp.walk[i]
			if code < 0 || !bytes.Equal(wk.val, sp.last) {
				code++
				sp.last = append(sp.last[:0], wk.val...)
				sp.stats.add(wk.val)
			}
			sp.codesOf(i).put(wk.pos, uint32(code))
			if err := sp.pop(false); err != nil {
				return false, err
			}
		}
		for i := 0; i < k; i++ {
			sp.codesOf(i).seal(sp.entries(i))
		}
		sp.codes = code + 1
	}
	if sp.codes < 2 || !sp.layout.set(&sp.stats) {
		return false, nil
	}
	sp.used = grow(sp.used, vec.BitmapWords(sp.codes))
	clear(sp.used)

	return true, nil
}

// codeOf returns the code of string number local of source src.
func (sp *sharedPlan) codeOf(src int, local uint32) uint32 {
	if len(sp.srcs) == 0 {
		return local
	}

	return sp.codesOf(src).get(int(local))
}

// eachUsed calls add with every string whose code a block used, in the
// order of the codes.
func (sp *sharedPlan) eachUsed(add func(val []byte)) error {
	if len(sp.srcs) == 0 {
		for place := 0; place < sp.codes; place++ {
			if sp.used.Get(place) {
				add(sp.set.value(place))
			}
		}

		return nil
	}
	if err := sp.startWalks(true); err != nil {
		return err
	}
	last := -1
	for len(sp.heap) > 0 {
		i := int(sp.heap[0])
		if code := int(sp.walk[i].code); code != last {
			last = code
			if sp.used.Get(code) {
				if err := sp.load(i); err != nil {
					return err
				}
				add(sp.walk[i].val)
			}
		}
		if err := sp.pop(true); err != nil {
			return err
		}
	}

	return nil
}

// fromDict turns codes, those that a block of the part of source src holds,
// into the codes of the plan, and puts the numbers of their strings among
// the stored strings of the source in locals.
func (sp *sharedPlan) fromDict(src int32, codes, locals []uint32) error {
	s := &sp.srcs[src]
	d := s.dict
	prev, prevLocal, prevCode := ^uint32(0), uint32(0), uint32(0)
	for i, c := range codes {
		if c != prev {
			at, err := d.index(c)
			if err != nil {
				return err
			}
			prev, prevLocal, prevCode = c, uint32(at), s.codes.get(at)
		}
		locals[i], codes[i] = prevLocal, prevCode
	}

	return nil
}

// fromNumbers puts in codes the codes of the strings of the set with the
// numbers given, and the numbers in locals.
func (sp *sharedPlan) fromNumbers(numbers, codes, locals []uint32) {
	src := len(sp.srcs)
	for i, number := range numbers {
		locals[i], codes[i] = number, sp.codeOf(src, sp.set.place(number))
	}
}

// put adds values of the block being gathered, which come from source src.
func (sp *sharedPlan) put(src int32, codes, locals []uint32) {
	sp.block = append(sp.block, codes...)
	sp.local = append(sp.local, locals...)
	if k := len(sp.from) - 1; k >= 0 && sp.from[k].src == src {
		sp.from[k].n += int32(len(codes))

		return
	}
	sp.from = append(sp.from, fromRun{src, int32(len(codes))})
}

// fresh reports whether code was not among the last codes it was asked for.
func (sp *sharedPlan) fresh(code uint32) bool {
	slot := &sp.seen[code%sharedSeen]
	if *slot == code+1 {
		return false
	}
	*slot = code + 1

	return true
}

// valueRun is the values from to to of a column of a batch.
type valueRun struct {
	c        *shred.Column
	from, to int
}

// isKey reports whether path is one that orders the rows of the part.
func (w *Writer) isKey(path string) bool {
	for _, k := range w.opts.SortKey {
		if k == path {
			return true
		}
	}

	return false
}

// sharedPlan returns the plan of the string column at a path, or nil when
// it is not shared. Plans go by the names of their paths: a path gets its
// number in the part when a block first holds it, as in a part without
// plans, since the order of the paths decides which column a block's timed
// columns follow.
func (w *Writer) sharedPlan(path string) *sharedPlan {
	if w.planned == 0 {
		return nil
	}

	return w.plans[path]
}

func (w *Writer) setPlan(sp *sharedPlan) {
	if w.plans == nil {
		w.plans = map[string]*sharedPlan{}
	}
	w.plans[sp.path] = sp
	w.planned++
}

// dropPlan gives a plan up; with skip set the writer leaves the column
// alone in the parts it writes next.
func (w *Writer) dropPlan(sp *sharedPlan, skip bool) {
	if skip {
		if w.shareSkip == nil {
			w.shareSkip = map[string]uint8{}
		}
		w.shareSkip[sp.path] = shareSkip
	}
	delete(w.plans, sp.path)
	w.planned--
	sp.release()
}

// dropPlans gives up every plan.
func (w *Writer) dropPlans() {
	for _, sp := range w.plans {
		sp.release()
	}
	clear(w.plans)
	w.planned = 0
}

// Share tells the writer, before the first block, every row that the part
// will hold, as runs of rows of batches, so that it can give the part a
// dictionary for the string columns whose values repeat across its blocks:
// those of one length for the most part, of which at least one in eight
// repeats an earlier one, and that the forms of single blocks do not hold
// in less. The batches may change once it returns. A part written without
// it has no dictionary.
func (w *Writer) Share(runs []shred.Run) {
	if w.err != nil || w.rows > 0 || !w.packs(vec.KindString) {
		return
	}
	rows := 0
	for i := range runs {
		rows += runs[i].To - runs[i].From
	}
	if rows < minSharedValues {
		return
	}
	// The runs of values hold columns of the batches until the next call.
	defer func() { clear(w.shareRuns[:cap(w.shareRuns)]) }()
	if w.shareSeen == nil {
		w.shareSeen = map[string]struct{}{}
	}
	clear(w.shareSeen)
	for i := range runs {
		for _, c := range runs[i].Batch.Columns() {
			if c.Kind != vec.KindString || c.Count == 0 || c.Path == TimePath || w.isKey(c.Path) {
				continue
			}
			if _, seen := w.shareSeen[c.Path]; seen {
				continue
			}
			w.shareSeen[c.Path] = struct{}{}
			w.shareRuns = w.shareRuns[:0]
			total := 0
			for j := i; j < len(runs); j++ {
				col := runs[j].Batch.Lookup(c.Path, vec.KindString)
				if col == nil || col.Count == 0 {
					continue
				}
				from, to := valueRange(col, runs[j].From, runs[j].To)
				if from < to {
					w.shareRuns = append(w.shareRuns, valueRun{col, from, to})
					total += to - from
				}
			}
			w.shareColumn(c.Path, w.shareRuns, total)
		}
	}
}

// valueRange returns the values of c that rows from to to hold.
func valueRange(c *shred.Column, from, to int) (first, end int) {
	if !c.Sparse {
		return min(from, c.Count), min(to, c.Count)
	}
	ids := c.RowIDs[:c.Count]

	return sort.Search(len(ids), func(i int) bool { return ids[i] >= uint32(from) }),
		sort.Search(len(ids), func(i int) bool { return ids[i] >= uint32(to) })
}

// shareColumn plans a dictionary for the string column at path, whose
// values in the part are those of runs, total of them, unless they repeat
// too little or do not pack.
func (w *Writer) shareColumn(path string, runs []valueRun, total int) {
	if total < minSharedValues {
		return
	}
	if left := w.shareSkip[path]; left > 0 {
		w.shareSkip[path] = left - 1

		return
	}
	// A column of strings of many lengths is not one of identifiers.
	var lengths [maxDictStride + 2]uint32
	seen, most := 0, uint32(0)
	for _, r := range runs {
		for i := r.from; i < r.to; i += max((r.to-r.from)/16, 1) {
			l := min(len(r.c.Bytes(i)), len(lengths)-1)
			lengths[l]++
			seen, most = seen+1, max(most, lengths[l])
		}
	}
	if int(most)*8 < seen*7 || lengths[len(lengths)-1] == most {
		return
	}
	set := valueSets.Get()
	reject := func() {
		valueSets.Put(set)
		if w.shareSkip == nil {
			w.shareSkip = map[string]uint8{}
		}
		w.shareSkip[path] = shareSkip
	}
	for pass := 0; pass < 2; pass++ {
		for _, r := range runs {
			for i := r.from; i < r.to; i++ {
				if ((i-r.from)/shareRun%shareSample == 0) == (pass == 0) && !set.add(r.c.Bytes(i)) {
					reject()

					return
				}
			}
		}
		// Values that hardly repeat gain nothing, and the few values of a
		// column that holds no others fit the dictionaries of its blocks,
		// which count their rows.
		if pass == 0 && set.added >= sharedTrialValues &&
			((set.added-uint64(set.len()))*shareRepeats < set.added || set.len() <= maxCounted) {
			reject()

			return
		}
		if pass == 0 && set.len() > 0 {
			n := min(total, set.len()*setAhead)
			set.reserve(n, n*(len(set.data)/set.len()+1))
		}
	}
	if set.len()*sharedGain > total*(sharedGain-1) {
		reject()

		return
	}
	set.seal()
	sp := sharedPlans.Get()
	sp.set, sp.tentative, sp.planned = set, true, uint64(total)
	if ok, _ := sp.number(); !ok {
		sp.set = nil
		sp.release()
		reject()

		return
	}
	sp.path = path
	w.setPlan(sp)
}

// weigh encodes the first block that holds enough values of a column that a
// flush planned to share in the form the block would take on its own, and
// keeps the plan when the dictionary and the codes of all blocks come out
// smaller by enough. A block of fewer values keeps its own form, and the
// plan waits for the next.
func (w *Writer) weigh(ent *dirEntry, c *shred.Column, sp *sharedPlan) bool {
	if c.Count < sharedTrialValues {
		return false
	}
	start := len(w.block)
	probe := *ent
	w.dry = true
	w.encodeBytes(&probe, c)
	w.dry = false
	own := uint64(len(w.block) - start)
	w.block = w.block[:start]
	flat := own * sp.planned / uint64(c.Count)
	blocks := sp.planned/uint64(c.Count) + 1
	codes := uint64(sp.codes)
	shared := sp.layout.size(codes, sp.stats.others, sp.stats.otherBytes, codes) +
		sp.planned*uint64(bits.Len(uint(sp.codes-1)))/8 + 4*blocks
	if shared*sharedGain > flat*(sharedGain-1) {
		w.dropPlan(sp, true)

		return false
	}
	sp.tentative, sp.flat = false, flat

	return true
}

// encodeShared appends the string column c as codes of its part dictionary
// and reports whether it did; when it did not, the block stores the column
// in a form of its own.
func (w *Writer) encodeShared(ent *dirEntry, c *shred.Column, sp *sharedPlan) bool {
	n := c.Count
	codes := sp.block
	if !sp.merged {
		if sp.tentative && !w.weigh(ent, c, sp) {
			return false
		}
		codes = w.codes32[:0]
		for i := 0; i < n; i++ {
			place, ok := sp.set.find(c.Bytes(i))
			if !ok {
				// A row that Share was not told of.
				return false
			}
			codes = append(codes, place)
		}
		w.codes32 = codes
	} else if len(codes) != n {
		w.err = errShared

		return false
	}
	w.block = appendSharedCodes(w.block, codes, &w.e.tmp)
	for _, code := range codes {
		sp.used.Set(int(code))
	}
	sp.values += uint64(n)
	ent.enc = encShared
	w.noSum(ent.pathID)
	if !w.opts.NoTokenFilter {
		w.tokenizeShared(c, sp, codes)
	}
	sp.block, sp.local, sp.from = sp.block[:0], sp.local[:0], sp.from[:0]

	return true
}

// tokenizeShared adds the tokens of the strings of a block of a shared
// column, each string once for the most part.
func (w *Writer) tokenizeShared(c *shred.Column, sp *sharedPlan, codes []uint32) {
	sp.seen = [sharedSeen]uint32{}
	if !sp.merged {
		for i, code := range codes {
			if sp.fresh(code) {
				w.tokenize(vec.KindString, c.Bytes(i))
			}
		}

		return
	}
	i := 0
	for _, run := range sp.from {
		for end := i + int(run.n); i < end; i++ {
			if !sp.fresh(codes[i]) {
				continue
			}
			if int(run.src) == len(sp.srcs) {
				w.tokenize(vec.KindString, sp.set.numbered(sp.local[i]))

				continue
			}
			var err error
			if w.e.tmp, err = sp.srcs[run.src].dict.entry(w.e.tmp[:0], int(sp.local[i])); err != nil {
				w.err = err

				return
			}
			w.tokenize(vec.KindString, w.e.tmp)
		}
	}
}

// appendSharedCodes appends the codes of a block of a shared column: as runs
// when those take at most half of what the codes take one by one.
func appendSharedCodes(dst []byte, codes []uint32, scratch *[]byte) []byte {
	lo, hi := codes[0], codes[0]
	for _, c := range codes {
		lo, hi = min(lo, c), max(hi, c)
	}
	width := uint(bits.Len32(hi - lo))
	if width == 0 {
		// One code for all the values is one run.
		dst = binary.AppendUvarint(append(dst, sharedRuns), uint64(lo))

		return binary.AppendUvarint(append(dst, 0), uint64(len(codes)))
	}
	limit := packedSize(len(codes), width) / 2
	runs := (*scratch)[:0]
	for i := 0; i < len(codes) && len(runs) < limit; {
		run := 1
		for i+run < len(codes) && codes[i+run] == codes[i] {
			run++
		}
		runs = binary.AppendUvarint(runs, uint64(codes[i]-lo))
		runs = binary.AppendUvarint(runs, uint64(run))
		i += run
	}
	*scratch = runs
	if len(runs) < limit {
		dst = binary.AppendUvarint(append(dst, sharedRuns), uint64(lo))

		return append(dst, runs...)
	}
	dst = binary.AppendUvarint(append(dst, sharedPacked), uint64(lo))
	dst = append(dst, byte(width))
	var acc uint64
	var filled uint
	for _, c := range codes {
		v := uint64(c - lo)
		acc |= v << filled
		filled += width
		if filled >= 64 {
			dst = binary.LittleEndian.AppendUint64(dst, acc)
			filled -= 64
			// The part of v that did not fit starts the next word.
			acc = v >> (width - filled)
		}
	}
	for ; filled > 0; filled -= min(filled, 8) {
		dst = append(dst, byte(acc))
		acc >>= 8
	}

	return dst
}

// readSharedCodes fills dst with the count codes of the codes section src,
// all of which are below limit, and reports whether the section is sound.
func readSharedCodes(dst []uint32, src []byte, count int, limit uint64) ([]uint32, bool) {
	if len(src) < 2 {
		return dst, false
	}
	mode := src[0]
	base, n := binary.Uvarint(src[1:])
	if n <= 0 || base >= limit {
		return dst, false
	}
	src = src[1+n:]
	dst = grow(dst, count)
	switch mode {
	case sharedPacked:
		if len(src) == 0 || src[0] == 0 || src[0] > 32 || len(src)-1 != packedSize(count, uint(src[0])) {
			return dst, false
		}

		if uint64(unpack32(dst, src[1:], uint(src[0])))+base >= limit {
			return dst, false
		}
		if base != 0 {
			for i := range dst {
				dst[i] += uint32(base)
			}
		}

		return dst, true
	case sharedRuns:
		at := 0
		for len(src) > 0 {
			code, a := binary.Uvarint(src)
			if a <= 0 {
				return dst, false
			}
			run, c := binary.Uvarint(src[a:])
			if c <= 0 || code >= limit-base || run == 0 || run > uint64(count-at) {
				return dst, false
			}
			src = src[a+c:]
			fill := dst[at : at+int(run)]
			for i := range fill {
				fill[i] = uint32(base + code)
			}
			at += int(run)
		}

		return dst, at == count
	}

	return dst, false
}

// unpack32 reads len(dst) values of width bits, 1 to 32, from src, which
// holds exactly packedSize bytes, and returns the highest of them.
func unpack32(dst []uint32, src []byte, width uint) uint32 {
	mask := uint64(1)<<width - 1
	highest := uint32(0)
	i := 0
	for ; i < len(dst); i++ {
		bit := uint(i) * width
		pos := int(bit >> 3)
		if pos+8 > len(src) {
			break
		}
		v := uint32(binary.LittleEndian.Uint64(src[pos:]) >> (bit & 7) & mask)
		dst[i] = v
		highest = max(highest, v)
	}
	if i == len(dst) {
		return highest
	}
	var tail [16]byte
	origin := (uint(i) * width) >> 3
	copy(tail[:], src[origin:])
	for ; i < len(dst); i++ {
		bit := uint(i)*width - origin*8
		v := uint32(binary.LittleEndian.Uint64(tail[bit>>3:]) >> (bit & 7) & mask)
		dst[i] = v
		highest = max(highest, v)
	}

	return highest
}

// writeDictionaries writes the dictionary of every shared column and
// returns what the footer says of them.
func (w *Writer) writeDictionaries() []byte {
	footer := w.dictFooter[:0]
	// The dictionaries lie in the order of their paths in the part; a path
	// that blocks hold as codes has a number.
	var plans []*sharedPlan
	for _, sp := range w.plans {
		if !sp.tentative && sp.values > 0 {
			plans = append(plans, sp)
		}
	}
	sort.Slice(plans, func(a, b int) bool { return w.ids[plans[a].path] < w.ids[plans[b].path] })
	footer = binary.AppendUvarint(footer, uint64(len(plans)))
	for _, sp := range plans {
		if w.err != nil {
			break
		}
		off := w.off
		dw := &w.dictw
		dw.begin(w, &sp.layout)
		if err := sp.eachUsed(dw.add); err != nil {
			w.err = err

			break
		}
		slots, others := dw.finish(sp.codes, sp.used)
		flat := sp.flat
		if sp.values < sp.planned {
			hi, lo := bits.Mul64(sp.flat, sp.values)
			flat, _ = bits.Div64(hi, lo, sp.planned)
		}
		footer = binary.AppendUvarint(footer, uint64(w.ids[sp.path]))
		footer = binary.AppendUvarint(footer, uint64(sp.codes))
		footer = binary.AppendUvarint(footer, dw.stored)
		footer = binary.AppendUvarint(footer, sp.values)
		footer = binary.AppendUvarint(footer, flat)
		footer = binary.AppendUvarint(footer, off)
		footer = binary.AppendUvarint(footer, slots)
		footer = binary.AppendUvarint(footer, uint64(len(dw.others)))
		footer = binary.AppendUvarint(footer, others)
		footer = append(footer, byte(sp.layout.lead))
		footer = binary.AppendUvarint(footer, uint64(len(sp.layout.head)))
		footer = append(footer, sp.layout.head...)
	}
	// The columns that merges are to leave alone, in the order of their
	// paths; one that no block of the part holds has no number.
	var tried []string
	for path := range w.tried {
		if _, ok := w.ids[path]; ok && w.plans[path] == nil {
			tried = append(tried, path)
		}
	}
	sort.Slice(tried, func(a, b int) bool { return w.ids[tried[a]] < w.ids[tried[b]] })
	footer = binary.AppendUvarint(footer, uint64(len(tried)))
	for _, path := range tried {
		footer = binary.AppendUvarint(footer, uint64(w.ids[path]))
		footer = binary.AppendUvarint(footer, w.tried[path])
	}
	w.dictFooter = footer

	return footer
}

// noteTried records that a merge found the column at path, of values values
// in the parts it read, not worth sharing.
func (w *Writer) noteTried(path string, values uint64) {
	if w.tried == nil {
		w.tried = map[string]uint64{}
	}
	w.tried[path] = max(w.tried[path], values)
}

// ownColumns returns the columns of b whose values the block holds itself:
// all but the shared ones.
func (w *Writer) ownColumns(b *shred.Batch) []*shred.Column {
	cols := b.Columns()
	if w.planned == 0 {
		return cols
	}
	w.own = w.own[:0]
	for _, c := range cols {
		if c.Kind == vec.KindString && w.plans[c.Path] != nil {
			continue
		}
		w.own = append(w.own, c)
	}

	return w.own
}

// sharesAlike reports whether block i of src can be copied into a part that
// shares columns: when w knows the dictionaries of src that the codes of the
// block are of, and has read and numbered the strings that the block stores
// itself of every column that w shares, so that the copy takes the codes of
// w's dictionaries for both.
func (w *Writer) sharesAlike(src *Part, i int) bool {
	e := src.index[i*indexEntrySize:]
	dir, err := src.directory(i, e)
	if err != nil {
		return false
	}
	n, size := binary.Uvarint(dir)
	if size <= 0 || n > uint64(len(dir)-size)/dirEntrySize {
		return false
	}
	for ent := dir[size : size+int(n)*dirEntrySize]; len(ent) > 0; ent = ent[dirEntrySize:] {
		id := binary.LittleEndian.Uint32(ent)
		if vec.Kind(ent[4]) != vec.KindString || uint64(id) >= uint64(len(src.paths)) {
			continue
		}
		sp := w.sharedPlan(src.paths[id])
		if ent[5] == encShared {
			if sp == nil || sp.source(src) < 0 {
				return false
			}
		} else if sp != nil {
			if _, numbered := sp.numbersAt[blockKey{src, i}]; !numbered {
				return false
			}
		}
	}

	return true
}

// recodedChunk is where the chunk of a directory entry of a copied block
// lies in the copy; shared is set when the copy stores codes where the block
// stored the strings.
type recodedChunk struct {
	at               int
	off, length, crc uint32
	shared           bool
}

// copyChunks makes w.block the chunks of block i of src, which data holds
// and ents describes: as they are when w shares no column of the block, and
// otherwise one by one in their order, those of the columns that w shares
// replaced by the codes that the plans give their strings: the codes of the
// dictionary of src turned into those of w's, and the strings that the block
// stores itself into the codes of the numbers that the plan gave them when
// it read them. It notes in w.recoded where the chunks then lie.
func (w *Writer) copyChunks(src *Part, i int, data, ents []byte) error {
	w.recoded = w.recoded[:0]
	recode := false
	for at := 0; at < len(ents) && !recode; at += dirEntrySize {
		id := binary.LittleEndian.Uint32(ents[at:])
		recode = ents[at+5] == encShared || (w.planned > 0 && vec.Kind(ents[at+4]) == vec.KindString &&
			uint64(id) < uint64(len(src.paths)) && w.plans[src.paths[id]] != nil)
	}
	if !recode {
		w.block = append(w.block[:0], data...)

		return nil
	}
	b := &w.copiedBlock
	if err := src.OpenBlock(i, b); err != nil {
		return err
	}
	w.block = w.block[:0]
	order := w.order[:0]
	for k := 0; k < len(ents)/dirEntrySize; k++ {
		order = append(order, int32(k))
	}
	sort.SliceStable(order, func(a, b int) bool {
		return binary.LittleEndian.Uint32(ents[int(order[a])*dirEntrySize+12:]) < binary.LittleEndian.Uint32(ents[int(order[b])*dirEntrySize+12:])
	})
	w.order = order
	for _, k := range order {
		e := ents[int(k)*dirEntrySize:]
		id, count := binary.LittleEndian.Uint32(e), int(binary.LittleEndian.Uint32(e[8:]))
		off, length, crc := binary.LittleEndian.Uint32(e[12:]), binary.LittleEndian.Uint32(e[16:]), binary.LittleEndian.Uint32(e[20:])
		if uint64(off)+uint64(length) > uint64(len(data)) || uint64(id) >= uint64(len(src.paths)) {
			return ErrCorrupt
		}
		chunk := data[off : off+length]
		start := len(w.block)
		var sp *sharedPlan
		if vec.Kind(e[4]) == vec.KindString {
			sp = w.sharedPlan(src.paths[id])
		}
		if sp == nil {
			if e[5] == encShared {
				return ErrCorrupt
			}
			w.block = append(w.block, chunk...)
			w.recoded = append(w.recoded, recodedChunk{at: int(k), off: uint32(start), length: length, crc: crc})

			continue
		}
		// The bytes taken over get a checksum of the copy's own, so they
		// are checked against the block's here.
		ref, ok := b.Column(id, vec.KindString)
		if !ok || ref.bad || count == 0 || count != ref.Count || !b.checked(ref, chunk) {
			return ErrCorrupt
		}
		rest := chunk
		if e[6]&flagSparse != 0 {
			if len(chunk) == 0 {
				return ErrCorrupt
			}
			if rest, _, ok = skipBlob(chunk[1:]); !ok {
				return ErrCorrupt
			}
			w.block = append(w.block, chunk[:len(chunk)-len(rest)]...)
		}
		w.codes32, w.locals32 = grow(w.codes32, count), grow(w.locals32, count)
		if e[5] == encShared {
			d, ok := src.Dictionary(id)
			if !ok {
				return ErrCorrupt
			}
			if w.codes32, ok = readSharedCodes(w.codes32, rest, count, uint64(d.codes)); !ok {
				return ErrCorrupt
			}
			if err := sp.fromDict(sp.source(src), w.codes32, w.locals32); err != nil {
				return err
			}
		} else {
			at, ok := sp.numbersAt[blockKey{src, i}]
			if !ok || at+count > len(sp.numbers) {
				return errShared
			}
			sp.fromNumbers(sp.numbers[at:at+count], w.codes32, w.locals32)
		}
		w.block = appendSharedCodes(w.block, w.codes32, &w.e.tmp)
		for _, code := range w.codes32 {
			sp.used.Set(int(code))
		}
		sp.values += uint64(count)
		w.recoded = append(w.recoded, recodedChunk{
			at: int(k), off: uint32(start), length: uint32(len(w.block) - start),
			crc: crc32.Checksum(w.block[start:], castagnoli), shared: e[5] != encShared,
		})
	}

	return nil
}

// patchCopied makes the directory entries ents of a copied block, whose
// statistics stats holds, describe the chunks as copyChunks laid them out,
// and returns the statistics of the copy: those of the block but for the
// entries whose strings became codes, which have none.
func (w *Writer) patchCopied(ents, stats []byte) []byte {
	converted := false
	for _, rc := range w.recoded {
		e := ents[rc.at*dirEntrySize:]
		binary.LittleEndian.PutUint32(e[12:], rc.off)
		binary.LittleEndian.PutUint32(e[16:], rc.length)
		binary.LittleEndian.PutUint32(e[20:], rc.crc)
		if rc.shared {
			// The zero byte of the entry marks it until its turn below.
			e[7], converted = 1, true
		}
	}
	if !converted {
		return stats
	}
	// What an entry holds among the statistics follows from its encoding,
	// so each is copied before the entry takes the other.
	w.stats = w.stats[:0]
	for at := 0; at < len(ents); at += dirEntrySize {
		e := ents[at:]
		old := binary.LittleEndian.Uint32(e[24:])
		binary.LittleEndian.PutUint32(e[24:], uint32(len(w.stats)))
		if e[7] != 0 {
			e[5], e[6], e[7] = encShared, e[6]&flagSparse, 0

			continue
		}
		if n := statSize(stats, old, vec.Kind(e[4]), e[5]); n > 0 {
			w.stats = append(w.stats, stats[old:int(old)+n]...)
		}
	}

	return w.stats
}
