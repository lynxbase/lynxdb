package exec

import (
	"encoding/binary"
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// A block that stores a string column as codes of the dictionary of its
// part gives a small integer per row in place of a string: equal codes of
// one part are equal strings, and the codes of two parts have nothing in
// common. A group key or the argument of a distinct count that is such a
// column is aggregated by code while the scan is in the part, in what the
// workers of the query share of the part, and turned into strings once the
// scan has left the part: a group or a distinct value at a time, not a row.
const (
	// fewCodes is the size of a part dictionary up to which a block
	// resolves the codes of a key through a table, as it does the entries
	// of a dictionary of its own, and codeRepeats the rows that a block
	// must have for every code of a larger dictionary for the same.
	fewCodes    = 1 << 10
	codeRepeats = 4
	// rangeBits is the number of codes, as a power of two, whose groups
	// share a range: about what the cache of a core holds the states of.
	// A part has at most 1<<maxRangeBits ranges, and four to a worker where
	// it has the codes for them, so that workers rarely meet in one.
	rangeBits    = 13
	maxRangeBits = 10
	// denseFill is the part of the codes of a range that must have a group
	// for the range to hold a state for every code, found by the code, in
	// place of a table of the codes that have one.
	denseFill = 8
	// logShare is the part of a worker's share of the query's budget that
	// the groups it holds for the ranges of a part may take before it
	// merges them into the ranges.
	logShare = 4
	// maxOthers bounds the combinations of the other keys that a query
	// numbers for the groups of a key of codes, and otherRepeats is the
	// number of rows that a block must have for each of those it may hold.
	maxOthers    = 1 << 20
	otherRepeats = 16
	// maxBitGroups bounds the groups whose distinct values a part counts
	// by code, bitsFloor and bitsPerRow the bytes of the bits that tell
	// which codes a group has: those the first block of a part may take,
	// and those that every row brought to the part allows beyond.
	maxBitGroups = 1 << 12
	bitsFloor    = 64 << 10
	bitsPerRow   = 16
	// bitsChunk is the number of words of those bits that are turned into
	// strings at a time.
	bitsChunk = 1 << 12
)

// minRangeGroups is the least number of groups that a range holds before
// it hands them on.
var minRangeGroups = 1 << 12

var (
	codeSlots mem.Sized[uint32]
	codeKeys  mem.Sized[uint64]
	bitWords  mem.Sized[uint64]
)

// codedPart is what the workers of a query share of one part while they
// aggregate by the codes of its dictionaries. The last worker to leave the
// part takes what it holds and hands it on to be turned into strings.
type codedPart struct {
	p  *part.Part
	mu sync.Mutex
	// active counts the workers that aggregate by codes of the part; only
	// they hold groups and bits.
	active int
	groups *codedGroups
	// bits holds, per distinct count of the plan, the codes that its
	// groups have.
	bits []*codedBits
}

// codedGroups holds the groups of a part whose key is a code of its
// dictionary, in ranges of codes that workers merge into one at a time.
type codedGroups struct {
	dict *part.Dictionary
	plan *aggPlan
	// key is the key of the plan that the codes are of; last says that it
	// is the last one, and single that it is the only one.
	key          int
	last, single bool
	// shift turns a code into the number of its range and spread that
	// number into the partition of a worker's log; limit is the number of
	// groups that a range holds before they are handed on.
	shift, spread uint
	limit         int
	ranges        []codeRange
}

// codeTable holds the groups of a range. Until many of its codes have one,
// it numbers the groups: a group is the code of its key in the low half of
// a word and the id of what its other keys hold in the high one, a slot
// holds the number of a group plus one, and the words of the groups follow
// one another. A dense table has the words of every code of the range, in
// the order of the codes; a code has a group when it has rows.
type codeTable struct {
	dense bool
	slots []uint32
	keys  []uint64
	words []int64
	n     int
}

type codeRange struct {
	mu sync.Mutex
	codeTable
	// The padding keeps the ranges that two workers hold out of each
	// other's cache lines.
	_ [64]byte
}

// codedBits holds the codes that the groups of one distinct count have in a
// part: a bit per code for every group, by the id that all workers share for
// the group. Workers set the bits with atomic operations.
type codedBits struct {
	dict *part.Dictionary
	d    int
	mu   sync.Mutex
	maps [][]uint64
	// held is the number of words of the bits, and rows the rows that
	// workers brought to the part for the count.
	held int
	rows atomic.Int64
	// pending counts the chunks of the bits that wait to be turned into
	// strings.
	pending atomic.Int32
}

// handover is what a part held when the scan left it and what waits to be
// turned into strings: the groups of range r of its codes, or the words
// from from to to of the bits of a distinct count.
type handover struct {
	groups   *codedGroups
	table    codeTable
	r        int
	bits     *codedBits
	from, to int
	bytes    int64
}

// codedWorker is what one worker aggregates by the codes of the part it
// scans.
type codedWorker struct {
	a *aggWorker
	// keys is set when a key of the plan may be grouped by code, and args
	// marks the arguments whose distinct values may be counted by code.
	keys bool
	args []bool
	// part is what the worker shares of the part it aggregates by code,
	// nil while it does not; groups and bits are those of the part that
	// the worker has asked for.
	part   *codedPart
	groups *codedGroups
	bits   []*codedBits
	// maps holds, per distinct count, the bits of the groups that the
	// worker has asked for, by the shared id of the group.
	maps [][][]uint64
	// log holds the groups that wait to be merged into the ranges of the
	// part, by range: the code of each in the low half of what stands for
	// the hash of a record, then the id of what its other keys hold when
	// the plan has other keys, and the words of its state.
	log *vec.KeyPartitions
	// other is the key of what the other keys of a group hold, as the
	// query numbers it.
	other []byte
	// blocks counts the blocks that the worker aggregated by code, and
	// counted is set once the open one is among them.
	blocks  int64
	counted bool
}

func (c *codedWorker) init(a *aggWorker) {
	plan := a.plan
	c.a, c.args = a, make([]bool, len(plan.args))
	if a.streaming || a.w.x.least != nil {
		return
	}
	// A group whose distinct values are counted has an id that every
	// worker shares, which is looked up by the strings of its key.
	c.keys = len(plan.keys) > 0 && len(plan.states) == 0 && plan.group < 0
	for i := range plan.args {
		arg := &plan.args[i]
		c.args[i] = len(arg.distinct) > 0 && len(arg.states) == 0 && !arg.numbers && !arg.picks && arg.expr.op == OpColumn
	}
	c.bits, c.maps = make([]*codedBits, plan.distinct), make([][][]uint64, plan.distinct)
}

// open prepares the worker for a block: it leaves the part that the scan
// has left, and takes its share of what waits to be turned into strings.
func (c *codedWorker) open() error {
	c.counted = false
	if c.part != nil && c.part.p != c.a.w.p {
		c.leave()
	}
	if c.a.w.x.queued.Load() > 0 {
		return c.help()
	}

	return nil
}

// count notes that the open block was aggregated by code.
func (c *codedWorker) count() {
	if !c.counted {
		c.counted = true
		c.blocks++
	}
}

// enter makes the worker one of those that aggregate by codes of the open
// part and returns what they share of it.
func (c *codedWorker) enter() *codedPart {
	if c.part != nil {
		return c.part
	}
	x, p := c.a.w.x, c.a.w.p
	x.codedMu.Lock()
	cp := x.coded[p]
	if cp == nil {
		if x.coded == nil {
			x.coded = map[*part.Part]*codedPart{}
		}
		cp = &codedPart{p: p}
		x.coded[p] = cp
	}
	x.codedMu.Unlock()
	cp.mu.Lock()
	cp.active++
	cp.mu.Unlock()
	c.part = cp

	return cp
}

// leave merges what the worker holds for its part into what the workers
// share of it, and hands that on when the worker is the last in the part.
func (c *codedWorker) leave() {
	cp := c.part
	if cp == nil {
		return
	}
	c.drain()
	cp.mu.Lock()
	cp.active--
	var groups *codedGroups
	var marked []*codedBits
	if cp.active == 0 {
		groups, marked, cp.groups, cp.bits = cp.groups, cp.bits, nil, nil
	}
	cp.mu.Unlock()
	c.part, c.groups = nil, nil
	clear(c.bits)
	for d := range c.maps {
		clear(c.maps[d])
		c.maps[d] = c.maps[d][:0]
	}
	c.a.w.x.hand(groups, marked)
}

// groupsFor returns the groups of the open part by the codes of dictionary
// d, which key of the plan stores, or nil when the part groups by the codes
// of another key.
func (c *codedWorker) groupsFor(d *part.Dictionary, key int) *codedGroups {
	cp := c.enter()
	if c.groups == nil {
		a := c.a
		cp.mu.Lock()
		if cp.groups == nil {
			cp.groups = newCodedGroups(d, a.plan, key, len(a.w.x.workers))
		}
		c.groups = cp.groups
		cp.mu.Unlock()
		if c.log == nil {
			c.log = vec.NewKeyPartitions(a.w.arena, maxRangeBits)
		}
	}
	if c.groups.key != key {
		return nil
	}

	return c.groups
}

func newCodedGroups(d *part.Dictionary, plan *aggPlan, key, workers int) *codedGroups {
	// The codes and the one that stands for no value take width bits.
	width := bits.Len32(uint32(d.Len()))
	ranges := min(max(width-rangeBits, bits.Len(uint(4*workers-1))), maxRangeBits, width)
	cg := &codedGroups{
		dict: d, plan: plan, key: key, last: key == len(plan.keys)-1, single: len(plan.keys) == 1,
		shift: uint(width - ranges), spread: uint(maxRangeBits - ranges), ranges: make([]codeRange, 1<<ranges),
	}
	cg.limit = max(minRangeGroups, stageBudget/4/len(cg.ranges)/(16+8*len(plan.words)))

	return cg
}

// group returns the number of the group of key, and whether the table did
// not hold it: the words of a new group follow those of the others.
func (t *codeTable) group(key uint64) (int, bool) {
	if 2*t.n >= len(t.slots) {
		t.grow()
	}
	mask := uint64(len(t.slots) - 1)
	i := key * 0x9e3779b97f4a7c15 >> 32 & mask
	for {
		s := t.slots[i]
		if s == 0 {
			break
		}
		if t.keys[s-1] == key {
			return int(s - 1), false
		}
		i = (i + 1) & mask
	}
	t.slots[i] = uint32(t.n + 1)
	t.keys = append(codeKeys.Grow(t.keys, 1), key)
	t.n++

	return t.n - 1, true
}

func (t *codeTable) grow() {
	n := max(2*len(t.slots), 256)
	codeSlots.Put(t.slots)
	t.slots = codeSlots.Get(n)
	clear(t.slots)
	mask := uint64(n - 1)
	for g, key := range t.keys {
		i := key * 0x9e3779b97f4a7c15 >> 32 & mask
		for t.slots[i] != 0 {
			i = (i + 1) & mask
		}
		t.slots[i] = uint32(g + 1)
	}
}

func (t *codeTable) release() {
	codeSlots.Put(t.slots)
	codeKeys.Put(t.keys)
	wordPool.Put(t.words)
	*t = codeTable{}
}

func (t *codeTable) bytes() int64 { return int64(4*cap(t.slots) + 8*cap(t.keys) + 8*cap(t.words)) }

// merge folds the groups of a chunk of a worker's log into t, the table of
// range r.
func (cg *codedGroups) merge(t *codeTable, r int, chunk []byte) {
	plan := cg.plan
	stride := len(plan.words)
	base := uint32(r) << cg.shift
	for at := 0; at < len(chunk); {
		h, start, end := vec.RecordAt(chunk, at)
		at = end
		state := chunk[end-8*stride : end]
		if t.dense {
			// The words of a code without a group are those a group
			// starts from.
			g := int(uint32(h)-base) * stride
			if plan.rowsOnly {
				t.words[g] += int64(binary.LittleEndian.Uint64(state))
			} else {
				plan.mergeRecord(t.words[g:g+stride], state)
			}

			continue
		}
		key := h & (1<<32 - 1)
		if !cg.single {
			key |= uint64(binary.LittleEndian.Uint32(chunk[start:])) << 32
		}
		g, fresh := t.group(key)
		switch {
		case fresh:
			t.words = wordPool.Grow(t.words, stride)
			for i := 0; i < stride; i++ {
				t.words = append(t.words, int64(binary.LittleEndian.Uint64(state[8*i:])))
			}
		case plan.rowsOnly:
			t.words[g] += int64(binary.LittleEndian.Uint64(state))
		default:
			plan.mergeRecord(t.words[g*stride:(g+1)*stride], state)
		}
	}
	if cg.single && !t.dense && t.n*denseFill >= 1<<cg.shift {
		cg.fill(t, base)
	}
}

// fill makes t, the table of the range that starts at code base, hold the
// words of every code of the range.
func (cg *codedGroups) fill(t *codeTable, base uint32) {
	stride := len(cg.plan.words)
	words := wordPool.Get(stride << cg.shift)
	for i := 0; i < len(words); i += stride {
		copy(words[i:], cg.plan.init)
	}
	for g, key := range t.keys {
		copy(words[int(uint32(key)-base)*stride:], t.words[g*stride:(g+1)*stride])
	}
	t.release()
	t.dense, t.words = true, words
}

// drain merges the groups that the worker holds for the ranges of its part
// into the ranges, each while it holds the range, and hands the groups of a
// range on once they reach the limit.
func (c *codedWorker) drain() {
	cg, log := c.groups, c.log
	if cg == nil || log.Size() == 0 {
		return
	}
	a := c.a
	a.drain(log, func(p int) *sync.Mutex { return &cg.ranges[p>>cg.spread].mu }, func(p int) {
		r := p >> cg.spread
		cr := &cg.ranges[r]
		log.Drain(p, func(chunk []byte) { cg.merge(&cr.codeTable, r, chunk) })
		if !cr.dense && cr.n >= cg.limit {
			a.w.x.push(handover{groups: cg, table: cr.codeTable, r: r, bytes: cr.bytes()})
			cr.codeTable = codeTable{}
		}
	})
}

// codedBlock groups the n selected rows of the open block by the code that
// the key s.wide has in the dictionary of the part, which has too many for
// a table, and by the id that all workers have for what the other keys
// hold; domain is the number of combined codes of those. It reports false
// when the rows are to be grouped by the strings instead.
func (a *aggWorker) codedBlock(n int, idx []uint32, domain int) (bool, error) {
	s, c := a.s, &a.coded
	p := &s.parts[s.wide]
	// What the other keys hold is numbered for all workers, a lookup for
	// each combination that a block has: too many to pay when they change
	// from row to row.
	if len(s.parts) > 1 && (domain > n/otherRepeats+otherRepeats || a.w.x.others.full.Load()) {
		return false, nil
	}
	cg := c.groupsFor(p.dict, s.wide)
	if cg == nil {
		return false, nil
	}
	codes := p.codes
	if idx != nil {
		s.picked = sized(s.picked, n)
		for j, r := range idx {
			s.picked[j] = codes[r]
		}
		codes = s.picked
	}
	codes = codes[:n]
	s.gid = sized(s.gid, n)
	gid := s.gid
	// A run of rows with one key is a group of the block.
	keys, prev := s.ckeys[:0], ^uint64(0)
	if cg.single || domain == 1 {
		other := uint64(0)
		if !cg.single {
			other = uint64(c.otherID(0)) << 32
		}
		for j, code := range codes {
			key := other | uint64(code)
			if key != prev {
				keys, prev = append(keys, key), key
			}
			gid[j] = uint32(len(keys) - 1)
		}
	} else {
		combined := a.codes(n, idx)
		s.cache = sized(s.cache, domain)
		cache := s.cache
		for i := range cache {
			cache[i] = -1
		}
		for j, code := range codes {
			id := cache[combined[j]]
			if id < 0 {
				id = int32(c.otherID(int(combined[j])))
				cache[combined[j]] = id
			}
			key := uint64(id)<<32 | uint64(code)
			if key != prev {
				keys, prev = append(keys, key), key
			}
			gid[j] = uint32(len(keys) - 1)
		}
	}
	s.ckeys = keys
	if a.err != nil {
		err := a.err
		a.err = nil

		return true, err
	}
	stride := a.stride
	s.lwords = sized(s.lwords, len(keys)*stride)
	for g := range keys {
		copy(s.lwords[g*stride:], a.plan.init)
	}
	if err := a.feed(&target{words: s.lwords, gid: gid}, n, idx); err != nil {
		return true, err
	}
	c.count()
	var other [4]byte
	held := other[:0]
	if !cg.single {
		held = other[:]
	}
	for g, key := range keys {
		code := uint32(key)
		binary.LittleEndian.PutUint32(other[:], uint32(key>>32))
		c.log.AddKey(uint64(code>>cg.shift<<cg.spread)<<32|uint64(code), 0, false, held, s.lwords[g*stride:(g+1)*stride], nil)
	}
	if c.log.Size() > stageBudget/len(a.w.x.workers)/logShare {
		c.drain()
	}

	return true, nil
}

// otherID returns the id that all workers have for what the keys of the
// open block other than the one of codes hold at a combined code. The key
// that the query numbers is the length of the values before the key of
// codes, in four bytes, and the values as a group key encodes them.
func (c *codedWorker) otherID(code int) uint32 {
	a := c.a
	s := a.s
	parts := s.parts
	var own [16]int
	pc := own[:0]
	if len(parts) > len(own) {
		pc = make([]int, 0, len(parts))
	}
	pc = pc[:len(parts)]
	for k := len(parts) - 1; k >= 0; k-- {
		pc[k] = 0
		if card := parts[k].card; card > 1 {
			pc[k] = code % card
			code /= card
		}
	}
	key := append(s.key[:0], 0, 0, 0, 0)
	for k := range parts {
		if k == s.wide {
			binary.LittleEndian.PutUint32(key, uint32(len(key)-4))

			continue
		}
		key = a.appendCode(key, &parts[k], pc[k], k == len(parts)-1)
	}
	s.key = key
	o := a.w.x.others
	id := o.id(key, vec.Hash(key))
	if id >= maxOthers {
		o.full.Store(true)
	}

	return id
}

// codedValues counts the values that argument i, which the open block
// stores as codes of dictionary d of its part, holds in the selected rows,
// and marks their codes for the distinct counts of the argument. It reports
// false when the values are to be read as strings instead: the bits of the
// groups would take more than the rows brought to the part so far warrant.
func (a *aggWorker) codedValues(i int, ref *part.ColumnRef, d *part.Dictionary, t *target, n int, idx []uint32) (bool, error) {
	c := &a.coded
	arg := &a.plan.args[i]
	c.enter()
	for _, dc := range arg.distinct {
		c.bitsFor(dc, d).rows.Add(int64(n))
	}
	codes, valid, err := a.w.rowCodes(arg.expr.path, ref, d)
	if err != nil {
		return false, err
	}
	gid, rows, count := t.gid, idx, n
	if valid != nil {
		gid, rows = a.compactValid(valid, t.gid, n, idx)
		count = len(rows)
	}
	// A bit set for a block that is then read as strings changes nothing:
	// the strings set it again.
	for _, dc := range arg.distinct {
		if !c.mark(dc, t, codes, gid, rows, count) {
			return false, nil
		}
	}
	a.count(arg, t, gid, count)
	c.count()

	return true, nil
}

// bitsFor returns the bits of distinct count d in the worker's part, whose
// codes are those of dict.
func (c *codedWorker) bitsFor(d int, dict *part.Dictionary) *codedBits {
	if cb := c.bits[d]; cb != nil {
		return cb
	}
	cp := c.part
	cp.mu.Lock()
	if cp.bits == nil {
		cp.bits = make([]*codedBits, len(c.bits))
	}
	cb := cp.bits[d]
	if cb == nil {
		cb = &codedBits{dict: dict, d: d}
		cp.bits[d] = cb
	}
	cp.mu.Unlock()
	c.bits[d] = cb

	return cb
}

// bitmap returns the bits of the codes that a group has in distinct count
// d, or nil when the part does not keep them: for a group numbered beyond
// those it counts by code, or when the bits would take more than the rows
// brought to the part allow.
func (c *codedWorker) bitmap(d int, group uint32) []uint64 {
	held := c.maps[d]
	if int(group) < len(held) && held[group] != nil {
		return held[group]
	}
	if group >= maxBitGroups {
		return nil
	}
	for int(group) >= len(held) {
		held = append(held, nil)
	}
	c.maps[d] = held
	cb := c.bits[d]
	cb.mu.Lock()
	for int(group) >= len(cb.maps) {
		cb.maps = append(cb.maps, nil)
	}
	bm := cb.maps[group]
	if bm == nil {
		words := vec.BitmapWords(cb.dict.Len())
		if size := 8 * int64(cb.held+words); size <= bitsFloor+bitsPerRow*cb.rows.Load() && size <= int64(stageBudget/8) {
			bm = bitWords.Get(words)
			clear(bm)
			cb.maps[group], cb.held = bm, cb.held+words
		}
	}
	cb.mu.Unlock()
	held[group] = bm

	return bm
}

// mark sets, for distinct count d, the bit of the code of each of count
// values in the bits of its group: the value of row rows[j], or of row j
// when rows is nil, belongs to group gid[j]. It reports false when the part
// does not keep the bits of one of the groups.
func (c *codedWorker) mark(d int, t *target, codes, gid, rows []uint32, count int) bool {
	if count == 0 {
		return true
	}
	if t.gid == nil {
		bm := c.bitmap(d, t.global[t.one])
		if bm == nil {
			return false
		}
		if rows == nil {
			for _, code := range codes[:count] {
				setBit(bm, code)
			}

			return true
		}
		for _, r := range rows[:count] {
			setBit(bm, codes[r])
		}

		return true
	}
	last, bm := ^uint32(0), []uint64(nil)
	for j, g := range gid[:count] {
		if g != last {
			if bm = c.bitmap(d, t.global[g]); bm == nil {
				return false
			}
			last = g
		}
		r := j
		if rows != nil {
			r = int(rows[j])
		}
		setBit(bm, codes[r])
	}

	return true
}

// setBit sets the bit of code in bits that several workers set at once;
// most codes were seen before, and reading their bit leaves its cache line
// shared.
func setBit(bm []uint64, code uint32) {
	word, bit := &bm[code>>6], uint64(1)<<(code&63)
	if atomic.LoadUint64(word)&bit == 0 {
		atomic.OrUint64(word, bit)
	}
}

// hand queues what a part held when the last worker left it.
func (x *execution) hand(cg *codedGroups, marked []*codedBits) {
	if cg != nil {
		for r := range cg.ranges {
			t := &cg.ranges[r].codeTable
			if !t.dense && t.n == 0 {
				t.release()

				continue
			}
			x.push(handover{groups: cg, table: *t, r: r, bytes: t.bytes()})
			*t = codeTable{}
		}
	}
	for _, cb := range marked {
		if cb == nil {
			continue
		}
		words := vec.BitmapWords(cb.dict.Len())
		if cb.held == 0 || words == 0 {
			continue
		}
		chunks := (words + bitsChunk - 1) / bitsChunk
		cb.pending.Store(int32(chunks))
		for from := 0; from < words; from += bitsChunk {
			x.push(handover{bits: cb, from: from, to: min(from+bitsChunk, words), bytes: 8 * int64(cb.held/chunks)})
		}
	}
}

func (x *execution) push(u handover) {
	x.queueMu.Lock()
	x.queue = append(x.queue, u)
	x.queueMu.Unlock()
	x.waiting.Add(u.bytes)
	x.queued.Add(1)
}

func (x *execution) pop() (handover, bool) {
	x.queueMu.Lock()
	defer x.queueMu.Unlock()
	n := len(x.queue)
	if n == 0 {
		return handover{}, false
	}
	u := x.queue[n-1]
	x.queue[n-1] = handover{}
	x.queue = x.queue[:n-1]
	x.waiting.Add(-u.bytes)
	x.queued.Add(-1)

	return u, true
}

// help turns into strings one of the things that wait for it, and more of
// them while they take more than a quarter of the query's budget.
func (c *codedWorker) help() error {
	x := c.a.w.x
	for {
		u, ok := x.pop()
		if !ok {
			return nil
		}
		if err := c.resolve(&u); err != nil {
			return err
		}
		if x.waiting.Load() <= int64(stageBudget/4) {
			return nil
		}
	}
}

// resolve turns what u holds into strings and stages it for the merges of
// the query, as the worker's own.
func (c *codedWorker) resolve(u *handover) error {
	if u.groups != nil {
		err := c.resolveGroups(u.groups, &u.table, u.r)
		u.table.release()

		return err
	}
	cb := u.bits
	err := c.resolveBits(cb, u.from, u.to)
	if cb.pending.Add(-1) == 0 {
		cb.release()
	}

	return err
}

func (cb *codedBits) release() {
	for i, bm := range cb.maps {
		bitWords.Put(bm)
		cb.maps[i] = nil
	}
	cb.held = 0
}

// resolveGroups stages the groups of t, the table of range r, under the
// keys that their codes and ids stand for.
func (c *codedWorker) resolveGroups(cg *codedGroups, t *codeTable, r int) error {
	a := c.a
	stride := a.stride
	if t.dense {
		base, rows := uint32(r)<<cg.shift, a.plan.rows
		for i := 0; i < len(t.words); i += stride {
			if t.words[i+rows] == 0 {
				continue
			}
			if err := c.resolveGroup(cg, uint64(base)+uint64(i/stride), t.words[i:i+stride]); err != nil {
				return err
			}
		}

		return nil
	}
	for g, key := range t.keys {
		if err := c.resolveGroup(cg, key, t.words[g*stride:(g+1)*stride]); err != nil {
			return err
		}
	}

	return nil
}

// resolveGroup stages a group of a part under the key that it stands for:
// the string of its code, or none for the code that follows the last, among
// what the other keys hold under its id.
func (c *codedWorker) resolveGroup(cg *codedGroups, key uint64, state []int64) error {
	a := c.a
	s := a.s
	code := uint32(key)
	none := code == uint32(cg.dict.Len())
	if !none {
		var err error
		if s.value, err = cg.dict.Value(s.value[:0], code); err != nil {
			return err
		}
	}
	if cg.single {
		if none {
			a.stage(tagSeeds[tagNull]^vec.Hash(nil), tagNull, true, nil, state)
		} else {
			a.stage(vec.Hash(s.value)^tagSeeds[tagString], tagString, true, s.value, state)
		}

		return nil
	}
	// Groups that follow one another often share what their other keys
	// hold.
	if id := uint32(key >> 32); len(c.other) == 0 || id != binary.LittleEndian.Uint32(c.other) {
		c.other = binary.LittleEndian.AppendUint32(c.other[:0], id)
		c.other = a.w.x.others.key(c.other, id)
	}
	before := 8 + int(binary.LittleEndian.Uint32(c.other[4:]))
	k := append(s.key[:0], c.other[8:before]...)
	if none {
		k = append(k, tagNull)
	} else {
		k = appendKeyBytes(k, tagString, s.value, cg.last)
	}
	k = append(k, c.other[before:]...)
	s.key = k
	a.stage(keyHash(k), 0, false, k, state)

	return nil
}

// resolveBits logs, for the distinct count of cb, the strings of the codes
// from word from to word to of the bits of every group.
func (c *codedWorker) resolveBits(cb *codedBits, from, to int) error {
	a := c.a
	s := a.s
	log := a.dlogs[cb.d]
	seed := kindSeeds[vec.KindString]
	var lo, hi [64]uint64
	var err error
	for i := from; i < to; i++ {
		// A string is hashed once for all the groups that hold it.
		hashed := uint64(0)
		for group, bm := range cb.maps {
			if bm == nil {
				continue
			}
			for word := bm[i]; word != 0; word &= word - 1 {
				b := bits.TrailingZeros64(word)
				if hashed>>b&1 == 0 {
					if s.value, err = cb.dict.Value(s.value[:0], uint32(i<<6|b)); err != nil {
						return err
					}
					lo[b], hi[b] = vec.Hash128(s.value)
					hashed |= 1 << b
				}
				a.addDistinct(log, uint32(group), lo[b], hi[b]^seed)
			}
		}
		if i&63 == 63 {
			a.settle(cb.d)
		}
	}
	a.settle(cb.d)

	return nil
}

// finishCoded turns into strings what the parts still hold by code when the
// scan is over, on every worker's goroutine.
func (x *execution) finishCoded(ws []*worker) error {
	if x.coded == nil {
		return nil
	}
	if err := forEach(len(ws), len(ws), func(_, i int) { ws[i].agg.coded.leave() }); err != nil {
		return err
	}
	units := x.queue
	x.queue = nil
	x.queued.Store(0)
	x.waiting.Store(0)
	var failed atomic.Pointer[error]
	err := forEach(len(units), len(ws), func(slot, i int) {
		if err := ws[slot].agg.coded.resolve(&units[i]); err != nil {
			failed.CompareAndSwap(nil, &err)
		}
	})
	if e := failed.Load(); e != nil {
		return *e
	}

	return err
}

// releaseCoded returns the memory of what parts hold by code, which a query
// that failed left there.
func (x *execution) releaseCoded() {
	for _, cp := range x.coded {
		if cg := cp.groups; cg != nil {
			for r := range cg.ranges {
				cg.ranges[r].release()
			}
		}
		for _, cb := range cp.bits {
			if cb != nil {
				cb.release()
			}
		}
	}
	for i := range x.queue {
		if u := &x.queue[i]; u.groups != nil {
			u.table.release()
		} else if u.bits.pending.Add(-1) == 0 {
			u.bits.release()
		}
	}
	x.coded, x.queue = nil, nil
}
