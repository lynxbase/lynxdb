package exec

import (
	"encoding/binary"
	"math"

	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

const (
	// partitionBits splits spilled groups and distinct values into
	// partitions that merge independently with a cache-sized table each.
	partitionBits = 8
	// maxCodes bounds the combinations of key codes a block may resolve
	// through a table indexed by code.
	maxCodes = 1 << 16
)

// maxTableGroups is the number of groups a worker's table holds before it is
// spilled to the worker's log.
var maxTableGroups = 1 << 16

type partMode uint8

const (
	// partNull: no row has the key.
	partNull partMode = iota
	// partConst: the rows that have the key share one string.
	partConst
	// partDict: a dictionary code per row.
	partDict
	// partInts: integers within a narrow range.
	partInts
	// partBytes: a string per row.
	partBytes
	// partRows: anything else, read row by row.
	partRows
)

// keyPart is how one key expression presents itself in a block.
type keyPart struct {
	mode partMode
	kind vec.Kind
	col  *vec.Column
	val  []byte
	// card is the number of codes the part takes; a nullable part uses the
	// last one for rows without the key, which absent tags as null or as
	// missing.
	card     int
	nullable bool
	absent   byte
	// ints holds the values of a partInts; one is set when they all equal
	// lo.
	ints []int64
	one  bool
	lo   int64
	// step is what the values of a partInts are multiples of, counted
	// from lo.
	step int64
	// dense is set when the part's columns and ints hold one value per
	// selected row instead of one per block row.
	dense bool
	// computed is set for integers that evalInt gave, one per selected row;
	// valid then marks the rows that have a value, nil when all do.
	computed bool
	valid    []uint8
	// cols are the columns of a partRows by kind.
	cols [kindSlots + 1]*vec.Column

	buf  []int64
	vbuf []uint8
}

// aggScratch is the aggregation memory a worker keeps between queries.
type aggScratch struct {
	parts []keyPart
	gid   []uint32
	gid2  []uint32
	idx2  []uint32
	codes []uint16
	tmp   []uint16
	hist  []uint32
	cache []int32
	key   []byte
	rec   []byte
	dense []int64
	fl    []float64

	// The groups of a block that goes to the log without the table: the
	// encoded keys or the rows they start at, their hashes, states and
	// shared ids.
	lkeys   []byte
	lends   []uint32
	lrows   []uint32
	lhash   []uint64
	lwords  []int64
	lglobal []uint32
}

// empty truncates every buffer, so that held counts the ones a query fills.
func (s *aggScratch) empty() {
	s.gid, s.gid2, s.idx2, s.hist, s.cache = s.gid[:0], s.gid2[:0], s.idx2[:0], s.hist[:0], s.cache[:0]
	s.codes, s.tmp, s.key, s.rec, s.dense, s.fl = s.codes[:0], s.tmp[:0], s.key[:0], s.rec[:0], s.dense[:0], s.fl[:0]
	s.lkeys, s.lends, s.lrows, s.lhash, s.lwords, s.lglobal = s.lkeys[:0], s.lends[:0], s.lrows[:0], s.lhash[:0], s.lwords[:0], s.lglobal[:0]
	for i := range s.parts {
		s.parts[i].buf, s.parts[i].vbuf = s.parts[i].buf[:0], s.parts[i].vbuf[:0]
	}
}

func (s *aggScratch) held() int {
	n := 4*(used(s.gid)+used(s.gid2)+used(s.idx2)+used(s.hist)+used(s.cache)+used(s.lends)+used(s.lrows)+used(s.lglobal)) +
		2*(used(s.codes)+used(s.tmp)) + used(s.key) + used(s.rec) + used(s.lkeys) +
		8*(used(s.dense)+used(s.fl)+used(s.lhash)+used(s.lwords))
	for i := range s.parts {
		n += 8*used(s.parts[i].buf) + used(s.parts[i].vbuf)
	}

	return n
}

// target is where a block's rows are aggregated: the states in words and
// the group of each selected row.
type target struct {
	words []int64
	// gid is the group of each selected row, or nil when every row is in
	// group one.
	gid []uint32
	one int
	// global maps a group to the id every worker shares for it.
	global []uint32
}

// aggWorker is the aggregation state of one worker for one query.
type aggWorker struct {
	w      *worker
	s      *aggScratch
	plan   *aggPlan
	stride int
	// table numbers the groups whose states are in words.
	table  *vec.ByteTable
	words  []int64
	global []uint32
	groups int
	// log holds the states of spilled groups and of blocks that bypassed
	// the table; dlogs hold the values of each distinct count.
	log   *vec.KeyPartitions
	dlogs []*vec.KeyPartitions
	// floats records which arguments read a float.
	floats []bool
	one    [1]uint32
	// streaming is set when groups arrive in key order, which counts
	// distinct values without logging them.
	streaming bool
	// times is the time column of the open block for picks by time.
	times *vec.Column
}

func (a *aggWorker) init(w *worker) {
	a.w, a.s, a.plan = w, &w.scratch.agg, w.q.agg
	a.stride = len(a.plan.words)
	a.floats = make([]bool, len(a.plan.args))
	a.s.parts = fit(a.s.parts, len(a.plan.keys))
	a.streaming = w.x.streaming
	for i := 0; i < a.plan.distinct && !a.streaming; i++ {
		a.dlogs = append(a.dlogs, vec.NewKeyPartitions(w.arena, partitionBits))
	}
	if len(a.plan.keys) == 0 {
		a.words = append(mem.Grow(w.arena, a.words, a.stride), a.plan.init...)
		a.groups = 1
	}
}

// group returns the worker's number for the group with the encoded key.
func (a *aggWorker) group(key []byte, h uint64) int {
	if a.table == nil {
		a.table = vec.NewByteTable(a.w.arena)
	}
	id := a.table.IDHashed(key, h)
	if id != a.groups {
		return id
	}
	a.groups++
	a.words = append(mem.Grow(a.w.arena, a.words, a.stride), a.plan.init...)
	if a.plan.group >= 0 {
		shared := a.w.x.shared.id(key, h)
		a.global = append(mem.Grow(a.w.arena, a.global, 1), shared)
		a.words[id*a.stride+a.plan.group] = int64(shared)
	}

	return id
}

func (a *aggWorker) openLog() *vec.KeyPartitions {
	if a.log == nil {
		a.log = vec.NewKeyPartitions(a.w.arena, partitionBits)
	}

	return a.log
}

// spill moves the table's groups to the log.
func (a *aggWorker) spill() {
	if a.groups == 0 || a.table == nil {
		return
	}
	log := a.openLog()
	for g := 0; g < a.groups; g++ {
		rec := append(a.s.rec[:0], a.table.Key(g)...)
		rec = appendWords(rec, a.words[g*a.stride:(g+1)*a.stride])
		a.s.rec = rec
		log.Add(a.table.KeyHash(g), rec)
	}
	a.table.Clear()
	a.words, a.global, a.groups = a.words[:0], a.global[:0], 0
}

func appendWords(dst []byte, words []int64) []byte {
	for _, w := range words {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(w))
	}

	return dst
}

// block aggregates the n selected rows of the open block, the rows listed
// in idx or every row when idx is nil.
func (a *aggWorker) block(n int, idx []uint32) error {
	if len(a.plan.keys) == 0 {
		return a.feed(&target{words: a.words, global: a.one[:]}, n, idx)
	}
	codes, domain, err := a.classify(n, idx)
	if err != nil {
		return err
	}
	if !codes {
		return a.byRow(n, idx)
	}
	switch {
	case domain == 1:
		g := a.group(a.codeKey(0))
		err = a.feed(&target{words: a.words, one: g, global: a.global}, n, idx)
	case a.plan.rowsOnly:
		a.histogram(n, idx, domain)
	default:
		s := a.s
		lc := a.codes(n, idx)
		s.cache = sized(s.cache, domain)
		cache := s.cache
		for i := range cache {
			cache[i] = -1
		}
		s.gid = sized(s.gid, n)
		gid := s.gid
		for j, c := range lc {
			g := cache[c]
			if g < 0 {
				g = int32(a.group(a.codeKey(int(c))))
				cache[c] = g
			}
			gid[j] = uint32(g)
		}
		err = a.feed(&target{words: a.words, gid: gid, global: a.global}, n, idx)
	}
	if a.groups > maxTableGroups {
		a.spill()
	}

	return err
}

// classify works out how each key presents itself in the block. It reports
// whether every combination of key values has a small code, and how many
// such codes there are.
func (a *aggWorker) classify(n int, idx []uint32) (codes bool, domain int, err error) {
	codes, domain = true, 1
	for k, e := range a.plan.keys {
		p := &a.s.parts[k]
		if err := a.classifyKey(e, p, n, idx); err != nil {
			return false, 0, err
		}
		if p.mode == partBytes || p.mode == partRows {
			codes = false
		}
		if codes {
			if domain *= p.card; domain > maxCodes {
				codes = false
			}
		}
	}
	// Resolving a code costs a table lookup, so codes pay only while a block
	// has fewer of them than rows.
	if domain > 8*n+1024 {
		codes = false
	}

	return codes, domain, nil
}

func (a *aggWorker) classifyKey(e *sexpr, p *keyPart, n int, idx []uint32) error {
	w := a.w
	p.mode, p.col, p.val, p.card, p.nullable, p.one, p.valid, p.dense, p.computed = partRows, nil, nil, 1, false, false, nil, false, false
	p.step, p.absent = 1, tagMissing
	if e.op != OpColumn {
		return a.classifyComputed(e, p, n, idx)
	}
	kind, found := e.kind, 0
	if e.kind == KindAny {
		for k := vec.KindBool; k <= vec.KindVariant; k++ {
			if w.ref(e.path, k) != nil {
				kind = k
				found++
			}
		}
	} else if e.kind != vec.KindNull && w.ref(e.path, kind) != nil {
		found = 1
	}
	p.kind = kind
	// Rows without a value hold a null where the path has one, and are
	// missing otherwise; a block with both is read row by row.
	both := false
	if nulls := w.ref(e.path, vec.KindNull); nulls != nil && e.kind == KindAny {
		held := nulls.Count
		for k := vec.KindBool; k <= vec.KindVariant; k++ {
			if ref := w.ref(e.path, k); ref != nil {
				held += ref.Count
			}
		}
		if p.absent = tagNull; held < w.rows {
			p.absent, both = tagMissing, true
		}
	}
	switch {
	case found == 0 && !both:
		p.mode = partNull

		return nil
	case found > 1 || both:
		p.mode = partRows
		for k := vec.KindNull; k <= vec.KindVariant; k++ {
			p.cols[k] = nil
			if k == vec.KindNull && !both {
				continue
			}
			c, err := w.column(e.path, k)
			if err != nil {
				return err
			}
			p.cols[k] = c
		}

		return nil
	}
	ref := w.ref(e.path, kind)
	switch kind {
	case vec.KindString, vec.KindVariant:
		if ref.Const() && ref.Count == w.rows {
			p.mode, p.val = partConst, ref.ConstBytes()

			return nil
		}
		c, err := w.column(e.path, kind)
		if err != nil {
			return err
		}
		p.col, p.nullable = c, c.Valid != nil
		switch c.Layout {
		case vec.Const:
			p.mode, p.val, p.card = partConst, c.DictValue(0), 1
		case vec.Dict:
			p.mode, p.card = partDict, c.DictLen()
		default:
			p.mode = partBytes

			return nil
		}
	case vec.KindInt:
		if uint64(ref.MaxInt)-uint64(ref.MinInt) >= maxCodes {
			break
		}
		p.mode, p.lo, p.card = partInts, ref.MinInt, int(ref.MaxInt-ref.MinInt)+1
		if p.card == 1 && ref.Count == w.rows {
			p.one = true

			return nil
		}
		c, err := w.column(e.path, kind)
		if err != nil {
			return err
		}
		p.col, p.ints, p.nullable, p.one = c, c.Ints, c.Valid != nil, c.Layout == vec.Const
	}
	if p.mode == partRows {
		c, err := w.column(e.path, kind)
		if err != nil {
			return err
		}
		p.cols = [kindSlots + 1]*vec.Column{}
		p.cols[kind] = c

		return nil
	}
	if p.nullable {
		p.card++
	}

	return nil
}

// classifyOperand presents the value of the computed key e.
func (a *aggWorker) classifyOperand(e *sexpr, o *operand, p *keyPart) {
	p.dense, p.absent = o.dense, tagNull
	var c *vec.Column
	found := 0
	for kind := vec.KindBool; kind <= vec.KindVariant; kind++ {
		if p.cols[kind] = o.cols[kind]; o.cols[kind] != nil {
			c = o.cols[kind]
			found++
		}
	}
	p.cols[vec.KindNull] = nil
	switch {
	case found == 0:
		p.mode = partNull

		return
	case found > 1:
		return
	}
	p.kind, p.col, p.nullable = c.Kind, c, c.Valid != nil
	switch c.Kind {
	case vec.KindString, vec.KindVariant:
		switch c.Layout {
		case vec.Const:
			p.mode, p.val = partConst, c.DictValue(0)
		case vec.Dict:
			p.mode, p.card = partDict, c.DictLen()
		default:
			p.mode = partBytes

			return
		}
	case vec.KindInt:
		lo, hi := c.Ints[0], c.Ints[0]
		if p.one = c.Layout == vec.Const; !p.one {
			lo, hi = vec.MinMaxInt64(c.Ints[:c.Rows])
		}
		// Bucket starts are multiples of the bucket width.
		if e.op == OpBin {
			if w := e.args[1]; w.op == OpLiteral && w.lit.Kind == vec.KindInt && w.lit.Int > 1 && lo%w.lit.Int == 0 {
				p.step = w.lit.Int
			}
		}
		if (uint64(hi)-uint64(lo))/uint64(p.step) >= maxCodes {
			p.step, p.absent = 1, tagMissing

			return
		}
		p.mode, p.lo, p.card, p.ints = partInts, lo, int((hi-lo)/p.step)+1, c.Ints
	default:
		return
	}
	if p.nullable {
		p.card++
	}
}

func (a *aggWorker) classifyComputed(e *sexpr, p *keyPart, n int, idx []uint32) error {
	w := a.w
	if !e.ints {
		o, err := w.operand(e, n, idx)
		if err != nil {
			return err
		}
		a.classifyOperand(e, &o, p)

		return nil
	}
	vals, valid, err := w.evalInt(e, n, idx, 0)
	if err != nil {
		return err
	}
	// The values live in a buffer of the evaluator; the part takes it over.
	p.buf, w.ints[0] = vals, p.buf
	p.kind, p.ints, p.dense, p.computed, p.absent = vec.KindInt, vals, true, true, tagNull
	if valid != nil {
		p.vbuf = append(p.vbuf[:0], valid...)
		p.valid, p.nullable = p.vbuf, true
	}
	lo, hi := int64(0), int64(23)
	if e.op != OpHourOfDay && n > 0 {
		lo, hi = vec.MinMaxInt64(vals[:n])
	}
	if uint64(hi)-uint64(lo) >= maxCodes {
		p.mode = partRows

		return nil
	}
	p.mode, p.lo, p.card = partInts, lo, int(hi-lo)+1
	if p.nullable {
		p.card++
	}

	return nil
}

// codes returns the combined key code of each selected row.
func (a *aggWorker) codes(n int, idx []uint32) []uint16 {
	s := a.s
	s.codes = sized(s.codes, n)
	out := s.codes
	first := true
	for k := range s.parts {
		p := &s.parts[k]
		if p.card == 1 {
			continue
		}
		if first {
			a.partCodes(p, out, idx)
			first = false

			continue
		}
		s.tmp = sized(s.tmp, n)
		a.partCodes(p, s.tmp, idx)
		card := uint16(p.card)
		for j, c := range s.tmp {
			out[j] = out[j]*card + c
		}
	}

	return out
}

// partCodes writes the code of one key for each selected row.
func (a *aggWorker) partCodes(p *keyPart, dst []uint16, idx []uint32) {
	if p.dense {
		idx = nil
	}
	null := uint16(p.card - 1)
	switch p.mode {
	case partConst:
		clear(dst)
	case partDict:
		c := p.col
		switch {
		case len(c.Codes8) != 0 && idx == nil:
			for j, v := range c.Codes8[:len(dst)] {
				dst[j] = uint16(v)
			}
		case len(c.Codes8) != 0:
			for j, r := range idx {
				dst[j] = uint16(c.Codes8[r])
			}
		case idx == nil:
			copy(dst, c.Codes16)
		default:
			for j, r := range idx {
				dst[j] = c.Codes16[r]
			}
		}
	case partInts:
		switch {
		case p.one:
			clear(dst)
		case p.step != 1:
			for j, v := range p.ints[:len(dst)] {
				dst[j] = uint16((v - p.lo) / p.step)
			}
		case idx == nil:
			for j, v := range p.ints[:len(dst)] {
				dst[j] = uint16(v - p.lo)
			}
		default:
			for j, r := range idx {
				dst[j] = uint16(p.ints[r] - p.lo)
			}
		}
	}
	if !p.nullable {
		return
	}
	if p.valid != nil {
		for j, ok := range p.valid[:len(dst)] {
			if ok == 0 {
				dst[j] = null
			}
		}

		return
	}
	valid := p.col.Valid
	if idx == nil {
		for j := range dst {
			if !valid.Get(j) {
				dst[j] = null
			}
		}

		return
	}
	for j, r := range idx {
		if !valid.Get(int(r)) {
			dst[j] = null
		}
	}
}

// codeKey encodes the key of a combined code and returns it with its hash.
func (a *aggWorker) codeKey(code int) ([]byte, uint64) {
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
	key := s.key[:0]
	for k := range parts {
		p := &parts[k]
		last := k == len(parts)-1
		switch {
		case p.mode == partNull, p.nullable && pc[k] == p.card-1:
			key = append(key, p.absent)
		case p.mode == partConst:
			key = appendKeyBytes(key, bytesTag(p.kind), p.val, last)
		case p.mode == partDict:
			key = appendKeyBytes(key, bytesTag(p.kind), p.col.DictValue(pc[k]), last)
		default:
			key = appendKeyInt(key, p.lo+int64(pc[k])*p.step)
		}
	}
	s.key = key

	return key, keyHash(key)
}

// histogram counts the selected rows per combined code and adds the counts
// to their groups, touching the table once per code that occurs.
func (a *aggWorker) histogram(n int, idx []uint32, domain int) {
	s := a.s
	s.hist = sized(s.hist, max(domain, 256))
	hist := s.hist
	clear(hist)
	var single *keyPart
	for k := range s.parts {
		if p := &s.parts[k]; p.card > 1 {
			if single != nil {
				single = nil

				break
			}
			single = p
		}
	}
	if single != nil && single.mode == partDict && len(single.col.Codes8) != 0 && (idx == nil || single.dense) {
		c := single.col
		vec.Histogram8((*[256]uint32)(hist), c.Codes8)
		if c.Valid != nil {
			// Rows without a value are decoded as code 0.
			missing := uint32(c.Rows - c.Valid.Count())
			hist[0] -= missing
			hist[single.card-1] += missing
		}
	} else {
		vec.Histogram16(hist, a.codes(n, idx))
	}
	for code, count := range hist[:domain] {
		if count != 0 {
			g := a.group(a.codeKey(code))
			a.words[g*a.stride+a.plan.rows] += int64(count)
		}
	}
}

// byRow aggregates a block whose keys rarely repeat: every run of rows with
// one key becomes a group of the block, and the groups go to the log, which
// leaves finding equal keys to the merge.
func (a *aggWorker) byRow(n int, idx []uint32) error {
	s := a.s
	s.lkeys, s.lends, s.lrows, s.lhash = s.lkeys[:0], s.lends[:0], s.lrows[:0], s.lhash[:0]
	s.gid = sized(s.gid, n)
	gid := s.gid
	p := &s.parts[0]
	inPlace := len(s.parts) == 1 && p.mode == partBytes
	if inPlace {
		a.runsOfBytes(p, gid, idx)
	} else {
		a.runsOfKeys(gid, idx)
	}
	groups := len(s.lhash)
	s.lwords = sized(s.lwords, groups*a.stride)
	for g := 0; g < groups; g++ {
		copy(s.lwords[g*a.stride:], a.plan.init)
	}
	t := target{words: s.lwords, gid: gid}
	if a.plan.distinct > 0 {
		s.lglobal = sized(s.lglobal, groups)
		for g := 0; g < groups; g++ {
			shared := a.w.x.shared.id(a.localKey(p, inPlace, g), s.lhash[g])
			s.lglobal[g] = shared
			s.lwords[g*a.stride+a.plan.group] = int64(shared)
		}
		t.global = s.lglobal
	}
	if err := a.feed(&t, n, idx); err != nil {
		return err
	}
	log := a.openLog()
	for g := 0; g < groups; g++ {
		rec := append(s.rec[:0], a.localKey(p, inPlace, g)...)
		rec = appendWords(rec, s.lwords[g*a.stride:(g+1)*a.stride])
		s.rec = rec
		log.Add(s.lhash[g], rec)
	}

	return nil
}

// localKey returns the encoded key of a group of the block.
func (a *aggWorker) localKey(p *keyPart, inPlace bool, g int) []byte {
	s := a.s
	if !inPlace {
		start := uint32(0)
		if g > 0 {
			start = s.lends[g-1]
		}

		return s.lkeys[start:s.lends[g]]
	}
	r := s.lrows[g]
	if p.col.Valid != nil && !p.col.Valid.Get(int(r)) {
		s.key = append(s.key[:0], p.absent)
	} else {
		s.key = append(append(s.key[:0], bytesTag(p.kind)), p.col.Data[p.col.Offsets[r]:p.col.Offsets[r+1]]...)
	}

	return s.key
}

// runsOfBytes numbers the runs of equal strings of a flat column, hashing
// each run's string in place.
func (a *aggWorker) runsOfBytes(p *keyPart, gid, idx []uint32) {
	if p.dense {
		idx = nil
	}
	s := a.s
	c := p.col
	offs, data, valid := c.Offsets, c.Data, c.Valid
	seed := tagSeeds[bytesTag(p.kind)]
	var prev []byte
	prevNull, cur := false, -1
	for j := range gid {
		r := uint32(j)
		if idx != nil {
			r = idx[j]
		}
		if valid != nil && !valid.Get(int(r)) {
			if cur < 0 || !prevNull {
				cur = len(s.lhash)
				s.lrows = append(s.lrows, r)
				s.lhash = append(s.lhash, keyHash([]byte{p.absent}))
				prevNull = true
			}
			gid[j] = uint32(cur)

			continue
		}
		v := data[offs[r]:offs[r+1]]
		if cur < 0 || prevNull || string(v) != string(prev) {
			cur = len(s.lhash)
			s.lrows = append(s.lrows, r)
			s.lhash = append(s.lhash, vec.Hash(v)^seed)
			prev, prevNull = v, false
		}
		gid[j] = uint32(cur)
	}
}

// runsOfKeys encodes the key of every selected row and numbers the runs of
// equal keys.
func (a *aggWorker) runsOfKeys(gid, idx []uint32) {
	s := a.s
	cur := -1
	var prev []byte
	for j := range gid {
		r := j
		if idx != nil {
			r = int(idx[j])
		}
		key := s.key[:0]
		for k := range s.parts {
			key = appendRowKey(key, &s.parts[k], j, r, k == len(s.parts)-1)
		}
		s.key = key
		if cur < 0 || string(key) != string(prev) {
			cur = len(s.lhash)
			start := len(s.lkeys)
			s.lkeys = append(s.lkeys, key...)
			s.lends = append(s.lends, uint32(len(s.lkeys)))
			s.lhash = append(s.lhash, keyHash(key))
			prev = s.lkeys[start:]
		}
		gid[j] = uint32(cur)
	}
}

// appendRowKey appends the encoded value of one key for selected row j,
// which is row r of the block.
func appendRowKey(dst []byte, p *keyPart, j, r int, last bool) []byte {
	if p.dense {
		r = j
	}
	switch p.mode {
	case partNull:
		return append(dst, p.absent)
	case partConst:
		if p.nullable && !p.col.Valid.Get(r) {
			return append(dst, p.absent)
		}

		return appendKeyBytes(dst, bytesTag(p.kind), p.val, last)
	case partDict, partBytes:
		if p.col.Valid != nil && !p.col.Valid.Get(r) {
			return append(dst, p.absent)
		}

		return appendKeyBytes(dst, bytesTag(p.kind), p.col.Bytes(r), last)
	case partInts:
		switch {
		case p.computed:
			if p.valid != nil && p.valid[j] == 0 {
				return append(dst, p.absent)
			}

			return appendKeyInt(dst, p.ints[j])
		case p.nullable && !p.col.Valid.Get(r):
			return append(dst, p.absent)
		case p.one:
			return appendKeyInt(dst, p.lo)
		}

		return appendKeyInt(dst, p.col.Int(r))
	}
	if p.computed {
		if p.valid != nil && p.valid[j] == 0 {
			return append(dst, p.absent)
		}

		return appendKeyInt(dst, p.ints[j])
	}
	for kind := vec.KindBool; kind <= vec.KindVariant; kind++ {
		c := p.cols[kind]
		if c == nil || !c.IsValid(r) {
			continue
		}
		switch kind {
		case vec.KindBool:
			return appendKeyBool(dst, c.Bool(r))
		case vec.KindInt:
			return appendKeyInt(dst, c.Int(r))
		case vec.KindFloat:
			return appendKeyFloat(dst, c.Float(r))
		}

		return appendKeyBytes(dst, bytesTag(kind), c.Bytes(r), last)
	}
	if c := p.cols[vec.KindNull]; c != nil && c.IsValid(r) {
		return append(dst, tagNull)
	}

	return append(dst, p.absent)
}

// feed adds the selected rows to the states of t.
func (a *aggWorker) feed(t *target, n int, idx []uint32) error {
	if n == 0 {
		return nil
	}
	if t.gid == nil {
		t.words[t.one*a.stride+a.plan.rows] += int64(n)
	} else {
		addRows(t.words, a.stride, a.plan.rows, t.gid)
	}
	for i := range a.plan.args {
		if a.streaming && len(a.plan.args[i].words) == 0 {
			continue
		}
		if err := a.feedArg(i, t, n, idx); err != nil {
			return err
		}
	}

	return nil
}

func (a *aggWorker) feedArg(i int, t *target, n int, idx []uint32) error {
	w := a.w
	arg := &a.plan.args[i]
	if arg.expr.op != OpColumn && !arg.expr.ints {
		o, err := w.operand(arg.expr, n, idx)
		if err != nil {
			return err
		}
		if o.dense {
			idx = nil
		}
		for kind := vec.KindBool; kind <= vec.KindVariant; kind++ {
			if c := o.cols[kind]; c != nil && arg.kinds&kindBit(kind) != 0 {
				a.feedValues(i, kind, c, t, n, idx)
			}
		}

		return nil
	}
	if arg.expr.op != OpColumn {
		vals, valid, err := w.evalInt(arg.expr, n, idx, 0)
		if err != nil {
			return err
		}
		gid := t.gid
		if valid != nil {
			vals, gid = a.compactDense(vals[:n], valid, t.gid)
		}
		a.feedInts(arg, t, gid, vals, nil, len(vals))

		return nil
	}
	for kind := vec.KindBool; kind <= vec.KindVariant; kind++ {
		if arg.kinds&kindBit(kind) == 0 || !w.has(arg.expr.path, kind) {
			continue
		}
		if err := a.feedColumn(i, kind, t, n, idx); err != nil {
			return err
		}
	}

	return nil
}

// feedColumn adds the values that one kind's column of an argument holds in
// the selected rows.
func (a *aggWorker) feedColumn(i int, kind vec.Kind, t *target, n int, idx []uint32) error {
	w := a.w
	arg := &a.plan.args[i]
	numeric := kind == vec.KindInt || kind == vec.KindFloat
	if w.shared == nil && len(arg.distinct) == 0 && !arg.picks && (!numeric || arg.counted) {
		// Only the number of values is asked for, which needs none of them.
		ref := w.ref(arg.expr.path, kind)
		switch {
		case ref.Count == w.rows:
			a.count(arg, t, t.gid, n)
		case t.gid == nil && idx == nil:
			a.count(arg, t, nil, ref.Count)
		default:
			valid, err := w.presence(arg.expr.path, kind)
			if err != nil {
				return err
			}
			a.countPresent(arg, t, valid, n, idx)
		}

		return nil
	}
	if arg.picks && a.plan.timePath >= 0 {
		var err error
		if a.times, err = w.column(a.plan.timePath, vec.KindInt); err != nil {
			return err
		}
	}
	if w.shared == nil && arg.placed {
		// The rows that hold a value are all a pick and a count need.
		gid, rows, count := t.gid, idx, n
		if w.ref(arg.expr.path, kind).Count != w.rows {
			valid, err := w.presence(arg.expr.path, kind)
			if err != nil {
				return err
			}
			gid, rows = a.compactValid(valid, t.gid, n, idx)
			count = len(rows)
		}
		a.count(arg, t, gid, count)
		a.pick(arg, t, gid, rows, count)

		return nil
	}
	c, err := w.column(arg.expr.path, kind)
	if err != nil {
		return err
	}
	a.feedValues(i, kind, c, t, n, idx)

	return nil
}

// pick offers the n rows that hold a value of an argument, row rows[j] or j
// of the block for group gid[j], to the picks of their groups.
func (a *aggWorker) pick(arg *aggArg, t *target, gid, rows []uint32, n int) {
	place, stride := a.w.place, a.stride
	for _, i := range arg.words {
		op := a.plan.words[i].op
		if op < wordFirst || op > wordLatest {
			continue
		}
		least := op == wordFirst || op == wordEarliest
		for j := 0; j < n; j++ {
			r, g := j, t.one
			if rows != nil {
				r = int(rows[j])
			}
			if t.gid != nil {
				g = int(gid[j])
			}
			loc := place | int64(r)
			key := loc
			if op >= wordEarliest {
				key = a.times.Int(r)
			}
			mergePick(t.words[g*stride+i:], key, loc, least)
		}
	}
}

// feedValues adds the values that column c of an argument holds in the
// selected rows.
func (a *aggWorker) feedValues(i int, kind vec.Kind, c *vec.Column, t *target, n int, idx []uint32) {
	arg := &a.plan.args[i]
	gid, rows, count := t.gid, idx, n
	if c.Valid != nil {
		gid, rows = a.compactValid(c.Valid, t.gid, n, idx)
		count = len(rows)
	}
	if arg.picks {
		a.pick(arg, t, gid, rows, count)
	}
	switch kind {
	case vec.KindInt:
		vals := c.Ints
		if c.Layout == vec.Const {
			a.s.dense = sized(a.s.dense, count)
			vals, rows = a.s.dense, nil
			for j := range vals {
				vals[j] = c.Ints[0]
			}
		}
		a.feedInts(arg, t, gid, vals, rows, count)
	case vec.KindFloat:
		a.floats[i] = a.floats[i] || count > 0
		vals := c.Floats
		if c.Layout == vec.Const {
			a.s.fl = sized(a.s.fl, count)
			vals, rows = a.s.fl, nil
			for j := range vals {
				vals[j] = c.Floats[0]
			}
		}
		a.feedFloats(arg, t, gid, vals, rows, count)
	default:
		a.count(arg, t, gid, count)
		for _, d := range arg.distinct {
			if !a.streaming {
				a.distinctBytes(d, t, c, kind, gid, rows, count)
			}
		}
	}
}

// compactValid returns the selected rows that valid marks and their groups.
func (a *aggWorker) compactValid(valid vec.Bitmap, gid []uint32, n int, idx []uint32) (groups, rows []uint32) {
	s := a.s
	s.idx2 = s.idx2[:0]
	if gid == nil {
		if idx == nil {
			s.idx2 = valid.AppendSet(s.idx2)
			// The bitmap's last word may mark positions past the rows.
			for len(s.idx2) > 0 && int(s.idx2[len(s.idx2)-1]) >= n {
				s.idx2 = s.idx2[:len(s.idx2)-1]
			}

			return nil, s.idx2
		}
		s.idx2 = vec.IndexesAndBitmap(append(s.idx2, idx...), valid)

		return nil, s.idx2
	}
	s.gid2 = s.gid2[:0]
	for j := 0; j < n; j++ {
		r := uint32(j)
		if idx != nil {
			r = idx[j]
		}
		if valid.Get(int(r)) {
			s.idx2 = append(s.idx2, r)
			s.gid2 = append(s.gid2, gid[j])
		}
	}

	return s.gid2, s.idx2
}

// compactDense returns the values that valid marks and their groups.
func (a *aggWorker) compactDense(vals []int64, valid []uint8, gid []uint32) ([]int64, []uint32) {
	s := a.s
	s.dense = s.dense[:0]
	s.gid2 = s.gid2[:0]
	for j, v := range vals {
		if valid[j] == 0 {
			continue
		}
		s.dense = append(s.dense, v)
		if gid != nil {
			s.gid2 = append(s.gid2, gid[j])
		}
	}
	if gid == nil {
		return s.dense, nil
	}

	return s.dense, s.gid2
}

// count adds n values to the argument's count words.
func (a *aggWorker) count(arg *aggArg, t *target, gid []uint32, n int) {
	for _, i := range arg.words {
		if a.plan.words[i].op != wordCount {
			continue
		}
		if t.gid == nil {
			t.words[t.one*a.stride+i] += int64(n)
		} else {
			addRows(t.words, a.stride, i, gid[:n])
		}
	}
}

// countPresent adds the selected rows that valid marks to the argument's
// count words.
func (a *aggWorker) countPresent(arg *aggArg, t *target, valid vec.Bitmap, n int, idx []uint32) {
	stride := a.stride
	for _, i := range arg.words {
		if a.plan.words[i].op != wordCount {
			continue
		}
		switch {
		case t.gid == nil && idx == nil:
			t.words[t.one*stride+i] += int64(valid.Rank(n))
		case t.gid == nil:
			var c uint64
			for _, r := range idx {
				c += valid[r>>6] >> (r & 63) & 1
			}
			t.words[t.one*stride+i] += int64(c)
		case idx == nil:
			for j, g := range t.gid[:n] {
				t.words[int(g)*stride+i] += int64(valid[j>>6] >> (uint(j) & 63) & 1)
			}
		default:
			for j, g := range t.gid[:n] {
				r := idx[j]
				t.words[int(g)*stride+i] += int64(valid[r>>6] >> (r & 63) & 1)
			}
		}
	}
}

// feedInts adds n integers to the argument's words: vals[rows[j]], or
// vals[j] when rows is nil, belongs to group gid[j].
func (a *aggWorker) feedInts(arg *aggArg, t *target, gid []uint32, vals []int64, rows []uint32, n int) {
	if n == 0 {
		return
	}
	stride := a.stride
	for _, i := range arg.words {
		op := a.plan.words[i].op
		if t.gid == nil {
			word := &t.words[t.one*stride+i]
			switch op {
			case wordCount, wordCountInt:
				*word += int64(n)
			case wordSumInt:
				*word += sumInts(vals, rows, n)
			case wordMinInt:
				lo, _ := minMaxInts(vals, rows, n)
				*word = min(*word, lo)
			case wordMaxInt:
				_, hi := minMaxInts(vals, rows, n)
				*word = max(*word, hi)
			case wordVarN:
				for j := 0; j < n; j++ {
					r := j
					if rows != nil {
						r = int(rows[j])
					}
					addSpread(t.words[t.one*stride+i:], float64(vals[r]))
				}
			}

			continue
		}
		switch op {
		case wordVarN:
			for j, g := range gid[:n] {
				r := j
				if rows != nil {
					r = int(rows[j])
				}
				addSpread(t.words[int(g)*stride+i:], float64(vals[r]))
			}
		case wordCount, wordCountInt:
			addRows(t.words, stride, i, gid[:n])
		case wordSumInt:
			addInts(t.words, stride, i, gid[:n], vals, rows)
		case wordMinInt:
			minInts(t.words, stride, i, gid[:n], vals, rows)
		case wordMaxInt:
			maxInts(t.words, stride, i, gid[:n], vals, rows)
		}
	}
	for _, d := range arg.distinct {
		if !a.streaming {
			a.distinctInts(d, t, gid, vals, rows, n)
		}
	}
}

func (a *aggWorker) feedFloats(arg *aggArg, t *target, gid []uint32, vals []float64, rows []uint32, n int) {
	stride := a.stride
	for _, i := range arg.words {
		op := a.plan.words[i].op
		for j := 0; j < n; j++ {
			g := t.one
			if t.gid != nil {
				g = int(gid[j])
			}
			v := vals[j]
			if rows != nil {
				v = vals[rows[j]]
			}
			word := &t.words[g*stride+i]
			switch op {
			case wordVarN:
				addSpread(t.words[g*stride+i:], v)
			case wordCount, wordCountFloat:
				*word++
			case wordSumFloat:
				*word = floatWord(wordFloat(*word) + v)
			case wordMinFloat:
				*word = floatWord(min(wordFloat(*word), v))
			case wordMaxFloat:
				*word = floatWord(max(wordFloat(*word), v))
			}
		}
	}
	for _, d := range arg.distinct {
		if a.streaming {
			break
		}
		log := a.dlogs[d]
		for j := 0; j < n; j++ {
			g := t.one
			if t.gid != nil {
				g = int(gid[j])
			}
			v := vals[j]
			if rows != nil {
				v = vals[rows[j]]
			}
			a.addDistinct(log, t.global[g], vec.Mix64(math.Float64bits(v)), vec.Mix64(math.Float64bits(v)^kindSeeds[vec.KindFloat]))
		}
	}
}

// kindSeeds keep equal bit patterns of different kinds apart in a distinct
// count; strings need none because they are hashed whole.
var kindSeeds = [kindSlots + 1]uint64{
	vec.KindBool:    0x9ae16a3b2f90404f,
	vec.KindInt:     0xc3a5c85c97cb3127,
	vec.KindFloat:   0xb492b66fbe98f273,
	vec.KindVariant: 0x9fb21c651e98df25,
}

// addDistinct logs one value of a group for a distinct count.
func (a *aggWorker) addDistinct(log *vec.KeyPartitions, group uint32, lo, hi uint64) {
	// Mixing the group into one word keeps equal values of different groups
	// apart without widening the record.
	lo ^= vec.Mix64(uint64(group) + 1)
	var rec [12]byte
	binary.LittleEndian.PutUint64(rec[:], hi)
	binary.LittleEndian.PutUint32(rec[8:], group)
	log.Add(lo, rec[:])
}

func (a *aggWorker) distinctInts(d int, t *target, gid []uint32, vals []int64, rows []uint32, n int) {
	log := a.dlogs[d]
	lastG, last := -1, int64(0)
	for j := 0; j < n; j++ {
		g := t.one
		if t.gid != nil {
			g = int(gid[j])
		}
		v := vals[j]
		if rows != nil {
			v = vals[rows[j]]
		}
		if g == lastG && v == last {
			continue
		}
		lastG, last = g, v
		a.addDistinct(log, t.global[g], vec.Mix64(uint64(v)), vec.Mix64(uint64(v)^kindSeeds[vec.KindInt]))
	}
}

// distinctBytes logs the strings, variants or booleans of c in the n rows
// listed in rows, or the first n rows when it is nil.
func (a *aggWorker) distinctBytes(d int, t *target, c *vec.Column, kind vec.Kind, gid, rows []uint32, n int) {
	log := a.dlogs[d]
	seed := kindSeeds[kind]
	if kind == vec.KindBool {
		for j := 0; j < n; j++ {
			g, r := t.one, j
			if t.gid != nil {
				g = int(gid[j])
			}
			if rows != nil {
				r = int(rows[j])
			}
			v := uint64(0)
			if c.Bool(r) {
				v = 1
			}
			a.addDistinct(log, t.global[g], vec.Mix64(v), vec.Mix64(v^seed))
		}

		return
	}
	lastG := -1
	var last []byte
	for j := 0; j < n; j++ {
		g, r := t.one, j
		if t.gid != nil {
			g = int(gid[j])
		}
		if rows != nil {
			r = int(rows[j])
		}
		var v []byte
		switch c.Layout {
		case vec.Flat:
			v = c.Data[c.Offsets[r]:c.Offsets[r+1]]
		case vec.Dict:
			v = c.DictValue(c.Code(r))
		default:
			v = c.DictValue(0)
		}
		// A run of rows with one group and value adds its value once.
		if g == lastG && string(v) == string(last) {
			continue
		}
		lastG, last = g, v
		lo, hi := vec.Hash128(v)
		a.addDistinct(log, t.global[g], lo, hi^seed)
	}
}

func addRows(words []int64, stride, off int, gid []uint32) {
	for _, g := range gid {
		words[int(g)*stride+off]++
	}
}

func addInts(words []int64, stride, off int, gid []uint32, vals []int64, rows []uint32) {
	if rows == nil {
		for j, g := range gid {
			words[int(g)*stride+off] += vals[j]
		}

		return
	}
	for j, g := range gid {
		words[int(g)*stride+off] += vals[rows[j]]
	}
}

func minInts(words []int64, stride, off int, gid []uint32, vals []int64, rows []uint32) {
	if rows == nil {
		for j, g := range gid {
			if w := &words[int(g)*stride+off]; vals[j] < *w {
				*w = vals[j]
			}
		}

		return
	}
	for j, g := range gid {
		if w, v := &words[int(g)*stride+off], vals[rows[j]]; v < *w {
			*w = v
		}
	}
}

func maxInts(words []int64, stride, off int, gid []uint32, vals []int64, rows []uint32) {
	if rows == nil {
		for j, g := range gid {
			if w := &words[int(g)*stride+off]; vals[j] > *w {
				*w = vals[j]
			}
		}

		return
	}
	for j, g := range gid {
		if w, v := &words[int(g)*stride+off], vals[rows[j]]; v > *w {
			*w = v
		}
	}
}

func sumInts(vals []int64, rows []uint32, n int) int64 {
	if rows == nil {
		return vec.SumInt64(vals[:n])
	}
	var s int64
	for _, r := range rows[:n] {
		s += vals[r]
	}

	return s
}

func minMaxInts(vals []int64, rows []uint32, n int) (lo, hi int64) {
	if rows == nil {
		return vec.MinMaxInt64(vals[:n])
	}
	lo, hi = math.MaxInt64, math.MinInt64
	for _, r := range rows[:n] {
		v := vals[r]
		lo, hi = min(lo, v), max(hi, v)
	}

	return lo, hi
}
