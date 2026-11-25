package colstore

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// ScanOptions narrows a scan and sets its parallelism.
type ScanOptions struct {
	// Workers is the number of goroutines; zero means one per CPU.
	Workers int
	// MinTime and MaxTime bound row timestamps, inclusive; both zero means no
	// bound.
	MinTime int64
	MaxTime int64
	// Tokens holds token hashes that a block must all possibly contain.
	Tokens []uint64
	// ColumnTokens holds sets of token hashes, each of which a block must
	// possibly contain in the strings of one column.
	ColumnTokens []ColumnTokens
	// Key prunes blocks by sort key; the snapshot's own filter applies when
	// it is empty.
	Key KeyFilter
	// Skip, when set, is asked once per block before the scan starts and
	// leaves out the blocks it returns true for.
	Skip func(p *part.Part, block int) bool
	// Condition, when not empty, names what the caller requires of a row;
	// scans share a name only for one requirement, of which the tokens and
	// the times are a part. The scan leaves out the blocks that an earlier
	// one learned to hold no such row and remembers those its tokens rule
	// out; the caller adds the blocks in which it finds none to the Misses
	// that the part keeps under the name.
	Condition string
	// ByTime, when not zero, makes the scan take the blocks by the times of
	// their rows: by their latest row, latest first, when it is negative,
	// and by their earliest row, earliest first, when it is positive. A
	// block is then left out when its turn comes if Bound rules it out: its
	// latest row is before Bound in the first order, or its earliest row
	// after Bound in the second. The caller moves Bound while the scan runs,
	// as it learns which rows it will keep.
	ByTime int
	Bound  *atomic.Int64
	// Ramp makes the scan take one block first and twice as many each time,
	// for a caller that expects to end the scan after few blocks by an
	// error from its function.
	Ramp bool
	// Kept, when set, tells a scan that starts with few blocks how many
	// rows its caller has and how many it wants; the scan then takes as
	// many blocks next as the rows so far say are needed, and at least
	// twice as many as before. It is asked while no block is scanned.
	Kept func() (have, want int)
}

// rampFrom is the most blocks that a scan takes all at once where it could
// start with one: so few cost less to read than to wait for in turns.
const rampFrom = 4

// ColumnTokens holds token hashes that the strings of one column of a block
// must all possibly contain: the string column at Path, or any one column
// when Path is empty.
type ColumnTokens struct {
	Path   string
	Hashes []uint64
	// Value, when set, is the string that the column of every matching
	// row equals, which the counts in a part's footer decide alone.
	Value []byte
}

// unprobed stands for the path id of a token set that the blocks of a part
// are not probed for.
const unprobed = anyColumn - 1

// decideCounted settles the token sets of a part whose value the part
// counts at the path: the part holds no matching row when it lacks the
// value, and its filters are not probed for a value held by at least one
// row per block, which lies in most blocks. It reports whether the part may
// hold matching rows.
func decideCounted(ids []uint32, p *part.Part, sets []ColumnTokens) bool {
	for k := range sets {
		if sets[k].Value == nil || ids[k] == anyColumn {
			continue
		}
		counts, ok := p.Counts(ids[k])
		if !ok {
			continue
		}
		i := slices.IndexFunc(counts.Values, func(v []byte) bool { return bytes.Equal(v, sets[k].Value) })
		if i < 0 {
			return false
		}
		if counts.Rows[i] >= uint64(p.NumBlocks()) {
			ids[k] = unprobed
		}
	}

	return true
}

// anyColumn stands for the path id of a token set without a path.
const anyColumn = ^uint32(0)

// resolveTokens appends the path id that each set has in p and reports
// whether p holds every path, without which none of its blocks holds a set.
func resolveTokens(dst []uint32, p *part.Part, sets []ColumnTokens) ([]uint32, bool) {
	for i := range sets {
		id := anyColumn
		if sets[i].Path != "" {
			var ok bool
			if id, ok = p.PathID(sets[i].Path); !ok {
				return dst, false
			}
		}
		dst = append(dst, id)
	}

	return dst, true
}

// mayContainSet reports whether the strings of the column of block i of p
// with the path id, or of any one column, may contain every hash.
func mayContainSet(p *part.Part, i int, id uint32, hashes []uint64) (bool, error) {
	if id == anyColumn {
		return p.MayContainTokens(i, hashes)
	}

	return p.MayContainTokensIn(i, id, vec.KindString, hashes)
}

// BlockMayContain reports whether block i of p may hold every set of tokens
// in the column it names.
func BlockMayContain(p *part.Part, i int, sets []ColumnTokens) (bool, error) {
	for s := range sets {
		id := anyColumn
		if sets[s].Path != "" {
			var ok bool
			if id, ok = p.PathID(sets[s].Path); !ok {
				return false, nil
			}
		}
		if may, err := mayContainSet(p, i, id, sets[s].Hashes); !may || err != nil {
			return false, err
		}
	}

	return true, nil
}

// KeyFilter names what rows must hold at the leading sort key paths: the
// string Values at Paths and, when In is not empty, one of the strings it
// lists at InPath, the path that follows them. It prunes blocks of parts
// ordered by those paths and nothing else, so callers still filter rows.
type KeyFilter struct {
	Paths  []string
	Values [][]byte
	InPath string
	In     [][]byte
}

func (k KeyFilter) empty() bool { return len(k.Paths) == 0 && len(k.In) == 0 }

func (k KeyFilter) prefix() []byte {
	var out []byte
	for _, v := range k.Values {
		out = part.AppendKeyString(out, v)
	}

	return out
}

// covers reports whether a part ordered by key can be pruned with k.
func (k KeyFilter) covers(key []string) bool {
	n := len(k.Paths)
	if k.empty() || n != len(k.Values) || n > len(key) || (len(k.In) > 0 && (n == len(key) || key[n] != k.InPath)) {
		return false
	}
	for i, p := range k.Paths {
		if key[i] != p {
			return false
		}
	}

	return true
}

// keyRange is the encoded sort keys from lo up to hi, hi excluded.
type keyRange struct{ lo, hi []byte }

// ranges returns the encoded sort keys of the rows that the strings of
// k.In allow, as ranges in ascending order that do not overlap.
func (k KeyFilter) ranges() []keyRange {
	prefix := k.prefix()
	keys := make([][]byte, 0, len(k.In))
	for _, v := range k.In {
		keys = append(keys, part.AppendKeyString(append([]byte(nil), prefix...), v))
	}
	slices.SortFunc(keys, bytes.Compare)
	keys = slices.CompactFunc(keys, bytes.Equal)
	// The encoding of a string ends with two bytes that end no other, so no
	// key of one value starts with the key of another.
	out := make([]keyRange, len(keys))
	for i, key := range keys {
		out[i] = keyRange{key, successor(key)}
	}

	return out
}

// successor returns the shortest key above every key that starts with key;
// a key that starts with the encoding of a value has one.
func successor(key []byte) []byte {
	out := append([]byte(nil), key...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xFF {
			out[i]++

			return out[:i+1]
		}
	}

	return nil
}

// keyBlocks appends to dst the runs of blocks, first to end, of an ordered
// part that can hold rows in ranges, in order and apart from one another.
func keyBlocks(dst [][2]int, p *part.Part, ranges []keyRange) [][2]int {
	for _, r := range ranges {
		first, end := p.BlocksInKeyRange(r.lo, r.hi)
		if first == end {
			continue
		}
		if last := len(dst) - 1; last >= 0 && first <= dst[last][1] {
			dst[last][1] = max(dst[last][1], end)

			continue
		}
		dst = append(dst, [2]int{first, end})
	}

	return dst
}

// BlockFunc handles one block, valid only during the call; worker numbers the
// calling goroutine from zero so callers can keep per-worker state without
// locks.
type BlockFunc func(worker int, p *part.Part, b *part.Block) error

// ScanStats counts what a scan skipped and read.
type ScanStats struct {
	Blocks         int64
	SkippedByTime  int64
	SkippedByToken int64
	SkippedByKey   int64
	// SkippedAsEmpty counts the blocks that an earlier scan with the same
	// condition learned to hold no row.
	SkippedAsEmpty int64
}

// blockPool keeps the decode buffers of scan workers across scans.
var blockPool mem.Pool[part.Block]

// scanUnit is a block to scan; ids is where the path ids of the scan's token
// sets in its part start among those of all parts.
type scanUnit struct {
	// part numbers the part among those of the snapshot: a unit holds no
	// pointer, so that the list of a scan costs the collector nothing.
	part  int32
	block int32
	ids   int32
	// first and last are the times of the block's earliest and latest row.
	first, last int64
}

// scanLists holds the lists a scan makes of its blocks, kept between scans.
type scanLists struct {
	units   []scanUnit
	ordered []scanUnit
	ids     []uint32
	empties []*part.Misses
	// runs holds the runs of blocks of one part that the key leaves.
	runs [][2]int
}

var scanListsPool = sync.Pool{New: func() any { return new(scanLists) }}

// Scan calls fn from several goroutines, in no particular order, for every
// block that may hold matching rows; a panic or memory fault in a worker
// fails the scan instead of the process.
func (s *Snapshot) Scan(ctx context.Context, opts ScanOptions, fn BlockFunc) (ScanStats, error) {
	minT, maxT := opts.MinTime, opts.MaxTime
	if minT == 0 && maxT == 0 {
		minT, maxT = math.MinInt64, math.MaxInt64
	}
	key := opts.Key
	if key.empty() {
		key = s.key
	}
	keyPrefix, keyRanges := key.prefix(), key.ranges()
	var stats ScanStats
	lists := scanListsPool.Get().(*scanLists)
	defer func() {
		clear(lists.empties)
		scanListsPool.Put(lists)
	}()
	parts := s.v.parts
	units, ids := lists.units[:0], lists.ids[:0]
	empties := lists.empties[:0]
	for range parts {
		empties = append(empties, nil)
	}
	sets := opts.ColumnTokens
	for n, h := range parts {
		if h.MaxTime < minT || h.MinTime > maxT {
			stats.SkippedByTime += int64(h.NumBlocks())

			continue
		}
		at := len(ids)
		var named bool
		ids, named = resolveTokens(ids, h.Part, sets)
		if named && !decideCounted(ids[at:], h.Part, sets) {
			stats.SkippedByToken += int64(h.NumBlocks())

			continue
		}
		var empty *part.Misses
		if opts.Condition != "" {
			empty = h.Misses(opts.Condition)
			empties[n] = empty
		}
		runs := append(lists.runs[:0], [2]int{0, h.NumBlocks()})
		if h.Ordered() && key.covers(h.SortKey()) {
			if len(key.In) == 0 {
				first, end := h.BlocksWithKeyPrefix(keyPrefix)
				runs[0] = [2]int{first, end}
			} else {
				runs = keyBlocks(runs[:0], h.Part, keyRanges)
			}
			stats.SkippedByKey += int64(h.NumBlocks())
			for _, r := range runs {
				stats.SkippedByKey -= int64(r[1] - r[0])
			}
		}
		lists.runs = runs
		for _, run := range runs {
			for i := run[0]; i < run[1]; i++ {
				_, lo, hi := h.BlockStats(i)
				if hi < minT || lo > maxT {
					stats.SkippedByTime++

					continue
				}
				if empty != nil && empty.Has(i) {
					stats.SkippedAsEmpty++

					continue
				}
				if opts.Skip != nil && opts.Skip(h.Part, i) {
					continue
				}
				if !named {
					stats.SkippedByToken++

					continue
				}
				units = append(units, scanUnit{part: int32(n), block: int32(i), ids: int32(at), first: lo, last: hi})
			}
		}
	}
	lists.units, lists.ids, lists.empties = units, ids, empties
	if opts.ByTime != 0 {
		lists.ordered = orderUnits(units, lists.ordered, opts.ByTime < 0)
		units = lists.ordered
	}

	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	workers = min(workers, len(units))

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	r := &scanRun{
		ctx: ctx, cancel: cancel, opts: &opts, fn: fn, parts: parts, units: units, ids: ids, empties: empties,
		minT: minT, maxT: maxT, workers: workers,
	}
	one := func(worker int, b *part.Block, i int) bool { return r.one(worker, b, &units[i]) }
	// The first blocks in the order of time often hold every row that the
	// caller keeps, and it then moves the bound past the others. Where some
	// block lies wholly beyond the first one, a single block is scanned
	// first and twice as many each time, until the bound moves; the blocks
	// that the bound then leaves are scanned at once.
	n, at := len(units), 0
	beyond := false
	if opts.Bound != nil && n > rampFrom {
		beyond = (opts.ByTime < 0 && units[n-1].last < units[0].first) || (opts.ByTime > 0 && units[n-1].first > units[0].last)
	}
	if beyond {
		unmoved := opts.Bound.Load()
		for wave := 1; wave < workers && at < n && ctx.Err() == nil; {
			bound := opts.Bound.Load()
			if bound == unmoved {
				end := min(at+wave, n)
				r.each(wave, at, end, one)
				at = end
				wave = r.grow(wave, at)

				continue
			}
			end := at
			for end < n && !r.ruledOut(&units[end], bound) {
				end++
			}
			if end == at {
				break
			}
			r.each(workers, at, end, one)
			at = end
		}
	}
	if opts.Ramp && n > rampFrom {
		for wave := 1; wave < workers && at < n && ctx.Err() == nil; wave = r.grow(wave, at) {
			end := min(at+wave, n)
			r.each(wave, at, end, one)
			at = end
		}
	}
	r.each(workers, at, n, one)
	stats.Blocks, stats.SkippedByToken = r.read.Load(), stats.SkippedByToken+r.skipped.Load()
	stats.SkippedByTime += r.late.Load()
	if err := context.Cause(ctx); err != nil {
		return stats, err
	}

	return stats, nil
}

// scanRun is what the goroutines of one scan share.
type scanRun struct {
	ctx        context.Context
	cancel     context.CancelCauseFunc
	opts       *ScanOptions
	fn         BlockFunc
	parts      []*Handle
	units      []scanUnit
	ids        []uint32
	empties    []*part.Misses
	minT, maxT int64
	workers    int
	// next numbers what the goroutines of each take next.
	next atomic.Int64
	// read, skipped and late count the units opened, left out by their
	// tokens and left out by their times.
	read, skipped, late atomic.Int64
}

// ruledOut reports whether bound leaves u out of a scan by time.
func (r *scanRun) ruledOut(u *scanUnit, bound int64) bool {
	return (r.opts.ByTime < 0 && u.last < bound) || (r.opts.ByTime > 0 && u.first > bound)
}

// admit reports whether u is to be opened: whether it may hold a row within
// the range of the scan and with its tokens. It opens no block.
func (r *scanRun) admit(u *scanUnit) bool {
	p := r.parts[u.part].Part
	// A block that the range cuts may hold no row inside it, which its
	// rows by bucket of time tell; reading them takes microseconds a
	// block, so the scan's goroutines do it.
	if (u.first < r.minT || u.last > r.maxT) && p.NoneWithin(int(u.block), r.minT, r.maxT) {
		r.late.Add(1)
		if empty := r.empties[u.part]; empty != nil {
			empty.Add(int(u.block))
		}

		return false
	}
	sets := r.opts.ColumnTokens
	may, err := mayContainAll(p, int(u.block), r.opts.Tokens)
	for k := 0; k < len(sets) && may && err == nil; k++ {
		if id := r.ids[int(u.ids)+k]; id != unprobed {
			may, err = mayContainSet(p, int(u.block), id, sets[k].Hashes)
		}
	}
	if err != nil {
		r.cancel(err)

		return false
	}
	if !may {
		r.skipped.Add(1)
		if empty := r.empties[u.part]; empty != nil {
			empty.Add(int(u.block))
		}
	}

	return may
}

// open hands the block of u to the scan's function and reports whether the
// scan goes on.
func (r *scanRun) open(worker int, b *part.Block, u *scanUnit) bool {
	p := r.parts[u.part].Part
	if err := p.OpenBlock(int(u.block), b); err != nil {
		r.cancel(err)

		return false
	}
	r.read.Add(1)
	if err := r.fn(worker, p, b); err != nil {
		r.cancel(err)

		return false
	}

	return true
}

// one scans u, unless the bound rules it out as its turn comes, and reports
// whether the scan goes on.
func (r *scanRun) one(worker int, b *part.Block, u *scanUnit) bool {
	if r.opts.Bound != nil && r.ruledOut(u, r.opts.Bound.Load()) {
		r.late.Add(1)

		return true
	}
	if !r.admit(u) {
		return r.ctx.Err() == nil
	}

	return r.open(worker, b, u)
}

// each calls do for every number from lo up to hi, in no order, from g
// goroutines that it numbers from zero, or from the caller's when g is one,
// until do reports that the scan ends. A panic or a memory fault in do
// fails the scan.
func (r *scanRun) each(g, lo, hi int, do func(worker int, b *part.Block, i int) bool) {
	r.next.Store(int64(lo))
	work := func(worker int) {
		// A fault while reading mapped part bytes becomes a panic, which
		// the recover below turns into a scan error.
		defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
		defer func() {
			if v := recover(); v != nil {
				r.cancel(fmt.Errorf("colstore: scan worker failed: %v", v))
			}
		}()
		b := blockPool.Get()
		defer blockPool.Put(b)
		for r.ctx.Err() == nil {
			i := int(r.next.Add(1)) - 1
			if i >= hi || !do(worker, b, i) {
				return
			}
		}
	}
	if g <= 1 {
		work(0)

		return
	}
	var wg sync.WaitGroup
	for w := range g {
		wg.Go(func() { work(w) })
	}
	wg.Wait()
}

// grow returns how many blocks to take after a round of wave blocks did not
// end the scan, done blocks in all: twice as many, or half again as many as
// the rows kept so far say the caller still needs, up to one for each
// goroutine.
func (r *scanRun) grow(wave, done int) int {
	wave *= 2
	if r.opts.Kept == nil {
		return wave
	}
	have, want := r.opts.Kept()
	if have > 0 && want > have && done > 0 {
		need := int64(want-have)*int64(done)/int64(have)*3/2 + 1
		wave = max(wave, int(min(need, int64(r.workers))))
	}

	return wave
}

func mayContainAll(p *part.Part, block int, tokens []uint64) (bool, error) {
	for _, h := range tokens {
		if may, err := p.MayContainToken(block, h); !may || err != nil {
			return false, err
		}
	}

	return true, nil
}
