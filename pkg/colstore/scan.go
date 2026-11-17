package colstore

import (
	"cmp"
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
}

// ColumnTokens holds token hashes that the strings of one column of a block
// must all possibly contain: the string column at Path, or any one column
// when Path is empty.
type ColumnTokens struct {
	Path   string
	Hashes []uint64
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

// KeyFilter names string values that rows must hold at the leading sort key
// paths; it prunes blocks of parts ordered by those paths and nothing else,
// so callers still filter rows.
type KeyFilter struct {
	Paths  []string
	Values [][]byte
}

func (k KeyFilter) prefix() []byte {
	var out []byte
	for _, v := range k.Values {
		out = part.AppendKeyString(out, v)
	}

	return out
}

// covers reports whether a part ordered by key can be pruned with k.
func (k KeyFilter) covers(key []string) bool {
	if len(k.Paths) == 0 || len(k.Paths) > len(key) || len(k.Paths) != len(k.Values) {
		return false
	}
	for i, p := range k.Paths {
		if key[i] != p {
			return false
		}
	}

	return true
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
	p     *part.Part
	empty *part.Misses
	block int32
	ids   int32
	// first and last are the times of the block's earliest and latest row.
	first, last int64
}

// Scan calls fn from several goroutines, in no particular order, for every
// block that may hold matching rows; a panic or memory fault in a worker
// fails the scan instead of the process.
func (s *Snapshot) Scan(ctx context.Context, opts ScanOptions, fn BlockFunc) (ScanStats, error) {
	minT, maxT := opts.MinTime, opts.MaxTime
	if minT == 0 && maxT == 0 {
		minT, maxT = math.MinInt64, math.MaxInt64
	}
	key := opts.Key
	if len(key.Paths) == 0 {
		key = s.key
	}
	keyPrefix := key.prefix()
	var stats ScanStats
	var units []scanUnit
	var ids []uint32
	sets := opts.ColumnTokens
	for _, h := range s.v.parts {
		if h.MaxTime < minT || h.MinTime > maxT {
			stats.SkippedByTime += int64(h.NumBlocks())

			continue
		}
		at := len(ids)
		var named bool
		ids, named = resolveTokens(ids, h.Part, sets)
		var empty *part.Misses
		if opts.Condition != "" {
			empty = h.Misses(opts.Condition)
		}
		first, end := 0, h.NumBlocks()
		if h.Ordered() && key.covers(h.SortKey()) {
			first, end = h.BlocksWithKeyPrefix(keyPrefix)
			stats.SkippedByKey += int64(h.NumBlocks() - (end - first))
		}
		for i := first; i < end; i++ {
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
			units = append(units, scanUnit{p: h.Part, empty: empty, block: int32(i), ids: int32(at), first: lo, last: hi})
		}
	}
	switch {
	case opts.ByTime < 0:
		slices.SortStableFunc(units, func(a, b scanUnit) int { return cmp.Compare(b.last, a.last) })
	case opts.ByTime > 0:
		slices.SortStableFunc(units, func(a, b scanUnit) int { return cmp.Compare(a.first, b.first) })
	}

	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	workers = min(workers, len(units))

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var next, read, skipped, late atomic.Int64
	// one scans a unit and reports whether the scan goes on.
	one := func(worker int, b *part.Block, u scanUnit) bool {
		if opts.Bound != nil {
			if bound := opts.Bound.Load(); (opts.ByTime < 0 && u.last < bound) || (opts.ByTime > 0 && u.first > bound) {
				late.Add(1)

				return true
			}
		}
		may, err := mayContainAll(u.p, int(u.block), opts.Tokens)
		for k := 0; k < len(sets) && may && err == nil; k++ {
			may, err = mayContainSet(u.p, int(u.block), ids[int(u.ids)+k], sets[k].Hashes)
		}
		if err != nil {
			cancel(err)

			return false
		}
		if !may {
			skipped.Add(1)
			if u.empty != nil {
				u.empty.Add(int(u.block))
			}

			return true
		}
		if err := u.p.OpenBlock(int(u.block), b); err != nil {
			cancel(err)

			return false
		}
		read.Add(1)
		if err := fn(worker, u.p, b); err != nil {
			cancel(err)

			return false
		}

		return true
	}
	// run scans the units from next on with the goroutines numbered below
	// workers, up to the unit numbered end.
	run := func(workers int, end int64) {
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				// A fault while reading mapped part bytes becomes a panic,
				// which the recover below turns into a scan error.
				debug.SetPanicOnFault(true)
				defer func() {
					if r := recover(); r != nil {
						cancel(fmt.Errorf("colstore: scan worker failed: %v", r))
					}
				}()
				b := blockPool.Get()
				defer blockPool.Put(b)
				for ctx.Err() == nil {
					i := next.Add(1) - 1
					if i >= end {
						next.Add(-1)

						return
					}
					if !one(worker, b, units[i]) {
						return
					}
				}
			}(w)
		}
		wg.Wait()
	}
	// The first blocks in the order of time often hold every row that the
	// caller keeps, and it then moves the bound past the others. Where some
	// block lies wholly beyond the first one, a single block is scanned
	// first and twice as many each time, until the bound moves; the blocks
	// that the bound then leaves are scanned at once.
	ruledOut := func(u scanUnit, bound int64) bool {
		return (opts.ByTime < 0 && u.last < bound) || (opts.ByTime > 0 && u.first > bound)
	}
	beyond := false
	if n := len(units); opts.Bound != nil && n > 1 {
		beyond = (opts.ByTime < 0 && units[n-1].last < units[0].first) || (opts.ByTime > 0 && units[n-1].first > units[0].last)
	}
	if n := len(units); beyond {
		unmoved := opts.Bound.Load()
		for wave := 1; wave < workers && ctx.Err() == nil; {
			at := next.Load()
			if at >= int64(n) {
				break
			}
			bound := opts.Bound.Load()
			if bound == unmoved {
				run(wave, min(at+int64(wave), int64(n)))
				wave *= 2

				continue
			}
			end := at
			for end < int64(n) && !ruledOut(units[end], bound) {
				end++
			}
			if end == at {
				break
			}
			run(workers, end)
		}
	}
	run(workers, int64(len(units)))
	stats.Blocks, stats.SkippedByToken = read.Load(), stats.SkippedByToken+skipped.Load()
	stats.SkippedByTime += late.Load()
	if err := context.Cause(ctx); err != nil {
		return stats, err
	}

	return stats, nil
}

func mayContainAll(p *part.Part, block int, tokens []uint64) (bool, error) {
	for _, h := range tokens {
		if may, err := p.MayContainToken(block, h); !may || err != nil {
			return false, err
		}
	}

	return true, nil
}
