package colstore

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
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
}

// blockPool keeps the decode buffers of scan workers across scans.
var blockPool mem.Pool[part.Block]

// scanUnit is a block to scan; ids is where the path ids of the scan's token
// sets in its part start among those of all parts.
type scanUnit struct {
	p     *part.Part
	block int32
	ids   int32
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
		first, end := 0, h.NumBlocks()
		if h.Ordered() && key.covers(h.SortKey()) {
			first, end = h.BlocksWithKeyPrefix(keyPrefix)
			stats.SkippedByKey += int64(h.NumBlocks() - (end - first))
		}
		for i := first; i < end; i++ {
			if _, lo, hi := h.BlockStats(i); hi < minT || lo > maxT {
				stats.SkippedByTime++

				continue
			}
			if opts.Skip != nil && opts.Skip(h.Part, i) {
				continue
			}
			if !named {
				stats.SkippedByToken++

				continue
			}
			units = append(units, scanUnit{p: h.Part, block: int32(i), ids: int32(at)})
		}
	}

	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	workers = min(workers, len(units))

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var next, read, skipped atomic.Int64
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
				if i >= int64(len(units)) {
					return
				}
				u := units[i]
				may, err := mayContainAll(u.p, int(u.block), opts.Tokens)
				for k := 0; k < len(sets) && may && err == nil; k++ {
					may, err = mayContainSet(u.p, int(u.block), ids[int(u.ids)+k], sets[k].Hashes)
				}
				if err != nil {
					cancel(err)

					return
				}
				if !may {
					skipped.Add(1)

					continue
				}
				if err := u.p.OpenBlock(int(u.block), b); err != nil {
					cancel(err)

					return
				}
				read.Add(1)
				if err := fn(worker, u.p, b); err != nil {
					cancel(err)

					return
				}
			}
		}(w)
	}
	wg.Wait()
	stats.Blocks, stats.SkippedByToken = read.Load(), stats.SkippedByToken+skipped.Load()
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
