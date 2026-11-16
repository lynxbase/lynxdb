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
	// Key prunes blocks by sort key; the snapshot's own filter applies when
	// it is empty.
	Key KeyFilter
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

type scanUnit struct {
	p     *part.Part
	block int
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
	for _, h := range s.v.parts {
		if h.MaxTime < minT || h.MinTime > maxT {
			stats.SkippedByTime += int64(h.NumBlocks())

			continue
		}
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
			units = append(units, scanUnit{p: h.Part, block: i})
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
				if err := u.p.OpenBlock(u.block, b); err != nil {
					cancel(err)

					return
				}
				may, err := mayContainAll(b, opts.Tokens)
				if err != nil {
					cancel(err)

					return
				}
				if !may {
					skipped.Add(1)

					continue
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
	stats.Blocks, stats.SkippedByToken = read.Load(), skipped.Load()
	if err := context.Cause(ctx); err != nil {
		return stats, err
	}

	return stats, nil
}

func mayContainAll(b *part.Block, tokens []uint64) (bool, error) {
	for _, h := range tokens {
		if may, err := b.MayContainToken(h); !may || err != nil {
			return false, err
		}
	}

	return true, nil
}
