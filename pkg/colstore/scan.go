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
)

// ScanOptions narrows a scan and sets its parallelism.
type ScanOptions struct {
	// Workers is the number of goroutines; zero means one per CPU.
	Workers int
	// MinTime and MaxTime bound row timestamps, inclusive. Both zero means
	// no bound.
	MinTime int64
	MaxTime int64
	// Tokens holds token hashes that a block must all possibly contain.
	Tokens []uint64
}

// BlockFunc handles one block. worker identifies the calling goroutine,
// counting from zero, so callers can keep per-worker state without locks.
// The block is only valid during the call.
type BlockFunc func(worker int, p *part.Part, b *part.Block) error

// ScanStats counts what a scan skipped and read.
type ScanStats struct {
	Blocks         int64
	SkippedByTime  int64
	SkippedByToken int64
}

type scanUnit struct {
	p     *part.Part
	block int
}

// Scan calls fn for every block of the snapshot that may hold matching
// rows. Blocks are handed to workers in no particular order. A panic or a
// memory fault inside a worker fails the scan instead of the process.
func (s *Snapshot) Scan(ctx context.Context, opts ScanOptions, fn BlockFunc) (ScanStats, error) {
	minT, maxT := opts.MinTime, opts.MaxTime
	if minT == 0 && maxT == 0 {
		minT, maxT = math.MinInt64, math.MaxInt64
	}
	var stats ScanStats
	var units []scanUnit
	for _, h := range s.v.parts {
		if h.MaxTime < minT || h.MinTime > maxT {
			stats.SkippedByTime += int64(h.NumBlocks())

			continue
		}
		for i := 0; i < h.NumBlocks(); i++ {
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
			var b part.Block
			for ctx.Err() == nil {
				i := next.Add(1) - 1
				if i >= int64(len(units)) {
					return
				}
				u := units[i]
				if err := u.p.OpenBlock(u.block, &b); err != nil {
					cancel(err)

					return
				}
				if !mayContainAll(&b, opts.Tokens) {
					skipped.Add(1)

					continue
				}
				read.Add(1)
				if err := fn(worker, u.p, &b); err != nil {
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

func mayContainAll(b *part.Block, tokens []uint64) bool {
	for _, h := range tokens {
		if !b.MayContainToken(h) {
			return false
		}
	}

	return true
}
