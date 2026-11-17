package exec

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// Options sets how a plan runs.
type Options struct {
	// Workers is the number of goroutines; zero means one per CPU.
	Workers int
	// Forgetful makes the query open every block that its filter admits,
	// whatever earlier queries with the same filter found there, and leave
	// nothing for later ones to go by.
	Forgetful bool
}

// sharedGroups gives every worker the same id for a group key, so that the
// ids can stand for the groups in what workers hand to a merge.
type sharedGroups struct {
	mu    sync.Mutex
	arena *mem.Arena
	table *vec.ByteTable
}

func (s *sharedGroups) id(key []byte, h uint64) uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return uint32(s.table.IDHashed(key, h))
}

// execution is one run of a query.
type execution struct {
	q       *query
	workers []*worker
	mergers []merger
	shared  *sharedGroups
	// streaming is set while the aggregation runs in key order.
	streaming bool
	// rows is the number of rows the workers aggregated.
	rows int64
	// ords numbers the parts of the snapshot for a plan that locates rows.
	ords map[*part.Part]int
	// bound is the time that a row must reach to be among the first rows by
	// time that the plan keeps, once some goroutine holds that many.
	bound *atomic.Int64
	// taken counts the rows that a plan with a limit and no order has
	// produced.
	taken atomic.Int64
	// emit, when set, takes the rows of a plan of rows block by block in
	// place of the workers' sinks.
	emit func(batch []colVec, n int) error
	// condition names the filter for what parts remember of it, or is
	// empty.
	condition string
}

// running counts the executions that chose their own number of goroutines
// and have not ended.
var running atomic.Int32

// errEnough ends a scan whose plan has all the rows it asked for.
var errEnough = errors.New("exec: enough rows")

// Supports reports whether plan is one that the executor runs over snap: nil,
// or an error that wraps ErrUnsupported.
func Supports(snap *colstore.Snapshot, plan Node) error {
	_, err := compile(snap, plan)

	return err
}

// Each runs a plan of rows that neither aggregates nor sorts, and hands the
// rows of each block that has any to fn, in no order among blocks. One
// goroutine calls fn at a time, and the result is the caller's to keep. A
// limit ends the scan once that many rows were handed over, and so does an
// error of fn, which Each returns.
func Each(ctx context.Context, snap *colstore.Snapshot, plan Node, opts Options, fn func(*Result) error) error {
	q, err := compile(snap, plan)
	if err != nil {
		return err
	}
	if q.agg != nil || len(q.order) != 0 {
		return unsupported("rows are handed over as they are scanned: without an aggregate or a sort")
	}
	n := opts.Workers
	if n <= 0 {
		cpus := runtime.GOMAXPROCS(0)
		n = max(1, min(cpus, 2*cpus/int(running.Add(1))))
		defer running.Add(-1)
	}
	x := &execution{q: q, workers: make([]*worker, n)}
	if q.places {
		x.ords = map[*part.Part]int{}
		for i, h := range snap.Parts() {
			x.ords[h.Part] = i
		}
	}
	defer x.release()
	var handing sync.Mutex
	handed := 0
	x.emit = func(batch []colVec, rows int) error {
		if q.limit > 0 {
			handing.Lock()
			rows = min(rows, q.limit-handed)
			handed += rows
			handing.Unlock()
			if rows <= 0 {
				return errEnough
			}
		}
		out := make([]colVec, len(batch))
		for c := range batch {
			for i := 0; i < rows; i++ {
				out[c].appendFrom(&batch[c], i)
			}
		}
		cols, _, err := x.shown(snap, out, rows)
		if err != nil {
			return err
		}
		handing.Lock()
		defer handing.Unlock()

		return fn(&Result{Columns: cols, Rows: rows})
	}
	scan := q.opts
	scan.Workers = n
	if opts.Forgetful {
		scan.Condition = ""
	}
	x.condition = scan.Condition
	// A plan that takes its first rows as they come ends the scan early.
	scan.Ramp = q.limit > 0 && len(q.order) == 0
	if _, err := snap.Scan(ctx, scan, x.block); err != nil && !errors.Is(err, errEnough) {
		return err
	}

	return nil
}

// Run executes plan over snap and returns its rows.
func Run(ctx context.Context, snap *colstore.Snapshot, plan Node, opts Options) (*Result, error) {
	q, err := compile(snap, plan)
	if err != nil {
		return nil, err
	}
	n := opts.Workers
	if n <= 0 {
		// Queries that run at once share the CPUs: each takes its part of
		// twice their number, since goroutines beyond the CPUs only add
		// switching and a query that finishes early leaves room.
		cpus := runtime.GOMAXPROCS(0)
		n = max(1, min(cpus, 2*cpus/int(running.Add(1))))
		defer running.Add(-1)
	}
	var ords map[*part.Part]int
	if q.located != nil || q.places {
		ords = map[*part.Part]int{}
		for i, h := range snap.Parts() {
			ords[h.Part] = i
		}
	}
	// hashedFor says why a plan that could run in key order did not.
	hashedFor := ""
	if q.ordered != nil {
		x := &execution{q: q, workers: make([]*worker, n), streaming: true, ords: ords}
		res, err := x.runOrdered(ctx, snap)
		x.release()
		if !errors.Is(err, errUnordered) {
			return res, err
		}
		hashedFor = err.Error()
	}
	x := &execution{q: q, workers: make([]*worker, n), ords: ords}
	if q.agg != nil && q.agg.group >= 0 {
		a := mem.NewArena()
		x.shared = &sharedGroups{arena: a, table: vec.NewByteTable(a)}
	}
	defer x.release()
	scan := q.opts
	scan.Workers = n
	if opts.Forgetful {
		scan.Condition = ""
	}
	x.condition = scan.Condition
	scan.Ramp = q.agg == nil && q.limit > 0 && len(q.order) == 0
	if q.byTime != 0 {
		x.bound = new(atomic.Int64)
		x.bound.Store(math.MinInt64)
		if q.byTime > 0 {
			x.bound.Store(math.MaxInt64)
		}
		scan.ByTime, scan.Bound = q.byTime, x.bound
	}
	stats, err := snap.Scan(ctx, scan, x.block)
	if err != nil && !errors.Is(err, errEnough) {
		return nil, err
	}
	active := x.workers[:0:0]
	for _, w := range x.workers {
		if w != nil {
			active = append(active, w)
		}
	}
	var sinks []*sink
	if q.agg != nil {
		if sinks, err = x.finishAggregate(active); err != nil {
			return nil, err
		}
	} else {
		for _, w := range active {
			sinks = append(sinks, &w.sink)
		}
	}
	res := &Result{}
	if res.Columns, res.Rows, err = x.finish(snap, sinks); err != nil {
		return nil, err
	}
	res.Stats = Stats{
		Blocks:         stats.Blocks,
		SkippedByKey:   stats.SkippedByKey,
		SkippedByToken: stats.SkippedByToken,
		SkippedByTime:  stats.SkippedByTime,
		SkippedAsEmpty: stats.SkippedAsEmpty,
		HeldBytes:      x.held(),
		HashedFor:      hashedFor,
	}
	for _, w := range active {
		res.Stats.SkippedByStats += w.undecoded
		res.Stats.ColumnsSearched += w.searched
		res.Stats.ColumnsSkippedByToken += w.unsearched
		res.Stats.BlocksMatched += w.blocks
		res.Stats.RowsMatched += w.matched
	}

	return res, nil
}

// held returns the bytes the execution holds in arenas and buffers.
func (x *execution) held() int64 {
	n := 0
	if x.shared != nil {
		n += x.shared.arena.Size()
	}
	for _, w := range x.workers {
		if w != nil {
			n += w.arena.Size() + w.held() + w.sink.held()
		}
	}
	for i := range x.mergers {
		n += x.mergers[i].held()
	}

	return int64(n)
}

func (x *execution) release() {
	if x.shared != nil {
		x.shared.arena.Free()
	}
	for _, w := range x.workers {
		if w != nil {
			w.release()
		}
	}
	for i := range x.mergers {
		if m := &x.mergers[i]; m.arena != nil {
			m.arena.Free()
			for c := range m.batch {
				m.batch[c].keys = nil
			}
			mergeScratchPool.Put(m.mergeScratch)
		}
	}
}

// block runs the plan's scan-side operators over one block.
func (x *execution) block(n int, p *part.Part, b *part.Block) error {
	w := x.workers[n]
	if w == nil {
		w = newWorker(x)
		x.workers[n] = w
	}
	w.open(p, b)
	q := x.q
	count, idx := w.rows, []uint32(nil)
	if q.filter != nil {
		v, err := w.eval(q.filter, 0)
		if err != nil {
			return err
		}
		switch s := &w.sels[0]; {
		case v == none:
			if !w.decoded {
				w.undecoded++
			}
			x.empty(p, b)

			return nil
		case v == all:
		case q.agg != nil && q.agg.rowsOnly && len(q.agg.keys) == 0:
			// A count of rows needs their number alone.
			if count = len(s.list); !s.listed {
				count = vec.MaskCount(s.mask)
			}
			w.agg.words[q.agg.rows] += int64(count)
			if count > 0 {
				w.blocks++
				w.matched += int64(count)
			} else {
				x.empty(p, b)
			}

			return nil
		case s.listed:
			idx, count = s.list, len(s.list)
		default:
			w.idx = vec.MaskIndexes(w.idx[:0], s.mask)
			idx, count = w.idx, len(w.idx)
		}
		switch count {
		case 0:
			x.empty(p, b)

			return nil
		case w.rows:
			idx = nil
		}
	}
	w.blocks++
	w.matched += int64(count)
	if q.agg != nil {
		return w.agg.block(count, idx)
	}

	limited := q.limit > 0 && len(q.order) == 0
	if limited && x.taken.Load() >= int64(q.limit) {
		return errEnough
	}
	if err := w.project(count, idx); err != nil {
		return err
	}
	if limited && x.taken.Add(int64(count)) >= int64(q.limit) {
		return errEnough
	}

	return nil
}

// empty remembers that no row of block b passes the filter, for the scans
// with the same filter that follow.
func (x *execution) empty(p *part.Part, b *part.Block) {
	if x.condition != "" {
		p.Misses(x.condition).Add(b.Index())
	}
}

// forEach calls fn for every i below n from up to workers goroutines,
// passing each goroutine's number so that it can own a state; a panic in fn
// becomes the returned error.
func forEach(n, workers int, fn func(slot, i int)) (err error) {
	workers = min(workers, n)
	if workers <= 1 {
		for i := 0; i < n; i++ {
			fn(0, i)
		}

		return nil
	}
	var next atomic.Int64
	var failed atomic.Pointer[error]
	var wg sync.WaitGroup
	for slot := 0; slot < workers; slot++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					e := fmt.Errorf("exec: merge failed: %v", r)
					failed.CompareAndSwap(nil, &e)
				}
			}()
			for failed.Load() == nil {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				fn(slot, i)
			}
		}(slot)
	}
	wg.Wait()
	if e := failed.Load(); e != nil {
		return *e
	}

	return nil
}
