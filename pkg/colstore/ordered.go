package colstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// BlockSpan is a run of consecutive blocks of one part.
type BlockSpan struct {
	Part       *part.Part
	First, End int
	// prev is the part before Part in its stream, if any.
	prev *part.Part
}

// KeyRange is the rows whose encoded sort key is at least Lo and below Hi,
// held by streams that each list blocks in key order.
type KeyRange struct {
	Lo, Hi  []byte
	Streams [][]BlockSpan
	// split is the number of ranges to cut this one into before it is
	// read, when the plan found too few keys to cut at.
	split int
}

// KeyPlan is how the rows matching a key filter lie in the ordered parts of a
// snapshot: in streams of parts whose key ranges follow one another.
type KeyPlan struct {
	prefix []byte
	// columns is the number of sort key columns of the filter and the
	// group, the ones rows must share to belong together.
	columns int
	parts   []*part.Part
	streams [][]*part.Part
	// Blocks and Rows count the blocks that can hold matching rows.
	Blocks int
	Rows   int
}

// PlanKeyRanges lays out the rows matching key for aggregation in key order,
// and reports false unless every part is ordered by the paths of key followed
// by group.
func (s *Snapshot) PlanKeyRanges(key KeyFilter, group []string) (*KeyPlan, bool) {
	if len(key.Paths) != len(key.Values) {
		return nil, false
	}
	paths := append(append([]string(nil), key.Paths...), group...)
	plan := &KeyPlan{prefix: key.prefix(), columns: len(paths)}
	for _, h := range s.v.parts {
		sk := h.SortKey()
		if !h.Ordered() || len(sk) < len(paths) {
			return nil, false
		}
		for i, path := range paths {
			if sk[i] != path {
				return nil, false
			}
		}
		first, end := h.BlocksWithKeyPrefix(plan.prefix)
		if first == end {
			continue
		}
		plan.Blocks += end - first
		for i := first; i < end; i++ {
			rows, _, _ := h.BlockStats(i)
			plan.Rows += rows
		}
		plan.parts = append(plan.parts, h.Part)
	}
	sort.SliceStable(plan.parts, func(i, j int) bool {
		a, _ := plan.parts[i].KeyRange()
		b, _ := plan.parts[j].KeyRange()

		return bytes.Compare(a, b) < 0
	})
	// A part joins the first stream that ends at or before its first key.
	for _, p := range plan.parts {
		first, _ := p.KeyRange()
		placed := false
		for i, st := range plan.streams {
			if _, last := st[len(st)-1].KeyRange(); bytes.Compare(last, first) <= 0 {
				plan.streams[i] = append(st, p)
				placed = true

				break
			}
		}
		if !placed {
			plan.streams = append(plan.streams, []*part.Part{p})
		}
	}

	return plan, true
}

// Streams returns the largest number of streams a range can hold.
func (p *KeyPlan) Streams() int { return len(p.streams) }

// ColumnID names the column of a path that holds values of one kind.
type ColumnID struct {
	Path string
	Kind vec.Kind
}

// Ranges cuts the matching rows into at most n key ranges of similar size
// whose readers also get the listed columns of every block.
func (p *KeyPlan) Ranges(n int, columns ...ColumnID) *RangeScan {
	bounds := [][]byte{p.prefix}
	bounds = append(bounds, part.SplitKeysWithPrefix(p.parts, n, p.prefix, p.columns)...)
	// Every key that starts with the prefix is below the prefix followed by
	// 0xFF, which no value kind starts with.
	bounds = append(bounds, append(append([]byte(nil), p.prefix...), 0xFF))
	scan := &RangeScan{columns: columns, keyColumns: p.columns, left: map[blockID]int{}, open: map[blockID]*SharedBlock{}}
	var rows []int
	total := 0
	for i := 0; i+1 < len(bounds); i++ {
		r := KeyRange{Lo: bounds[i], Hi: bounds[i+1]}
		held := 0
		for _, st := range p.streams {
			var spans []BlockSpan
			for at, pt := range st {
				first, end := pt.BlocksInKeyRange(r.Lo, r.Hi)
				if first == end {
					continue
				}
				span := BlockSpan{Part: pt, First: first, End: end}
				if at > 0 {
					span.prev = st[at-1]
				}
				spans = append(spans, span)
				for b := first; b < end; b++ {
					n, _, _ := pt.BlockStats(b)
					held += n
				}
			}
			if len(spans) > 0 {
				r.Streams = append(r.Streams, spans)
			}
		}
		if len(r.Streams) > 0 {
			scan.Ranges = append(scan.Ranges, r)
			rows = append(rows, held)
			total += held
		}
	}
	// Block starts gave too few keys to cut at: the ranges that hold more
	// than their share are cut again at keys found inside their blocks.
	scarce := 2*len(scan.Ranges) < n
	for i := range scan.Ranges {
		r := &scan.Ranges[i]
		if want := (2*n*rows[i] + total) / (2 * total); scarce && want > 1 {
			r.split = want

			continue
		}
		scan.expect(r)
	}

	return scan
}

// BlockRanges cuts the matching rows of a plan with one stream into at most
// n ranges of whole blocks, for scans that learn from RangeReader.Shared
// whether the rows on both sides of a block border belong together; a plan
// with several streams is cut by key, as Ranges does.
func (p *KeyPlan) BlockRanges(n int, columns ...ColumnID) *RangeScan {
	if len(p.streams) != 1 {
		return p.Ranges(n, columns...)
	}
	lo, hi := p.prefix, append(append([]byte(nil), p.prefix...), 0xFF)
	scan := &RangeScan{columns: columns, keyColumns: p.columns, left: map[blockID]int{}, open: map[blockID]*SharedBlock{}}
	share := max(1, (p.Rows+n-1)/n)
	var spans []BlockSpan
	held := 0
	flush := func() {
		if len(spans) > 0 {
			scan.Ranges = append(scan.Ranges, KeyRange{Lo: lo, Hi: hi, Streams: [][]BlockSpan{spans}})
			spans, held = nil, 0
		}
	}
	var prev *part.Part
	for _, pt := range p.streams[0] {
		first, end := pt.BlocksInKeyRange(lo, hi)
		for b := first; b < end; b++ {
			if last := len(spans) - 1; last >= 0 && spans[last].Part == pt {
				spans[last].End = b + 1
			} else {
				spans = append(spans, BlockSpan{Part: pt, First: b, End: b + 1, prev: prev})
			}
			rows, _, _ := pt.BlockStats(b)
			if held += rows; held >= share {
				flush()
			}
		}
		prev = pt
	}
	flush()
	for i := range scan.Ranges {
		scan.expect(&scan.Ranges[i])
	}

	return scan
}

type blockID struct {
	part  *part.Part
	block int
}

// SharedBlock holds what a range scan decodes of one block, for every reader
// whose range holds rows of the block; readers must not change it.
type SharedBlock struct {
	// Keys holds the sort key columns.
	Keys part.BlockKeys
	// Rows is the number of rows of the block.
	Rows int

	columns []vec.Column
	present []bool
	ready   sync.WaitGroup
	err     error
}

// Column returns column i of those the scan was asked for, or nil when the
// block does not have it.
func (b *SharedBlock) Column(i int) *vec.Column {
	if !b.present[i] {
		return nil
	}

	return &b.columns[i]
}

// RangeScan is a set of key ranges over blocks that are decoded once each,
// however many ranges they have rows in.
type RangeScan struct {
	Ranges []KeyRange
	// Decoded counts the blocks decoded so far.
	Decoded atomic.Int64

	columns []ColumnID
	// keyColumns is the number of sort key columns that rows of one group
	// share.
	keyColumns int
	// queue holds the ranges to read, next the first one not taken, and
	// splitting the workers that may still add ranges.
	queue     []*KeyRange
	next      int
	splitting int
	more      *sync.Cond
	mu        sync.Mutex
	// left counts the readers that have yet to finish with a block, open
	// holds the blocks in use and free the ones to reuse.
	left map[blockID]int
	open map[blockID]*SharedBlock
	free []*SharedBlock
	all  []*SharedBlock
	// retained is the size of the decode buffers when Run ended.
	retained int
}

// sharedBlockPool keeps the decode buffers of range scans across scans.
var sharedBlockPool mem.Pool[SharedBlock]

var errBlockNotDecoded = errors.New("colstore: block was not decoded")

// expect counts a reader for every block of r.
func (s *RangeScan) expect(r *KeyRange) {
	for _, spans := range r.Streams {
		for _, span := range spans {
			for b := span.First; b < span.End; b++ {
				s.left[blockID{span.Part, b}]++
			}
		}
	}
}

// take returns the next range to read, waiting while a worker may still add
// ranges, and nil when none is left.
func (s *RangeScan) take() *KeyRange {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.next == len(s.queue) && s.splitting > 0 {
		s.more.Wait()
	}
	if s.next == len(s.queue) {
		return nil
	}
	r := s.queue[s.next]
	s.next++
	if r.split > 1 {
		s.splitting++
	}

	return r
}

// cut replaces r, which take returned, by ranges cut at keys sampled from
// the blocks of its first stream, read through b.
func (s *RangeScan) cut(r *KeyRange, b *part.Block) error {
	// Waiting workers go on even when reading a block fails or faults.
	defer func() {
		s.mu.Lock()
		s.splitting--
		s.more.Broadcast()
		s.mu.Unlock()
	}()
	// The sampled blocks stay decoded until the readers of the cut ranges,
	// counted below, are done with them.
	type hold struct {
		id blockID
		sb *SharedBlock
	}
	var held []hold
	var samples [][]byte
	var scratch []byte
	err := func() error {
		for _, span := range r.Streams[0] {
			for i := span.First; i < span.End && len(samples) < 16*r.split; i++ {
				id := blockID{span.Part, i}
				s.mu.Lock()
				s.left[id]++
				s.mu.Unlock()
				sb, err := s.acquire(id, b)
				if err != nil {
					return err
				}
				held = append(held, hold{id, sb})
				from, to := sb.Keys.Search(r.Lo, &scratch), sb.Keys.Search(r.Hi, &scratch)
				for j := 1; j < 4*r.split && from < to; j++ {
					row := from + j*(to-from)/(4*r.split)
					samples = append(samples, part.KeyPrefix(sb.Keys.AppendKey(nil, row), s.keyColumns))
				}
			}
		}

		return nil
	}()
	bounds := [][]byte{r.Lo}
	for j := 1; j < r.split && len(samples) > 0; j++ {
		if key := samples[j*len(samples)/r.split]; bytes.Compare(key, bounds[len(bounds)-1]) > 0 {
			bounds = append(bounds, key)
		}
	}
	bounds = append(bounds, r.Hi)
	if err != nil {
		return err
	}
	s.mu.Lock()
	for i := 0; i+1 < len(bounds); i++ {
		sub := &KeyRange{Lo: bounds[i], Hi: bounds[i+1]}
		for _, spans := range r.Streams {
			var in []BlockSpan
			for _, span := range spans {
				first, end := span.Part.BlocksInKeyRange(sub.Lo, sub.Hi)
				if first, end = max(first, span.First), min(end, span.End); first < end {
					in = append(in, BlockSpan{Part: span.Part, First: first, End: end, prev: span.prev})
				}
			}
			if len(in) > 0 {
				sub.Streams = append(sub.Streams, in)
			}
		}
		if len(sub.Streams) > 0 {
			s.expect(sub)
			s.queue = append(s.queue, sub)
		}
	}
	s.mu.Unlock()
	for _, h := range held {
		s.release(h.id, h.sb)
	}

	return nil
}

// acquire returns the decoded block id, decoding it through b unless another
// reader has.
func (s *RangeScan) acquire(id blockID, b *part.Block) (*SharedBlock, error) {
	s.mu.Lock()
	sb := s.open[id]
	owner := sb == nil
	if owner {
		if n := len(s.free); n > 0 {
			sb, s.free = s.free[n-1], s.free[:n-1]
		} else {
			sb = sharedBlockPool.Get()
			for len(sb.columns) < len(s.columns) {
				sb.columns, sb.present = append(sb.columns, vec.Column{}), append(sb.present, false)
			}
			s.all = append(s.all, sb)
		}
		// Waiting readers see this error when the decode panics.
		sb.err = errBlockNotDecoded
		sb.ready.Add(1)
		s.open[id] = sb
	}
	s.mu.Unlock()
	if !owner {
		sb.ready.Wait()

		return sb, sb.err
	}
	defer sb.ready.Done()
	sb.err = s.decode(id, b, sb)

	return sb, sb.err
}

func (s *RangeScan) decode(id blockID, b *part.Block, sb *SharedBlock) error {
	if err := id.part.OpenBlock(id.block, b); err != nil {
		return err
	}
	s.Decoded.Add(1)
	sb.Rows = b.Rows
	if err := sb.Keys.Load(b); err != nil {
		return err
	}
	for i, c := range s.columns {
		sb.present[i] = false
		pathID, ok := id.part.PathID(c.Path)
		if !ok {
			continue
		}
		ref, ok := b.Column(pathID, c.Kind)
		if !ok {
			continue
		}
		if err := b.Decode(ref, &sb.columns[i]); err != nil {
			return err
		}
		sb.present[i] = true
	}

	return nil
}

// release ends one reader's use of a block.
func (s *RangeScan) release(id blockID, sb *SharedBlock) {
	s.mu.Lock()
	if s.left[id]--; s.left[id] == 0 {
		delete(s.open, id)
		s.free = append(s.free, sb)
	}
	s.mu.Unlock()
}

// skip ends one reader's use of a block it did not read.
func (s *RangeScan) skip(id blockID) {
	s.mu.Lock()
	if s.left[id]--; s.left[id] == 0 {
		if sb := s.open[id]; sb != nil {
			delete(s.open, id)
			s.free = append(s.free, sb)
		}
	}
	s.mu.Unlock()
}

// Retained returns the bytes of the decode buffers the scan held when Run
// ended.
func (s *RangeScan) Retained() int { return s.retained }

// recycle hands the decode buffers to later scans.
func (s *RangeScan) recycle() {
	for _, sb := range s.all {
		s.retained += sb.Keys.Retained()
		for i := range sb.columns {
			s.retained += sb.columns[i].Retained()
		}
		sharedBlockPool.Put(sb)
	}
	s.all, s.free = nil, nil
}

// Run calls fn once for every range from up to workers goroutines, zero
// meaning one per CPU, each with a block buffer of its own; a panic or memory
// fault in a worker fails the scan instead of the process. Blocks handed to
// readers are invalid once Run returns.
func (s *RangeScan) Run(ctx context.Context, workers int, fn func(worker int, r *KeyRange, b *part.Block) error) error {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	s.more = sync.NewCond(&s.mu)
	tasks := 0
	for i := range s.Ranges {
		s.queue = append(s.queue, &s.Ranges[i])
		tasks += max(1, s.Ranges[i].split)
	}
	workers = min(workers, tasks)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
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
					cancel(fmt.Errorf("colstore: range worker failed: %v", r))
				}
			}()
			b := blockPool.Get()
			defer blockPool.Put(b)
			for ctx.Err() == nil {
				r := s.take()
				if r == nil {
					return
				}
				var err error
				if r.split > 1 {
					err = s.cut(r, b)
				} else {
					err = fn(worker, r, b)
				}
				if err != nil {
					cancel(err)

					return
				}
			}
		}(w)
	}
	wg.Wait()
	s.recycle()

	return context.Cause(ctx)
}

// RangeReader reads the blocks of one stream of a key range in key order.
type RangeReader struct {
	// Block is the current block; From and To bound its rows inside the
	// range.
	Block    *SharedBlock
	From, To int

	scan    *RangeScan
	id      blockID
	lo, hi  []byte
	spans   []BlockSpan
	span    int
	next    int
	scratch []byte
}

// Reset points the reader at the start of a stream of a range of s.
func (r *RangeReader) Reset(s *RangeScan, kr *KeyRange, stream int) {
	r.drop()
	r.scan, r.lo, r.hi, r.spans, r.span = s, kr.Lo, kr.Hi, kr.Streams[stream], 0
	r.next = r.spans[0].First
}

func (r *RangeReader) drop() {
	if r.Block != nil {
		r.scan.release(r.id, r.Block)
		r.Block = nil
	}
}

// Peek ends the use of the current block and reports the next block of the
// stream without reading it: its part and number, and whether the part's
// index proves that every row of it is inside the range. It reports false at
// the end of the stream.
func (r *RangeReader) Peek() (p *part.Part, block int, whole, ok bool) {
	r.drop()
	for r.span < len(r.spans) {
		s := &r.spans[r.span]
		if r.next < s.End {
			whole = bytes.Compare(s.Part.FirstKey(r.next), r.lo) >= 0 && s.Part.BlockBelowKey(r.next, r.hi)

			return s.Part, r.next, whole, true
		}
		if r.span++; r.span < len(r.spans) {
			r.next = r.spans[r.span].First
		}
	}

	return nil, 0, false, false
}

// Shared returns how many leading sort key columns the first row of the
// block that Peek reports shares with the row before it in the stream, which
// can lie in another range.
func (r *RangeReader) Shared() int {
	s := &r.spans[r.span]
	if r.next > 0 {
		return s.Part.KeyShared(r.next)
	}
	if s.prev == nil {
		return 0
	}
	_, last := s.prev.KeyRange()

	return part.SharedKeyColumns(last, s.Part.FirstKey(0))
}

// Skip passes over the block that Peek reports without reading it.
func (r *RangeReader) Skip() {
	r.scan.skip(blockID{r.spans[r.span].Part, r.next})
	r.next++
}

// Read moves to the block that Peek reports, decoding it through b unless
// another reader has, and reports whether it has rows inside the range.
func (r *RangeReader) Read(b *part.Block) (bool, error) {
	s := &r.spans[r.span]
	id := blockID{s.Part, r.next}
	r.next++
	sb, err := r.scan.acquire(id, b)
	if err != nil {
		return false, err
	}
	r.Block, r.id, r.From, r.To = sb, id, 0, sb.Rows
	if bytes.Compare(s.Part.FirstKey(id.block), r.lo) < 0 {
		r.From = sb.Keys.Search(r.lo, &r.scratch)
	}
	if !s.Part.BlockBelowKey(id.block, r.hi) {
		r.To = sb.Keys.Search(r.hi, &r.scratch)
	}
	if r.From < r.To {
		return true, nil
	}
	r.drop()

	return false, nil
}

// Next moves to the next block of the stream that has rows inside the range
// and reports false at the end of the stream.
func (r *RangeReader) Next(b *part.Block) (bool, error) {
	for {
		if _, _, _, ok := r.Peek(); !ok {
			return false, nil
		}
		if ok, err := r.Read(b); ok || err != nil {
			return ok, err
		}
	}
}
