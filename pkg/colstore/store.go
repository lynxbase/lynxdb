package colstore

import (
	"bytes"
	"errors"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// StoreOptions configures a Store.
type StoreOptions struct {
	// SortKey orders the rows of every part; empty keeps arrival order,
	// and parts are then never merged.
	SortKey []string
	// TimeField is the path whose value becomes the row timestamp.
	TimeField string
	// BlockRows is the number of rows per block; zero means 16384.
	BlockRows int
	// FlushBytes is the input a writer buffers before it writes a run;
	// zero means 128 MiB.
	FlushBytes int
	// MergeFanIn is the number of runs of one tier that are merged into a
	// run of the next tier; zero means 8, a negative value never merges.
	MergeFanIn int
	// FlushInterval is the longest that rows taken by Append stay in
	// memory before they are written; zero means one second, a negative
	// value writes them only when a buffer is full or on Sync.
	FlushInterval time.Duration
	// MergeBlocks bounds the memory of merging: a range merge holds one
	// decoded block per run it reads, and the merges running at once hold
	// at most this many blocks together; zero means 96.
	MergeBlocks int
	// Partition is the span of time whose rows are kept and merged apart
	// from all others, so that a time range reads only the spans it meets
	// and old spans can be dropped whole; zero keeps one partition.
	Partition time.Duration
	// Part sets how parts are encoded; its ordering fields are overridden,
	// and a ValueSlack of zero means 50, a negative one none.
	Part part.Options
	// FlushLevel is the compression effort of the runs writers flush, which
	// merges replace by parts of Part.Level.
	FlushLevel part.Level
}

const (
	defaultStoreBlockRows = 16384
	defaultFlushBytes     = 128 << 20
	defaultMergeFanIn     = 8
	defaultFlushInterval  = time.Second
	// mergedPartRows is the least a part of a merged run holds. Range merges
	// that are narrower decode the blocks at their borders several times
	// over and scatter the rows over many files.
	mergedPartRows = 1 << 17
	// defaultMergeBlocks bounds the decoded blocks that all range merges
	// hold together, each several megabytes of column values.
	defaultMergeBlocks = 96
	// defaultValueSlack lets text that shrinks little take the codec that
	// scans read several times faster, for at most half again its size.
	defaultValueSlack = 50
)

// Store keeps a table in a directory, takes documents through writers that
// each turn their buffer into a sorted run, and merges runs in the
// background so that queries meet few of them.
type Store struct {
	dir  *Dir
	opts StoreOptions

	wake    chan struct{}
	done    chan struct{}
	stopped chan struct{}
	// cores bounds the range merges that run at once, over all merges, and
	// blocks the decoded blocks they hold.
	cores  chan struct{}
	blocks blockBudget

	// merging is held while merges run and while partitions are dropped,
	// so that neither removes a part the other reads.
	merging sync.Mutex

	// shards are the buffers Append fills, each behind its own lock.
	shards   []appendShard
	next     atomic.Uint32
	flushing chan struct{}

	// closed is set when Close begins; closing runs it once.
	closed   atomic.Bool
	closing  sync.Once
	closeErr error

	mu   sync.Mutex
	idle *sync.Cond
	// kicks counts requests to look for merges and seen the requests the
	// merger has answered by finding nothing left to do.
	kicks, seen uint64
	compact     bool
	err         error
	// over is set when the merger has stopped for good.
	over bool
}

// OpenStore opens the table stored in path and starts its merger.
func OpenStore(path string, opts StoreOptions) (*Store, error) {
	if opts.BlockRows <= 0 || opts.BlockRows > shred.MaxRows {
		opts.BlockRows = defaultStoreBlockRows
	}
	if opts.FlushBytes <= 0 {
		opts.FlushBytes = defaultFlushBytes
	}
	if opts.MergeFanIn == 0 {
		opts.MergeFanIn = defaultMergeFanIn
	}
	if opts.FlushInterval == 0 {
		opts.FlushInterval = defaultFlushInterval
	}
	if opts.MergeBlocks <= 0 {
		opts.MergeBlocks = defaultMergeBlocks
	}
	if opts.Part.ValueSlack == 0 {
		opts.Part.ValueSlack = defaultValueSlack
	}
	opts.Part.Ordered, opts.Part.SortKey = len(opts.SortKey) > 0, opts.SortKey
	dir, err := OpenDir(path)
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir: dir, opts: opts,
		wake: make(chan struct{}, 1), done: make(chan struct{}), stopped: make(chan struct{}),
		cores: make(chan struct{}, runtime.GOMAXPROCS(0)),
	}
	s.blocks.init(opts.MergeBlocks)
	s.idle = sync.NewCond(&s.mu)
	s.shards = make([]appendShard, runtime.GOMAXPROCS(0))
	s.flushing = make(chan struct{})
	go s.mergeLoop()
	go s.flushLoop()
	s.kick()

	return s, nil
}

// Table returns the table of live parts.
func (s *Store) Table() *Table { return s.dir.Table() }

// Lost is Dir.Lost for the store\x27s directory.
func (s *Store) Lost() []string { return s.dir.Lost() }

// ReleasePages is Dir.ReleasePages for the store's directory.
func (s *Store) ReleasePages() { s.dir.ReleasePages() }

// Close writes what Append buffered, stops the merger and closes the
// directory; writers must be closed first. Rows of an Append that returned
// nil are written, and from then on the store returns ErrClosed. Closing
// again returns what the first Close returned.
func (s *Store) Close() error {
	s.closing.Do(func() {
		s.closed.Store(true)
		close(s.done)
		<-s.flushing
		syncErr := s.flushShards(0)
		<-s.stopped
		s.mu.Lock()
		err := s.err
		s.mu.Unlock()
		s.closeErr = errors.Join(syncErr, err, s.dir.Close())
	})

	return s.closeErr
}

// appendShard is one buffer of Append; since is when its oldest unwritten
// row arrived.
type appendShard struct {
	mu    sync.Mutex
	w     *Writer
	since time.Time
}

// Append buffers the documents of lines, one per line, and returns the
// number of lines that are not JSON objects. It is safe for concurrent use;
// the rows become durable and visible within the flush interval, or as soon
// as a buffer is full, or on Sync.
func (s *Store) Append(lines []byte) (rejected int64, err error) {
	var sh *appendShard
	// The first free buffer in a fixed order, so that a light load fills
	// one buffer and not all of them.
	for i := range s.shards {
		if s.shards[i].mu.TryLock() {
			sh = &s.shards[i]

			break
		}
	}
	if sh == nil {
		sh = &s.shards[int(s.next.Add(1))%len(s.shards)]
		sh.mu.Lock()
	}
	defer sh.mu.Unlock()
	// Close flushes every buffer after it has set closed, so the rows of
	// a caller that got here before are written by it.
	if s.closed.Load() {
		return 0, ErrClosed
	}
	if sh.w == nil {
		sh.w = s.NewWriter()
	}
	before := sh.w.Rejected
	err = sh.w.add(lines)
	switch {
	case sh.w.buffered == 0:
		sh.since = time.Time{}
	case sh.since.IsZero():
		sh.since = time.Now()
	}

	return sh.w.Rejected - before, err
}

// Sync writes everything Append has buffered and returns once it is durable.
func (s *Store) Sync() error {
	if s.closed.Load() {
		return ErrClosed
	}

	return s.flushShards(0)
}

// flushShards writes the buffers whose oldest row is at least age old.
func (s *Store) flushShards(age time.Duration) error {
	errs := make([]error, len(s.shards))
	var wg sync.WaitGroup
	for i := range s.shards {
		sh := &s.shards[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			sh.mu.Lock()
			defer sh.mu.Unlock()
			if sh.w == nil || sh.w.buffered == 0 || time.Since(sh.since) < age {
				return
			}
			errs[i] = sh.w.flush()
			sh.since = time.Time{}
		}()
	}
	wg.Wait()

	return errors.Join(errs...)
}

// flushLoop writes buffers that have waited for the flush interval.
func (s *Store) flushLoop() {
	defer close(s.flushing)
	interval := s.opts.FlushInterval
	if interval < 0 {
		<-s.done

		return
	}
	tick := time.NewTicker(max(interval/4, time.Millisecond))
	defer tick.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-tick.C:
			if err := s.flushShards(interval); err != nil {
				s.mu.Lock()
				if s.err == nil {
					s.err = err
				}
				s.idle.Broadcast()
				s.mu.Unlock()
			}
		}
	}
}

// kick asks the merger to look for work.
func (s *Store) kick() {
	s.mu.Lock()
	s.kicks++
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Settle waits until no merge is due or running and returns the first error
// a merge or a timed flush met, or ErrClosed once the merger has stopped.
func (s *Store) Settle() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.seen != s.kicks && s.err == nil && !s.over {
		s.idle.Wait()
	}
	if s.err == nil && s.over {
		return ErrClosed
	}

	return s.err
}

// DropBefore removes every partition that ends at or before the time t, in
// unix nanoseconds, and returns the number of rows removed. A store that is
// not partitioned keeps everything.
func (s *Store) DropBefore(t int64) (uint64, error) {
	if s.closed.Load() {
		return 0, ErrClosed
	}
	width := int64(s.opts.Partition)
	if width <= 0 {
		return 0, nil
	}
	s.merging.Lock()
	defer s.merging.Unlock()
	snap := s.Table().Snapshot()
	defer snap.Release()
	var old []*Handle
	var rows uint64
	for _, h := range snap.Parts() {
		// A partition near the end of the time range has no end to compare.
		if end := (h.Partition + 1) * width; end/width == h.Partition+1 && end <= t {
			old = append(old, h)
			rows += h.Rows
		}
	}
	if len(old) == 0 {
		return 0, nil
	}
	if _, err := s.dir.Commit(nil, old); err != nil {
		return 0, err
	}

	return rows, nil
}

// Compact merges the runs of every partition into one and waits for it.
func (s *Store) Compact() error {
	s.mu.Lock()
	s.compact = true
	s.mu.Unlock()
	s.kick()

	return s.Settle()
}

func (s *Store) mergeLoop() {
	defer close(s.stopped)
	defer func() {
		s.mu.Lock()
		s.over = true
		s.idle.Broadcast()
		s.mu.Unlock()
	}()
	for {
		select {
		case <-s.done:
			return
		case <-s.wake:
		}
		for {
			s.mu.Lock()
			kicks, compact := s.kicks, s.compact
			s.mu.Unlock()
			merged, err := s.mergeOnce(compact)
			s.mu.Lock()
			if err != nil && s.err == nil {
				s.err = err
			}
			if !merged || err != nil {
				s.seen = kicks
				if s.kicks == kicks {
					s.compact = false
				}
				s.idle.Broadcast()
			}
			again := s.kicks != kicks && err == nil
			s.mu.Unlock()
			if !merged && !again || err != nil {
				break
			}
			select {
			case <-s.done:
				return
			default:
			}
		}
	}
}

// blockBudget hands out a fixed number of units to callers that wait until
// enough are free.
type blockBudget struct {
	mu    sync.Mutex
	freed *sync.Cond
	total int
	free  int
}

func (b *blockBudget) init(total int) {
	b.total, b.free = total, total
	b.freed = sync.NewCond(&b.mu)
}

// acquire takes n units, or the whole budget when n exceeds it.
func (b *blockBudget) acquire(n int) {
	n = min(n, b.total)
	b.mu.Lock()
	for b.free < n {
		b.freed.Wait()
	}
	b.free -= n
	b.mu.Unlock()
}

func (b *blockBudget) release(n int) {
	b.mu.Lock()
	b.free += min(n, b.total)
	b.mu.Unlock()
	b.freed.Broadcast()
}

// sortedRun is the parts of one run.
type sortedRun struct {
	id        uint64
	tier      int
	partition int64
	parts     []*Handle
}

// pickMerges chooses the sets of runs to merge next, never mixing
// partitions: all runs of a partition as one set when compacting, otherwise
// the runs of each tier in sets of fanIn, oldest first, leaving out what
// does not fill a set.
func pickMerges(parts []*Handle, fanIn int, compact bool) [][]sortedRun {
	index := map[uint64]int{}
	var runs []sortedRun
	for _, h := range parts {
		i, ok := index[h.Run]
		if !ok {
			i = len(runs)
			index[h.Run] = i
			runs = append(runs, sortedRun{id: h.Run, tier: h.Tier, partition: h.Partition})
		}
		runs[i].parts = append(runs[i].parts, h)
	}
	sort.Slice(runs, func(i, j int) bool {
		a, b := &runs[i], &runs[j]
		if a.partition != b.partition {
			return a.partition < b.partition
		}
		// Compaction takes a partition whatever the tiers of its runs.
		if a.tier != b.tier && !compact {
			return a.tier < b.tier
		}

		return a.id < b.id
	})
	if !compact && fanIn < 2 {
		return nil
	}
	var sets [][]sortedRun
	for first := 0; first < len(runs); {
		end := first
		for end < len(runs) && runs[end].partition == runs[first].partition && (compact || runs[end].tier == runs[first].tier) {
			end++
		}
		if compact {
			if end-first > 1 {
				sets = append(sets, runs[first:end])
			}
		} else {
			for ; end-first >= fanIn; first += fanIn {
				sets = append(sets, runs[first:first+fanIn])
			}
		}
		first = end
	}

	return sets
}

// mergeOnce runs the merges that are due, side by side, and reports whether
// there were any.
func (s *Store) mergeOnce(compact bool) (bool, error) {
	if !s.opts.Part.Ordered {
		return false, nil
	}
	s.merging.Lock()
	defer s.merging.Unlock()
	snap := s.Table().Snapshot()
	defer snap.Release()
	sets := pickMerges(snap.Parts(), s.opts.MergeFanIn, compact)
	errs := make([]error, len(sets))
	var wg sync.WaitGroup
	for i, runs := range sets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = s.mergeRuns(runs)
		}()
	}
	wg.Wait()

	return len(sets) > 0, errors.Join(errs...)
}

// mergeRuns replaces runs by one run of the tier above the highest of them,
// cut into parts by key range that are merged side by side.
func (s *Store) mergeRuns(runs []sortedRun) error {
	var inputs []*Handle
	var srcs []*part.Part
	var rows uint64
	tier := 0
	for _, r := range runs {
		tier = max(tier, r.tier)
		for _, h := range r.parts {
			inputs = append(inputs, h)
			srcs = append(srcs, h.Part)
			rows += h.Rows
		}
	}
	cores := runtime.GOMAXPROCS(0)
	cuts := part.SplitKeys(srcs, int(min(uint64(cores), max(1, rows/mergedPartRows))))
	meta := PartMeta{Run: s.dir.NewRun(), Tier: tier + 1, Partition: runs[0].partition}
	files := make([]*PartFile, len(cuts)+1)
	errs := make([]error, len(files))
	var wg sync.WaitGroup
	for i := range files {
		wg.Add(1)
		// Each range merge holds one decoded block per run.
		s.blocks.acquire(len(runs))
		s.cores <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-s.cores; s.blocks.release(len(runs)) }()
			var lo, hi []byte
			if i > 0 {
				lo = cuts[i-1]
			}
			if i < len(cuts) {
				hi = cuts[i]
			}
			pf, err := s.dir.Create()
			if err != nil {
				errs[i] = err

				return
			}
			pf.Meta = meta
			files[i] = pf
			w := part.NewWriter(pf, s.opts.Part)
			if err := part.MergeRange(w, srcs, s.opts.BlockRows, lo, hi); err != nil {
				errs[i] = err

				return
			}
			errs[i] = w.Close()
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		for _, pf := range files {
			if pf != nil {
				pf.Abort()
			}
		}

		return err
	}
	_, err := s.dir.Commit(files, inputs)

	return err
}

// Writer buffers documents and writes them to the store as sorted runs. It
// is not safe for concurrent use; a store takes any number of writers.
type Writer struct {
	// Rejected counts the lines that were not JSON objects.
	Rejected int64

	s        *Store
	batch    *shred.Batch
	part     *part.Writer // reused from run to run for its buffers
	buffered int          // input bytes in batch
	buckets  []shred.TimeBucket
	errs     []shred.LineError
}

// NewWriter returns a writer with an empty buffer.
func (s *Store) NewWriter() *Writer {
	return &Writer{
		s:     s,
		batch: shred.NewBatch(shred.Options{TimeField: s.opts.TimeField}),
	}
}

// AddNDJSON buffers the documents of lines, one per line, and writes a run
// each time the buffer has grown to the store's flush size.
func (w *Writer) AddNDJSON(lines []byte) error {
	if w.s.closed.Load() {
		return ErrClosed
	}

	return w.add(lines)
}

func (w *Writer) add(lines []byte) error {
	for len(lines) > 0 {
		chunk := lines
		if room := w.s.opts.FlushBytes - w.buffered; room < len(chunk) {
			// The buffer takes whole lines, so the cut moves to a line end.
			if nl := bytes.IndexByte(chunk[max(room, 0):], '\n'); nl >= 0 {
				chunk = chunk[:max(room, 0)+nl+1]
			}
		}
		lines = lines[len(chunk):]
		w.errs = w.batch.AddNDJSON(chunk, 0, w.errs[:0])
		w.Rejected += int64(len(w.errs))
		if w.buffered += len(chunk); w.buffered >= w.s.opts.FlushBytes {
			if err := w.flush(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Flush writes the buffered rows as one run per partition they fall in;
// when it returns nil they are durable and visible to new snapshots.
func (w *Writer) Flush() error {
	if w.batch.Rows > 0 && w.s.closed.Load() {
		return ErrClosed
	}

	return w.flush()
}

func (w *Writer) flush() error {
	if w.batch.Rows == 0 {
		return nil
	}
	s := w.s
	w.buckets = w.buckets[:0]
	switch {
	case s.opts.Partition > 0:
		w.buckets = w.batch.SortByTimeBuckets(w.buckets, int64(s.opts.Partition), s.opts.SortKey)
	case s.opts.Part.Ordered:
		w.batch.SortBy(s.opts.SortKey)
	}
	if len(w.buckets) == 0 {
		w.buckets = append(w.buckets, shred.TimeBucket{Rows: w.batch.Rows})
	}
	flushed := s.opts.Part
	flushed.Level = s.opts.FlushLevel
	// The buffer that blocks are cut into is needed only during a flush,
	// so writers share a few of them instead of holding one each.
	block := shred.Get(shred.Options{})
	defer shred.Put(block)
	files := make([]*PartFile, 0, len(w.buckets))
	abort := func(err error) error {
		for _, pf := range files {
			pf.Abort()
		}

		return err
	}
	from := 0
	for _, bucket := range w.buckets {
		pf, err := s.dir.Create()
		if err != nil {
			return abort(err)
		}
		pf.Meta.Partition = bucket.ID
		files = append(files, pf)
		pw := w.part
		if pw == nil {
			pw = part.NewWriter(pf, flushed)
			w.part = pw
		} else {
			pw.Reset(pf, flushed)
		}
		for end := from + bucket.Rows; from < end; from += min(s.opts.BlockRows, end-from) {
			block.Reset()
			w.batch.CopyRows(block, from, min(from+s.opts.BlockRows, end))
			if err := pw.WriteBatch(block); err != nil {
				return abort(err)
			}
		}
		if err := pw.Close(); err != nil {
			return abort(err)
		}
	}
	if _, err := s.dir.Commit(files, nil); err != nil {
		return err
	}
	w.batch.Reset()
	w.buffered = 0
	s.kick()

	return nil
}

// Close flushes the buffer.
func (w *Writer) Close() error { return w.Flush() }
