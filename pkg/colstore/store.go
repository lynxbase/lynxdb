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
	// SortKey orders the rows of every run, rows equal in it by their
	// time; empty orders them by time alone.
	SortKey []string
	// TimeField is the path whose value becomes the row timestamp, and
	// TimeFields further paths tried in their order for a row without one
	// there; shred.Options says which values are times.
	TimeField  string
	TimeFields []string
	// KeepText keeps a line that is not a JSON object as a row holding its
	// text; without it such a line is rejected.
	KeepText bool
	// BlockRows is the number of rows per block; zero means 16384.
	BlockRows int
	// BufferBytes is the input that the buffers of Append hold together
	// before they are written as one run; zero means eight times
	// FlushBytes. While a run is written Append takes as much again before
	// it waits, and the run's rows are copied once into key ranges, so the
	// buffers take at most about three times this input in memory.
	BufferBytes int
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
	// MergeBacklog makes merges give way to the rows that wait in Append's
	// buffers and to their flushes, which the callers of Append and Sync
	// wait for, while fewer runs than this are due to be merged; zero
	// means 64, and a negative value lets both share the disk.
	MergeBacklog int
	// Partition is the span of time whose rows are kept and merged apart
	// from all others, so that a time range reads only the spans it meets
	// and old spans can be dropped whole; zero keeps one partition. A span
	// that ended an hour ago, or its own length ago when that is less, is
	// merged into one run, and again once rows that came later make up an
	// eighth of it.
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
	// defaultMergeBacklog lets a burst of rows be written before the runs
	// it leaves are merged, and bounds the runs that queries then meet.
	defaultMergeBacklog = 64
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

	// dropping is held while partitions are dropped and while the
	// directory closes, one at a time. merges counts the merges under way
	// for the merger to wait for when it stops.
	dropping sync.Mutex
	merges   sync.WaitGroup

	// shards are the buffers Append fills, each behind its own lock.
	shards   []appendShard
	next     atomic.Uint32
	flushing chan struct{}
	// appending is held shared by every call of Append while it runs.
	appending sync.RWMutex
	// flushWrites counts the flushes of the buffers that are writing, and
	// due the runs in the table that merges are to take.
	flushWrites atomic.Int32
	due         atomic.Int32
	// full asks the flusher to write buffers that hold their budget. room
	// tells the callers of Append that wait for it that the buffers were
	// taken or that a flush ended; flushErr is what the last flush of the
	// buffers returned.
	full     chan struct{}
	roomMu   sync.Mutex
	room     *sync.Cond
	flushErr error
	// flushMu lets one flush of the buffers run at a time. buffered counts
	// the input bytes in the buffers and in unwritten, the batches that a
	// failed flush took and the next one writes first.
	flushMu        sync.Mutex
	buffered       atomic.Int64
	unwritten      []*shred.Batch
	unwrittenBytes int64

	// Callers of Sync share flushes: one flush runs at a time, and all who
	// asked while it ran are served by the next. syncBegun and syncEnded
	// count the flushes begun and ended, syncErr is the result of the last.
	syncMu               sync.Mutex
	synced               *sync.Cond
	syncing              bool
	syncBegun, syncEnded uint64
	syncErr              error

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
	// busy holds the runs that the merges under way read, with the
	// partition of each, and running is the number of those merges;
	// settling is set while one of them merges a closed partition into one
	// run.
	busy     map[uint64]int64
	running  int
	settling bool
	// drops is set while the partitions that end at or before dropTo are
	// being dropped; merges leave them alone.
	drops  bool
	dropTo int64
}

// OpenStore opens the table stored in path and starts its merger.
func OpenStore(path string, opts StoreOptions) (*Store, error) {
	if opts.BlockRows <= 0 || opts.BlockRows > shred.MaxRows {
		opts.BlockRows = defaultStoreBlockRows
	}
	if opts.FlushBytes <= 0 {
		opts.FlushBytes = defaultFlushBytes
	}
	if opts.BufferBytes <= 0 {
		opts.BufferBytes = 8 * opts.FlushBytes
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
	if opts.MergeBacklog == 0 {
		opts.MergeBacklog = defaultMergeBacklog
	}
	if opts.Part.ValueSlack == 0 {
		opts.Part.ValueSlack = defaultValueSlack
	}
	opts.Part.Ordered, opts.Part.SortKey = true, opts.SortKey
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
	s.busy = map[uint64]int64{}
	s.shards = make([]appendShard, runtime.GOMAXPROCS(0))
	s.countRuns()
	s.synced = sync.NewCond(&s.syncMu)
	s.room = sync.NewCond(&s.roomMu)
	s.full = make(chan struct{}, 1)
	s.flushing = make(chan struct{})
	go s.mergeLoop()
	go s.flushLoop()
	s.kick()

	return s, nil
}

// Table returns the table of live parts.
func (s *Store) Table() *Table { return s.dir.Table() }

// Lost is Dir.Lost for the store's directory.
func (s *Store) Lost() []string { return s.dir.Lost() }

// Written is Dir.Written for the store's directory.
func (s *Store) Written() (flushed, merged int64) { return s.dir.Written() }

// ReleasePages is Dir.ReleasePages for the store's directory.
func (s *Store) ReleasePages() { s.dir.ReleasePages() }

// Close writes what Append buffered, stops the merger and closes the
// directory; writers must be closed first. Rows of an Append that returned
// nil are written, and from then on the store returns ErrClosed. Closing
// again returns what the first Close returned.
func (s *Store) Close() error {
	s.closing.Do(func() {
		s.closed.Store(true)
		s.appending.Lock()
		//nolint:staticcheck // The lock is taken only to wait for the calls of Append under way.
		s.appending.Unlock()
		close(s.done)
		<-s.flushing
		syncErr := s.flush(0)
		<-s.stopped
		s.mu.Lock()
		err := s.err
		s.mu.Unlock()
		s.dropping.Lock()
		s.closeErr = errors.Join(syncErr, err, s.dir.Close())
		s.dropping.Unlock()
	})

	return s.closeErr
}

// appendShard is one buffer of Append; since is when its oldest unwritten
// row arrived.
type appendShard struct {
	mu       sync.Mutex
	batch    *shred.Batch
	buffered int // input bytes in batch
	since    time.Time
	errs     []shred.LineError
}

// appendPiece is the most input Append shreds before it looks at the
// budget of the buffers again.
const appendPiece = 4 << 20

// Append buffers the documents of lines, one per line, and returns the
// number of lines that are not JSON objects. It is safe for concurrent use;
// the rows become durable and visible within the flush interval, or as soon
// as the buffers are full, or on Sync.
func (s *Store) Append(lines []byte) (rejected int64, err error) {
	return s.AppendAt(lines, 0)
}

// AppendAt is Append for lines that arrived at the time at, in unix
// nanoseconds, which becomes the time of every row that carries none.
func (s *Store) AppendAt(lines []byte, at int64) (rejected int64, err error) {
	// Close waits for the calls under way, so that it writes all the rows
	// of a call or refuses the call whole.
	s.appending.RLock()
	defer s.appending.RUnlock()
	if s.closed.Load() {
		return 0, ErrClosed
	}
	for len(lines) > 0 {
		// Full buffers, or rows that a failed flush left, are written
		// before more lines are taken, so that a caller told of a failure
		// can send all again.
		room := int64(s.opts.BufferBytes) - s.buffered.Load()
		if room <= 0 {
			if err := s.makeRoom(); err != nil {
				return rejected, err
			}

			continue
		}
		piece := lines
		if at := int(min(room, appendPiece)); len(piece) > at {
			// The buffers take whole lines, so the cut moves to a line end.
			if nl := bytes.IndexByte(piece[at:], '\n'); nl >= 0 {
				piece = piece[:at+nl+1]
			}
		}
		n, err := s.appendPiece(piece, at)
		rejected += n
		if err != nil {
			return rejected, err
		}
		lines = lines[len(piece):]
	}
	// Full buffers are written by the flusher; the caller does not wait.
	if s.buffered.Load() >= int64(s.opts.BufferBytes) {
		s.askFlush()
	}

	return rejected, nil
}

// askFlush has the flusher write the buffers that are full.
func (s *Store) askFlush() {
	select {
	case s.full <- struct{}{}:
	default:
	}
}

// makeRoom waits until the flusher has taken the full buffers, which it
// does before it writes them. Rows that a flush could not write fill the
// buffers until one can: no more are taken then, and the caller is told so
// that it can send its lines again.
func (s *Store) makeRoom() error {
	s.roomMu.Lock()
	defer s.roomMu.Unlock()
	for s.buffered.Load() >= int64(s.opts.BufferBytes) {
		s.askFlush()
		if s.flushErr != nil {
			return s.flushErr
		}
		s.room.Wait()
	}

	return nil
}

func (s *Store) appendPiece(lines []byte, at int64) (rejected int64, err error) {
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
	if sh.batch == nil {
		sh.batch = shred.Get(s.shredOptions())
	}
	sh.errs = sh.batch.AddNDJSON(lines, at, sh.errs[:0])
	if sh.buffered == 0 {
		sh.since = time.Now()
	}
	sh.buffered += len(lines)
	s.buffered.Add(int64(len(lines)))

	return int64(len(sh.errs)), nil
}

func (s *Store) shredOptions() shred.Options {
	return shred.Options{TimeField: s.opts.TimeField, TimeFields: s.opts.TimeFields, KeepText: s.opts.KeepText}
}

// Sync writes everything Append has buffered and returns once it is durable.
// Concurrent callers share a flush, so many small synchronous writes cost
// one part and one wait for the disk per flush, not one per caller.
func (s *Store) Sync() error {
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	// A flush under way may have passed a buffer before this caller's rows
	// reached it, so only one that begins from here on vouches for them.
	want := s.syncBegun + 1
	for s.syncEnded < want {
		if s.closed.Load() {
			return ErrClosed
		}
		if s.syncing {
			s.synced.Wait()

			continue
		}
		s.syncing = true
		s.syncBegun++
		flush := s.syncBegun
		s.syncMu.Unlock()
		err := s.flush(0)
		s.syncMu.Lock()
		s.syncing, s.syncEnded, s.syncErr = false, flush, err
		s.synced.Broadcast()
	}
	// A later flush that succeeded has written what an earlier one left.
	return s.syncErr
}

// flushFull writes the buffers when they hold their budget; a caller that
// waited for another flush may find them written.
func (s *Store) flushFull() error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	if s.buffered.Load() < int64(s.opts.BufferBytes) {
		return nil
	}

	return s.writeBuffers(0)
}

// flush writes the rows that Append has buffered when one of them has
// waited for age, and returns once they are durable.
func (s *Store) flush(age time.Duration) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	return s.writeBuffers(age)
}

// writeBuffers takes the batch of every buffer and writes them all as one
// run per partition, committed in one manifest record. The buffers are free
// for Append while the run is written. The caller holds flushMu.
func (s *Store) writeBuffers(age time.Duration) error {
	if age > 0 {
		due := len(s.unwritten) > 0
		for i := range s.shards {
			sh := &s.shards[i]
			sh.mu.Lock()
			due = due || (sh.buffered > 0 && time.Since(sh.since) >= age)
			sh.mu.Unlock()
		}
		if !due {
			return nil
		}
	}
	taken, size := s.unwritten, s.unwrittenBytes
	s.unwritten, s.unwrittenBytes = nil, 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		if sh.buffered > 0 {
			size += int64(sh.buffered)
			if sh.batch.Rows > 0 {
				taken = append(taken, sh.batch)
				sh.batch = nil
			}
			sh.buffered, sh.since = 0, time.Time{}
		}
		sh.mu.Unlock()
	}
	// The buffers are free again at once, unless the flush before failed:
	// then they count as full until these rows are written, or every
	// failed attempt would let in as much again.
	s.roomMu.Lock()
	again := s.flushErr != nil
	if !again {
		s.buffered.Add(-size)
		s.room.Broadcast()
	}
	s.roomMu.Unlock()
	var err error
	if len(taken) > 0 {
		s.flushWrites.Add(1)
		var files []*PartFile
		if files, err = s.writeRuns(taken); err == nil {
			_, err = s.dir.Commit(files, nil)
		}
		s.flushWrites.Add(-1)
		s.countRuns()
	}
	s.roomMu.Lock()
	switch {
	case err != nil && !again:
		s.buffered.Add(size)
	case err == nil && again:
		s.buffered.Add(-size)
	}
	s.flushErr = err
	s.room.Broadcast()
	s.roomMu.Unlock()
	if err != nil {
		s.unwritten, s.unwrittenBytes = taken, size

		return err
	}
	for _, b := range taken {
		shred.Put(b)
	}
	if len(taken) > 0 {
		s.kick()
	}

	return nil
}

// flushLoop writes the buffers that are full, and those whose rows have
// waited for the flush interval.
func (s *Store) flushLoop() {
	defer close(s.flushing)
	interval := s.opts.FlushInterval
	var tick <-chan time.Time
	if interval >= 0 {
		ticker := time.NewTicker(max(interval/4, time.Millisecond))
		defer ticker.Stop()
		tick = ticker.C
	}
	looked := time.Now()
	for {
		select {
		case <-s.done:
			return
		case <-s.full:
			// Whoever waits for room learns of a failure from the buffers.
			_ = s.flushFull()
		case <-tick:
			// Partitions close as time passes, with or without new rows.
			if s.opts.Partition > 0 && time.Since(looked) >= closedLook {
				looked = time.Now()
				s.kick()
			}
			if err := s.flush(interval); err != nil {
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
	s.dropping.Lock()
	defer s.dropping.Unlock()
	// Close closes the directory under the same lock, so a store found
	// open here stays open until the partitions are dropped.
	if s.closed.Load() {
		return 0, ErrClosed
	}
	if s.opts.Partition <= 0 {
		return 0, nil
	}
	// Merges start on none of the partitions from here on, and those that
	// read one of them end first: what they write would bring its rows
	// back.
	s.mu.Lock()
	s.drops, s.dropTo = true, t
	for s.mergesBefore(t) {
		s.idle.Wait()
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.drops = false
		s.mu.Unlock()
	}()
	snap := s.Table().Snapshot()
	defer snap.Release()
	var old []*Handle
	var rows uint64
	for _, h := range snap.Parts() {
		if s.endsBy(h.Partition, t) {
			old = append(old, h)
			rows += h.Rows
		}
	}
	if len(old) == 0 {
		return 0, nil
	}
	defer s.countRuns()
	if _, err := s.dir.Commit(nil, old); err != nil {
		return 0, err
	}

	return rows, nil
}

// endsBy reports whether partition p ends at or before the time t; a
// partition near the end of the time range has no end to compare.
func (s *Store) endsBy(p, t int64) bool {
	width := int64(s.opts.Partition)
	end := (p + 1) * width

	return width > 0 && end/width == p+1 && end <= t
}

// mergesBefore reports whether a merge under way reads a partition that
// ends at or before t; the caller holds the lock of the merger's state.
func (s *Store) mergesBefore(t int64) bool {
	for _, p := range s.busy {
		if s.endsBy(p, t) {
			return true
		}
	}

	return false
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
		s.merges.Wait()
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
			started := s.startMerges(compact)
			s.mu.Lock()
			// A merge that ends asks for another look, so the merger is
			// idle only when it found nothing to start and none runs.
			if (!started && s.running == 0) || s.err != nil {
				s.seen = kicks
				if s.kicks == kicks {
					s.compact = false
				}
				s.idle.Broadcast()
			}
			again := s.kicks != kicks && s.err == nil
			s.mu.Unlock()
			if !again {
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

// beforeMergeCommit, when set, runs before a merge commits its run.
var beforeMergeCommit func(meta PartMeta)

// timeNow is the clock by which partitions close.
var timeNow = time.Now

// closedLook is how often a store with partitions looks for ones that have
// closed while no rows arrived.
const closedLook = time.Minute

// closedSet returns all the runs of the earliest partition that has closed,
// that neither a merge under way nor a set of taken reads and of which no
// single run holds seven eighths of the rows. One such partition is merged
// at a time.
func (s *Store) closedSet(free []*Handle, taken [][]sortedRun) [][]sortedRun {
	width := int64(s.opts.Partition)
	if width <= 0 || s.settling || s.opts.MergeFanIn < 2 {
		return nil
	}
	now, grace := timeNow().UnixNano(), min(width, int64(time.Hour))
	skip := map[int64]bool{}
	for _, set := range taken {
		skip[set[0].partition] = true
	}
	rows, most := map[int64]uint64{}, map[uint64]uint64{}
	var closed []*Handle
	for _, h := range free {
		if skip[h.Partition] || !s.endsBy(h.Partition, now-grace) {
			continue
		}
		closed = append(closed, h)
		rows[h.Partition] += h.Rows
		most[h.Run] += h.Rows
	}
	settled := map[int64]bool{}
	for _, h := range closed {
		if most[h.Run]*8 >= rows[h.Partition]*7 {
			settled[h.Partition] = true
		}
	}
	var open []*Handle
	for _, h := range closed {
		if !settled[h.Partition] {
			open = append(open, h)
		}
	}

	if sets := pickMerges(open, s.opts.MergeFanIn, true); len(sets) > 0 {
		return sets[:1]
	}

	return nil
}

// startMerges starts the merges that are due among the runs that no merge
// under way reads, each on its own, and reports whether there were any. A
// merge that ends asks the merger to look again.
func (s *Store) startMerges(compact bool) bool {
	s.mu.Lock()
	// A merge lets go of its runs once it has committed, so the table as it
	// is under the lock holds no part that a merge has replaced and that
	// counts as free.
	snap := s.Table().Snapshot()
	defer snap.Release()
	// Compaction takes a partition whole, so it leaves a partition alone
	// while a merge reads one of its runs.
	held := map[int64]bool{}
	for _, h := range snap.Parts() {
		if _, busy := s.busy[h.Run]; busy && compact {
			held[h.Partition] = true
		}
	}
	free := make([]*Handle, 0, len(snap.Parts()))
	for _, h := range snap.Parts() {
		_, busy := s.busy[h.Run]
		if dropped := s.drops && s.endsBy(h.Partition, s.dropTo); !busy && !held[h.Partition] && !dropped {
			free = append(free, h)
		}
	}
	sets := pickMerges(free, s.opts.MergeFanIn, compact)
	// settles is the set that merges a closed partition, if there is one.
	settles := -1
	if !compact {
		// A partition some of whose runs a merge reads waits for it.
		whole := free[:0:0]
		partial := map[int64]bool{}
		for _, h := range snap.Parts() {
			if _, busy := s.busy[h.Run]; busy {
				partial[h.Partition] = true
			}
		}
		for _, h := range free {
			if !partial[h.Partition] {
				whole = append(whole, h)
			}
		}
		if closed := s.closedSet(whole, sets); len(closed) > 0 {
			sets, settles = append(sets, closed...), len(sets)
			s.settling = true
		}
	}
	for _, runs := range sets {
		for _, r := range runs {
			s.busy[r.id] = r.partition
		}
	}
	s.running += len(sets)
	s.mu.Unlock()
	for i, runs := range sets {
		// A merge keeps the parts it reads; the others go as soon as the
		// merges that replace them commit.
		var parts []*Handle
		for _, r := range runs {
			parts = append(parts, r.parts...)
		}
		kept := keep(parts)
		s.merges.Add(1)
		go func() {
			defer s.merges.Done()
			err := s.mergeRuns(runs)
			kept.Release()
			s.mu.Lock()
			for _, r := range runs {
				delete(s.busy, r.id)
			}
			s.running--
			if i == settles {
				s.settling = false
			}
			if err != nil && s.err == nil {
				s.err = err
			}
			// A drop may wait for this merge.
			s.idle.Broadcast()
			s.mu.Unlock()
			s.kick()
		}()
	}

	return len(sets) > 0
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
			if s.opts.MergeBacklog > 0 {
				pf.BeforeWrite(s.giveWay)
			}
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
	if beforeMergeCommit != nil {
		beforeMergeCommit(meta)
	}
	_, err := s.dir.Commit(files, inputs)
	s.countRuns()

	return err
}

// countRuns notes the number of runs in the table that merges are to take.
func (s *Store) countRuns() {
	snap := s.dir.Table().Snapshot()
	defer snap.Release()
	due := 0
	for _, set := range pickMerges(snap.Parts(), s.opts.MergeFanIn, false) {
		due += len(set)
	}
	s.due.Store(int32(due))
}

// pressingBytes is the input in Append's buffers from which a flush is
// near enough for merges to leave it the disk.
const pressingBytes = 16 << 20

// giveWay holds a merge back while rows wait in the buffers for a flush or
// a flush is writing them, and fewer runs are due to be merged than the
// backlog that merges may not let grow.
func (s *Store) giveWay() {
	for (s.flushWrites.Load() > 0 || s.buffered.Load() >= pressingBytes) && int(s.due.Load()) < s.opts.MergeBacklog && !s.closed.Load() {
		time.Sleep(2 * time.Millisecond)
	}
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
		batch: shred.NewBatch(s.shredOptions()),
	}
}

// AddNDJSON buffers the documents of lines, one per line, and writes a run
// each time the buffer has grown to the store's flush size.
func (w *Writer) AddNDJSON(lines []byte) error { return w.AddNDJSONAt(lines, 0) }

// AddNDJSONAt is AddNDJSON for lines that arrived at the time at, in unix
// nanoseconds, which becomes the time of every row that carries none.
func (w *Writer) AddNDJSONAt(lines []byte, at int64) error {
	if w.s.closed.Load() {
		return ErrClosed
	}

	return w.add(lines, at)
}

func (w *Writer) add(lines []byte, at int64) error {
	for len(lines) > 0 {
		// A buffer that a failed flush left full takes no line before it is
		// written, so that a caller told of the failure can send all again.
		if w.buffered >= w.s.opts.FlushBytes {
			if err := w.flush(); err != nil {
				return err
			}
		}
		chunk := lines
		if room := w.s.opts.FlushBytes - w.buffered; room < len(chunk) {
			// The buffer takes whole lines, so the cut moves to a line end.
			if nl := bytes.IndexByte(chunk[max(room, 0):], '\n'); nl >= 0 {
				chunk = chunk[:max(room, 0)+nl+1]
			}
		}
		lines = lines[len(chunk):]
		w.errs = w.batch.AddNDJSON(chunk, at, w.errs[:0])
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
	files, err := w.write()
	if err != nil || len(files) == 0 {
		return err
	}
	if _, err := w.s.dir.Commit(files, nil); err != nil {
		return err
	}
	w.s.countRuns()
	w.committed()
	w.s.kick()

	return nil
}

// write writes the buffered rows as one run per partition they fall in and
// returns the files, which the caller commits; the rows stay buffered until
// committed is called.
func (w *Writer) write() ([]*PartFile, error) {
	if w.batch.Rows == 0 {
		return nil, nil
	}
	s := w.s
	w.buckets = w.buckets[:0]
	if s.opts.Partition > 0 {
		w.buckets = w.batch.SortByTimeBuckets(w.buckets, int64(s.opts.Partition), s.opts.SortKey)
	} else {
		w.batch.SortBy(s.opts.SortKey)
		w.buckets = append(w.buckets, shred.TimeBucket{Rows: w.batch.Rows})
	}
	flushed := s.opts.Part
	flushed.Level = s.opts.FlushLevel
	// The buffer that blocks are cut into is needed only during a flush,
	// so writers share a few of them instead of holding one each.
	block := shred.Get(shred.Options{})
	defer shred.Put(block)
	files := make([]*PartFile, 0, len(w.buckets))
	abort := func(err error) ([]*PartFile, error) {
		for _, pf := range files {
			pf.Abort()
		}

		return nil, err
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

	return files, nil
}

// committed empties the buffer once the files written from it are durable.
func (w *Writer) committed() {
	w.batch.Reset()
	w.buffered = 0
}

// Close flushes the buffer.
func (w *Writer) Close() error { return w.Flush() }
