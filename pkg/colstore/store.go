package colstore

import (
	"bytes"
	"errors"
	"runtime"
	"slices"
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
	// FlushBytes. While runs are written Append takes as much again, theirs
	// and the buffers' together, before it waits, and a second run is
	// written beside the first once the buffers fill again. The rows of a
	// run are copied once into key ranges, so the buffers take at most
	// about three times this input in memory.
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
	// at most this many blocks together; zero means 64 per core and at
	// least 96, so that a whole merge of up to 64 runs keeps every core busy.
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
	// Part sets how parts are encoded; its ordering fields are overridden.
	// Text is stored at its smallest unless ValueSlack is set: on a volume
	// bound by throughput the bytes decide how long a scan of cold text
	// and a load take.
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
	// mergeBlocksPerCore and leastMergeBlocks set the default bound of the
	// decoded blocks that all range merges hold together, each several
	// megabytes of column values.
	mergeBlocksPerCore = 64
	leastMergeBlocks   = 96
	// defaultMergeBacklog lets a burst of rows be written before the runs
	// it leaves are merged, and bounds the runs that queries then meet.
	defaultMergeBacklog = 64
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
	// full asks the flusher to write buffers that hold their budget.
	// roomWait tells the callers of Append that wait for room that the
	// buffers were taken or that a flush ended; flushErr is the error of the
	// first flush of the buffers whose rows are not durable.
	full     chan struct{}
	roomMu   sync.Mutex
	roomWait *sync.Cond
	flushErr error
	// spare holds the batches of written buffers for the buffers to take
	// again, joined the batches that the rows of a part are brought
	// together in, and cut those that the blocks of a part are cut into.
	spare, joined, cut spareBatches
	// Two flushes of the buffers write at once, one sorting and encoding
	// while the other waits for the disk; slots holds their turns, and
	// launched counts those the flusher started. takeMu orders them: each
	// takes the batches of the buffers and of unwritten, those that failed
	// flushes left for the next one, under it and gets the next number.
	// buffered counts the input bytes in the buffers and in unwritten, and
	// inflight those that flushes write.
	slots          chan struct{}
	launched       atomic.Int32
	loopFlushes    sync.WaitGroup
	takeMu         sync.Mutex
	flushSeq       uint64
	unwritten      []*shred.Batch
	unwrittenBytes int64
	unwrittenSeqs  []uint64
	buffered       atomic.Int64
	inflight       atomic.Int64
	// pending holds the flushes, by number, from the first whose rows are
	// not durable yet; pendingMu guards it and ended tells of a flush that
	// ended.
	pendingMu sync.Mutex
	ended     *sync.Cond
	pending   []*flushRecord

	// appended counts the calls of Append that buffered rows, and written
	// is the count that the flushes up to the last one durable together
	// with all before it found: while the two are equal a Sync has nothing
	// to wait for.
	appended, written atomic.Uint64

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
	// arrived holds, by partition, when rows were last flushed to it.
	arrived map[int64]int64
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
		opts.MergeBlocks = max(leastMergeBlocks, mergeBlocksPerCore*runtime.GOMAXPROCS(0))
	}
	if opts.MergeBacklog == 0 {
		opts.MergeBacklog = defaultMergeBacklog
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
	s.arrived = map[int64]int64{}
	s.shards = make([]appendShard, runtime.GOMAXPROCS(0))
	s.countRuns()
	s.synced = sync.NewCond(&s.syncMu)
	s.roomWait = sync.NewCond(&s.roomMu)
	s.ended = sync.NewCond(&s.pendingMu)
	s.slots = make(chan struct{}, flushSlots)
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
// nil are written, and from then on the store returns ErrClosed. Merges under
// way give up their output and leave their runs as they were. Closing again
// returns what the first Close returned.
func (s *Store) Close() error {
	s.closing.Do(func() {
		s.closed.Store(true)
		s.blocks.close()
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
// row arrived, and sizes what the columns of the last batch written from it
// held, for the next to reserve.
type appendShard struct {
	mu       sync.Mutex
	batch    *shred.Batch
	buffered int // input bytes in batch
	since    time.Time
	errs     []shred.LineError
	sizes    []shred.ColumnSize
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
	_, rejected, err = s.AppendRows(lines, at)

	return rejected, err
}

// AppendRows is AppendAt that also returns the number of rows the lines
// became, which with the rejected lines makes every line that holds more
// than blanks.
func (s *Store) AppendRows(lines []byte, at int64) (rows, rejected int64, err error) {
	// Close waits for the calls under way, so that it writes all the rows
	// of a call or refuses the call whole.
	s.appending.RLock()
	defer s.appending.RUnlock()
	if s.closed.Load() {
		return 0, 0, ErrClosed
	}
	for len(lines) > 0 {
		// Full buffers, or rows that a failed flush left, are written
		// before more lines are taken, so that a caller told of a failure
		// can send all again.
		room := s.room()
		if room <= 0 {
			if err := s.makeRoom(); err != nil {
				return rows, rejected, err
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
		taken, n := s.appendPiece(piece, at)
		rows += taken
		rejected += n
		lines = lines[len(piece):]
	}
	// Full buffers are written by the flusher; the caller does not wait.
	if s.flushDue() {
		s.askFlush()
	}

	return rows, rejected, nil
}

// askFlush has the flusher write the buffers that are full.
func (s *Store) askFlush() {
	select {
	case s.full <- struct{}{}:
	default:
	}
}

// room returns the input the buffers take before Append waits: up to their
// budget, and up to twice that together with the runs being written.
func (s *Store) room() int64 {
	budget, buffered := int64(s.opts.BufferBytes), s.buffered.Load()

	return min(budget-buffered, 2*budget-buffered-s.inflight.Load())
}

// flushDue reports whether the buffers are full: they hold their budget,
// or half of it while the runs being written leave them no more room.
func (s *Store) flushDue() bool {
	budget, buffered := int64(s.opts.BufferBytes), s.buffered.Load()

	return buffered >= budget || buffered >= budget/2 && s.room() <= 0
}

// makeRoom waits until the flusher has taken the full buffers, which it
// does before it writes them, and until the runs being written leave room.
// Rows that a flush could not write fill the buffers until one can: no more
// are taken then, and the caller is told so that it can send its lines
// again.
func (s *Store) makeRoom() error {
	s.roomMu.Lock()
	defer s.roomMu.Unlock()
	for s.room() <= 0 {
		if s.flushDue() {
			s.askFlush()
		}
		if s.flushErr != nil {
			return s.flushErr
		}
		s.roomWait.Wait()
	}

	return nil
}

func (s *Store) appendPiece(lines []byte, at int64) (rows, rejected int64) {
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
		sh.batch = s.spare.take(s.shredOptions())
		sh.batch.Reserve(sh.sizes)
	}
	before := sh.batch.Rows
	sh.errs = sh.batch.AddNDJSON(lines, at, sh.errs[:0])
	if sh.buffered == 0 {
		sh.since = time.Now()
	}
	sh.buffered += len(lines)
	s.buffered.Add(int64(len(lines)))
	s.appended.Add(1)

	return int64(sh.batch.Rows - before), int64(len(sh.errs))
}

// spareBatches keeps batches between the uses a store has for them. The
// pool lets go of what a collection finds in it, and a batch that grew to
// the size of its use is costly to grow again.
type spareBatches struct {
	mu   sync.Mutex
	held []*shred.Batch
	// used is when a batch was last asked for.
	used time.Time
}

// take returns a kept batch, with the room it grew to, or one of the pool.
func (sp *spareBatches) take(opts shred.Options) *shred.Batch {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.used = time.Now()
	if n := len(sp.held); n > 0 {
		b := sp.held[n-1]
		sp.held[n-1] = nil
		sp.held = sp.held[:n-1]

		return b
	}

	return shred.Get(opts)
}

// keep takes a batch back, emptied, unless it holds most already.
func (sp *spareBatches) keep(b *shred.Batch, most int) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if len(sp.held) >= most {
		shred.Put(b)

		return
	}
	b.Reset()
	sp.held = append(sp.held, b)
}

// drop gives the batches up once none was asked for during idle.
func (sp *spareBatches) drop(idle time.Duration) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if len(sp.held) == 0 || time.Since(sp.used) < idle {
		return
	}
	for i, b := range sp.held {
		shred.Put(b)
		sp.held[i] = nil
	}
	sp.held = sp.held[:0]
}

// dropSpare gives the kept batches up once the store has not used them for
// spareIdle, and the buffers of sorting once no sort began for as long, so
// that a store nobody writes to holds no memory for writes.
func (s *Store) dropSpare() {
	s.spare.drop(spareIdle)
	s.joined.drop(spareIdle)
	s.cut.drop(spareIdle)
	shred.ReleaseSorting(spareIdle)
}

func (s *Store) shredOptions() shred.Options {
	return shred.Options{TimeField: s.opts.TimeField, TimeFields: s.opts.TimeFields, KeepText: s.opts.KeepText}
}

// Sync writes everything Append has buffered and returns once it is durable.
// Concurrent callers share a flush, so many small synchronous writes cost
// one part and one wait for the disk per flush, not one per caller.
func (s *Store) Sync() error {
	if s.written.Load() == s.appended.Load() && !s.closed.Load() {
		return nil
	}
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

// flushSlots is the number of flushes of the buffers that write at once.
const flushSlots = 2

// flushRecord follows a flush of the buffers until its rows are durable:
// found is the count of the calls of Append whose rows it or a flush before
// it took. durable is set once its rows are, by it or by a later flush that
// took them over; err is what it met while they are not.
type flushRecord struct {
	seq     uint64
	found   uint64
	ended   bool
	durable bool
	err     error
}

// flushFull writes the buffers when they hold their budget; a caller that
// waited for another flush may find them written.
func (s *Store) flushFull() error { return s.writeBuffers(0, true) }

// flush writes the rows that Append has buffered when one of them has
// waited for age, and returns once they and the rows that every flush
// before it took are durable.
func (s *Store) flush(age time.Duration) error { return s.writeBuffers(age, false) }

// writeBuffers takes the batch of every buffer, and those that failed
// flushes left, and writes them all as one run per partition, committed in
// one manifest record. The buffers are free for Append while the run is
// written, and another flush may take them again meanwhile. Unless full is
// set it returns once the flushes that took the buffers before it have
// ended too, with the error of the first whose rows are not durable.
func (s *Store) writeBuffers(age time.Duration, full bool) error {
	s.slots <- struct{}{}
	rec, taken, covers, size, again := s.takeBuffers(age, full)
	if rec == nil {
		<-s.slots

		return nil
	}
	var err error
	if len(taken) > 0 {
		s.flushWrites.Add(1)
		var files []*PartFile
		if files, err = s.writeRuns(taken); err == nil {
			if _, err = s.dir.Commit(files, nil); err == nil {
				s.noteArrival(files)
			}
		}
		s.flushWrites.Add(-1)
		s.countRuns()
	}
	if err != nil {
		s.takeMu.Lock()
		s.unwritten = append(s.unwritten, taken...)
		s.unwrittenBytes += size
		s.unwrittenSeqs = append(append(s.unwrittenSeqs, covers...), rec.seq)
		s.takeMu.Unlock()
	} else {
		for _, b := range taken {
			s.spare.keep(b, len(s.shards))
		}
		if len(taken) > 0 {
			s.kick()
		}
	}
	s.endFlush(rec, covers, err, size, again)
	<-s.slots
	// A flush that failed is tried again when Append, Sync or the flush
	// interval asks, not at once over and over.
	if err == nil && s.flushDue() {
		s.askFlush()
	}
	if full {
		return err
	}

	return s.awaitFlushes(rec)
}

// takeBuffers takes the batches of the buffers and of unwritten for a
// flush and numbers it, or returns a nil record when there is nothing to
// write: full buffers when full is set, or rows that waited for age. covers
// lists the failed flushes whose rows it takes over, and again is set while
// a flush has failed and its rows are not durable: the rows then count as
// buffered until they are written, or every failed attempt would let in as
// much again.
func (s *Store) takeBuffers(age time.Duration, full bool) (rec *flushRecord, taken []*shred.Batch, covers []uint64, size int64, again bool) {
	s.takeMu.Lock()
	defer s.takeMu.Unlock()
	if full && !s.flushDue() {
		return nil, nil, nil, 0, false
	}
	if age > 0 {
		due := len(s.unwritten) > 0
		for i := range s.shards {
			sh := &s.shards[i]
			sh.mu.Lock()
			due = due || (sh.buffered > 0 && time.Since(sh.since) >= age)
			sh.mu.Unlock()
		}
		if !due {
			return nil, nil, nil, 0, false
		}
	}
	// Rows of the calls counted so far are in the buffers taken below or in
	// those of the flushes before.
	rec = &flushRecord{found: s.appended.Load()}
	taken, size, covers = s.unwritten, s.unwrittenBytes, s.unwrittenSeqs
	s.unwritten, s.unwrittenBytes, s.unwrittenSeqs = nil, 0, nil
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		if sh.buffered > 0 {
			size += int64(sh.buffered)
			if sh.batch.Rows > 0 {
				sh.sizes = sh.batch.Sizes(sh.sizes[:0])
				taken = append(taken, sh.batch)
				sh.batch = nil
			}
			sh.buffered, sh.since = 0, time.Time{}
		}
		sh.mu.Unlock()
	}
	s.flushSeq++
	rec.seq = s.flushSeq
	s.pendingMu.Lock()
	s.pending = append(s.pending, rec)
	s.pendingMu.Unlock()
	s.roomMu.Lock()
	if again = s.flushErr != nil; !again {
		s.buffered.Add(-size)
	}
	s.inflight.Add(size)
	s.roomWait.Broadcast()
	s.roomMu.Unlock()

	return rec, taken, covers, size, again
}

// endFlush records how a flush of size input bytes ended, and the rows of
// the calls that are durable once every flush up to one has made its rows
// durable. It gives the buffers back the room that the flush held, unless
// its rows come back to them, and sets flushErr to the error of the first
// flush whose rows are not durable.
func (s *Store) endFlush(rec *flushRecord, covers []uint64, err error, size int64, again bool) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	rec.ended, rec.err = true, err
	if err == nil {
		rec.durable = true
		for _, r := range s.pending {
			if slices.Contains(covers, r.seq) {
				r.durable, r.err = true, nil
			}
		}
	}
	n := 0
	for n < len(s.pending) && s.pending[n].durable {
		s.written.Store(s.pending[n].found)
		n++
	}
	s.pending = append(s.pending[:0], s.pending[n:]...)
	var failed error
	for _, r := range s.pending {
		if r.err != nil && !r.durable {
			failed = r.err

			break
		}
	}
	s.roomMu.Lock()
	s.inflight.Add(-size)
	switch {
	case err != nil && !again:
		s.buffered.Add(size)
	case err == nil && again:
		s.buffered.Add(-size)
	}
	s.flushErr = failed
	s.roomWait.Broadcast()
	s.roomMu.Unlock()
	s.ended.Broadcast()
}

// awaitFlushes waits until the flushes up to rec have ended and returns the
// error of rec, or else of the first of them whose rows are not durable.
func (s *Store) awaitFlushes(rec *flushRecord) error {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	for {
		var first error
		waiting := false
		for _, r := range s.pending {
			if r.seq > rec.seq {
				break
			}
			if !r.ended && !r.durable {
				waiting = true

				break
			}
			if first == nil && !r.durable {
				first = r.err
			}
		}
		if !waiting {
			if rec.err != nil {
				return rec.err
			}

			return first
		}
		s.ended.Wait()
	}
}

// flushLoop writes the buffers that are full, and those whose rows have
// waited for the flush interval, in flushes that it starts flushSlots at a
// time at most.
func (s *Store) flushLoop() {
	defer close(s.flushing)
	defer s.loopFlushes.Wait()
	interval := s.opts.FlushInterval
	var tick <-chan time.Time
	if interval >= 0 {
		ticker := time.NewTicker(max(interval/4, time.Millisecond))
		defer ticker.Stop()
		tick = ticker.C
	}
	looked := time.Now()
	rechecked := looked
	for {
		select {
		case <-s.done:
			return
		case <-s.full:
			// Whoever waits for room learns of a failure from the buffers.
			s.launch(func() { _ = s.flushFull() })
		case <-tick:
			s.dropSpare()
			if time.Since(rechecked) >= recheckEvery {
				rechecked = time.Now()
				s.recheck()
			}
			// Partitions close as time passes, with or without new rows.
			if s.opts.Partition > 0 && time.Since(looked) >= closedLook {
				looked = time.Now()
				s.kick()
			}
			s.launch(func() {
				if err := s.flush(interval); err != nil {
					s.mu.Lock()
					if s.err == nil {
						s.err = err
					}
					s.idle.Broadcast()
					s.mu.Unlock()
				}
			})
		}
	}
}

// launch runs a flush of the flusher on its own, unless flushSlots of them
// run already: one that ends asks for another while the buffers are full,
// and the next tick brings the timed one again.
func (s *Store) launch(flush func()) {
	if s.launched.Load() >= flushSlots {
		return
	}
	s.launched.Add(1)
	s.loopFlushes.Add(1)
	go func() {
		defer s.loopFlushes.Done()
		defer s.launched.Add(-1)
		flush()
	}()
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

// MergeState returns the number of merges under way and of the runs that no
// merge reads yet and that the merger is still to merge, closed partitions
// that wait to go quiet included; the store is quiet when both are zero.
func (s *Store) MergeState() (running, due int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Under the lock the table holds no part that a committed merge
	// replaced, as in startMerges.
	snap := s.Table().Snapshot()
	defer snap.Release()
	free := make([]*Handle, 0, len(snap.Parts()))
	for _, h := range snap.Parts() {
		if _, busy := s.busy[h.Run]; !busy {
			free = append(free, h)
		}
	}
	whole := s.wholeDue(free)
	for _, set := range pickMerges(free, s.opts.MergeFanIn, false) {
		if _, ok := whole[set[0].partition]; !ok {
			due += len(set)
		}
	}
	for _, runs := range whole {
		due += runs
	}

	return s.running, due
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
	mu     sync.Mutex
	freed  *sync.Cond
	total  int
	free   int
	closed bool
}

func (b *blockBudget) init(total int) {
	b.total, b.free = total, total
	b.freed = sync.NewCond(&b.mu)
}

// acquire takes n units, or the whole budget when n exceeds it, and reports
// whether it did: once the budget is closed it takes none.
func (b *blockBudget) acquire(n int) bool {
	n = min(n, b.total)
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.free < n && !b.closed {
		b.freed.Wait()
	}
	if b.closed {
		return false
	}
	b.free -= n

	return true
}

// close makes the callers that wait, and all later ones, take nothing.
func (b *blockBudget) close() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	b.freed.Broadcast()
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

// closedQuiet is how long a closed partition goes without rows before it
// is merged whole: rows that still arrive would have it merged again.
const closedQuiet = 10 * time.Second

// closedLook is how often a store with partitions looks for ones that have
// closed or gone quiet while no rows arrived.
const closedLook = closedQuiet / 2

// noteArrival records that rows were flushed to the partitions of files.
func (s *Store) noteArrival(files []*PartFile) {
	now := timeNow().UnixNano()
	s.mu.Lock()
	for _, f := range files {
		s.arrived[f.Meta.Partition] = now
	}
	s.mu.Unlock()
}

// wholeDue returns the closed partitions among parts that are due to be
// merged whole, each with the number of its runs: those of several runs of
// which none holds seven eighths of the rows.
func (s *Store) wholeDue(parts []*Handle) map[int64]int {
	width := int64(s.opts.Partition)
	if width <= 0 || s.opts.MergeFanIn < 2 {
		return nil
	}
	now, grace := timeNow().UnixNano(), min(width, int64(time.Hour))
	rows, most, runs := map[int64]uint64{}, map[uint64]uint64{}, map[int64]int{}
	for _, h := range parts {
		if !s.endsBy(h.Partition, now-grace) {
			continue
		}
		if _, seen := most[h.Run]; !seen {
			runs[h.Partition]++
		}
		rows[h.Partition] += h.Rows
		most[h.Run] += h.Rows
	}
	for _, h := range parts {
		if n, closed := runs[h.Partition]; closed && (n < 2 || most[h.Run]*8 >= rows[h.Partition]*7) {
			delete(runs, h.Partition)
		}
	}

	return runs
}

// leaveToWhole drops the merges by tier in the partitions that are due to
// be merged whole, which would write their rows twice. A partition that
// holds fanIn runs for each of fanIn keeps one at a time, so that its runs
// stay few while rows keep arriving; merging names the partitions that a
// merge under way reads.
func (s *Store) leaveToWhole(sets [][]sortedRun, parts []*Handle, merging map[int64]bool) [][]sortedRun {
	due := s.wholeDue(parts)
	if len(due) == 0 {
		return sets
	}
	most := s.opts.MergeFanIn * s.opts.MergeFanIn
	out, kept := sets[:0], map[int64]bool{}
	for _, set := range sets {
		p := set[0].partition
		switch n, whole := due[p]; {
		case !whole:
			out = append(out, set)
		case n >= most && !kept[p] && !merging[p]:
			kept[p] = true
			out = append(out, set)
		}
	}

	return out
}

// recheckEvery is how often the parts are told to check what is read from
// them against its checksum again.
const recheckEvery = time.Hour

// recheck has every read of the parts check what it reads again.
func (s *Store) recheck() {
	snap := s.Table().Snapshot()
	defer snap.Release()
	for _, h := range snap.Parts() {
		h.Recheck()
	}
}

// spareIdle is how long the batches of written buffers wait to be filled
// again before they are given up.
const spareIdle = 10 * time.Second

// closedSet returns all the runs of the earliest partition that has closed
// and gone quiet, that neither a merge under way nor a set of taken reads
// and of which no single run holds seven eighths of the rows. One such
// partition is merged at a time.
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
		if skip[h.Partition] || !s.endsBy(h.Partition, now-grace) || now-s.arrived[h.Partition] < int64(closedQuiet) {
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
		sets = s.leaveToWhole(sets, snap.Parts(), partial)
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
			if err != nil && !errors.Is(err, errStopped) && s.err == nil {
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
		// Each range merge holds one decoded block per run.
		if !s.blocks.acquire(len(runs)) {
			errs[i] = errStopped

			break
		}
		select {
		case s.cores <- struct{}{}:
		case <-s.done:
			s.blocks.release(len(runs))
			errs[i] = errStopped
		}
		if errs[i] != nil {
			break
		}
		wg.Add(1)
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
			pf.BeforeWrite(s.giveWay)
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
	err := errors.Join(errs...)
	if err == nil && s.closed.Load() {
		err = errStopped
	}
	if err != nil {
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
	_, err = s.dir.Commit(files, inputs)
	s.countRuns()

	return err
}

// countRuns notes the number of runs in the table that merges are to take.
func (s *Store) countRuns() {
	snap := s.dir.Table().Snapshot()
	defer snap.Release()
	due := 0
	for _, set := range s.leaveToWhole(pickMerges(snap.Parts(), s.opts.MergeFanIn, false), snap.Parts(), nil) {
		due += len(set)
	}
	s.due.Store(int32(due))
}

// pressingBytes is the input in Append's buffers from which a flush is
// near enough for merges to leave it the disk.
const pressingBytes = 16 << 20

// errStopped is what a merge returns when the store closed while it ran.
var errStopped = errors.New("colstore: merge stopped by Close")

// giveWay runs before every write of a merge: it holds the merge back while
// rows wait in the buffers for a flush or a flush is writing them, and fewer
// runs are due to be merged than the backlog that merges may not let grow,
// and it stops the merge once the store closes.
func (s *Store) giveWay() error {
	for (s.flushWrites.Load() > 0 || s.buffered.Load() >= pressingBytes) && int(s.due.Load()) < s.opts.MergeBacklog && !s.closed.Load() {
		time.Sleep(2 * time.Millisecond)
	}
	if s.closed.Load() {
		return errStopped
	}

	return nil
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
	w.s.noteArrival(files)
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
	block := s.cut.take(shred.Options{})
	defer s.cut.keep(block, runtime.GOMAXPROCS(0))
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
