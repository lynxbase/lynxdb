package colstore

import (
	"errors"
	"runtime"
	"sort"
	"sync"

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
	// Part sets how parts are encoded; its ordering fields are overridden.
	Part part.Options
}

const (
	defaultStoreBlockRows = 16384
	defaultFlushBytes     = 128 << 20
	defaultMergeFanIn     = 8
	// mergedPartRows is the least a part of a merged run holds, which keeps
	// small merges from scattering their rows over many files.
	mergedPartRows = 1 << 18
	// mergeBlockBudget bounds the decoded blocks the range merges of one
	// merge hold together, each several megabytes of column values.
	mergeBlockBudget = 1024
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

	mu   sync.Mutex
	idle *sync.Cond
	// kicks counts requests to look for merges and seen the requests the
	// merger has answered by finding nothing left to do.
	kicks, seen uint64
	compact     bool
	err         error
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
	opts.Part.Ordered, opts.Part.SortKey = len(opts.SortKey) > 0, opts.SortKey
	dir, err := OpenDir(path)
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir: dir, opts: opts,
		wake: make(chan struct{}, 1), done: make(chan struct{}), stopped: make(chan struct{}),
	}
	s.idle = sync.NewCond(&s.mu)
	go s.mergeLoop()
	s.kick()

	return s, nil
}

// Table returns the table of live parts.
func (s *Store) Table() *Table { return s.dir.Table() }

// Close stops the merger and closes the directory; writers must be closed
// first.
func (s *Store) Close() error {
	close(s.done)
	<-s.stopped
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()

	return errors.Join(err, s.dir.Close())
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
// a merge met.
func (s *Store) Settle() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.seen != s.kicks && s.err == nil {
		s.idle.Wait()
	}

	return s.err
}

// Compact merges all runs into one and waits for it.
func (s *Store) Compact() error {
	s.mu.Lock()
	s.compact = true
	s.mu.Unlock()
	s.kick()

	return s.Settle()
}

func (s *Store) mergeLoop() {
	defer close(s.stopped)
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

// sortedRun is the parts of one run.
type sortedRun struct {
	id    uint64
	tier  int
	parts []*Handle
}

// pickRuns chooses the runs to merge next: every run when compacting,
// otherwise the oldest fanIn runs of the lowest tier that has that many.
func pickRuns(parts []*Handle, fanIn int, compact bool) []sortedRun {
	byID := map[uint64]*sortedRun{}
	var runs []*sortedRun
	for _, h := range parts {
		r := byID[h.Run]
		if r == nil {
			r = &sortedRun{id: h.Run, tier: h.Tier}
			byID[h.Run] = r
			runs = append(runs, r)
		}
		r.parts = append(r.parts, h)
	}
	sort.Slice(runs, func(i, j int) bool {
		if runs[i].tier != runs[j].tier {
			return runs[i].tier < runs[j].tier
		}

		return runs[i].id < runs[j].id
	})
	var picked []sortedRun
	switch {
	case compact && len(runs) > 1:
		for _, r := range runs {
			picked = append(picked, *r)
		}
	case fanIn > 1:
		for first := 0; first < len(runs); {
			end := first
			for end < len(runs) && runs[end].tier == runs[first].tier {
				end++
			}
			if end-first >= fanIn {
				for _, r := range runs[first : first+fanIn] {
					picked = append(picked, *r)
				}

				break
			}
			first = end
		}
	}

	return picked
}

// mergeOnce merges one set of runs when one is due and reports whether it
// did.
func (s *Store) mergeOnce(compact bool) (bool, error) {
	if !s.opts.Part.Ordered {
		return false, nil
	}
	snap := s.Table().Snapshot()
	defer snap.Release()
	runs := pickRuns(snap.Parts(), s.opts.MergeFanIn, compact)
	if len(runs) == 0 {
		return false, nil
	}
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
	meta := PartMeta{Run: s.dir.NewRun(), Tier: tier + 1}
	files := make([]*PartFile, len(cuts)+1)
	errs := make([]error, len(files))
	// Each range merge holds one decoded block per run.
	slots := make(chan struct{}, max(1, min(cores, mergeBlockBudget/len(runs))))
	var wg sync.WaitGroup
	for i := range files {
		wg.Add(1)
		slots <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
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

		return false, err
	}
	if _, err := s.dir.Commit(files, inputs); err != nil {
		return false, err
	}

	return true, nil
}

// Writer buffers documents and writes them to the store as sorted runs. It
// is not safe for concurrent use; a store takes any number of writers.
type Writer struct {
	// Rejected counts the lines that were not JSON objects.
	Rejected int64

	s     *Store
	batch *shred.Batch
	block *shred.Batch
	errs  []shred.LineError
}

// NewWriter returns a writer with an empty buffer.
func (s *Store) NewWriter() *Writer {
	return &Writer{
		s:     s,
		batch: shred.NewBatch(shred.Options{TimeField: s.opts.TimeField}),
		block: shred.NewBatch(shred.Options{}),
	}
}

// AddNDJSON buffers the documents of lines, one per line, and writes a run
// when the buffer has grown to the store's flush size.
func (w *Writer) AddNDJSON(lines []byte) error {
	w.errs = w.batch.AddNDJSON(lines, 0, w.errs[:0])
	w.Rejected += int64(len(w.errs))
	if w.batch.Bytes < w.s.opts.FlushBytes {
		return nil
	}

	return w.Flush()
}

// Flush writes the buffered rows as one run; when it returns nil they are
// durable and visible to new snapshots.
func (w *Writer) Flush() error {
	if w.batch.Rows == 0 {
		return nil
	}
	s := w.s
	if s.opts.Part.Ordered {
		w.batch.SortBy(s.opts.SortKey)
	}
	pf, err := s.dir.Create()
	if err != nil {
		return err
	}
	pw := part.NewWriter(pf, s.opts.Part)
	for from := 0; from < w.batch.Rows && err == nil; from += s.opts.BlockRows {
		w.block.Reset()
		w.batch.CopyRows(w.block, from, min(from+s.opts.BlockRows, w.batch.Rows))
		err = pw.WriteBatch(w.block)
	}
	if err == nil {
		err = pw.Close()
	}
	if err != nil {
		pf.Abort()

		return err
	}
	if _, err := s.dir.Commit([]*PartFile{pf}, nil); err != nil {
		return err
	}
	w.batch.Reset()
	s.kick()

	return nil
}

// Close flushes the buffer.
func (w *Writer) Close() error { return w.Flush() }
