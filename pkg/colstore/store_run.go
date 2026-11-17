package colstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// segment is rows of a batch that follow one another in sort key order.
type segment struct {
	b        *shred.Batch
	from, to int
}

// runPart is the rows that one part of a run is written from.
type runPart struct {
	partition int64
	run       uint64
	segs      []segment
	rows      int
}

const (
	// runSamples is the number of keys sampled per part when a run is cut.
	runSamples = 32
	// runPartBlocks is the least a part of a flushed run holds, in blocks:
	// small enough for a flush to use every CPU, since merges make larger
	// parts of them later.
	runPartBlocks = 4
)

// writeRuns writes the rows of batches as one run per partition they fall
// in, cut into parts of disjoint key ranges that are written side by side,
// and returns the files, which the caller commits. It sorts the batches.
func (s *Store) writeRuns(batches []*shred.Batch) ([]*PartFile, error) {
	buckets := make([][]shred.TimeBucket, len(batches))
	sideBySide(len(batches), runtime.GOMAXPROCS(0), func(i int) {
		b := batches[i]
		if s.opts.Partition > 0 {
			buckets[i] = b.SortByTimeBuckets(nil, int64(s.opts.Partition), s.opts.SortKey)

			return
		}
		b.SortBy(s.opts.SortKey)
		buckets[i] = []shred.TimeBucket{{Rows: b.Rows}}
	})
	segments := map[int64][]segment{}
	var partitions []int64
	for i, b := range batches {
		from := 0
		for _, bucket := range buckets[i] {
			if _, seen := segments[bucket.ID]; !seen {
				partitions = append(partitions, bucket.ID)
			}
			segments[bucket.ID] = append(segments[bucket.ID], segment{b, from, from + bucket.Rows})
			from += bucket.Rows
		}
	}
	slices.Sort(partitions)
	var parts []runPart
	for _, id := range partitions {
		parts = s.cutRun(parts, id, segments[id])
	}
	files := make([]*PartFile, len(parts))
	errs := make([]error, len(parts))
	// Every part in the making holds a copy of its rows, so a quarter of
	// the CPUs write at a time and the copies stay a fraction of the run.
	writers := max(2, runtime.GOMAXPROCS(0)/4)
	sideBySide(len(parts), writers, func(i int) { files[i], errs[i] = s.writeRunPart(&parts[i]) })
	if err := errors.Join(errs...); err != nil {
		for _, pf := range files {
			if pf != nil {
				pf.Abort()
			}
		}

		return nil, err
	}

	return files, nil
}

// cutRun appends the parts of the run that the sorted segments of one
// partition make: ranges of the sort key of similar size, so that no key is
// in two parts, or ranges of time for rows that have no sort key.
func (s *Store) cutRun(parts []runPart, partition int64, segs []segment) []runPart {
	rows := 0
	for _, sg := range segs {
		rows += sg.to - sg.from
	}
	run := s.dir.NewRun()
	n := min(4*runtime.GOMAXPROCS(0), rows/(runPartBlocks*s.opts.BlockRows))
	if n <= 1 {
		return append(parts, runPart{partition, run, segs, rows})
	}
	key := s.opts.SortKey
	position := func(dst []byte, b *shred.Batch, row int) []byte {
		if len(key) == 0 {
			// The sign bit flipped orders times as bytes.
			return binary.BigEndian.AppendUint64(dst, uint64(b.Times[row])^1<<63)
		}

		return part.AppendRowKey(dst, b, key, row)
	}
	var samples [][]byte
	step := max(1, rows/(n*runSamples))
	for _, sg := range segs {
		for row := sg.from + step/2; row < sg.to; row += step {
			samples = append(samples, position(nil, sg.b, row))
		}
	}
	slices.SortFunc(samples, bytes.Compare)
	var cuts [][]byte
	for i := 1; i < n; i++ {
		if k := samples[i*len(samples)/n]; len(cuts) == 0 || !bytes.Equal(cuts[len(cuts)-1], k) {
			cuts = append(cuts, k)
		}
	}
	from := make([]int, len(segs))
	for i, sg := range segs {
		from[i] = sg.from
	}
	var scratch []byte
	for c := 0; c <= len(cuts); c++ {
		p := runPart{partition: partition, run: run}
		for i, sg := range segs {
			to := sg.to
			if c < len(cuts) {
				// The first row at or above the cut; rows of one key stay
				// in one part.
				to = from[i] + sort.Search(sg.to-from[i], func(k int) bool {
					scratch = position(scratch[:0], sg.b, from[i]+k)

					return bytes.Compare(scratch, cuts[c]) >= 0
				})
			}
			if to > from[i] {
				p.segs = append(p.segs, segment{sg.b, from[i], to})
				p.rows += to - from[i]
			}
			from[i] = to
		}
		if p.rows > 0 {
			parts = append(parts, p)
		}
	}

	return parts
}

// runWriters keeps part writers between flushes for their buffers.
var runWriters sync.Pool

// writeRunPart writes one part of a run and returns its file.
func (s *Store) writeRunPart(p *runPart) (*PartFile, error) {
	src, from, to := p.segs[0].b, p.segs[0].from, p.segs[0].to
	if len(p.segs) > 1 {
		// Rows of several batches are brought together and into order.
		local := shred.Get(s.shredOptions())
		defer shred.Put(local)
		for _, sg := range p.segs {
			sg.b.CopyRows(local, sg.from, sg.to)
		}
		local.SortBy(s.opts.SortKey)
		src, from, to = local, 0, local.Rows
	}
	pf, err := s.dir.Create()
	if err != nil {
		return nil, err
	}
	pf.Meta.Partition, pf.Meta.Run = p.partition, p.run
	flushed := s.opts.Part
	flushed.Level = s.opts.FlushLevel
	pw, _ := runWriters.Get().(*part.Writer)
	if pw == nil {
		pw = part.NewWriter(pf, flushed)
	} else {
		pw.Reset(pf, flushed)
	}
	defer runWriters.Put(pw)
	block := shred.Get(shred.Options{})
	defer shred.Put(block)
	for ; from < to; from += min(s.opts.BlockRows, to-from) {
		block.Reset()
		src.CopyRows(block, from, min(from+s.opts.BlockRows, to))
		if err := pw.WriteBatch(block); err != nil {
			pf.Abort()

			return nil, err
		}
	}
	if err := pw.Close(); err != nil {
		pf.Abort()

		return nil, err
	}

	return pf, nil
}

// sideBySide calls fn for every i below n on workers goroutines at most.
func sideBySide(n, workers int, fn func(i int)) {
	workers = min(n, workers)
	if workers <= 1 {
		for i := 0; i < n; i++ {
			fn(i)
		}

		return
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := int(next.Add(1)) - 1; i < n; i = int(next.Add(1)) - 1 {
				fn(i)
			}
		}()
	}
	wg.Wait()
}
