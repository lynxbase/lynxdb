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
	"github.com/lynxbase/lynxdb/pkg/vec"
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
// and returns the files, which the caller commits. It sorts the batches; a
// part merges the sorted rows of several by key.
func (s *Store) writeRuns(batches []*shred.Batch, sorted chan struct{}) ([]*PartFile, error) {
	buckets := make([][]shred.TimeBucket, len(batches))
	// Each of the flushes that write at once sorts and writes on half of
	// the CPUs, which bounds the buffers of sorting and encoding they hold.
	workers := max(2, runtime.GOMAXPROCS(0)/2)
	sideBySide(len(batches), workers, func(i int) {
		b := batches[i]
		if s.opts.Partition > 0 {
			buckets[i] = b.SortByTimeBuckets(nil, int64(s.opts.Partition), s.opts.SortKey)

			return
		}
		b.SortBy(s.opts.SortKey)
		buckets[i] = []shred.TimeBucket{{Rows: b.Rows}}
	})
	close(sorted)
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
	sideBySide(len(parts), workers, func(i int) { files[i], errs[i] = s.writeRunPart(&parts[i]) })
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

// writeRunPart writes one part of a run and returns its file: the rows of
// its segments in the order of their keys and times, and those equal in
// both in the order of the segments.
func (s *Store) writeRunPart(p *runPart) (*PartFile, error) {
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
	block := s.cut.take(shred.Options{})
	defer s.cut.keep(block, runtime.GOMAXPROCS(0))
	fail := func(err error) (*PartFile, error) {
		pf.Abort()

		return nil, err
	}
	runs := make([]shred.Run, len(p.segs))
	for i, sg := range p.segs {
		runs[i] = shred.Run{Batch: sg.b, From: sg.from, To: sg.to}
	}
	// The rows of the part are all in memory, so the writer can tell which
	// columns repeat across its blocks before it writes the first.
	pw.Share(runs)
	if len(p.segs) == 1 {
		sg := p.segs[0]
		for from := sg.from; from < sg.to; from += min(s.opts.BlockRows, sg.to-from) {
			block.Reset()
			sg.b.CopyRows(block, from, min(from+s.opts.BlockRows, sg.to))
			if err := pw.WriteBatch(block); err != nil {
				return fail(err)
			}
		}
	} else {
		picks := make([]uint32, 0, s.opts.BlockRows)
		m := s.newSegmentMerge(p.segs)
		for {
			src, ok := m.next()
			if ok {
				picks = append(picks, uint32(src))
			}
			if len(picks) == s.opts.BlockRows || !ok && len(picks) > 0 {
				block.Reset()
				block.Interleave(runs, picks)
				picks = picks[:0]
				if err := pw.WriteBatch(block); err != nil {
					return fail(err)
				}
			}
			if !ok {
				break
			}
		}
	}
	if err := pw.Close(); err != nil {
		return fail(err)
	}

	return pf, nil
}

// segmentMerge yields the rows of sorted segments in the order of their
// keys, then times, then segments: heap holds the segments that have rows
// left, the one whose next row comes first on top, and at, keys and times
// the next row of each.
type segmentMerge struct {
	segs    []segment
	cursors []keyCursor
	at      []int
	keys    [][]byte
	times   []int64
	heap    []int
}

func (s *Store) newSegmentMerge(segs []segment) *segmentMerge {
	m := &segmentMerge{segs: segs, cursors: make([]keyCursor, len(segs)), at: make([]int, len(segs)),
		keys: make([][]byte, len(segs)), times: make([]int64, len(segs))}
	for i, sg := range segs {
		m.cursors[i] = newKeyCursor(sg.b, s.opts.SortKey, sg.from)
		m.at[i] = sg.from
		if sg.to > sg.from {
			m.load(i)
			m.heap = append(m.heap, i)
		}
	}
	for i := len(m.heap)/2 - 1; i >= 0; i-- {
		m.down(i)
	}

	return m
}

// load reads the key and the time of the next row of segment i.
func (m *segmentMerge) load(i int) {
	row := m.at[i]
	m.keys[i] = m.cursors[i].appendKey(m.keys[i][:0], row)
	m.times[i] = m.segs[i].b.Times[row]
}

func (m *segmentMerge) less(a, b int) bool {
	if c := bytes.Compare(m.keys[a], m.keys[b]); c != 0 {
		return c < 0
	}
	if m.times[a] != m.times[b] {
		return m.times[a] < m.times[b]
	}

	return a < b
}

func (m *segmentMerge) down(i int) {
	h := m.heap
	for {
		least, l := i, 2*i+1
		if l < len(h) && m.less(h[l], h[least]) {
			least = l
		}
		if r := l + 1; r < len(h) && m.less(h[r], h[least]) {
			least = r
		}
		if least == i {
			return
		}
		h[i], h[least] = h[least], h[i]
		i = least
	}
}

// next returns the segment whose next row comes next, and false once every
// row was taken.
func (m *segmentMerge) next() (int, bool) {
	if len(m.heap) == 0 {
		return 0, false
	}
	i := m.heap[0]
	if m.at[i]++; m.at[i] < m.segs[i].to {
		m.load(i)
	} else {
		last := len(m.heap) - 1
		m.heap[0] = m.heap[last]
		m.heap = m.heap[:last]
	}
	if len(m.heap) > 1 {
		m.down(0)
	}

	return i, true
}

// keyCursor appends the encoded sort keys of rows of a batch in ascending
// order of rows: cols holds per path the columns of its values, by kind,
// and next the first value of each at or after the last row asked for.
type keyCursor struct {
	cols [][]*shred.Column
	next [][]int
}

func newKeyCursor(b *shred.Batch, key []string, from int) keyCursor {
	k := keyCursor{cols: make([][]*shred.Column, len(key)), next: make([][]int, len(key))}
	for p, path := range key {
		for kind := vec.KindNull; kind <= vec.KindVariant; kind++ {
			if c := b.Lookup(path, kind); c != nil && c.Count > 0 {
				i, _ := c.ValueAt(from)
				k.cols[p], k.next[p] = append(k.cols[p], c), append(k.next[p], i)
			}
		}
	}

	return k
}

func (k *keyCursor) appendKey(dst []byte, row int) []byte {
	for p, cols := range k.cols {
		found := false
		for j, c := range cols {
			i := k.next[p][j]
			for i < c.Count && c.Row(i) < row {
				i++
			}
			k.next[p][j] = i
			if !found && i < c.Count && c.Row(i) == row {
				dst, found = part.AppendKeyValue(dst, c, i), true
			}
		}
		if !found {
			dst = append(dst, 0)
		}
	}

	return dst
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
