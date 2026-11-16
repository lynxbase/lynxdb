package colstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// The five JSONBench queries over the Bluesky event stream, written directly
// against the scan interface: per-worker partial aggregation, then a merge.

// partitionBits splits high-cardinality keys into partitions small enough
// for one partition's table to stay in a core's cache.
const partitionBits = 8

// sharedGroups gives every scan worker the same id for a group key, so the
// ids can be part of the keys that workers hand to the merge.
type sharedGroups struct {
	mu    sync.Mutex
	arena *mem.Arena
	table *vec.ByteTable
}

func newSharedGroups() *sharedGroups {
	a := mem.NewArena()

	return &sharedGroups{arena: a, table: vec.NewByteTable(a)}
}

func (s *sharedGroups) id(key []byte, h uint64) uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return uint32(s.table.IDHashed(key, h))
}

// benchWorker is the state one scan goroutine owns for one query.
type benchWorker struct {
	arena  *mem.Arena
	groups *vec.ByteTable
	counts []int64
	mins   []int64
	maxs   []int64
	nulls  int64
	hours  [3 * 24]int64
	// shared, when set, assigns global[id] to each group of this worker.
	shared *sharedGroups
	global []uint32
	// log holds one record per selected row for the merge to aggregate.
	log *vec.KeyPartitions

	a, b, c, tmp vec.Column
	mask, mask2  []uint8
	idx          []uint32
	hist         [256]uint32
	codeGroup    []int32
	key          []byte
}

func newBenchWorkers(n int) []*benchWorker {
	ws := make([]*benchWorker, n)
	for i := range ws {
		a := mem.NewArena()
		ws[i] = &benchWorker{arena: a, groups: vec.NewByteTable(a)}
	}

	return ws
}

func freeBenchWorkers(ws []*benchWorker) {
	for _, w := range ws {
		w.arena.Free()
	}
}

func (w *benchWorker) group(key []byte) int {
	id := w.groups.ID(key)
	for len(w.counts) <= id {
		w.counts = append(w.counts, 0)
		w.mins = append(w.mins, 1<<62)
		w.maxs = append(w.maxs, -1<<62)
		if w.shared != nil {
			w.global = append(w.global, w.shared.id(key, w.groups.KeyHash(id)))
		}
	}

	return id
}

// column decodes the column of path holding kind into dst.
func column(p *part.Part, b *part.Block, path string, kind vec.Kind, dst *vec.Column) (*part.ColumnRef, error) {
	id, ok := p.PathID(path)
	if !ok {
		return nil, nil
	}
	ref, ok := b.Column(id, kind)
	if !ok {
		return nil, nil
	}

	return ref, b.Decode(ref, dst)
}

// eq fills mask with the rows whose string at path equals want and reports
// whether any row can match.
func (w *benchWorker) eq(p *part.Part, b *part.Block, path, want string, mask *[]uint8) (bool, error) {
	id, ok := p.PathID(path)
	if !ok {
		return false, nil
	}
	ref, ok := b.Column(id, vec.KindString)
	if !ok || (ref.Const() && string(ref.ConstBytes()) != want) {
		return false, nil
	}
	if err := b.Decode(ref, &w.tmp); err != nil {
		return false, err
	}
	if cap(*mask) < b.Rows {
		*mask = make([]uint8, b.Rows)
	}
	*mask = (*mask)[:b.Rows]
	vec.MaskEqBytes(*mask, &w.tmp, []byte(want))

	return true, nil
}

// created selects the rows of commit events that create a record, the
// filter shared by queries two to five.
func (w *benchWorker) created(p *part.Part, b *part.Block) (bool, error) {
	if ok, err := w.eq(p, b, "kind", "commit", &w.mask); !ok || err != nil {
		return false, err
	}
	if ok, err := w.eq(p, b, "commit.operation", "create", &w.mask2); !ok || err != nil {
		return false, err
	}
	vec.MaskAnd(w.mask, w.mask, w.mask2)

	return true, nil
}

type eventCount struct {
	event string
	count int64
	users int64
}

func mergeCounts(ws []*benchWorker) (map[string]*eventCount, int64) {
	out := map[string]*eventCount{}
	var nulls int64
	for _, w := range ws {
		nulls += w.nulls
		for id := 0; id < w.groups.Len(); id++ {
			name := string(w.groups.Key(id))
			if out[name] == nil {
				out[name] = &eventCount{event: name}
			}
			out[name].count += w.counts[id]
		}
	}

	return out, nulls
}

func sortedCounts(m map[string]*eventCount) []eventCount {
	out := make([]eventCount, 0, len(m))
	for _, e := range m {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].count != out[j].count {
			return out[i].count > out[j].count
		}

		return out[i].event < out[j].event
	})

	return out
}

// queryTopEvents is Q1: count() by commit.collection, largest first.
func queryTopEvents(snap *Snapshot, workers int) ([]eventCount, error) {
	ws := newBenchWorkers(workers)
	defer freeBenchWorkers(ws)
	_, err := snap.Scan(context.Background(), ScanOptions{Workers: workers}, func(n int, p *part.Part, b *part.Block) error {
		w := ws[n]
		ref, err := column(p, b, "commit.collection", vec.KindString, &w.a)
		if err != nil || ref == nil {
			w.nulls += int64(b.Rows)

			return err
		}
		w.nulls += int64(b.Rows - ref.Count)
		c := &w.a
		switch {
		case c.Layout == vec.Const:
			w.counts[w.group(c.DictValue(0))] += int64(ref.Count)
		case c.Layout == vec.Dict && len(c.Codes8) != 0:
			clear(w.hist[:])
			vec.Histogram8(&w.hist, c.Codes8)
			// Rows without a value were decoded as code 0.
			w.hist[0] -= uint32(b.Rows - ref.Count)
			for code := 0; code < c.DictLen(); code++ {
				if n := w.hist[code]; n != 0 {
					w.counts[w.group(c.DictValue(code))] += int64(n)
				}
			}
		default:
			for i := 0; i < b.Rows; i++ {
				if c.IsValid(i) {
					w.counts[w.group(c.Bytes(i))]++
				}
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	merged, nulls := mergeCounts(ws)
	if nulls > 0 {
		merged[""] = &eventCount{count: nulls}
	}

	return sortedCounts(merged), nil
}

// groupOfRow returns the worker's group for the collection of row r,
// resolving each dictionary code once per block.
func (w *benchWorker) groupOfRow(c *vec.Column, r int) int {
	if c.Layout != vec.Dict {
		return w.group(c.Bytes(r))
	}
	code := c.Code(r)
	if g := w.codeGroup[code]; g >= 0 {
		return int(g)
	}
	g := w.group(c.DictValue(code))
	w.codeGroup[code] = int32(g)

	return g
}

func (w *benchWorker) resetCodeGroups(c *vec.Column) {
	if c.Layout != vec.Dict {
		return
	}
	w.codeGroup = w.codeGroup[:0]
	for i := 0; i < c.DictLen(); i++ {
		w.codeGroup = append(w.codeGroup, -1)
	}
}

// distinctMerger is the state one goroutine owns while it counts the
// distinct keys of partitions.
type distinctMerger struct {
	arena *mem.Arena
	keys  *vec.ByteTable
	set   *vec.HashSet128
	users []int64
}

// queryEventsWithUsers is Q2: count() and the number of distinct did by
// commit.collection, over created commits. The distinct count compares whole
// keys, or only their 128-bit hashes when hashed is set.
func queryEventsWithUsers(snap *Snapshot, workers int, hashed bool) ([]eventCount, error) {
	ws := newBenchWorkers(workers)
	defer freeBenchWorkers(ws)
	shared := newSharedGroups()
	defer shared.arena.Free()
	for _, w := range ws {
		w.shared = shared
		w.log = vec.NewKeyPartitions(w.arena, partitionBits)
	}
	_, err := snap.Scan(context.Background(), ScanOptions{Workers: workers}, func(n int, p *part.Part, b *part.Block) error {
		w := ws[n]
		if ok, err := w.created(p, b); !ok || err != nil {
			return err
		}
		if w.idx = vec.MaskIndexes(w.idx[:0], w.mask); len(w.idx) == 0 {
			return nil
		}
		if ref, err := column(p, b, "commit.collection", vec.KindString, &w.a); err != nil || ref == nil {
			return err
		}
		if ref, err := column(p, b, "did", vec.KindString, &w.b); err != nil || ref == nil {
			return err
		}
		w.resetCodeGroups(&w.a)
		// A run of rows with one group and did adds its key once.
		lastGroup, lastDID := -1, []byte(nil)
		for _, r := range w.idx {
			row := int(r)
			if !w.a.IsValid(row) {
				continue
			}
			g := w.groupOfRow(&w.a, row)
			w.counts[g]++
			if !w.b.IsValid(row) {
				continue
			}
			did := w.b.Bytes(row)
			if g == lastGroup && string(did) == string(lastDID) {
				continue
			}
			lastGroup, lastDID = g, did
			if hashed {
				// The record is the high hash word and the group.
				lo, hi := vec.Hash128(did)
				w.key = binary.LittleEndian.AppendUint32(binary.LittleEndian.AppendUint64(w.key[:0], hi), w.global[g])
				w.log.Add(lo, w.key)

				continue
			}
			// The record is the key: the group followed by the did.
			w.key = append(binary.LittleEndian.AppendUint32(w.key[:0], w.global[g]), did...)
			w.log.Add(vec.Hash(w.key), w.key)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	groups := shared.table.Len()
	mergers := make([]distinctMerger, workers)
	for i := range mergers {
		m := &mergers[i]
		m.arena = mem.NewArena()
		defer m.arena.Free()
		m.keys, m.set = vec.NewByteTable(m.arena), vec.NewHashSet128(m.arena)
		m.users = make([]int64, groups)
	}
	// Equal keys always land in the same partition, so partitions count
	// independently and their distinct counts add up.
	forEachPartition(1<<partitionBits, workers, func(slot, partition int) {
		m := &mergers[slot]
		if hashed {
			m.set.Clear()
			for _, w := range ws {
				w.log.Each(partition, func(lo uint64, rec []byte) {
					g := binary.LittleEndian.Uint32(rec[8:])
					// Mixing the group into one word keeps equal dids of
					// different groups apart without widening the value.
					if m.set.Add(lo^vec.Mix64(uint64(g)+1), binary.LittleEndian.Uint64(rec)) {
						m.users[g]++
					}
				})
			}

			return
		}
		m.keys.Clear()
		for _, w := range ws {
			w.log.Each(partition, func(h uint64, key []byte) {
				if n := m.keys.Len(); m.keys.IDHashed(key, h) == n {
					m.users[binary.LittleEndian.Uint32(key)]++
				}
			})
		}
	})

	out := make([]eventCount, groups)
	for g := range out {
		out[g].event = string(shared.table.Key(g))
		for i := range mergers {
			out[g].users += mergers[i].users[g]
		}
	}
	for _, w := range ws {
		for local, g := range w.global {
			out[g].count += w.counts[local]
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].count != out[j].count {
			return out[i].count > out[j].count
		}

		return out[i].event < out[j].event
	})

	return out, nil
}

type hourCount struct {
	event string
	hour  int
	count int64
}

var feedEvents = []string{"app.bsky.feed.like", "app.bsky.feed.post", "app.bsky.feed.repost"}

// queryEventsByHour is Q3: count() by collection and hour of day for the
// three feed collections.
func queryEventsByHour(snap *Snapshot, workers int) ([]hourCount, error) {
	ws := newBenchWorkers(workers)
	defer freeBenchWorkers(ws)
	_, err := snap.Scan(context.Background(), ScanOptions{Workers: workers}, func(n int, p *part.Part, b *part.Block) error {
		w := ws[n]
		if ok, err := w.created(p, b); !ok || err != nil {
			return err
		}
		if w.idx = vec.MaskIndexes(w.idx[:0], w.mask); len(w.idx) == 0 {
			return nil
		}
		if ref, err := column(p, b, "commit.collection", vec.KindString, &w.a); err != nil || ref == nil {
			return err
		}
		if ref, err := column(p, b, "time_us", vec.KindInt, &w.c); err != nil || ref == nil {
			return err
		}
		// slot maps a dictionary code to the event's position, or -1.
		c := &w.a
		w.codeGroup = w.codeGroup[:0]
		if c.Layout != vec.Flat {
			for code := 0; code < c.DictLen(); code++ {
				w.codeGroup = append(w.codeGroup, int32(feedEvent(c.DictValue(code))))
			}
		}
		for _, r := range w.idx {
			row := int(r)
			if !c.IsValid(row) || !w.c.IsValid(row) {
				continue
			}
			var slot int
			switch c.Layout {
			case vec.Dict:
				slot = int(w.codeGroup[c.Code(row)])
			case vec.Const:
				slot = int(w.codeGroup[0])
			default:
				slot = feedEvent(c.Bytes(row))
			}
			if slot < 0 {
				continue
			}
			hour := w.c.Int(row) / 3_600_000_000 % 24
			w.hours[slot*24+int(hour)]++
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	var out []hourCount
	for hour := 0; hour < 24; hour++ {
		for slot, name := range feedEvents {
			var total int64
			for _, w := range ws {
				total += w.hours[slot*24+hour]
			}
			if total > 0 {
				out = append(out, hourCount{event: name, hour: hour, count: total})
			}
		}
	}

	return out, nil
}

func feedEvent(name []byte) int {
	for i, e := range feedEvents {
		if string(name) == e {
			return i
		}
	}

	return -1
}

type userSpan struct {
	did      string
	min, max int64
}

// spanMillis is the time between a user's first and last post.
func spanMillis(lo, hi int64) int64 { return hi/1000 - lo/1000 }

type spanEntry struct {
	did      []byte
	min, max int64
}

// earlier orders by first post, then by did.
func earlier(a, b *spanEntry) bool {
	if a.min != b.min {
		return a.min < b.min
	}

	return bytes.Compare(a.did, b.did) < 0
}

// longer orders by the longest time between first and last post, then by did.
func longer(a, b *spanEntry) bool {
	if sa, sb := spanMillis(a.min, a.max), spanMillis(b.min, b.max); sa != sb {
		return sa > sb
	}

	return bytes.Compare(a.did, b.did) < 0
}

// topSpans keeps the n best entries seen, best first, ordered by longer when
// byLength is set and by earlier otherwise; it copies the did only of an
// entry it keeps.
type topSpans struct {
	byLength bool
	best     []spanEntry
	n        int
}

func (t *topSpans) less(a, b *spanEntry) bool {
	if t.byLength {
		return longer(a, b)
	}

	return earlier(a, b)
}

func (t *topSpans) offer(e spanEntry) {
	if len(t.best) == t.n && !t.less(&e, &t.best[t.n-1]) {
		return
	}
	var did []byte
	if len(t.best) < t.n {
		t.best = append(t.best, spanEntry{})
	} else {
		did = t.best[t.n-1].did
	}
	i := len(t.best) - 1
	for ; i > 0 && t.less(&e, &t.best[i-1]); i-- {
		t.best[i] = t.best[i-1]
	}
	e.did = append(did[:0], e.did...)
	t.best[i] = e
}

func (t *topSpans) spans() []userSpan {
	out := make([]userSpan, len(t.best))
	for i, e := range t.best {
		out[i] = userSpan{did: string(e.did), min: e.min, max: e.max}
	}

	return out
}

// forEachPartition calls fn for every partition from up to workers
// goroutines, passing each goroutine's number so it can own a merge state.
func forEachPartition(partitions, workers int, fn func(slot, partition int)) {
	workers = min(workers, partitions)
	var next atomic.Int64
	var wg sync.WaitGroup
	for slot := 0; slot < workers; slot++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= partitions {
					return
				}
				fn(slot, i)
			}
		}(slot)
	}
	wg.Wait()
}

// addSpan logs the first and last post time of a run of rows with one did.
func (w *benchWorker) addSpan(did []byte, lo, hi int64) {
	w.key = binary.LittleEndian.AppendUint64(append(w.key[:0], did...), uint64(lo))
	w.key = binary.LittleEndian.AppendUint64(w.key, uint64(hi))
	w.log.Add(vec.Hash(did), w.key)
}

// spanMerger is the state one goroutine owns while it merges partitions.
type spanMerger struct {
	arena          *mem.Arena
	groups         *vec.ByteTable
	mins, maxs     []int64
	first, longest topSpans
}

// queryPostTimes answers Q4 and Q5 in one pass: the limit users with the
// earliest first created post, and the limit users with the longest time
// between their first and last one.
func queryPostTimes(snap *Snapshot, workers, limit int) (first, longest []userSpan, err error) {
	ws := newBenchWorkers(workers)
	defer freeBenchWorkers(ws)
	for _, w := range ws {
		w.log = vec.NewKeyPartitions(w.arena, partitionBits)
	}
	_, err = snap.Scan(context.Background(), ScanOptions{Workers: workers}, func(n int, p *part.Part, b *part.Block) error {
		w := ws[n]
		if ok, err := w.created(p, b); !ok || err != nil {
			return err
		}
		if ok, err := w.eq(p, b, "commit.collection", "app.bsky.feed.post", &w.mask2); !ok || err != nil {
			return err
		}
		vec.MaskAnd(w.mask, w.mask, w.mask2)
		if w.idx = vec.MaskIndexes(w.idx[:0], w.mask); len(w.idx) == 0 {
			return nil
		}
		if ref, err := column(p, b, "did", vec.KindString, &w.b); err != nil || ref == nil {
			return err
		}
		if ref, err := column(p, b, "time_us", vec.KindInt, &w.c); err != nil || ref == nil {
			return err
		}
		// A run of rows with one did adds one record.
		var did []byte
		var lo, hi int64
		open := false
		for _, r := range w.idx {
			row := int(r)
			if !w.b.IsValid(row) || !w.c.IsValid(row) {
				continue
			}
			ts := w.c.Int(row)
			if cur := w.b.Bytes(row); !open || string(cur) != string(did) {
				if open {
					w.addSpan(did, lo, hi)
				}
				did, lo, hi, open = cur, ts, ts, true

				continue
			}
			lo, hi = min(lo, ts), max(hi, ts)
		}
		if open {
			w.addSpan(did, lo, hi)
		}

		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	mergers := make([]spanMerger, workers)
	for i := range mergers {
		m := &mergers[i]
		m.arena = mem.NewArena()
		defer m.arena.Free()
		m.groups = vec.NewByteTable(m.arena)
		m.first = topSpans{n: limit}
		m.longest = topSpans{byLength: true, n: limit}
	}
	// A did lives in one partition, so partitions aggregate independently.
	forEachPartition(1<<partitionBits, workers, func(slot, partition int) {
		m := &mergers[slot]
		m.groups.Clear()
		m.mins, m.maxs = m.mins[:0], m.maxs[:0]
		for _, w := range ws {
			w.log.Each(partition, func(h uint64, rec []byte) {
				n := len(rec) - 16
				lo, hi := int64(binary.LittleEndian.Uint64(rec[n:])), int64(binary.LittleEndian.Uint64(rec[n+8:]))
				g := m.groups.IDHashed(rec[:n], h)
				if g == len(m.mins) {
					m.mins, m.maxs = append(m.mins, lo), append(m.maxs, hi)

					return
				}
				m.mins[g], m.maxs[g] = min(m.mins[g], lo), max(m.maxs[g], hi)
			})
		}
		for g := range m.mins {
			e := spanEntry{did: m.groups.Key(g), min: m.mins[g], max: m.maxs[g]}
			m.first.offer(e)
			m.longest.offer(e)
		}
	})
	all := spanMerger{first: topSpans{n: limit}, longest: topSpans{byLength: true, n: limit}}
	for i := range mergers {
		for _, e := range mergers[i].first.best {
			all.first.offer(e)
		}
		for _, e := range mergers[i].longest.best {
			all.longest.offer(e)
		}
	}

	return all.first.spans(), all.longest.spans(), nil
}

// loadNDJSON shreds data on every core, one part per worker and one block
// per blockRows lines, and returns the table, its size in bytes and the
// number of lines that are not JSON objects.
func loadNDJSON(t testing.TB, data []byte, workers, blockRows int) (*Table, int64, int64) {
	t.Helper()
	chunks := make([][]byte, 0, workers)
	for rest := data; len(rest) > 0; {
		n := min(len(data)/workers+1, len(rest))
		if n < len(rest) {
			if nl := bytes.IndexByte(rest[n:], '\n'); nl >= 0 {
				n += nl + 1
			} else {
				n = len(rest)
			}
		}
		chunks = append(chunks, rest[:n])
		rest = rest[n:]
	}
	handles := make([]*Handle, len(chunks))
	var rejected atomic.Int64
	var wg sync.WaitGroup
	for i, chunk := range chunks {
		wg.Add(1)
		go func(i int, chunk []byte) {
			defer wg.Done()
			var buf bytes.Buffer
			buf.Grow(len(chunk) / 3)
			w := part.NewWriter(&buf, part.Options{})
			batch := shred.Get(shred.Options{TimeField: "time_us"})
			defer shred.Put(batch)
			var errs []shred.LineError
			for len(chunk) > 0 {
				var lines []byte
				lines, chunk = shred.SplitLines(chunk, blockRows)
				errs = batch.AddNDJSON(lines, 0, errs[:0])
				rejected.Add(int64(len(errs)))
				if err := w.WriteBatch(batch); err != nil {
					t.Error(err)
				}
				batch.Reset()
			}
			if err := w.Close(); err != nil {
				t.Error(err)
			}
			p, err := part.Open(buf.Bytes())
			if err != nil {
				t.Error(err)

				return
			}
			handles[i] = NewHandle(p, nil)
		}(i, chunk)
	}
	wg.Wait()
	tbl := NewTable()
	if err := tbl.Apply(handles, nil); err != nil {
		t.Fatal(err)
	}
	var size int64
	for _, h := range handles {
		size += int64(h.Size())
	}

	return tbl, size, rejected.Load()
}

// constantBlocks counts the blocks in which the string column of path holds
// one value, and the blocks that do not have the column.
func constantBlocks(t testing.TB, snap *Snapshot, path string) (constant, absent int) {
	t.Helper()
	var b part.Block
	for _, h := range snap.Parts() {
		id, known := h.PathID(path)
		for i := 0; i < h.NumBlocks(); i++ {
			if err := h.OpenBlock(i, &b); err != nil {
				t.Fatal(err)
			}
			ref, ok := b.Column(id, vec.KindString)
			switch {
			case !known || !ok:
				absent++
			case ref.Const():
				constant++
			}
		}
	}

	return constant, absent
}

// envInt reads a positive integer setting of the benchmark.
func envInt(t testing.TB, name string, def int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("%s=%q is not a positive integer", name, v)
	}

	return n
}

// timeRuns runs fn n times and returns the durations in ascending order and
// the process CPU time of the shortest run, writing a CPU profile of the runs
// to profile when it is not empty.
func timeRuns(n int, profile string, fn func() error) (took []time.Duration, cpu time.Duration, err error) {
	if profile != "" {
		f, err := os.Create(profile)
		if err != nil {
			return nil, 0, err
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			return nil, 0, err
		}
		defer pprof.StopCPUProfile()
	}
	took = make([]time.Duration, n)
	best := time.Duration(-1)
	for i := range took {
		cpuBefore, begin := cpuTime(), time.Now()
		if err := fn(); err != nil {
			return nil, 0, err
		}
		took[i] = time.Since(begin)
		if used := cpuTime() - cpuBefore; best < 0 || took[i] < best {
			best, cpu = took[i], used
		}
	}
	sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })

	return took, cpu, nil
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1e3 }

// TestJSONBench times the queries over the file named by LYNXDB_BENCH_NDJSON.
// LYNXDB_BENCH_QUERY keeps the queries whose name contains it,
// LYNXDB_BENCH_RUNS sets the timed runs per query, LYNXDB_BENCH_BLOCK_ROWS
// the rows per block, and LYNXDB_BENCH_PROFILE names a directory that
// receives one CPU profile per query and worker count.
func TestJSONBench(t *testing.T) {
	path := os.Getenv("LYNXDB_BENCH_NDJSON")
	if path == "" {
		t.Skip("set LYNXDB_BENCH_NDJSON to a Bluesky NDJSON file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cores := runtime.GOMAXPROCS(0)
	runs := envInt(t, "LYNXDB_BENCH_RUNS", 7)
	blockRows := envInt(t, "LYNXDB_BENCH_BLOCK_ROWS", 16384)
	only := os.Getenv("LYNXDB_BENCH_QUERY")
	profileDir := os.Getenv("LYNXDB_BENCH_PROFILE")

	start := time.Now()
	tbl, size, rejected := loadNDJSON(t, data, cores, blockRows)
	loadTime := time.Since(start)
	defer tbl.Close()
	snap := tbl.Snapshot()
	defer snap.Release()
	var rows uint64
	blocks := 0
	for _, h := range snap.Parts() {
		rows += h.Rows
		blocks += h.NumBlocks()
	}
	t.Logf("load: %d rows and %d rejected lines, %.1f MB of JSON -> %.1f MB in %d parts / %d blocks, %.2fs on %d cores (%.0fk docs/s, %.0f MB/s)",
		rows, rejected, float64(len(data))/1e6, float64(size)/1e6, len(snap.Parts()), blocks, loadTime.Seconds(), cores,
		float64(rows)/loadTime.Seconds()/1e3, float64(len(data))/loadTime.Seconds()/1e6)

	layout := ""
	for _, path := range []string{"kind", "commit.operation", "commit.collection"} {
		constant, absent := constantBlocks(t, snap, path)
		layout += fmt.Sprintf(" %s %d constant and %d absent,", path, constant, absent)
	}
	t.Logf("of %d blocks:%s", blocks, strings.TrimSuffix(layout, ","))

	var q1, q2, q2h []eventCount
	var q3 []hourCount
	var first, longest []userSpan
	queries := []struct {
		id, name string
		run      func(workers int) error
	}{
		{"q1", "Q1 top events", func(n int) (err error) { q1, err = queryTopEvents(snap, n); return }},
		{"q2", "Q2 events with distinct users", func(n int) (err error) { q2, err = queryEventsWithUsers(snap, n, false); return }},
		{"q2h", "Q2 with users by 128-bit hash", func(n int) (err error) { q2h, err = queryEventsWithUsers(snap, n, true); return }},
		{"q3", "Q3 events by hour", func(n int) (err error) { q3, err = queryEventsByHour(snap, n); return }},
		{"q4", "Q4/Q5 post times per user", func(n int) (err error) { first, longest, err = queryPostTimes(snap, n, 3); return }},
	}
	ran := map[string]bool{}
	for _, q := range queries {
		if !strings.Contains(q.name, only) {
			continue
		}
		id := q.id
		ran[id] = true
		var took [2][]time.Duration
		var cpu [2]time.Duration
		for i, workers := range []int{cores, 1} {
			profile := ""
			if profileDir != "" {
				profile = filepath.Join(profileDir, fmt.Sprintf("%s_%dw.prof", id, workers))
			}
			took[i], cpu[i], err = timeRuns(runs, profile, func() error { return q.run(workers) })
			if err != nil {
				t.Fatalf("%s: %v", q.name, err)
			}
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		if err := q.run(cores); err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&after)
		all, one := took[0], took[1]
		t.Logf("%-30s %7.2f ms on %d cores (median %.2f, max %.2f, cpu %.1f), %7.2f ms on 1 core (median %.2f, max %.2f, cpu %.1f), %d heap allocations (%.1f MB)",
			q.name, ms(all[0]), cores, ms(all[runs/2]), ms(all[runs-1]), ms(cpu[0]),
			ms(one[0]), ms(one[runs/2]), ms(one[runs-1]), ms(cpu[1]),
			after.Mallocs-before.Mallocs, float64(after.TotalAlloc-before.TotalAlloc)/1e6)
	}

	if rows != 1_000_000 {
		t.Logf("results are only checked for the first benchmark file (1,000,000 rows)")

		return
	}
	// Reference results produced by ClickHouse on the same file.
	if ran["q1"] {
		want := "app.bsky.feed.like=448944 app.bsky.graph.follow=360374 app.bsky.feed.post=90816 app.bsky.feed.repost=58540 " +
			"app.bsky.graph.block=14040 app.bsky.actor.profile=11762 app.bsky.graph.listitem=8103 =5328 " +
			"app.bsky.graph.listblock=895 app.bsky.graph.starterpack=405 app.bsky.graph.list=356 app.bsky.feed.threadgate=255 " +
			"app.bsky.feed.postgate=104 app.bsky.feed.generator=74 app.bsky.labeler.service=4"
		got := ""
		for i, e := range q1 {
			got += fmt.Sprintf("%s%s=%d", sep(i), e.event, e.count)
		}
		if got != want {
			t.Errorf("Q1\n got  %s\n want %s", got, want)
		}
	}
	for _, q := range []struct {
		id  string
		got []eventCount
	}{{"q2", q2}, {"q2h", q2h}} {
		if !ran[q.id] {
			continue
		}
		want := "app.bsky.feed.like=444523/117617 app.bsky.graph.follow=337978/63957 app.bsky.feed.post=86812/50464 " +
			"app.bsky.feed.repost=56993/26581 app.bsky.graph.block=13838/5785 app.bsky.graph.listitem=7568/1078 " +
			"app.bsky.actor.profile=5337/5337 app.bsky.graph.listblock=860/449 app.bsky.graph.list=259/218 " +
			"app.bsky.feed.threadgate=228/196 app.bsky.graph.starterpack=104/101 app.bsky.feed.postgate=101/82 app.bsky.feed.generator=10/9"
		got := ""
		for i, e := range q.got {
			got += fmt.Sprintf("%s%s=%d/%d", sep(i), e.event, e.count, e.users)
		}
		if got != want {
			t.Errorf("%s\n got  %s\n want %s", q.id, got, want)
		}
	}
	if ran["q3"] {
		got := ""
		for i, e := range q3 {
			got += fmt.Sprintf("%s%s@%d=%d", sep(i), e.event, e.hour, e.count)
		}
		if want := "app.bsky.feed.like@16=444523 app.bsky.feed.post@16=86812 app.bsky.feed.repost@16=56993"; got != want {
			t.Errorf("Q3\n got  %s\n want %s", got, want)
		}
	}
	if !ran["q4"] {
		return
	}
	gotQ4 := ""
	for i, s := range first {
		gotQ4 += fmt.Sprintf("%s%s@%d", sep(i), s.did, s.min)
	}
	if want := "did:plc:yj3sjq3blzpynh27cumnp5ks@1732206349000167 did:plc:l5o3qjrmfztir54cpwlv2eme@1732206349001905 did:plc:s4bwqchfzm6gjqfeb6mexgbu@1732206349003907"; gotQ4 != want {
		t.Errorf("Q4\n got  %s\n want %s", gotQ4, want)
	}
	gotQ5 := ""
	for i, s := range longest {
		gotQ5 += fmt.Sprintf("%s%s=%d", sep(i), s.did, spanMillis(s.min, s.max))
	}
	if want := "did:plc:tsyymlun4eqjuw7hqrhmwagd=813007 did:plc:3ug235sfy2pz7cawmpsftb65=811602 did:plc:doxhhgtxqiv47tmcovpbcqai=811404"; gotQ5 != want {
		t.Errorf("Q5\n got  %s\n want %s", gotQ5, want)
	}
}

func sep(i int) string {
	if i == 0 {
		return ""
	}

	return " "
}
