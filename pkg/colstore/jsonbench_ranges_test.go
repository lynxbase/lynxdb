package colstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// The grouped queries over parts ordered by their filter and grouping paths:
// every worker merges the sorted streams of one key range, so the rows of a
// group meet without a table.

var (
	createdKey = KeyFilter{
		Paths:  []string{"kind", "commit.operation"},
		Values: [][]byte{[]byte("commit"), []byte("create")},
	}
	postsKey = KeyFilter{
		Paths:  []string{"kind", "commit.operation", "commit.collection"},
		Values: [][]byte{[]byte("commit"), []byte("create"), []byte("app.bsky.feed.post")},
	}
)

// rangeRows is the number of rows a key range is cut to hold: the smaller a
// range, the fewer blocks the ranges in progress keep decoded between them.
const rangeRows = 1 << 16

// keyRanges cuts a plan into ranges for workers, several per worker so that
// they balance.
func keyRanges(plan *KeyPlan, workers int, columns ...ColumnID) *RangeScan {
	return plan.Ranges(max(4*workers, plan.Rows/rangeRows), columns...)
}

// rangeCursor walks the values of one stream of a key range in key order.
type rangeCursor struct {
	r RangeReader
	// col holds the dids of the current block and cur the current one, which
	// belongs to group gid.
	col *vec.Column
	cur []byte
	gid int
	// h0 and h1 hold the first sixteen bytes of cur, so that most
	// comparisons of two cursors read nothing else.
	h0, h1 uint64
	// The values left in the block are col's entries i to end, or, with
	// indexed set, its entries idx[i:end], of the groups gids[i:end] if any.
	i, end  int
	indexed bool
	idx     []uint32
	gids    []int32
	// unique marks values that strictly ascend.
	unique bool
	// time holds the post times of the block, and lo and hi the first and
	// last time of the current run of rows with one did.
	time   *vec.Column
	lo, hi int64
}

// entry returns the position in col of the value of row.
func entry(col *vec.Column, row int) int {
	switch col.Layout {
	case vec.Dict:
		return col.Code(row)
	case vec.Const:
		return 0
	}

	return row
}

// set loads the value at i.
func (c *rangeCursor) set() {
	k := c.i
	if c.indexed {
		k = int(c.idx[c.i])
		if len(c.gids) != 0 {
			c.gid = int(c.gids[c.i])
		}
	}
	c.head(c.col.Data[c.col.Offsets[k]:c.col.Offsets[k+1]])
}

// head makes v the current value.
func (c *rangeCursor) head(v []byte) {
	c.cur = v
	if len(v) >= 16 {
		c.h0, c.h1 = binary.BigEndian.Uint64(v), binary.BigEndian.Uint64(v[8:])

		return
	}
	var pad [16]byte
	copy(pad[:], v)
	c.h0, c.h1 = binary.BigEndian.Uint64(pad[:]), binary.BigEndian.Uint64(pad[8:])
}

// nextSpan moves to the next run of rows i to end that share a did and have
// a time, and reports false when no such row is left in the block.
func (c *rangeCursor) nextSpan() bool {
	did, ts := c.col, c.time
	row := c.i
	for row < c.end && (!did.IsValid(row) || !ts.IsValid(row)) {
		row++
	}
	if row == c.end {
		c.i = row

		return false
	}
	k := entry(did, row)
	c.head(did.Data[did.Offsets[k]:did.Offsets[k+1]])
	lo := ts.Int(row)
	hi := lo
	flat := did.Layout == vec.Flat
	for row++; row < c.end; row++ {
		if !did.IsValid(row) {
			continue
		}
		if flat {
			if string(did.Data[did.Offsets[row]:did.Offsets[row+1]]) != string(c.cur) {
				break
			}
		} else if entry(did, row) != k {
			break
		}
		if ts.IsValid(row) {
			t := ts.Int(row)
			lo, hi = min(lo, t), max(hi, t)
		}
	}
	c.i, c.lo, c.hi = row, lo, hi

	return true
}

// rangeWorker is the state one goroutine owns while it aggregates key
// ranges.
type rangeWorker struct {
	*benchWorker
	scan *RangeScan
	// groupPath and didPath are the positions of the grouping paths in the
	// sort key; groupPath is negative when the did alone is the group.
	groupPath, didPath int
	users              []int64
	cursors            []*rangeCursor
	order, heap        []*rangeCursor
	turn               int
	// lastGroup and last are the latest key the merge has seen, and lo and
	// hi the first and last time of its rows.
	lastGroup      int
	last           []byte
	lo, hi         int64
	first, longest topSpans
}

func newRangeWorkers(scan *RangeScan, n, groupPath, didPath, limit int) []*rangeWorker {
	ws := make([]*rangeWorker, n)
	for i, w := range newBenchWorkers(n) {
		ws[i] = &rangeWorker{
			benchWorker: w, scan: scan, groupPath: groupPath, didPath: didPath, turn: i,
			first: topSpans{n: limit}, longest: topSpans{byLength: true, n: limit},
		}
	}

	return ws
}

// start points a cursor of the worker at every stream of kr and returns
// them. Workers begin at different streams, so that those whose ranges share
// blocks decode different ones instead of waiting for one another.
func (w *rangeWorker) start(kr *KeyRange) []*rangeCursor {
	for len(w.cursors) < len(kr.Streams) {
		w.cursors = append(w.cursors, &rangeCursor{})
	}
	w.order = w.order[:0]
	for s := range kr.Streams {
		c := w.cursors[(s+w.turn)%len(kr.Streams)]
		c.r.Reset(w.scan, kr, (s+w.turn)%len(kr.Streams))
		w.order = append(w.order, c)
	}
	w.turn++
	w.heap = w.heap[:0]

	return w.order
}

func (w *rangeWorker) less(a, b *rangeCursor) bool {
	if a.gid != b.gid {
		return bytes.Compare(w.groups.Key(a.gid), w.groups.Key(b.gid)) < 0
	}

	if a.h0 != b.h0 {
		return a.h0 < b.h0
	}
	if a.h1 != b.h1 {
		return a.h1 < b.h1
	}

	return bytes.Compare(a.cur, b.cur) < 0
}

// down restores the heap order below position i.
func (w *rangeWorker) down(i int) {
	h := w.heap
	for {
		l := 2*i + 1
		if l >= len(h) {
			return
		}
		if r := l + 1; r < len(h) && w.less(h[r], h[l]) {
			l = r
		}
		if !w.less(h[l], h[i]) {
			return
		}
		h[i], h[l] = h[l], h[i]
		i = l
	}
}

func (w *rangeWorker) heapify() {
	for i := len(w.heap)/2 - 1; i >= 0; i-- {
		w.down(i)
	}
}

// pop drops the cursor at the top of the heap.
func (w *rangeWorker) pop() {
	n := len(w.heap) - 1
	w.heap[0] = w.heap[n]
	w.heap = w.heap[:n]
}

// loadDids moves c to its next block that has a did inside the range, adds
// the rows of the block to the counts of their groups, and reports false at
// the end of the stream.
func (w *rangeWorker) loadDids(c *rangeCursor, b *part.Block) (bool, error) {
	for {
		ok, err := c.r.Next(b)
		if !ok || err != nil {
			return false, err
		}
		from, to := c.r.From, c.r.To
		w.blocks++
		w.rows += int64(to - from)
		keys := &c.r.Block.Keys
		coll := keys.Column(w.groupPath, vec.KindString)
		if coll == nil {
			continue
		}
		did := keys.Column(w.didPath, vec.KindString)
		c.col, c.i, c.indexed, c.unique = did, 0, true, false
		c.idx, c.gids = c.idx[:0], c.gids[:0]
		if coll.Layout != vec.Const || coll.Valid != nil {
			w.resetCodeGroups(coll)
			for row := from; row < to; row++ {
				if !coll.IsValid(row) {
					continue
				}
				g := w.groupOfRow(coll, row)
				w.counts[g]++
				if did != nil && did.IsValid(row) {
					c.idx, c.gids = append(c.idx, uint32(entry(did, row))), append(c.gids, int32(g))
				}
			}
		} else {
			c.gid = w.group(coll.DictValue(0))
			w.counts[c.gid] += int64(to - from)
			switch {
			case did == nil:
			case did.Layout == vec.Dict && from == 0 && to == c.r.Block.Rows:
				// The dictionary lists the dids of the whole block in order.
				c.indexed, c.unique, c.end = false, true, did.DictLen()
			case did.Layout == vec.Flat && did.Valid == nil:
				c.indexed, c.i, c.end = false, from, to
			default:
				c.unique = did.Layout != vec.Flat
				prev := -1
				for row := from; row < to; row++ {
					if k := entry(did, row); did.IsValid(row) && k != prev {
						c.idx = append(c.idx, uint32(k))
						prev = k
					}
				}
			}
		}
		for len(w.users) < len(w.counts) {
			w.users = append(w.users, 0)
		}
		if c.indexed {
			c.end = len(c.idx)
		}
		if c.i < c.end {
			c.set()

			return true, nil
		}
	}
}

// countUsers adds to the worker's groups the rows of kr and the distinct
// dids among them.
func (w *rangeWorker) countUsers(kr *KeyRange, b *part.Block) error {
	for _, c := range w.start(kr) {
		ok, err := w.loadDids(c, b)
		if err != nil {
			return err
		}
		if ok {
			w.heap = append(w.heap, c)
		}
	}
	w.heapify()
	w.lastGroup = -1
	for len(w.heap) > 1 {
		c := w.heap[0]
		if c.gid != w.lastGroup || string(c.cur) != string(w.last) {
			w.users[c.gid]++
			w.lastGroup, w.last = c.gid, append(w.last[:0], c.cur...)
		}
		if c.i++; c.i < c.end {
			c.set()
		} else if ok, err := w.loadDids(c, b); err != nil {
			return err
		} else if !ok {
			w.pop()
		}
		w.down(0)
	}
	if len(w.heap) == 0 {
		return nil
	}
	// The last stream needs no merge.
	for c := w.heap[0]; ; {
		group, last := w.lastGroup, w.last
		if c.unique {
			n := c.end - c.i
			if c.gid == group && string(c.cur) == string(last) {
				n--
			}
			w.users[c.gid] += int64(n)
			c.i = c.end - 1
			c.set()
			group, last = c.gid, c.cur
		} else {
			for ; c.i < c.end; c.i++ {
				c.set()
				if c.gid != group || string(c.cur) != string(last) {
					w.users[c.gid]++
					group, last = c.gid, c.cur
				}
			}
		}
		// The next block overwrites the column that last points into.
		w.lastGroup, w.last = group, append(w.last[:0], last...)
		if ok, err := w.loadDids(c, b); !ok || err != nil {
			return err
		}
	}
}

// eventsWithUsersOrdered is Q2 over parts ordered by the filter paths, the
// collection and the did.
func eventsWithUsersOrdered(snap *Snapshot, plan *KeyPlan, workers int) ([]eventCount, error) {
	scan := keyRanges(plan, workers)
	ws := newRangeWorkers(scan, workers, len(createdKey.Paths), len(createdKey.Paths)+1, 0)
	defer freeRangeWorkers(ws)
	err := scan.Run(context.Background(), workers, func(n int, kr *KeyRange, b *part.Block) error {
		return ws[n].countUsers(kr, b)
	})
	if err != nil {
		return nil, err
	}
	recordRanges(snap, plan, scan, ws)
	merged := map[string]*eventCount{}
	for _, w := range ws {
		for id := 0; id < w.groups.Len(); id++ {
			name := string(w.groups.Key(id))
			if merged[name] == nil {
				merged[name] = &eventCount{event: name}
			}
			merged[name].count += w.counts[id]
			merged[name].users += w.users[id]
		}
	}

	return sortedCounts(merged), nil
}

// loadSpans moves c to its next block that has a post with a did and a time
// inside the range, and reports false at the end of the stream.
func (w *rangeWorker) loadSpans(c *rangeCursor, b *part.Block) (bool, error) {
	for {
		ok, err := c.r.Next(b)
		if !ok || err != nil {
			return false, err
		}
		w.blocks++
		w.rows += int64(c.r.To - c.r.From)
		did, ts := c.r.Block.Keys.Column(w.didPath, vec.KindString), c.r.Block.Column(0)
		if did == nil || ts == nil {
			continue
		}
		c.col, c.time, c.i, c.end = did, ts, c.r.From, c.r.To
		if c.nextSpan() {
			return true, nil
		}
	}
}

// offerSpan hands the did the merge has finished to the worker's results.
func (w *rangeWorker) offerSpan() {
	e := spanEntry{did: w.last, min: w.lo, max: w.hi}
	w.first.offer(e)
	w.longest.offer(e)
}

// mergeSpans offers every did of kr with its first and last post time.
func (w *rangeWorker) mergeSpans(kr *KeyRange, b *part.Block) error {
	for _, c := range w.start(kr) {
		ok, err := w.loadSpans(c, b)
		if err != nil {
			return err
		}
		if ok {
			w.heap = append(w.heap, c)
		}
	}
	w.heapify()
	open := false
	for len(w.heap) > 0 {
		c := w.heap[0]
		if open && string(c.cur) == string(w.last) {
			w.lo, w.hi = min(w.lo, c.lo), max(w.hi, c.hi)
		} else {
			if open {
				w.offerSpan()
			}
			w.last, w.lo, w.hi, open = append(w.last[:0], c.cur...), c.lo, c.hi, true
		}
		if !c.nextSpan() {
			if ok, err := w.loadSpans(c, b); err != nil {
				return err
			} else if !ok {
				w.pop()
			}
		}
		w.down(0)
	}
	if open {
		w.offerSpan()
	}

	return nil
}

// postTimesOrdered is Q4 and Q5 over parts ordered by the filter paths and
// the did.
func postTimesOrdered(snap *Snapshot, plan *KeyPlan, workers, limit int) (first, longest []userSpan, err error) {
	scan := keyRanges(plan, workers, ColumnID{Path: "time_us", Kind: vec.KindInt})
	ws := newRangeWorkers(scan, workers, -1, len(postsKey.Paths), limit)
	defer freeRangeWorkers(ws)
	err = scan.Run(context.Background(), workers, func(n int, kr *KeyRange, b *part.Block) error {
		return ws[n].mergeSpans(kr, b)
	})
	if err != nil {
		return nil, nil, err
	}
	recordRanges(snap, plan, scan, ws)
	all := rangeWorker{first: topSpans{n: limit}, longest: topSpans{byLength: true, n: limit}}
	for _, w := range ws {
		for _, e := range w.first.best {
			all.first.offer(e)
		}
		for _, e := range w.longest.best {
			all.longest.offer(e)
		}
	}

	return all.first.spans(), all.longest.spans(), nil
}

func freeRangeWorkers(ws []*rangeWorker) {
	for _, w := range ws {
		w.arena.Free()
	}
}

// recordRanges stores in benchMemory and benchScan what the workers of a
// query over key ranges hold and read.
func recordRanges(snap *Snapshot, plan *KeyPlan, scan *RangeScan, ws []*rangeWorker) {
	arenas, buffers := 0, scan.Retained()
	var blocks, rows int64
	for _, w := range ws {
		arenas += w.arena.Size()
		buffers += cap(w.last) + 4*cap(w.codeGroup)
		for _, c := range w.cursors {
			buffers += 4*cap(c.idx) + 4*cap(c.gids)
		}
		blocks += w.blocks
		rows += w.rows
	}
	total := 0
	for _, h := range snap.Parts() {
		total += h.NumBlocks()
	}
	benchMemory.arenas.Store(int64(arenas))
	benchMemory.buffers.Store(int64(buffers))
	benchScan.opened.Store(scan.Decoded.Load())
	benchScan.skippedByKey.Store(int64(total - plan.Blocks))
	benchScan.blocks.Store(blocks)
	benchScan.rows.Store(rows)
}

func TestOrderedQueriesMatchHashed(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	collections := []string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow", "app.bsky.feed.repost"}
	var data bytes.Buffer
	for i := 0; i < 20000; i++ {
		kind := []string{"commit", "commit", "commit", "commit", "identity"}[rng.Intn(5)]
		commit := fmt.Sprintf(`"operation":%q`, []string{"create", "create", "create", "delete"}[rng.Intn(4)])
		if rng.Intn(40) != 0 {
			commit += fmt.Sprintf(`,"collection":%q`, collections[rng.Intn(1+rng.Intn(len(collections)))])
		}
		did := fmt.Sprintf(`"did:plc:%04d"`, rng.Intn(1+rng.Intn(700)))
		switch rng.Intn(60) {
		case 0:
			did = "17"
		case 1:
			did = "null"
		}
		fmt.Fprintf(&data, `{"did":%s,"kind":%q,"commit":{%s}`, did, kind, commit)
		if rng.Intn(50) != 0 {
			fmt.Fprintf(&data, `,"time_us":%d`, 1_700_000_000_000_000+int64(rng.Intn(1_000_000_000)))
		}
		data.WriteString("}\n")
	}
	key := []string{"kind", "commit.operation", "commit.collection", "did"}
	for _, merged := range []bool{true, false} {
		for _, runs := range []int{1, 6} {
			tbl := orderedTable(t, data.Bytes(), runs, 256, key, merged)
			snap := tbl.Snapshot()
			users, ok := snap.PlanKeyRanges(createdKey, key[2:])
			posts, ok2 := snap.PlanKeyRanges(postsKey, key[3:])
			if !ok || !ok2 {
				t.Fatal("ordered parts were not planned")
			}
			wantUsers, err := eventsWithUsersHashed(snap, 2, false)
			if err != nil {
				t.Fatal(err)
			}
			wantFirst, wantLongest, err := postTimesHashed(snap, 2, 5)
			if err != nil {
				t.Fatal(err)
			}
			if len(wantUsers) < len(collections) || len(wantFirst) != 5 {
				t.Fatalf("%d groups and %d users to compare", len(wantUsers), len(wantFirst))
			}
			for _, workers := range []int{1, 4} {
				label := fmt.Sprintf("merged %v, %d runs, %d workers", merged, runs, workers)
				gotUsers, err := eventsWithUsersOrdered(snap, users, workers)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(gotUsers, wantUsers) {
					t.Errorf("%s:\n got  %v\n want %v", label, gotUsers, wantUsers)
				}
				gotFirst, gotLongest, err := postTimesOrdered(snap, posts, workers, 5)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(gotFirst, wantFirst) || !reflect.DeepEqual(gotLongest, wantLongest) {
					t.Errorf("%s:\n got  %v %v\n want %v %v", label, gotFirst, gotLongest, wantFirst, wantLongest)
				}
			}
			snap.Release()
			tbl.Close()
		}
	}
}
