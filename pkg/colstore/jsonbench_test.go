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

const distinctPartitions = 64

// benchWorker is the state one scan goroutine owns for one query.
type benchWorker struct {
	arena  *mem.Arena
	groups *vec.ByteTable
	counts []int64
	mins   []int64
	maxs   []int64
	nulls  int64
	// distinct holds (group hash, value) keys, split by hash so that the
	// merge of each partition can run on its own core.
	distinct [distinctPartitions]*vec.ByteTable
	hours    [3 * 24]int64

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

// queryEventsWithUsers is Q2: count() and the exact number of distinct did
// by commit.collection, over created commits.
func queryEventsWithUsers(snap *Snapshot, workers int) ([]eventCount, error) {
	ws := newBenchWorkers(workers)
	defer freeBenchWorkers(ws)
	for _, w := range ws {
		for i := range w.distinct {
			w.distinct[i] = vec.NewByteTable(w.arena)
		}
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
			w.key = binary.LittleEndian.AppendUint64(w.key[:0], w.groups.KeyHash(g))
			w.key = append(w.key, w.b.Bytes(row)...)
			h := vec.Hash(w.key)
			w.distinct[h>>58].IDHashed(w.key, h)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	merged, _ := mergeCounts(ws)
	byHash := map[uint64]*eventCount{}
	for name, e := range merged {
		byHash[vec.Hash([]byte(name))] = e
	}
	// Equal keys always land in the same partition, so partitions merge
	// independently and their distinct counts add up.
	users := make([]map[uint64]int64, distinctPartitions)
	var wg sync.WaitGroup
	for part := 0; part < distinctPartitions; part++ {
		wg.Add(1)
		go func(part int) {
			defer wg.Done()
			arena := mem.NewArena()
			defer arena.Free()
			all := vec.NewByteTable(arena)
			for _, w := range ws {
				t := w.distinct[part]
				for id := 0; id < t.Len(); id++ {
					all.IDHashed(t.Key(id), t.KeyHash(id))
				}
			}
			counts := map[uint64]int64{}
			for id := 0; id < all.Len(); id++ {
				counts[binary.LittleEndian.Uint64(all.Key(id))]++
			}
			users[part] = counts
		}(part)
	}
	wg.Wait()
	for _, counts := range users {
		for h, n := range counts {
			byHash[h].users += n
		}
	}

	return sortedCounts(merged), nil
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

// queryPostTimes feeds Q4 and Q5: the first and last time_us of created
// posts per did.
func queryPostTimes(snap *Snapshot, workers int) ([]userSpan, error) {
	ws := newBenchWorkers(workers)
	defer freeBenchWorkers(ws)
	_, err := snap.Scan(context.Background(), ScanOptions{Workers: workers}, func(n int, p *part.Part, b *part.Block) error {
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
		for _, r := range w.idx {
			row := int(r)
			if !w.b.IsValid(row) || !w.c.IsValid(row) {
				continue
			}
			g := w.group(w.b.Bytes(row))
			ts := w.c.Int(row)
			w.mins[g] = min(w.mins[g], ts)
			w.maxs[g] = max(w.maxs[g], ts)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	arena := mem.NewArena()
	defer arena.Free()
	all := vec.NewByteTable(arena)
	var spans []userSpan
	for _, w := range ws {
		for id := 0; id < w.groups.Len(); id++ {
			g := all.IDHashed(w.groups.Key(id), w.groups.KeyHash(id))
			if g == len(spans) {
				spans = append(spans, userSpan{min: 1 << 62, max: -1 << 62})
			}
			spans[g].min = min(spans[g].min, w.mins[id])
			spans[g].max = max(spans[g].max, w.maxs[id])
		}
	}
	for g := range spans {
		spans[g].did = string(all.Key(g))
	}

	return spans, nil
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

// timeRuns runs fn n times and returns the durations in ascending order,
// writing a CPU profile of the runs to profile when it is not empty.
func timeRuns(n int, profile string, fn func() error) ([]time.Duration, error) {
	if profile != "" {
		f, err := os.Create(profile)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			return nil, err
		}
		defer pprof.StopCPUProfile()
	}
	took := make([]time.Duration, n)
	for i := range took {
		begin := time.Now()
		if err := fn(); err != nil {
			return nil, err
		}
		took[i] = time.Since(begin)
	}
	sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })

	return took, nil
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

	var q1, q2 []eventCount
	var q3 []hourCount
	var spans []userSpan
	queries := []struct {
		name string
		run  func(workers int) error
	}{
		{"Q1 top events", func(n int) (err error) { q1, err = queryTopEvents(snap, n); return }},
		{"Q2 events with distinct users", func(n int) (err error) { q2, err = queryEventsWithUsers(snap, n); return }},
		{"Q3 events by hour", func(n int) (err error) { q3, err = queryEventsByHour(snap, n); return }},
		{"Q4/Q5 post times per user", func(n int) (err error) { spans, err = queryPostTimes(snap, n); return }},
	}
	ran := map[string]bool{}
	for _, q := range queries {
		if !strings.Contains(q.name, only) {
			continue
		}
		id := q.name[:2]
		ran[id] = true
		var took [2][]time.Duration
		for i, workers := range []int{cores, 1} {
			profile := ""
			if profileDir != "" {
				profile = filepath.Join(profileDir, fmt.Sprintf("%s_%dw.prof", strings.ToLower(id), workers))
			}
			took[i], err = timeRuns(runs, profile, func() error { return q.run(workers) })
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
		t.Logf("%-30s %7.2f ms on %d cores (median %.2f, max %.2f), %7.2f ms on 1 core (median %.2f, max %.2f), %d heap allocations (%.1f MB)",
			q.name, ms(all[0]), cores, ms(all[runs/2]), ms(all[runs-1]), ms(one[0]), ms(one[runs/2]), ms(one[runs-1]),
			after.Mallocs-before.Mallocs, float64(after.TotalAlloc-before.TotalAlloc)/1e6)
	}

	if rows != 1_000_000 {
		t.Logf("results are only checked for the first benchmark file (1,000,000 rows)")

		return
	}
	// Reference results produced by ClickHouse on the same file.
	if ran["Q1"] {
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
	if ran["Q2"] {
		want := "app.bsky.feed.like=444523/117617 app.bsky.graph.follow=337978/63957 app.bsky.feed.post=86812/50464 " +
			"app.bsky.feed.repost=56993/26581 app.bsky.graph.block=13838/5785 app.bsky.graph.listitem=7568/1078 " +
			"app.bsky.actor.profile=5337/5337 app.bsky.graph.listblock=860/449 app.bsky.graph.list=259/218 " +
			"app.bsky.feed.threadgate=228/196 app.bsky.graph.starterpack=104/101 app.bsky.feed.postgate=101/82 app.bsky.feed.generator=10/9"
		got := ""
		for i, e := range q2 {
			got += fmt.Sprintf("%s%s=%d/%d", sep(i), e.event, e.count, e.users)
		}
		if got != want {
			t.Errorf("Q2\n got  %s\n want %s", got, want)
		}
	}
	if ran["Q3"] {
		got := ""
		for i, e := range q3 {
			got += fmt.Sprintf("%s%s@%d=%d", sep(i), e.event, e.hour, e.count)
		}
		if want := "app.bsky.feed.like@16=444523 app.bsky.feed.post@16=86812 app.bsky.feed.repost@16=56993"; got != want {
			t.Errorf("Q3\n got  %s\n want %s", got, want)
		}
	}
	if !ran["Q4"] {
		return
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].min < spans[j].min })
	gotQ4 := fmt.Sprintf("%s@%d %s@%d %s@%d", spans[0].did, spans[0].min, spans[1].did, spans[1].min, spans[2].did, spans[2].min)
	if want := "did:plc:yj3sjq3blzpynh27cumnp5ks@1732206349000167 did:plc:l5o3qjrmfztir54cpwlv2eme@1732206349001905 did:plc:s4bwqchfzm6gjqfeb6mexgbu@1732206349003907"; gotQ4 != want {
		t.Errorf("Q4\n got  %s\n want %s", gotQ4, want)
	}
	span := func(s userSpan) int64 { return s.max/1000 - s.min/1000 }
	sort.Slice(spans, func(i, j int) bool { return span(spans[i]) > span(spans[j]) })
	gotQ5 := fmt.Sprintf("%s=%d %s=%d %s=%d", spans[0].did, span(spans[0]), spans[1].did, span(spans[1]), spans[2].did, span(spans[2]))
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
