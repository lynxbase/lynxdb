package colstore

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// storeDocs returns NDJSON documents {"n": first+i, "svc": ...} for i below
// count, with a few distinct services so that sorting by svc reorders them.
func storeDocs(first, count int) []byte {
	var buf bytes.Buffer
	for i := first; i < first+count; i++ {
		fmt.Fprintf(&buf, `{"n":%d,"svc":"s%02d","id":"r%dx"}`+"\n", i, (i*7)%23, i)
	}

	return buf.Bytes()
}

type runShape struct {
	tier  int
	parts int
	rows  uint64
}

// runShapes checks that every part is ordered and that the parts of one run
// cover disjoint key ranges, and returns the runs by number.
func runShapes(t testing.TB, snap *Snapshot) map[uint64]*runShape {
	t.Helper()
	type span struct{ first, last []byte }
	spans := map[uint64][]span{}
	shapes := map[uint64]*runShape{}
	for _, h := range snap.Parts() {
		if !h.Ordered() {
			t.Fatal("a part of an ordered store is not ordered")
		}
		first, last := h.KeyRange()
		if first == nil || bytes.Compare(first, last) > 0 {
			t.Fatalf("part of run %d has key range %x .. %x", h.Run, first, last)
		}
		s := shapes[h.Run]
		if s == nil {
			s = &runShape{tier: h.Tier}
			shapes[h.Run] = s
		}
		if s.tier != h.Tier {
			t.Fatalf("run %d has parts of tiers %d and %d", h.Run, s.tier, h.Tier)
		}
		s.parts++
		s.rows += h.Rows
		for _, other := range spans[h.Run] {
			if bytes.Compare(first, other.last) <= 0 && bytes.Compare(other.first, last) <= 0 {
				t.Fatalf("two parts of run %d overlap in key range", h.Run)
			}
		}
		spans[h.Run] = append(spans[h.Run], span{first, last})
	}

	return shapes
}

func TestStoreFlushesSortedRunsAndMergesThemByTier(t *testing.T) {
	skipSyncs(t)
	path := filepath.Join(t.TempDir(), "store")
	opts := StoreOptions{SortKey: []string{"svc"}, BlockRows: 64, FlushBytes: 4 << 10, MergeFanIn: 3}
	s, err := OpenStore(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	const writers, perWriter = 4, 3000
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wr := s.NewWriter()
			for i := 0; i < perWriter; i += 50 {
				if err := wr.AddNDJSON(storeDocs(w*perWriter+i, 50)); err != nil {
					t.Error(err)
				}
			}
			if err := wr.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	total := int64(writers * perWriter)
	wantSum := (total - 1) * total / 2
	// Everything is visible once the writers are closed, merged or not.
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != total || sum != wantSum {
		t.Fatalf("after the writers closed: %d rows, sum %d", rows, sum)
	}
	if err := s.Settle(); err != nil {
		t.Fatal(err)
	}
	snap := s.Table().Snapshot()
	shapes := runShapes(t, snap)
	snap.Release()
	perTier := map[int]int{}
	merged := false
	for _, r := range shapes {
		perTier[r.tier]++
		merged = merged || r.tier > 0
	}
	if !merged {
		t.Fatalf("no run was merged: %v", perTier)
	}
	for tier, n := range perTier {
		if n >= opts.MergeFanIn {
			t.Fatalf("tier %d is left with %d runs, fan-in is %d", tier, n, opts.MergeFanIn)
		}
	}
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != total || sum != wantSum {
		t.Fatalf("after merges: %d rows, sum %d", rows, sum)
	}

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	snap = s.Table().Snapshot()
	if shapes = runShapes(t, snap); len(shapes) != 1 {
		t.Fatalf("%d runs after compaction", len(shapes))
	}
	checkOrderedBySvc(t, snap)
	snap.Release()
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != total || sum != wantSum {
		t.Fatalf("after compaction: %d rows, sum %d", rows, sum)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = OpenStore(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Settle(); err != nil {
		t.Fatal(err)
	}
	snap = s.Table().Snapshot()
	defer snap.Release()
	if shapes = runShapes(t, snap); len(shapes) != 1 {
		t.Fatalf("%d runs after reopen", len(shapes))
	}
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != total || sum != wantSum {
		t.Fatalf("after reopen: %d rows, sum %d", rows, sum)
	}
}

func rowsAndSumOf(t testing.TB, tbl *Table) (rows, sum int64) {
	t.Helper()
	snap := tbl.Snapshot()
	defer snap.Release()
	rows, sum, _ = sumN(t, snap, ScanOptions{})

	return rows, sum
}

// checkOrderedBySvc requires the parts of a single run, taken in key order,
// to hold their rows in ascending order of svc.
func checkOrderedBySvc(t testing.TB, snap *Snapshot) {
	t.Helper()
	parts := append([]*Handle(nil), snap.Parts()...)
	for i := range parts {
		for j := i + 1; j < len(parts); j++ {
			a, _ := parts[i].KeyRange()
			b, _ := parts[j].KeyRange()
			if bytes.Compare(a, b) > 0 {
				parts[i], parts[j] = parts[j], parts[i]
			}
		}
	}
	var prev []byte
	var b part.Block
	var col vec.Column
	for _, h := range parts {
		for i := 0; i < h.NumBlocks(); i++ {
			if err := h.OpenBlock(i, &b); err != nil {
				t.Fatal(err)
			}
			ref, err := column(h.Part, &b, "svc", vec.KindString, &col)
			if err != nil || ref == nil || ref.Count != b.Rows {
				t.Fatalf("svc column of a block: %v", err)
			}
			for row := 0; row < b.Rows; row++ {
				if svc := col.Bytes(row); bytes.Compare(svc, prev) < 0 {
					t.Fatalf("svc %q follows %q", svc, prev)
				} else {
					prev = append(prev[:0], svc...)
				}
			}
		}
	}
}

func TestStoreServesReadersWhileItMerges(t *testing.T) {
	skipSyncs(t)
	s, err := OpenStore(filepath.Join(t.TempDir(), "store"), StoreOptions{
		SortKey: []string{"svc"}, BlockRows: 32, FlushBytes: 2 << 10, MergeFanIn: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	stop := make(chan struct{})
	var readers sync.WaitGroup
	var scans atomic.Int64
	for r := 0; r < 3; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			var last int64
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap := s.Table().Snapshot()
				rows, sum, _ := sumN(t, snap, ScanOptions{})
				snap.Release()
				// Documents arrive in order of n and in whole flushes, so a
				// consistent view holds a prefix-free multiple of 20 rows
				// whose sum no merge may change.
				if rows < last || rows%20 != 0 {
					t.Errorf("a reader saw %d rows after %d", rows, last)
				}
				if rows > 0 && sum != (rows-1)*rows/2 {
					t.Errorf("a reader saw %d rows with sum %d", rows, sum)
				}
				last = rows
				scans.Add(1)
			}
		}()
	}
	w := s.NewWriter()
	for i := 0; i < 4000; i += 20 {
		if err := w.AddNDJSON(storeDocs(i, 20)); err != nil {
			t.Fatal(err)
		}
		// One flush per batch keeps the visible rows a prefix of the input.
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Settle(); err != nil {
		t.Fatal(err)
	}
	close(stop)
	readers.Wait()
	if scans.Load() == 0 {
		t.Fatal("no reader finished a scan")
	}
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != 4000 || sum != 3999*4000/2 {
		t.Fatalf("%d rows, sum %d", rows, sum)
	}
}

// checkTimeOrder requires every run of the snapshot to hold its rows in the
// order of their times, across parts whose time ranges do not meet.
func checkTimeOrder(t testing.TB, snap *Snapshot) {
	t.Helper()
	byRun := map[uint64][]*Handle{}
	for _, h := range snap.Parts() {
		if !h.Ordered() || len(h.SortKey()) != 0 {
			t.Fatalf("part is ordered=%v by %v", h.Ordered(), h.SortKey())
		}
		byRun[h.Run] = append(byRun[h.Run], h)
	}
	for run, hs := range byRun {
		slices.SortFunc(hs, func(x, y *Handle) int { return cmp.Compare(x.MinTime, y.MinTime) })
		last := int64(math.MinInt64)
		for i, h := range hs {
			if i > 0 && h.MinTime <= last {
				t.Fatalf("run %d: a part starts at %d, the one before ends at %d", run, h.MinTime, last)
			}
			for b := 0; b < h.NumBlocks(); b++ {
				_, lo, hi := h.BlockStats(b)
				if lo < last || hi < lo {
					t.Fatalf("run %d: block %d spans %d to %d after %d", run, b, lo, hi, last)
				}
				last = hi
			}
		}
	}
}

// fallingDocs gives count documents numbered from first whose times fall as
// their numbers rise.
func fallingDocs(first, count int) []byte {
	var buf bytes.Buffer
	for i := first; i < first+count; i++ {
		fmt.Fprintf(&buf, `{"n":%d,"ts":%d}`+"\n", i, 1_700_000_000_000-i)
	}

	return buf.Bytes()
}

func TestStoreWithoutASortKeyOrdersRowsByTimeAndMergesItsRuns(t *testing.T) {
	skipSyncs(t)
	s, err := OpenStore(filepath.Join(t.TempDir(), "store"), StoreOptions{TimeField: "ts", FlushBytes: 1 << 10, MergeFanIn: 2, BlockRows: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w := s.NewWriter()
	for i := 0; i < 1000; i += 100 {
		if err := w.AddNDJSON(append(fallingDocs(i, 100), "not json\n"...)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if w.Rejected != 10 {
		t.Fatalf("%d lines rejected, want 10", w.Rejected)
	}
	before := s.Table().Snapshot()
	runs := map[uint64]bool{}
	for _, h := range before.Parts() {
		runs[h.Run] = true
	}
	checkTimeOrder(t, before)
	before.Release()
	if len(runs) < 2 {
		t.Fatalf("%d runs before compaction", len(runs))
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	snap := s.Table().Snapshot()
	defer snap.Release()
	checkTimeOrder(t, snap)
	for _, h := range snap.Parts() {
		if h.Run != snap.Parts()[0].Run || h.Tier == 0 {
			t.Fatalf("part of run %d at tier %d after compaction", h.Run, h.Tier)
		}
	}
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != 1000 || sum != 999*1000/2 {
		t.Fatalf("%d rows, sum %d", rows, sum)
	}
}

func TestWriterCutsALargeRequestIntoRunsOfTheFlushSize(t *testing.T) {
	skipSyncs(t)
	s, err := OpenStore(filepath.Join(t.TempDir(), "store"), StoreOptions{
		SortKey: []string{"svc"}, FlushBytes: 8 << 10, MergeFanIn: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	docs := storeDocs(0, 2000)
	w := s.NewWriter()
	if err := w.AddNDJSON(docs); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	snap := s.Table().Snapshot()
	defer snap.Release()
	want := (len(docs) + 8<<10 - 1) / (8 << 10)
	if got := len(snap.Parts()); got < want-1 || got > want+1 {
		t.Fatalf("%d bytes became %d runs, want about %d", len(docs), got, want)
	}
	for _, h := range snap.Parts() {
		// A run exceeds the flush size by less than one line.
		if h.RawBytes > 8<<10+128 {
			t.Fatalf("a run holds %d input bytes", h.RawBytes)
		}
	}
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != 2000 || sum != 1999*2000/2 {
		t.Fatalf("%d rows, sum %d", rows, sum)
	}
}

func TestPickMergesGroupsRunsByTier(t *testing.T) {
	var parts []*Handle
	add := func(run uint64, tier, n int) {
		for i := 0; i < n; i++ {
			parts = append(parts, &Handle{PartMeta: PartMeta{Run: run, Tier: tier}})
		}
	}
	// Seven runs at tier 0, one of them in two parts, and two at tier 1.
	for run := uint64(1); run <= 7; run++ {
		add(run, 0, 1+int(run)/7)
	}
	add(20, 1, 3)
	add(21, 1, 1)
	ids := func(set []sortedRun) (out []uint64) {
		for _, r := range set {
			out = append(out, r.id)
		}

		return out
	}
	sets := pickMerges(parts, 3, false)
	if len(sets) != 2 || fmt.Sprint(ids(sets[0]), ids(sets[1])) != "[1 2 3] [4 5 6]" {
		t.Fatalf("picked %v", sets)
	}
	// With pairs, tier 0 gives three sets and tier 1 one.
	pairs := pickMerges(parts, 2, false)
	if len(pairs) != 4 || fmt.Sprint(ids(pairs[3])) != "[20 21]" || len(pairs[3][0].parts) != 3 {
		t.Fatalf("picked %v", pairs)
	}
	if sets := pickMerges(parts, 8, false); sets != nil {
		t.Fatalf("picked %v with too few runs per tier", sets)
	}
	all := pickMerges(parts, -1, true)
	if len(all) != 1 || len(all[0]) != 9 {
		t.Fatalf("compaction picked %v", all)
	}
	if sets := pickMerges(parts[:1], 2, true); sets != nil {
		t.Fatalf("compaction of one run picked %v", sets)
	}
}

// firstTimedHour is the hour, counted from the epoch, at which timedDocs
// starts.
const firstTimedHour = 277778

// timedDocs returns NDJSON documents {"n": first+i, "svc": ..., "ts": ...}
// whose timestamps, in microseconds, are spread over the given number of
// hours from firstTimedHour on, in an order that mixes the hours.
func timedDocs(first, count, hours int) []byte {
	var buf bytes.Buffer
	for i := first; i < first+count; i++ {
		ts := int64(firstTimedHour+(i*7)%hours)*3600e6 + int64(i%3600)*1e6
		fmt.Fprintf(&buf, `{"n":%d,"svc":"s%02d","ts":%d}`+"\n", i, (i*5)%11, ts)
	}

	return buf.Bytes()
}

func TestStoreKeepsPartitionsApartAndDropsOldOnes(t *testing.T) {
	skipSyncs(t)
	const hours = 5
	hour, base := int64(time.Hour), int64(firstTimedHour)
	opts := StoreOptions{
		SortKey: []string{"svc"}, TimeField: "ts", BlockRows: 64, FlushBytes: 16 << 10, MergeFanIn: 2, Partition: time.Hour,
	}
	path := filepath.Join(t.TempDir(), "store")
	s, err := OpenStore(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	const total = 6000
	w := s.NewWriter()
	if err := w.AddNDJSON(timedDocs(0, total, hours)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Settle(); err != nil {
		t.Fatal(err)
	}
	check := func(when string, wantPartitions int, wantRows int64) {
		t.Helper()
		snap := s.Table().Snapshot()
		defer snap.Release()
		runShapes(t, snap)
		seen := map[int64]map[uint64]struct{}{}
		for _, h := range snap.Parts() {
			if h.Partition < base || h.Partition >= base+hours {
				t.Fatalf("%s: partition %d is outside %d..%d", when, h.Partition, base, base+hours-1)
			}
			if lo, hi := h.Partition*hour, (h.Partition+1)*hour; h.MinTime < lo || h.MaxTime >= hi {
				t.Fatalf("%s: a part of partition %d holds times %d..%d", when, h.Partition, h.MinTime, h.MaxTime)
			}
			if seen[h.Partition] == nil {
				seen[h.Partition] = map[uint64]struct{}{}
			}
			seen[h.Partition][h.Run] = struct{}{}
		}
		if len(seen) != wantPartitions {
			t.Fatalf("%s: %d partitions, want %d", when, len(seen), wantPartitions)
		}
		if rows, _ := rowsAndSumOf(t, s.Table()); rows != wantRows {
			t.Fatalf("%s: %d rows, want %d", when, rows, wantRows)
		}
	}
	check("after merges", hours, total)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	check("after compaction", hours, total)
	snap := s.Table().Snapshot()
	runs := map[int64]map[uint64]struct{}{}
	for _, h := range snap.Parts() {
		if runs[h.Partition] == nil {
			runs[h.Partition] = map[uint64]struct{}{}
		}
		runs[h.Partition][h.Run] = struct{}{}
	}
	for partition, ids := range runs {
		if len(ids) != 1 {
			t.Fatalf("partition %d has %d runs after compaction", partition, len(ids))
		}
	}
	// A time range of one hour reads the blocks of that partition only.
	oneHour := ScanOptions{MinTime: (base + 2) * hour, MaxTime: (base+3)*hour - 1}
	rows, _, stats := sumN(t, snap, oneHour)
	var inHour, blocks int64
	for _, h := range snap.Parts() {
		if h.Partition == base+2 {
			inHour += int64(h.Rows)
			blocks += int64(h.NumBlocks())
		}
	}
	snap.Release()
	if rows != inHour || stats.Blocks != blocks || stats.SkippedByTime == 0 {
		t.Fatalf("one hour: %d rows in %d blocks, %d skipped by time; the partition has %d rows in %d blocks",
			rows, stats.Blocks, stats.SkippedByTime, inHour, blocks)
	}

	dropped, err := s.DropBefore((base + 2) * hour)
	if err != nil {
		t.Fatal(err)
	}
	if left, _ := rowsAndSumOf(t, s.Table()); dropped == 0 || left != total-int64(dropped) {
		t.Fatalf("dropped %d rows, %d left of %d", dropped, left, total)
	}
	check("after dropping two partitions", hours-2, total-int64(dropped))
	if again, err := s.DropBefore((base + 2) * hour); err != nil || again != 0 {
		t.Fatalf("dropping again removed %d rows: %v", again, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = OpenStore(path, opts); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	check("after reopen", hours-2, total-int64(dropped))
}

func TestPickMergesNeverMixesPartitions(t *testing.T) {
	var parts []*Handle
	for run := uint64(1); run <= 6; run++ {
		parts = append(parts, &Handle{PartMeta: PartMeta{Run: run, Partition: int64(run % 2)}})
	}
	parts = append(parts, &Handle{PartMeta: PartMeta{Run: 9, Tier: 3, Partition: 1}})
	for _, compact := range []bool{false, true} {
		sets := pickMerges(parts, 3, compact)
		if len(sets) != 2 {
			t.Fatalf("compact=%v: %d sets", compact, len(sets))
		}
		for _, set := range sets {
			for _, r := range set {
				if r.partition != set[0].partition {
					t.Fatalf("compact=%v: a set mixes partitions %d and %d", compact, r.partition, set[0].partition)
				}
			}
		}
		// Compaction also takes the run of the higher tier.
		if want := map[bool]int{false: 3, true: 4}[compact]; len(sets[1]) != want {
			t.Fatalf("compact=%v: partition 1 gives a set of %d runs, want %d", compact, len(sets[1]), want)
		}
	}
}

func TestAppendFromManyGoroutinesLosesNothing(t *testing.T) {
	skipSyncs(t)
	path := filepath.Join(t.TempDir(), "store")
	opts := StoreOptions{SortKey: []string{"svc"}, FlushBytes: 8 << 10, MergeFanIn: 4, FlushInterval: -1}
	s, err := OpenStore(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	const callers, perCaller = 16, 1500
	var rejected atomic.Int64
	var wg sync.WaitGroup
	for c := 0; c < callers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perCaller; i += 30 {
				n, err := s.Append(append(storeDocs(c*perCaller+i, 30), "not json\n"...))
				if err != nil {
					t.Error(err)
				}
				rejected.Add(n)
			}
		}()
	}
	wg.Wait()
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	total := int64(callers * perCaller)
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != total || sum != (total-1)*total/2 {
		t.Fatalf("after Sync: %d rows, sum %d", rows, sum)
	}
	if got := rejected.Load(); got != callers*perCaller/30 {
		t.Fatalf("%d lines rejected, want %d", got, callers*perCaller/30)
	}
	// Rows that are still in a buffer at Close are written by it.
	if _, err := s.Append(storeDocs(int(total), 10)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = OpenStore(path, opts); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if rows, _ := rowsAndSumOf(t, s.Table()); rows != total+10 {
		t.Fatalf("after reopen: %d rows, want %d", rows, total+10)
	}
}

func TestAppendedRowsBecomeVisibleWithinTheFlushInterval(t *testing.T) {
	s, err := OpenStore(filepath.Join(t.TempDir(), "store"), StoreOptions{FlushInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// One caller at a time fills one buffer, so the rows become one part.
	for i := 0; i < 500; i += 100 {
		if _, err := s.Append(storeDocs(i, 100)); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows, _ := rowsAndSumOf(t, s.Table())
		if rows == 500 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of 500 rows visible without Sync", rows)
		}
		time.Sleep(5 * time.Millisecond)
	}
	snap := s.Table().Snapshot()
	defer snap.Release()
	if n := len(snap.Parts()); n > 5 {
		t.Fatalf("a light load was spread over %d parts", n)
	}
}

func TestBlockBudgetBoundsWhatIsHeldAtOnce(t *testing.T) {
	var b blockBudget
	b.init(10)
	var held, peak atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		// Requests of 1 to 4 units, and some larger than the whole budget.
		n := 1 + i%4
		if i%13 == 0 {
			n = 25
		}
		go func() {
			defer wg.Done()
			b.acquire(n)
			now := held.Add(int64(min(n, 10)))
			for {
				p := peak.Load()
				if now <= p || peak.CompareAndSwap(p, now) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			held.Add(-int64(min(n, 10)))
			b.release(n)
		}()
	}
	wg.Wait()
	if peak.Load() > 10 || peak.Load() < 5 {
		t.Fatalf("%d units were held at once under a budget of 10", peak.Load())
	}
	if b.free != 10 {
		t.Fatalf("%d units are free after every release", b.free)
	}
}

func TestSyncCallersShareFlushesAndSeeTheirRows(t *testing.T) {
	// A slow disk, so that callers arrive while a flush is under way.
	replaceSyncFile(t, func(*os.File) error {
		time.Sleep(time.Millisecond)

		return nil
	})
	store, err := OpenStore(t.TempDir(), StoreOptions{SortKey: []string{"svc"}, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const callers, rounds, docs = 24, 12, 5
	var acked atomic.Int64
	var wg sync.WaitGroup
	for c := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range rounds {
				if _, err := store.Append(storeDocs((c*rounds+r)*docs, docs)); err != nil {
					t.Error(err)

					return
				}
				if err := store.Sync(); err != nil {
					t.Error(err)

					return
				}
				// Every row a Sync has vouched for is in the table.
				want := acked.Add(docs)
				snap := store.Table().Snapshot()
				var rows int64
				for _, h := range snap.Parts() {
					rows += int64(h.Rows)
				}
				snap.Release()
				if rows < want {
					t.Errorf("%d rows in the table after syncs of %d", rows, want)

					return
				}
			}
		}()
	}
	wg.Wait()
	const total = callers * rounds * docs
	if rows, sum := rowsAndSumOf(t, store.Table()); rows != total || sum != (total-1)*total/2 {
		t.Fatalf("%d rows, sum %d; want %d rows", rows, sum, total)
	}
	store.syncMu.Lock()
	flushes := store.syncBegun
	store.syncMu.Unlock()
	if flushes == 0 || flushes > callers*rounds/2 {
		t.Fatalf("%d syncs took %d flushes", callers*rounds, flushes)
	}
}

func TestSyncAfterCloseIsRefused(t *testing.T) {
	store, err := OpenStore(t.TempDir(), StoreOptions{FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(storeDocs(0, 3)); err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Sync after Close: %v", err)
	}
}
