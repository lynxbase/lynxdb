package colstore

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

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

func TestStoreWithoutSortKeyKeepsArrivalOrderAndNeverMerges(t *testing.T) {
	s, err := OpenStore(filepath.Join(t.TempDir(), "store"), StoreOptions{FlushBytes: 1 << 10, MergeFanIn: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w := s.NewWriter()
	for i := 0; i < 1000; i += 100 {
		if err := w.AddNDJSON(append(storeDocs(i, 100), "not json\n"...)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if w.Rejected != 10 {
		t.Fatalf("%d lines rejected, want 10", w.Rejected)
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	snap := s.Table().Snapshot()
	defer snap.Release()
	if len(snap.Parts()) < 2 {
		t.Fatalf("%d parts", len(snap.Parts()))
	}
	for _, h := range snap.Parts() {
		if h.Ordered() || h.Tier != 0 {
			t.Fatalf("part is ordered=%v at tier %d", h.Ordered(), h.Tier)
		}
	}
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != 1000 || sum != 999*1000/2 {
		t.Fatalf("%d rows, sum %d", rows, sum)
	}
}

func TestWriterCutsALargeRequestIntoRunsOfTheFlushSize(t *testing.T) {
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
