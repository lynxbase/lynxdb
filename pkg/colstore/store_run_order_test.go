package colstore

import (
	"slices"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

func TestRunPartsTakeTheRowsOfTheirBatchesInTheOrderOfAllOfThemSorted(t *testing.T) {
	key := []string{"svc", "user"}
	opts := StoreOptions{SortKey: key, TimeField: "ts", BlockRows: 128, MergeFanIn: -1, FlushInterval: -1, Partition: time.Hour}
	s, err := OpenStore(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Every batch also holds the same rows of one key and one time each, so
	// that the order of the batches decides between them.
	var batches []*shred.Batch
	all := shred.NewBatch(s.shredOptions())
	for i := range 5 {
		b := shred.NewBatch(s.shredOptions())
		if errs := b.AddNDJSON(runDocs(i*3000, 3000), 0, nil); len(errs) != 0 {
			t.Fatal(errs[0].Err)
		}
		if errs := b.AddNDJSON(runDocs(0, 200), 0, nil); len(errs) != 0 {
			t.Fatal(errs[0].Err)
		}
		b.CopyRows(all, 0, b.Rows)
		batches = append(batches, b)
	}
	n := func(b *shred.Batch, row int) int64 { return b.Lookup("n", vec.KindInt).Ints[row] }
	var want []int64
	all.SortByTimeBuckets(nil, int64(time.Hour), key)
	for row := range all.Rows {
		want = append(want, n(all, row))
	}
	segments := map[int64][]segment{}
	var partitions []int64
	for _, b := range batches {
		from := 0
		for _, bucket := range b.SortByTimeBuckets(nil, int64(time.Hour), key) {
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
	if len(parts) <= len(partitions) {
		t.Fatalf("%d parts for %d partitions", len(parts), len(partitions))
	}
	var got []int64
	for _, p := range parts {
		m := s.newSegmentMerge(p.segs)
		next := make([]int, len(p.segs))
		for i, sg := range p.segs {
			next[i] = sg.from
		}
		for src, ok := m.next(); ok; src, ok = m.next() {
			got = append(got, n(p.segs[src].b, next[src]))
			next[src]++
		}
	}
	if !slices.Equal(got, want) {
		t.Fatal("the parts take the rows in another order than sorting all the batches joined")
	}
}
