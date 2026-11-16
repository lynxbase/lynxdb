package colstore

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// orderedByTag builds a part of rows {"tag": tags[i%len], "n": i} ordered by
// tag in blocks of blockRows.
func orderedByTag(t testing.TB, rows, blockRows int, tags []string) *part.Part {
	t.Helper()
	opts := part.Options{Ordered: true, SortKey: []string{"tag"}}
	batch := shred.NewBatch(shred.Options{})
	for i := 0; i < rows; i++ {
		doc := fmt.Sprintf(`{"tag":%q,"n":%d}`, tags[i%len(tags)], i)
		if err := batch.AddJSON([]byte(doc), int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	batch.SortBy(opts.SortKey)
	var one bytes.Buffer
	w := part.NewWriter(&one, opts)
	if err := w.WriteBatch(batch); err != nil || w.Close() != nil {
		t.Fatal("write failed")
	}
	src, err := part.Open(one.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	w = part.NewWriter(&out, opts)
	if err := part.Merge(w, []*part.Part{src}, blockRows); err != nil || w.Close() != nil {
		t.Fatal("merge failed")
	}
	p, err := part.Open(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	return p
}

func TestScanPrunesBlocksBySortKey(t *testing.T) {
	tags := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	tbl := NewTable()
	defer tbl.Close()
	if err := tbl.Apply([]*Handle{
		NewHandle(orderedByTag(t, 5000, 100, tags), nil),
		NewHandle(orderedByTag(t, 2000, 100, tags), nil),
		NewHandle(makePart(t, 0, 300, 100, "gamma"), nil),
	}, nil); err != nil {
		t.Fatal(err)
	}
	snap := tbl.Snapshot()
	defer snap.Release()

	count := func(s *Snapshot, opts ScanOptions, tag string) (rows int64, stats ScanStats) {
		t.Helper()
		opts.Workers = 1
		var col vec.Column
		mask := make([]uint8, shred.MaxRows)
		stats, err := s.Scan(context.Background(), opts, func(_ int, p *part.Part, b *part.Block) error {
			id, _ := p.PathID("tag")
			ref, ok := b.Column(id, vec.KindString)
			if !ok {
				return nil
			}
			if err := b.Decode(ref, &col); err != nil {
				return err
			}
			vec.MaskEqBytes(mask[:b.Rows], &col, []byte(tag))
			rows += int64(vec.MaskCount(mask[:b.Rows]))

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		return rows, stats
	}

	all, none := count(snap, ScanOptions{}, "gamma")
	if all != 1000+400+300 || none.SkippedByKey != 0 || none.Blocks != 73 {
		t.Fatalf("unfiltered scan: %d rows, %+v", all, none)
	}
	key := KeyFilter{Paths: []string{"tag"}, Values: [][]byte{[]byte("gamma")}}
	rows, stats := count(snap, ScanOptions{Key: key}, "gamma")
	// Each ordered part keeps its gamma blocks and at most one neighbour on
	// each side; the arrival-ordered part is read in full.
	if rows != all || stats.SkippedByKey < 50 || stats.Blocks > 10+2+4+2+3 {
		t.Fatalf("key filter: %d rows, %+v", rows, stats)
	}

	narrow := snap.Narrow(key)
	rows, stats = count(narrow, ScanOptions{}, "gamma")
	narrow.Release()
	if rows != all || stats.SkippedByKey < 50 {
		t.Fatalf("narrowed snapshot: %d rows, %+v", rows, stats)
	}
	if rows, stats = count(snap, ScanOptions{Key: KeyFilter{Paths: []string{"tag"}, Values: [][]byte{[]byte("omega")}}}, "omega"); rows != 0 || stats.Blocks > 2+3 {
		t.Fatalf("absent key: %d rows, %+v", rows, stats)
	}
	if _, stats = count(snap, ScanOptions{Key: KeyFilter{Paths: []string{"n"}, Values: [][]byte{[]byte("1")}}}, "gamma"); stats.SkippedByKey != 0 {
		t.Fatalf("a filter on a path the parts are not ordered by pruned blocks: %+v", stats)
	}
}
