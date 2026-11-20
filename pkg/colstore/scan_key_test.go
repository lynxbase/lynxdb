package colstore

import (
	"bytes"
	"context"
	"encoding/json"
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
		tag, _ := json.Marshal(tags[i%len(tags)])
		doc := fmt.Sprintf(`{"tag":%s,"n":%d}`, tag, i)
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

func TestScanKeepsTheBlocksOfEveryListedKey(t *testing.T) {
	// Values that start with one another and hold the byte the key encoding
	// escapes sort next to one another.
	tags := []string{"a", "a\x00", "a\x00b", "ab", "b", "b\u00ff", "c"}
	tbl := NewTable()
	defer tbl.Close()
	if err := tbl.Apply([]*Handle{
		NewHandle(orderedByTag(t, 7000, 50, tags), nil),
		NewHandle(orderedByTag(t, 1400, 50, tags[2:]), nil),
	}, nil); err != nil {
		t.Fatal(err)
	}
	snap := tbl.Snapshot()
	defer snap.Release()
	rowsOf := func(opts ScanOptions) (map[string]int, ScanStats) {
		t.Helper()
		opts.Workers = 1
		got := map[string]int{}
		var col vec.Column
		stats, err := snap.Scan(context.Background(), opts, func(_ int, p *part.Part, b *part.Block) error {
			id, _ := p.PathID("tag")
			ref, ok := b.Column(id, vec.KindString)
			if !ok {
				return nil
			}
			if err := b.Decode(ref, &col); err != nil {
				return err
			}
			for r := 0; r < b.Rows; r++ {
				got[string(col.Bytes(r))]++
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		return got, stats
	}
	all, _ := rowsOf(ScanOptions{})
	for _, in := range [][]string{
		{"a"}, {"a\x00"}, {"ab", "a"}, {"c", "a", "c"}, {"b"}, {"b\u00ff", "zz"}, {"none", ""}, tags,
	} {
		key := KeyFilter{InPath: "tag"}
		for _, v := range in {
			key.In = append(key.In, []byte(v))
		}
		got, stats := rowsOf(ScanOptions{Key: key})
		for _, v := range in {
			if got[v] != all[v] {
				t.Fatalf("list %q: %d rows of %q, want %d", in, got[v], v, all[v])
			}
		}
		if len(in) < len(tags) && stats.SkippedByKey == 0 {
			t.Fatalf("list %q pruned no block: %+v", in, stats)
		}
	}
}
