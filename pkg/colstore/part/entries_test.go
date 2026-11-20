package part

import (
	"bytes"
	"errors"
	"testing"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// TestDictionaryEntriesAreReadWithoutTheCodes requires that the distinct
// values of a dictionary column come from the dictionary's bytes alone,
// checked and asked for ahead, and equal the values of its rows; it logs
// the bytes per block that this reads against the whole chunk.
func TestDictionaryEntriesAreReadWithoutTheCodes(t *testing.T) {
	for _, users := range []int{1024, 4096, 8192} {
		g := benchdata.New(1, users)
		all := shred.NewBatch(shred.Options{TimeField: "time_us"})
		if errs := all.AddNDJSON(g.Append(nil, 1<<17), 0, nil); len(errs) > 0 {
			t.Fatal(errs[0])
		}
		all.SortBy(blueskyKey)
		var buf bytes.Buffer
		w := NewWriter(&buf, Options{Ordered: true, SortKey: blueskyKey})
		block := shred.NewBatch(shred.Options{})
		for from := 0; from < all.Rows; from += 16384 {
			block.Reset()
			all.CopyRows(block, from, min(from+16384, all.Rows))
			if err := w.WriteBatch(block); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		p, err := Open(buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		var asked [][2]int
		p.SetReadAhead(func(off, n int) { asked = append(asked, [2]int{off, n}) })
		id, _ := p.PathID("did")
		var blk Block
		var entries, rows vec.Column
		dicts, whole, head, wholePages, headPages, dir := 0, 0, 0, 0, 0, 0
		for i := 0; i < p.NumBlocks(); i++ {
			if err := p.OpenBlock(i, &blk); err != nil {
				t.Fatal(err)
			}
			ref, ok := blk.Column(id, vec.KindString)
			if !ok || ref.enc != encDict {
				continue
			}
			asked = asked[:0]
			if listed, err := blk.DecodeEntries(ref, &entries); err != nil || !listed {
				t.Fatalf("users %d, block %d: entries of a dictionary column: %v, %v", users, i, listed, err)
			}
			start := blk.start + int(ref.off)
			for _, a := range asked {
				if a[0] < start || a[0]+a[1] > start+int(ref.head) {
					t.Fatalf("users %d, block %d: asked for bytes %d to %d, the dictionary is %d to %d", users, i, a[0], a[0]+a[1], start, start+int(ref.head))
				}
			}
			if ref.head >= ref.length {
				t.Fatalf("users %d, block %d: the dictionary takes %d of the chunk's %d bytes", users, i, ref.head, ref.length)
			}
			if err := blk.Decode(ref, &rows); err != nil {
				t.Fatal(err)
			}
			values := map[string]bool{}
			for r := 0; r < blk.Rows; r++ {
				values[string(rows.Bytes(r))] = true
			}
			var listed []string
			for e := 0; e < entries.Rows; e++ {
				listed = append(listed, string(entries.Bytes(e)))
				if !values[listed[e]] || (e > 0 && listed[e] <= listed[e-1]) {
					t.Fatalf("users %d, block %d: entry %d is not the next value the rows hold", users, i, e)
				}
			}
			if len(listed) != len(values) {
				t.Fatalf("users %d, block %d: %d entries, the rows hold %d values", users, i, len(listed), len(values))
			}
			dicts++
			whole += int(ref.length)
			head += int(ref.head)
			// A cold read brings in whole pages.
			pages := func(from, to int) int { return (to+4095)/4096 - from/4096 }
			wholePages += pages(start, start+int(ref.length))
			headPages += pages(start, start+int(ref.head))
			dir += len(blk.dir) + len(blk.stats)
		}
		if dicts == 0 {
			t.Fatalf("users %d: no block holds did as a dictionary", users)
		}
		t.Logf("users %d: %d of %d blocks hold did as a dictionary; per block the chunk is %d bytes in %.1f pages, the dictionary %d bytes (%.0f%%) in %.1f pages, the directory %d bytes",
			users, dicts, p.NumBlocks(), whole/dicts, float64(wholePages)/float64(dicts), head/dicts, 100*float64(head)/float64(whole),
			float64(headPages)/float64(dicts), dir/dicts)
	}
}

// TestDamageToADictionaryOrItsCodesIsFound requires that a byte changed in
// the codes of a dictionary column fails the decode of its rows and not the
// read of its entries, and one changed in the dictionary fails both.
func TestDamageToADictionaryOrItsCodesIsFound(t *testing.T) {
	g := benchdata.New(1, 1024)
	all := shred.NewBatch(shred.Options{TimeField: "time_us"})
	all.AddNDJSON(g.Append(nil, 16384), 0, nil)
	all.SortBy(blueskyKey)
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{Ordered: true, SortKey: blueskyKey})
	if err := w.WriteBatch(all); err != nil || w.Close() != nil {
		t.Fatal("write failed")
	}
	clean, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	var blk Block
	if err := clean.OpenBlock(0, &blk); err != nil {
		t.Fatal(err)
	}
	id, _ := clean.PathID("did")
	ref, ok := blk.Column(id, vec.KindString)
	if !ok || ref.enc != encDict {
		t.Fatal("did is not a dictionary")
	}
	start := blk.start + int(ref.off)
	for _, c := range []struct {
		name    string
		at      int
		entries bool
	}{{"codes", start + int(ref.length) - 1, true}, {"dictionary", start + int(ref.head) - 1, false}} {
		data := bytes.Clone(buf.Bytes())
		data[c.at] ^= 0x40
		p, err := Open(data)
		if err != nil {
			t.Fatal(err)
		}
		var b Block
		var col vec.Column
		if err := p.OpenBlock(0, &b); err != nil {
			t.Fatal(err)
		}
		r, _ := b.Column(id, vec.KindString)
		if _, err := b.DecodeEntries(r, &col); (err == nil) != c.entries {
			t.Fatalf("damaged %s: entries read with %v", c.name, err)
		}
		if err := b.Decode(r, &col); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("damaged %s: rows decoded with %v", c.name, err)
		}
	}
}
