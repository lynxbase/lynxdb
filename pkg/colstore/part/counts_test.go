package part

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// TestDictionaryOfFewEntriesCountsItsRows requires a dictionary of at most
// 256 entries to say how many rows hold each entry, as many as Decode
// gives it, whether every row holds a value or some do not, and a larger
// dictionary not to.
func TestDictionaryOfFewEntriesCountsItsRows(t *testing.T) {
	const rows = 5000
	for name, c := range map[string]struct {
		values  int
		holds   func(row int) bool
		counted bool
	}{
		"twenty values in every row":     {20, func(int) bool { return true }, true},
		"two values in every row":        {2, func(int) bool { return true }, true},
		"256 values in most rows":        {256, func(row int) bool { return row%9 != 4 }, true},
		"seven values in a few rows":     {7, func(row int) bool { return row%40 == 1 }, true},
		"257 values in every row":        {257, func(int) bool { return true }, false},
		"a thousand values in every row": {1000, func(int) bool { return true }, false},
	} {
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(21, 22))
			docs := make([]string, rows)
			for row := range docs {
				docs[row] = fmt.Sprintf(`{"n":%d}`, row)
				if c.holds(row) {
					// Every value occurs, and some far more often than others.
					v := row % c.values
					if row >= 2*c.values {
						v = rng.IntN(1 + rng.IntN(c.values))
					}
					docs[row] = fmt.Sprintf(`{"n":%d,"v":"value-%04d"}`, row, v)
				}
			}
			p := buildPart(t, docs, rows, Options{})
			path, _ := p.PathID("v")
			var b Block
			if err := p.OpenBlock(0, &b); err != nil {
				t.Fatal(err)
			}
			ref, ok := b.Column(path, vec.KindString)
			if !ok || ref.enc != encDict {
				t.Fatalf("the column is not a dictionary: %v", ok)
			}
			if ref.Counted() != c.counted {
				t.Fatalf("counted %v, want %v", ref.Counted(), c.counted)
			}
			var full, entries vec.Column
			if err := b.Decode(ref, &full); err != nil {
				t.Fatal(err)
			}
			if !c.counted {
				if _, err := b.DecodeCounts(ref, &entries, nil); err == nil {
					t.Fatal("a dictionary without counts gave some")
				}

				return
			}
			counts, err := b.DecodeCounts(ref, &entries, nil)
			if err != nil {
				t.Fatal(err)
			}
			if entries.DictLen() != full.DictLen() || len(counts) != full.DictLen() {
				t.Fatalf("%d entries with %d counts, the column has %d", entries.DictLen(), len(counts), full.DictLen())
			}
			want := make([]uint32, full.DictLen())
			for row := 0; row < full.Rows; row++ {
				if full.IsValid(row) {
					want[full.Code(row)]++
				}
			}
			for e := range want {
				if string(entries.DictValue(e)) != string(full.DictValue(e)) || counts[e] != want[e] {
					t.Fatalf("entry %d is %q with %d rows, want %q with %d", e, entries.DictValue(e), counts[e], full.DictValue(e), want[e])
				}
			}
			// The values are found as before.
			found, searched, err := b.FindEqual(ref, full.DictValue(0), nil)
			if err != nil || (searched && uint32(len(found)) != want[0]) {
				t.Fatalf("a search of the first entry: %d rows, searched %v: %v", len(found), searched, err)
			}
		})
	}
}
