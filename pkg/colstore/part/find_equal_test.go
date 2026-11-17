package part

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// TestFindEqualGivesTheRowsOfAColumnWithGaps requires the search of
// identifiers in the form they are stored in to give the rows that Decode
// gives the value, whether every row holds one, few do, few do not, or
// about half.
func TestFindEqualGivesTheRowsOfAColumnWithGaps(t *testing.T) {
	const rows = 4000
	for name, holds := range map[string]func(row int) bool{
		"every row":       func(int) bool { return true },
		"few rows":        func(row int) bool { return row%97 == 3 },
		"all but a few":   func(row int) bool { return row%97 != 3 },
		"every other row": func(row int) bool { return row%2 == 1 },
	} {
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(1, 2))
			id := func() string { return fmt.Sprintf("id%016x%016x", rng.Uint64(), rng.Uint64()) }
			docs := make([]string, rows)
			var ids []string
			for row := range docs {
				docs[row] = fmt.Sprintf(`{"n":%d}`, row)
				if holds(row) {
					ids = append(ids, id())
					// A value now and then repeats the one before it.
					if len(ids) > 1 && row%61 == 0 {
						ids[len(ids)-1] = ids[len(ids)-2]
					}
					docs[row] = fmt.Sprintf(`{"n":%d,"id":%q}`, row, ids[len(ids)-1])
				}
			}
			p := buildPart(t, docs, rows, Options{})
			path, ok := p.PathID("id")
			if !ok {
				t.Fatal("no id column")
			}
			var b Block
			if err := p.OpenBlock(0, &b); err != nil {
				t.Fatal(err)
			}
			ref, ok := b.Column(path, vec.KindString)
			if !ok {
				t.Fatal("the block has no id column")
			}
			var col vec.Column
			if err := b.Decode(ref, &col); err != nil {
				t.Fatal(err)
			}
			wanted := append([]string{id()}, ids[0], ids[len(ids)/2], ids[len(ids)-1])
			for _, want := range wanted {
				var expect []uint32
				for row := 0; row < col.Rows; row++ {
					if col.IsValid(row) && string(col.Bytes(row)) == want {
						expect = append(expect, uint32(row))
					}
				}
				got, searched, err := b.FindEqual(ref, []byte(want), nil)
				if err != nil || !searched {
					t.Fatalf("%s: searched %v: %v", want, searched, err)
				}
				if !slices.Equal(got, expect) {
					t.Fatalf("%s: rows %v, want %v", want, got, expect)
				}
			}
		})
	}
}
