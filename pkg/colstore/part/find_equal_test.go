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
// gives the value: for values stored as they are and as a dictionary with
// codes of either width or in runs, whether every row holds a value, few
// do, few do not, or about half.
func TestFindEqualGivesTheRowsOfAColumnWithGaps(t *testing.T) {
	const rows = 4000
	presence := map[string]func(row int) bool{
		"every row":       func(int) bool { return true },
		"few rows":        func(row int) bool { return row%29 == 3 },
		"all but a few":   func(row int) bool { return row%97 != 3 },
		"every other row": func(row int) bool { return row%2 == 1 },
	}
	// pick returns the number of the identifier of the i-th of n values.
	values := map[string]struct {
		enc  uint8
		pick func(rng *rand.Rand, i, n int) int
	}{
		"distinct but for a few": {encFlat, func(_ *rand.Rand, i, _ int) int { return i - i%61/60 }},
		"six in ten distinct":    {encDict, func(rng *rand.Rand, i, _ int) int { return rng.IntN(i*6/10 + 1) }},
		"of forty":               {encDict, func(rng *rand.Rand, _, _ int) int { return rng.IntN(40) }},
		"in twenty runs":         {encDict, func(_ *rand.Rand, i, n int) int { return i * 20 / n }},
	}
	for vname, v := range values {
		for pname, holds := range presence {
			t.Run(vname+", "+pname, func(t *testing.T) {
				rng := rand.New(rand.NewPCG(1, 2))
				var ids []string
				id := func(n int) string {
					for len(ids) <= n {
						ids = append(ids, fmt.Sprintf("id%016x%016x", rng.Uint64(), rng.Uint64()))
					}

					return ids[n]
				}
				docs := make([]string, rows)
				n := 0
				for row := range docs {
					if holds(row) {
						n++
					}
				}
				var held []string
				for row := range docs {
					docs[row] = fmt.Sprintf(`{"n":%d}`, row)
					if holds(row) {
						held = append(held, id(v.pick(rng, len(held), n)))
						docs[row] = fmt.Sprintf(`{"n":%d,"id":%q}`, row, held[len(held)-1])
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
				if ref.enc != v.enc {
					t.Fatalf("stored with encoding %d, want %d", ref.enc, v.enc)
				}
				var col vec.Column
				if err := b.Decode(ref, &col); err != nil {
					t.Fatal(err)
				}
				absent := fmt.Sprintf("id%032x", 7)
				for _, want := range []string{absent, "short", held[0], held[len(held)/2], held[len(held)-1]} {
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
}
