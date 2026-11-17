package vec

import (
	"fmt"
	"math/rand"
	"testing"
)

// windowColumns returns columns of every kind and layout over rows rows, a
// third of which hold no value when sparse is set.
func windowColumns(rng *rand.Rand, rows int, sparse bool) []Column {
	var valid Bitmap
	if sparse {
		valid = make(Bitmap, BitmapWords(rows))
		for i := 0; i < rows; i++ {
			if rng.Intn(3) != 0 {
				valid.Set(i)
			}
		}
	}
	ints, floats := make([]int64, rows), make([]float64, rows)
	bools := make(Bitmap, BitmapWords(rows))
	offsets, data := []uint32{0}, []byte(nil)
	codes8, codes16 := make([]uint8, rows), make([]uint16, rows)
	for i := 0; i < rows; i++ {
		ints[i], floats[i] = rng.Int63(), rng.Float64()
		if rng.Intn(2) == 0 {
			bools.Set(i)
		}
		data = append(data, fmt.Sprintf("v%d", rng.Intn(1000))...)
		offsets = append(offsets, uint32(len(data)))
		codes8[i], codes16[i] = uint8(rng.Intn(3)), uint16(rng.Intn(3))
	}
	dictOffsets, dictData := []uint32{0, 1, 3, 6}, []byte("abbccc")

	return []Column{
		{Kind: KindInt, Rows: rows, Valid: valid, Ints: ints},
		{Kind: KindFloat, Rows: rows, Valid: valid, Floats: floats},
		{Kind: KindBool, Rows: rows, Valid: valid, Bools: bools},
		{Kind: KindString, Rows: rows, Valid: valid, Offsets: offsets, Data: data},
		{Kind: KindVariant, Rows: rows, Valid: valid, Offsets: offsets, Data: data},
		{Kind: KindString, Layout: Dict, Rows: rows, Valid: valid, Offsets: dictOffsets, Data: dictData, Codes8: codes8},
		{Kind: KindString, Layout: Dict, Rows: rows, Valid: valid, Offsets: dictOffsets, Data: dictData, Codes16: codes16},
		{Kind: KindString, Layout: Const, Rows: rows, Valid: valid, Offsets: dictOffsets[:2], Data: dictData},
		{Kind: KindInt, Layout: Const, Rows: rows, Valid: valid, Ints: []int64{7}},
		{Kind: KindFloat, Layout: Const, Rows: rows, Valid: valid, Floats: []float64{2.5}},
		{Kind: KindBool, Layout: Const, Rows: rows, Valid: valid, Bools: Bitmap{1}},
	}
}

func rowText(c *Column, i int) string {
	if !c.IsValid(i) {
		return "none"
	}
	switch c.Kind {
	case KindInt:
		return fmt.Sprint(c.Int(i))
	case KindFloat:
		return fmt.Sprint(c.Float(i))
	case KindBool:
		return fmt.Sprint(c.Bool(i))
	}

	return string(c.Bytes(i))
}

func TestWindowShowsTheRowsOfAColumn(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	const rows = 300
	var w Window
	for _, sparse := range []bool{false, true} {
		for ci, c := range windowColumns(rng, rows, sparse) {
			for trial := 0; trial < 200; trial++ {
				from := rng.Intn(rows + 1)
				to := from + rng.Intn(rows+1-from)
				if trial == 0 {
					from, to = 0, rows
				}
				w.Set(&c, from, to)
				if w.Rows != to-from || w.Kind != c.Kind || w.Layout != c.Layout {
					t.Fatalf("column %d window %d..%d: %d rows of kind %v", ci, from, to, w.Rows, w.Kind)
				}
				missing := 0
				for i := from; i < to; i++ {
					if got, want := rowText(&w.Column, i-from), rowText(&c, i); got != want {
						t.Fatalf("column %d sparse %v window %d..%d row %d: %s, want %s", ci, sparse, from, to, i, got, want)
					}
					if !c.IsValid(i) {
						missing++
					}
				}
				if (w.Valid != nil) != (missing > 0) {
					t.Fatalf("column %d window %d..%d: presence bits for %d missing rows: %v", ci, from, to, missing, w.Valid != nil)
				}
				if w.Valid != nil && w.Valid.Count() != to-from-missing {
					t.Fatalf("column %d window %d..%d: %d presence bits, want %d", ci, from, to, w.Valid.Count(), to-from-missing)
				}
			}
		}
	}
	w.Drop()
	if w.Ints != nil || w.Data != nil || w.Valid != nil {
		t.Fatal("a dropped window keeps the column's memory")
	}
}
