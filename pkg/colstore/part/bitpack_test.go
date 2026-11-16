package part

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

func TestSpreadPlacesValuesAtSetBits(t *testing.T) {
	r := rand.New(rand.NewSource(4))
	for _, rows := range []int{1, 63, 64, 65, 200, 4096, 5000} {
		for _, density := range []float64{0, 0.01, 0.5, 0.99, 1} {
			valid := make(vec.Bitmap, vec.BitmapWords(rows))
			var src []int64
			want := make([]int64, rows)
			for i := 0; i < rows; i++ {
				if r.Float64() < density {
					valid.Set(i)
					src = append(src, int64(i)+1)
					want[i] = int64(i) + 1
				}
			}
			dst := make([]int64, rows)
			spread(dst, src, valid)
			for i := range want {
				if dst[i] != want[i] {
					t.Fatalf("rows %d density %g: dst[%d] = %d, want %d", rows, density, i, dst[i], want[i])
				}
			}
		}
	}
}

func TestPresenceLayoutsRoundTrip(t *testing.T) {
	var docs []string
	for i := 0; i < 6000; i++ {
		doc := fmt.Sprintf(`{"n":%d`, i)
		if i%1000 != 3 {
			doc += fmt.Sprintf(`,"mostly":"m%d"`, i%5)
		}
		if i%1000 == 3 {
			doc += `,"rarely":"r"`
		}
		if i%2 == 0 {
			doc += fmt.Sprintf(`,"half":%d`, i)
		}
		docs = append(docs, doc+"}")
	}
	p := buildPart(t, docs, 6000, Options{})
	got, _ := readAll(t, p)
	for i := range docs {
		if !sameJSON(t, got[i], docs[i]) {
			t.Fatalf("row %d\n got  %s\n want %s", i, got[i], docs[i])
		}
	}

	var b Block
	if err := p.OpenBlock(0, &b); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path string
		kind vec.Kind
		want byte
	}{
		{"mostly", vec.KindString, presenceHoles},
		{"rarely", vec.KindString, presenceList},
		{"half", vec.KindInt, presenceBitmap},
	} {
		id, _ := p.PathID(tc.path)
		ref, ok := b.Column(id, tc.kind)
		if !ok {
			t.Fatalf("no %s column %s", tc.kind, tc.path)
		}
		if mode := b.data[ref.off]; mode != tc.want {
			t.Errorf("%s: presence mode %d, want %d", tc.path, mode, tc.want)
		}
	}
}
