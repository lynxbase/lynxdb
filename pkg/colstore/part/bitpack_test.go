package part

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

func TestPackedCodesRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(21))
	for width := uint(1); width <= 16; width++ {
		for _, size := range []int{1 << width, 1<<width - 1, 1<<(width-1) + 1} {
			for _, n := range []int{0, 1, 7, 8, 9, 63, 64, 65, 1000, 1003} {
				codes := make([]uint32, n)
				rank := make([]uint32, size)
				for i := range rank {
					rank[i] = uint32(i)
				}
				for i := range codes {
					codes[i] = uint32(r.Intn(size))
				}
				if n > 2 {
					codes[n-1], codes[n-2] = uint32(size-1), 0
				}
				packed := appendPacked(nil, codes, rank, width)
				if len(packed) != packedSize(n, width) {
					t.Fatalf("width %d: %d codes took %d bytes, want %d", width, n, len(packed), packedSize(n, width))
				}
				got, ok := unpackAny(packed, n, width, size)
				if !ok {
					t.Fatalf("width %d size %d n %d: valid codes rejected", width, size, n)
				}
				for i := range codes {
					if got[i] != codes[i] {
						t.Fatalf("width %d size %d n %d: code %d = %d, want %d", width, size, n, i, got[i], codes[i])
					}
				}
				if size == 1<<width || n == 0 {
					continue
				}
				// A code equal to the dictionary size must be noticed
				// wherever it sits.
				for _, at := range []int{0, n / 2, n - 1} {
					bad := append([]uint32(nil), codes...)
					bad[at] = uint32(size)
					wide := append(rank, uint32(size))
					if _, ok := unpackAny(appendPacked(nil, bad, wide, width), n, width, size); ok {
						t.Fatalf("width %d size %d n %d: code %d out of range at %d went unnoticed", width, size, n, size, at)
					}
				}
			}
		}
	}
}

func unpackAny(packed []byte, n int, width uint, size int) ([]uint32, bool) {
	out := make([]uint32, n)
	if width <= 8 {
		dst := make([]uint8, n)
		ok := unpack8(dst, packed, width, size)
		for i, v := range dst {
			out[i] = uint32(v)
		}

		return out, ok
	}
	dst := make([]uint16, n)
	ok := unpack16(dst, packed, width, size)
	for i, v := range dst {
		out[i] = uint32(v)
	}

	return out, ok
}

func TestReadCodesExpandsRuns(t *testing.T) {
	runs := []byte{2, 3, 0, 1, 5, 4}
	dst := make([]uint8, 8)
	if !readCodes(dst, runs, codesRuns, 3, 6, unpack8) {
		t.Fatal("valid runs rejected")
	}
	if want := []uint8{2, 2, 2, 0, 5, 5, 5, 5}; string(dst) != string(want) {
		t.Fatalf("runs expanded to %v, want %v", dst, want)
	}
	for name, bad := range map[string][]byte{
		"short":         {2, 3, 0, 1, 5, 3},
		"long":          {2, 3, 0, 1, 5, 5},
		"empty run":     {2, 3, 0, 0, 5, 5},
		"code too high": {2, 3, 6, 1, 5, 4},
		"cut":           {2, 3, 0, 1, 5},
	} {
		if readCodes(make([]uint8, 8), bad, codesRuns, 3, 6, unpack8) {
			t.Errorf("%s: damaged runs accepted", name)
		}
	}
}

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

func TestCodesAndPresenceLayoutsRoundTrip(t *testing.T) {
	var docs []string
	r := rand.New(rand.NewSource(8))
	for i := 0; i < 6000; i++ {
		// level changes rarely, so its codes form long runs; shard does not.
		doc := fmt.Sprintf(`{"level":"%s","shard":"s%d"`, []string{"debug", "info", "warn"}[i/2500], r.Intn(700))
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
	chunk := func(path string, kind vec.Kind) []byte {
		t.Helper()
		id, _ := p.PathID(path)
		ref, ok := b.Column(id, kind)
		if !ok {
			t.Fatalf("no %s column %s", kind, path)
		}

		return b.data[ref.off : ref.off+ref.length]
	}
	for path, want := range map[string]byte{"mostly": presenceHoles, "rarely": presenceList} {
		if mode := chunk(path, vec.KindString)[0]; mode != want {
			t.Errorf("%s: presence mode %d, want %d", path, mode, want)
		}
	}
	if mode := chunk("half", vec.KindInt)[0]; mode != presenceBitmap {
		t.Errorf("half: presence mode %d, want %d", mode, presenceBitmap)
	}
	if n := len(chunk("level", vec.KindString)); n > 64 {
		t.Errorf("level: three runs of codes took %d bytes", n)
	}
	if n, packed := len(chunk("shard", vec.KindString)), packedSize(6000, 10); n < packed {
		t.Errorf("shard: %d bytes cannot hold %d bytes of packed codes", n, packed)
	}
}
