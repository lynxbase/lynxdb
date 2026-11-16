package vec

import (
	"math/rand"
	"testing"
)

func TestBitmap(t *testing.T) {
	const n = 200
	b := make(Bitmap, BitmapWords(n))
	want := []uint32{0, 63, 64, 65, 127, 199}
	for _, i := range want {
		b.Set(int(i))
	}
	if b.Count() != len(want) {
		t.Fatalf("Count = %d", b.Count())
	}
	got := b.AppendSet(nil)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("AppendSet = %v, want %v", got, want)
		}
	}
	if b.Rank(65) != 3 || b.Rank(0) != 0 || b.Rank(200) != 6 {
		t.Fatalf("Rank wrong: %d %d %d", b.Rank(65), b.Rank(0), b.Rank(200))
	}

	all := make(Bitmap, BitmapWords(n))
	all.SetAll(n)
	if all.Count() != n {
		t.Fatalf("SetAll count = %d", all.Count())
	}
}

func TestMaskKernelsMatchReference(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	for _, n := range []int{0, 1, 7, 8, 15, 16, 17, 31, 63, 64, 65, 255, 1000, 4097, 40000} {
		src := make([]uint8, n)
		src16 := make([]uint16, n)
		ints := make([]int64, n)
		for i := range src {
			src[i] = uint8(r.Intn(5))
			src16[i] = uint16(r.Intn(700))
			ints[i] = r.Int63n(2000) - 1000
		}

		eq := make([]uint8, n)
		MaskEq8(eq, src, 3)
		eq16 := make([]uint8, n)
		MaskEq16(eq16, src16, 77)
		rng := make([]uint8, n)
		MaskRangeInt64(rng, ints, -100, 250)
		and := make([]uint8, n)
		MaskAnd(and, eq, rng)
		or := make([]uint8, n)
		MaskOr(or, eq, rng)

		wantCount := 0
		var wantIdx []uint32
		for i := 0; i < n; i++ {
			e := src[i] == 3
			g := ints[i] >= -100 && ints[i] <= 250
			check := func(name string, got uint8, want bool) {
				t.Helper()
				if (got == 0xFF) != want || (got != 0 && got != 0xFF) {
					t.Fatalf("n=%d %s[%d] = %#x, want %v", n, name, i, got, want)
				}
			}
			check("eq", eq[i], e)
			check("eq16", eq16[i], src16[i] == 77)
			check("range", rng[i], g)
			check("and", and[i], e && g)
			check("or", or[i], e || g)
			if e && g {
				wantCount++
				wantIdx = append(wantIdx, uint32(i))
			}
		}
		if got := MaskCount(and); got != wantCount {
			t.Fatalf("n=%d MaskCount = %d, want %d", n, got, wantCount)
		}
		idx := MaskIndexes(nil, and)
		if len(idx) != len(wantIdx) {
			t.Fatalf("n=%d MaskIndexes len = %d, want %d", n, len(idx), len(wantIdx))
		}
		for i := range idx {
			if idx[i] != wantIdx[i] {
				t.Fatalf("n=%d MaskIndexes[%d] = %d, want %d", n, i, idx[i], wantIdx[i])
			}
		}
	}
}

func TestAggregateKernels(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	ints := make([]int64, 1003)
	codes := make([]uint8, 1003)
	var sum int64
	var want [256]uint32
	lo, hi := int64(1<<62), int64(-1<<62)
	for i := range ints {
		ints[i] = r.Int63n(1_000_000) - 500_000
		codes[i] = uint8(r.Intn(256))
		sum += ints[i]
		want[codes[i]]++
		lo, hi = min(lo, ints[i]), max(hi, ints[i])
	}
	if got := SumInt64(ints); got != sum {
		t.Fatalf("SumInt64 = %d, want %d", got, sum)
	}
	if gl, gh := MinMaxInt64(ints); gl != lo || gh != hi {
		t.Fatalf("MinMaxInt64 = %d,%d want %d,%d", gl, gh, lo, hi)
	}
	var got [256]uint32
	Histogram8(&got, codes)
	if got != want {
		t.Fatal("Histogram8 mismatch")
	}
}

func TestColumnAccessors(t *testing.T) {
	dict := Column{
		Kind: KindString, Layout: Dict, Rows: 3,
		Offsets: []uint32{0, 1, 3}, Data: []byte("abb"), Codes8: []uint8{1, 0, 1},
	}
	if string(dict.Bytes(0)) != "bb" || string(dict.Bytes(1)) != "a" || dict.DictLen() != 2 {
		t.Fatal("dict accessors wrong")
	}
	flat := Column{Kind: KindString, Rows: 2, Offsets: []uint32{0, 2, 5}, Data: []byte("hiyou")}
	if string(flat.Bytes(1)) != "you" {
		t.Fatal("flat accessor wrong")
	}
	valid := make(Bitmap, 1)
	valid.Set(1)
	c := Column{Kind: KindInt, Layout: Const, Rows: 2, Ints: []int64{9}, Valid: valid}
	if c.Int(1) != 9 || c.IsValid(0) || !c.IsValid(1) {
		t.Fatal("const accessor wrong")
	}
}

var (
	benchCodes = func() []uint8 {
		b := make([]uint8, 1<<16)
		r := rand.New(rand.NewSource(1))
		for i := range b {
			b[i] = uint8(r.Intn(8))
		}

		return b
	}()
	benchMask = make([]uint8, 1<<16)
	benchSink int
)

func BenchmarkMaskEq8(b *testing.B) {
	b.SetBytes(int64(len(benchCodes)))
	for i := 0; i < b.N; i++ {
		MaskEq8(benchMask, benchCodes, 3)
	}
}

func BenchmarkMaskCount(b *testing.B) {
	MaskEq8(benchMask, benchCodes, 3)
	b.SetBytes(int64(len(benchMask)))
	for i := 0; i < b.N; i++ {
		benchSink = MaskCount(benchMask)
	}
}

func BenchmarkHistogram8(b *testing.B) {
	b.SetBytes(int64(len(benchCodes)))
	var counts [256]uint32
	for i := 0; i < b.N; i++ {
		Histogram8(&counts, benchCodes)
	}
}

func TestMaskEqBytesAcrossLayouts(t *testing.T) {
	values := []string{"post", "", "like", "post", "repost", "like", "post"}
	absent := 1
	valid := make(Bitmap, 1)
	for i := range values {
		if i != absent {
			valid.Set(i)
		}
	}

	flat := Column{Kind: KindString, Rows: len(values), Valid: valid, Offsets: []uint32{0}}
	for _, v := range values {
		flat.Data = append(flat.Data, v...)
		flat.Offsets = append(flat.Offsets, uint32(len(flat.Data)))
	}
	dict := Column{
		Kind: KindString, Layout: Dict, Rows: len(values), Valid: valid,
		Data: []byte("likepostrepost"), Offsets: []uint32{0, 4, 8, 14},
		Codes8: []uint8{1, 0, 0, 1, 2, 0, 1},
	}
	wide := dict
	wide.Codes8, wide.Codes16 = nil, []uint16{1, 0, 0, 1, 2, 0, 1}
	constant := Column{Kind: KindString, Layout: Const, Rows: len(values), Valid: valid, Data: []byte("post"), Offsets: []uint32{0, 4}}

	for _, want := range []string{"post", "like", "repost", "", "missing", "zzz"} {
		mask := make([]uint8, len(values))
		for name, c := range map[string]*Column{"flat": &flat, "dict": &dict, "wide dict": &wide} {
			MaskEqBytes(mask, c, []byte(want))
			for i, v := range values {
				if sel := mask[i] == 0xFF; sel != (v == want && i != absent) {
					t.Fatalf("%s: row %d selected=%v for %q", name, i, sel, want)
				}
			}
		}
		MaskEqBytes(mask, &constant, []byte(want))
		for i := range values {
			if sel := mask[i] == 0xFF; sel != (want == "post" && i != absent) {
				t.Fatalf("const: row %d selected=%v for %q", i, sel, want)
			}
		}
	}
	if code, ok := dict.DictFind([]byte("post")); !ok || code != 1 {
		t.Fatalf("DictFind(post) = %d, %v", code, ok)
	}
	if _, ok := dict.DictFind([]byte("pos")); ok {
		t.Fatal("DictFind matched a prefix")
	}
}

func TestColumnRetainedCountsItsBuffers(t *testing.T) {
	var c Column
	if c.Retained() != 0 {
		t.Fatalf("empty column retains %d bytes", c.Retained())
	}
	c.Ints = make([]int64, 3, 10)
	c.Offsets = make([]uint32, 5)
	c.Data = make([]byte, 0, 7)
	c.Codes16 = make([]uint16, 4)
	c.UseValid(100)
	if want := 80 + 20 + 7 + 8 + 16; c.Retained() != want {
		t.Fatalf("Retained = %d, want %d", c.Retained(), want)
	}
}
