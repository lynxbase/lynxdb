package part

import (
	"math"
	"math/rand"
	"testing"
)

func roundTripInts(t *testing.T, name string, vals []int64) int {
	t.Helper()
	var e encodeScratch
	var d decodeScratch
	for _, level := range []Level{LevelFast, LevelCompact} {
		enc := appendInts([]byte{0xAA}, vals, &blobCodec{level: level}, &e)
		enc = append(enc, 0xBB)
		got, rest, err := readInts(nil, enc[1:], len(vals), &d)
		if err != nil {
			t.Fatalf("%s: decode: %v", name, err)
		}
		if len(rest) != 1 || rest[0] != 0xBB {
			t.Fatalf("%s: section length misjudged, %d bytes left", name, len(rest))
		}
		for i := range vals {
			if got[i] != vals[i] {
				t.Fatalf("%s: value %d = %d, want %d (mode %d)", name, i, got[i], vals[i], enc[1])
			}
		}
		if level == LevelCompact {
			return len(enc) - 2
		}
	}

	return 0
}

func TestIntCodecRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	roundTripInts(t, "empty", nil)
	roundTripInts(t, "single", []int64{-7})
	roundTripInts(t, "extremes", []int64{math.MinInt64, math.MaxInt64, 0, -1, 1, math.MinInt64, math.MaxInt64})

	for width := 0; width <= 64; width++ {
		for _, n := range []int{1, 2, 63, 127, 128, 129, 300, 1000} {
			vals := make([]int64, n)
			for i := range vals {
				var u uint64
				if width > 0 {
					u = r.Uint64() >> (64 - width)
				}
				vals[i] = int64(u) - 1000
			}
			roundTripInts(t, "random width", vals)
		}
	}
}

// TestIntCodecTakesOutACommonFactor requires that values that are whole
// multiples above their least take the space of the multiples.
func TestIntCodecTakesOutACommonFactor(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	const n = 4096
	for _, factor := range []int64{2, 7, 1000, 1_000_000, 1_000_000_000, 1 << 40} {
		for _, base := range []int64{0, -12345, 1_700_000_000_000_000_000, math.MinInt64 + 5} {
			vals, counts := make([]int64, n), make([]int64, n)
			for i := range vals {
				counts[i] = r.Int63n(1 << 20)
				vals[i] = base + counts[i]*factor
			}
			counts[0], vals[0] = 0, base
			counts[1], vals[1] = 1, base+factor
			size, plain := roundTripInts(t, "multiples", vals), roundTripInts(t, "counts", counts)
			if size > plain+24 {
				t.Errorf("multiples of %d above %d took %d bytes, their counts %d", factor, base, size, plain)
			}
		}
	}
	roundTripInts(t, "the widest span", append(make([]int64, 30), math.MinInt64, math.MaxInt64-1, 0, 2, -2))
	if got := commonFactor([]int64{10, 16, 22, 13}, 10); got != 3 {
		t.Errorf("the common factor of distances 0, 6, 12, 3 is %d", got)
	}
}

func TestIntCodecPicksCompactModes(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	const n = 16384

	timestamps := make([]int64, n)
	ts := int64(1732206349000167)
	for i := range timestamps {
		ts += 200 + r.Int63n(600)
		timestamps[i] = ts
	}
	if size := roundTripInts(t, "timestamps", timestamps); size > n*3/2 {
		t.Errorf("increasing timestamps took %d bytes, more than 1.5 per value", size)
	}

	status := make([]int64, n)
	for i := range status {
		status[i] = 200
		if r.Intn(50) == 0 {
			status[i] = 404
		}
	}
	if size := roundTripInts(t, "status", status); size > n/4 {
		t.Errorf("skewed status codes took %d bytes, more than 2 bits per value", size)
	}

	constant := make([]int64, n)
	for i := range constant {
		constant[i] = 42
	}
	if size := roundTripInts(t, "constant", constant); size > 600 {
		t.Errorf("constant column took %d bytes", size)
	}
}

func TestReadIntsRejectsTruncatedSections(t *testing.T) {
	var e encodeScratch
	var d decodeScratch
	vals := make([]int64, 500)
	for i := range vals {
		vals[i] = int64(i*i) - 9000
	}
	thousands := make([]int64, 200)
	for i := range thousands {
		thousands[i] = int64(i*i%977) * 1000
	}
	for _, src := range [][]int64{vals, thousands, {5, 5, 5, 6, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5}} {
		enc := appendInts(nil, src, &blobCodec{}, &e)
		for cut := 0; cut < len(enc); cut++ {
			if _, _, err := readInts(nil, enc[:cut], len(src), &d); err == nil {
				t.Fatalf("mode %d: prefix of %d/%d bytes decoded without error", enc[0], cut, len(enc))
			}
		}
	}
}

func BenchmarkReadInts(b *testing.B) {
	r := rand.New(rand.NewSource(3))
	vals := make([]int64, 16384)
	ts := int64(1732206349000167)
	for i := range vals {
		ts += 200 + r.Int63n(600)
		vals[i] = ts
	}
	var e encodeScratch
	var d decodeScratch
	enc := appendInts(nil, vals, &blobCodec{}, &e)
	dst := make([]int64, len(vals))
	b.SetBytes(int64(len(vals) * 8))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := readInts(dst, enc, len(vals), &d); err != nil {
			b.Fatal(err)
		}
	}
}
