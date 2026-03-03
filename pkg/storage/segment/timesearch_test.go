package segment

import (
	"math/rand"
	"sort"
	"testing"
)

func TestFindTimeRangeExact(t *testing.T) {
	// Timestamps at boundaries
	ts := []int64{10, 20, 30, 40, 50}
	start, end := FindTimeRange(ts, 20, 40)
	if start != 1 || end != 4 {
		t.Errorf("got [%d, %d), want [1, 4)", start, end)
	}
}

func TestFindTimeRangeSubset(t *testing.T) {
	ts := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	start, end := FindTimeRange(ts, 35, 75)
	if start != 3 || end != 7 {
		t.Errorf("got [%d, %d), want [3, 7)", start, end)
	}
}

func TestFindTimeRangeEmpty(t *testing.T) {
	ts := []int64{10, 20, 30, 40, 50}
	// Range outside segment
	start, end := FindTimeRange(ts, 60, 100)
	if start != end {
		t.Errorf("expected empty range, got [%d, %d)", start, end)
	}
}

func TestFindTimeRangeFull(t *testing.T) {
	ts := []int64{10, 20, 30, 40, 50}
	start, end := FindTimeRange(ts, 0, 100)
	if start != 0 || end != 5 {
		t.Errorf("got [%d, %d), want [0, 5)", start, end)
	}
}

func TestFindTimeRangeEmptySlice(t *testing.T) {
	var ts []int64
	start, end := FindTimeRange(ts, 0, 100)
	if start != 0 || end != 0 {
		t.Errorf("got [%d, %d), want [0, 0)", start, end)
	}
}

func TestInterpolationSearch(t *testing.T) {
	// Uniform data
	ts := make([]int64, 1000)
	for i := range ts {
		ts[i] = int64(i * 10)
	}
	idx := InterpolationSearch(ts, 500)
	if ts[idx] != 500 {
		t.Errorf("got index %d (val=%d), want val=500", idx, ts[idx])
	}
}

func TestInterpolationSearchSkewed(t *testing.T) {
	// Skewed data: exponential distribution
	ts := make([]int64, 100)
	for i := range ts {
		ts[i] = int64(i * i) // 0, 1, 4, 9, 16, 25, ...
	}
	target := int64(49) // 7*7
	idx := InterpolationSearch(ts, target)
	if idx < 0 || idx >= len(ts) {
		t.Fatalf("out of bounds: %d", idx)
	}
	if ts[idx] != target {
		t.Errorf("got index %d (val=%d), want val=%d", idx, ts[idx], target)
	}
}

func TestInterpolationSearchNotFound(t *testing.T) {
	ts := []int64{10, 20, 30, 40, 50}
	// Target between values
	idx := InterpolationSearch(ts, 25)
	// Should return position where 25 would be inserted (2 or 3)
	if idx < 1 || idx > 3 {
		t.Errorf("unexpected index for missing value: %d", idx)
	}
}

func TestInterpolationSearchSingleElement(t *testing.T) {
	ts := []int64{42}
	idx := InterpolationSearch(ts, 42)
	if idx != 0 {
		t.Errorf("got %d, want 0", idx)
	}
}

func BenchmarkBinarySearchVsFullScan(b *testing.B) {
	n := 1000000
	ts := make([]int64, n)
	for i := range ts {
		ts[i] = int64(i)
	}
	earliest := int64(n / 2)
	latest := int64(n/2 + 100)

	b.Run("BinarySearch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			FindTimeRange(ts, earliest, latest)
		}
	})

	b.Run("FullScan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			start, end := -1, -1
			for j, v := range ts {
				if v >= earliest && start == -1 {
					start = j
				}
				if v > latest {
					end = j

					break
				}
			}
			if end == -1 {
				end = n
			}
			_ = start
			_ = end
		}
	})
}

func BenchmarkInterpolationVsBinary(b *testing.B) {
	n := 1000000
	ts := make([]int64, n)
	for i := range ts {
		ts[i] = int64(i * 10)
	}
	target := int64(n * 5) // middle

	b.Run("Interpolation", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			InterpolationSearch(ts, target)
		}
	})

	b.Run("Binary", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sort.Search(n, func(i int) bool { return ts[i] >= target })
		}
	})
}

// Verify binary search produces same results as naive scan for random data.
func TestFindTimeRangeRandomData(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for trial := 0; trial < 100; trial++ {
		n := rng.Intn(1000) + 1
		ts := make([]int64, n)
		for i := range ts {
			ts[i] = int64(rng.Intn(10000))
		}
		sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })

		earliest := int64(rng.Intn(10000))
		latest := earliest + int64(rng.Intn(2000))

		start, end := FindTimeRange(ts, earliest, latest)

		// Verify with naive scan
		naiveStart, naiveEnd := n, n
		for i := 0; i < n; i++ {
			if ts[i] >= earliest {
				naiveStart = i

				break
			}
		}
		for i := 0; i < n; i++ {
			if ts[i] > latest {
				naiveEnd = i

				break
			}
		}

		if start != naiveStart || end != naiveEnd {
			t.Errorf("trial %d: binary [%d,%d) != naive [%d,%d) for earliest=%d latest=%d",
				trial, start, end, naiveStart, naiveEnd, earliest, latest)
		}
	}
}
