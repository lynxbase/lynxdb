package segment

import "sort"

// FindTimeRange returns [startRow, endRow) for events within [earliest, latest] (nanoseconds).
// Binary search on sorted timestamp column. O(log n).
func FindTimeRange(timestamps []int64, earliest, latest int64) (start, end int) {
	start = sort.Search(len(timestamps), func(i int) bool {
		return timestamps[i] >= earliest
	})
	end = sort.Search(len(timestamps), func(i int) bool {
		return timestamps[i] > latest
	})

	return start, end
}

// InterpolationSearch finds the position of target in a sorted int64 slice.
// O(log log n) for uniformly distributed data.
func InterpolationSearch(timestamps []int64, target int64) int {
	lo, hi := 0, len(timestamps)-1
	if hi < 0 {
		return 0
	}
	for lo <= hi && target >= timestamps[lo] && target <= timestamps[hi] {
		if lo == hi {
			return lo
		}
		// Guard against division by zero
		denom := float64(timestamps[hi] - timestamps[lo])
		if denom == 0 {
			return lo
		}
		pos := lo + int(float64(hi-lo)*float64(target-timestamps[lo])/denom)
		if pos < lo {
			pos = lo
		}
		if pos > hi {
			pos = hi
		}
		if timestamps[pos] == target {
			return pos
		} else if timestamps[pos] < target {
			lo = pos + 1
		} else {
			hi = pos - 1
		}
	}

	return lo
}
