package pipeline

import (
	"math"
	"sort"
)

// TDigest implements the t-digest algorithm for approximate quantile estimation.
// Uses a simplified merging approach with a compression parameter.
type TDigest struct {
	centroids   []centroid
	compression float64
	count       float64
	maxSize     int
}

type centroid struct {
	mean  float64
	count float64
}

// NewTDigest creates a new t-digest with the given compression parameter.
// Higher compression = more accuracy but more memory. Default: 100.
func NewTDigest(compression float64) *TDigest {
	if compression <= 0 {
		compression = 100
	}

	return &TDigest{
		compression: compression,
		maxSize:     int(compression) * 5,
	}
}

// Add adds a value to the t-digest.
func (td *TDigest) Add(value float64) {
	td.centroids = append(td.centroids, centroid{mean: value, count: 1})
	td.count++
	if len(td.centroids) > td.maxSize {
		td.compress()
	}
}

// Quantile returns the approximate value at the given quantile (0.0 to 1.0).
func (td *TDigest) Quantile(q float64) float64 {
	if len(td.centroids) == 0 {
		return 0
	}
	td.compress()

	if q <= 0 {
		return td.centroids[0].mean
	}
	if q >= 1 {
		return td.centroids[len(td.centroids)-1].mean
	}

	target := q * td.count
	cumulative := 0.0

	for i, c := range td.centroids {
		cumulative += c.count
		if cumulative >= target {
			if i == 0 {
				return c.mean
			}
			// Linear interpolation between centroids.
			prev := td.centroids[i-1]
			prevCum := cumulative - c.count
			frac := (target - prevCum) / c.count

			return prev.mean + frac*(c.mean-prev.mean)
		}
	}

	return td.centroids[len(td.centroids)-1].mean
}

// Merge merges another t-digest into this one.
func (td *TDigest) Merge(other *TDigest) {
	td.centroids = append(td.centroids, other.centroids...)
	td.count += other.count
	if len(td.centroids) > td.maxSize {
		td.compress()
	}
}

// Count returns the total number of values added.
func (td *TDigest) Count() float64 {
	return td.count
}

func (td *TDigest) compress() {
	if len(td.centroids) <= 1 {
		return
	}
	sort.Slice(td.centroids, func(i, j int) bool {
		return td.centroids[i].mean < td.centroids[j].mean
	})

	// Merge adjacent centroids respecting the k-size bound from the t-digest
	// paper. The maximum weight for a centroid at quantile q is:
	//   maxWeight = 4 * N * q * (1-q) / δ
	// where N = total count, δ = compression parameter. This ensures:
	//   - Tails (q≈0, q≈1) have small centroids → high accuracy
	//   - Middle (q≈0.5) has large centroids → aggressive compression
	//   - When N < δ, no merging occurs (more data points than needed)
	merged := make([]centroid, 0, len(td.centroids)/2+1)
	merged = append(merged, td.centroids[0])
	// cumWeight tracks the total weight of all finalized (non-last) centroids.
	cumWeight := 0.0

	for i := 1; i < len(td.centroids); i++ {
		last := &merged[len(merged)-1]
		proposedCount := last.count + td.centroids[i].count
		// q is the quantile of the right edge of the proposed merged centroid.
		qRight := (cumWeight + proposedCount) / td.count
		maxSize := 4 * td.count * qRight * (1 - qRight) / td.compression
		if maxSize < 1 {
			maxSize = 1
		}

		if proposedCount <= maxSize {
			// Merge: weighted average.
			last.mean = (last.mean*last.count + td.centroids[i].mean*td.centroids[i].count) / proposedCount
			last.count = proposedCount
		} else {
			cumWeight += last.count
			merged = append(merged, td.centroids[i])
		}
	}

	td.centroids = merged
}

// MedianAbsoluteError returns the estimated error for the given quantile.
func (td *TDigest) MedianAbsoluteError(q float64) float64 {
	if len(td.centroids) < 2 {
		return 0
	}
	// Very rough estimate of error based on compression parameter.
	return 1.0 / td.compression * math.Abs(q*(1-q))
}

// MarshalBinary serializes the t-digest to a byte slice for spill persistence.
// Layout: [compression:8][count:8][numCentroids:4][centroids: n*(mean:8+count:8)].
func (td *TDigest) MarshalBinary() []byte {
	n := len(td.centroids)
	buf := make([]byte, 8+8+4+n*16)
	off := 0
	putFloat64(buf[off:], td.compression)
	off += 8
	putFloat64(buf[off:], td.count)
	off += 8
	buf[off] = byte(n)
	buf[off+1] = byte(n >> 8)
	buf[off+2] = byte(n >> 16)
	buf[off+3] = byte(n >> 24)
	off += 4
	for _, c := range td.centroids {
		putFloat64(buf[off:], c.mean)
		off += 8
		putFloat64(buf[off:], c.count)
		off += 8
	}

	return buf
}

// UnmarshalTDigest deserializes a t-digest from bytes produced by MarshalBinary.
// Returns nil if data is too short or corrupted.
func UnmarshalTDigest(data []byte) *TDigest {
	if len(data) < 20 { // 8+8+4 minimum
		return nil
	}
	off := 0
	compression := getFloat64(data[off:])
	off += 8
	count := getFloat64(data[off:])
	off += 8
	n := int(data[off]) | int(data[off+1])<<8 | int(data[off+2])<<16 | int(data[off+3])<<24
	off += 4
	if len(data) < off+n*16 {
		return nil
	}
	centroids := make([]centroid, n)
	for i := 0; i < n; i++ {
		centroids[i].mean = getFloat64(data[off:])
		off += 8
		centroids[i].count = getFloat64(data[off:])
		off += 8
	}

	td := &TDigest{
		centroids:   centroids,
		compression: compression,
		count:       count,
		maxSize:     int(compression) * 5,
	}

	return td
}

func putFloat64(buf []byte, v float64) {
	bits := math.Float64bits(v)
	buf[0] = byte(bits)
	buf[1] = byte(bits >> 8)
	buf[2] = byte(bits >> 16)
	buf[3] = byte(bits >> 24)
	buf[4] = byte(bits >> 32)
	buf[5] = byte(bits >> 40)
	buf[6] = byte(bits >> 48)
	buf[7] = byte(bits >> 56)
}

func getFloat64(buf []byte) float64 {
	bits := uint64(buf[0]) | uint64(buf[1])<<8 | uint64(buf[2])<<16 | uint64(buf[3])<<24 |
		uint64(buf[4])<<32 | uint64(buf[5])<<40 | uint64(buf[6])<<48 | uint64(buf[7])<<56

	return math.Float64frombits(bits)
}
