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
	td.AddWeighted(value, 1)
}

// AddWeighted adds a value with positive weight to the t-digest.
func (td *TDigest) AddWeighted(value, weight float64) {
	if weight <= 0 || math.IsNaN(value) || math.IsNaN(weight) ||
		math.IsInf(value, 0) || math.IsInf(weight, 0) {
		return
	}
	td.centroids = append(td.centroids, centroid{mean: value, count: weight})
	td.count += weight
	if len(td.centroids) > td.maxSize {
		td.compress()
	}
}

// Quantile returns the approximate value at the given quantile (0.0 to 1.0).
//
// Interpolation follows the t-digest paper: each centroid's mean is anchored
// at the midpoint of its weight span (cumulative + count/2), and the target
// rank is interpolated between adjacent centroid midpoints. Anchoring at
// midpoints (rather than spans) keeps tail quantiles from being biased toward
// heavy centroids.
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
	prevCenter := 0.0
	prevMean := td.centroids[0].mean

	for i, c := range td.centroids {
		center := cumulative + c.count/2
		if target < center {
			if i == 0 || center == prevCenter {
				return c.mean
			}
			frac := (target - prevCenter) / (center - prevCenter)

			return prevMean + frac*(c.mean-prevMean)
		}
		prevCenter = center
		prevMean = c.mean
		cumulative += c.count
	}

	return td.centroids[len(td.centroids)-1].mean
}

// Count returns the total number of values added.
func (td *TDigest) Count() float64 {
	return td.count
}

func (td *TDigest) compress() {
	if len(td.centroids) <= 1 {
		return
	}
	// Tie-break equal means by count so compression outcomes are
	// deterministic across runs (sort.Slice is not stable).
	sort.Slice(td.centroids, func(i, j int) bool {
		if td.centroids[i].mean != td.centroids[j].mean {
			return td.centroids[i].mean < td.centroids[j].mean
		}

		return td.centroids[i].count < td.centroids[j].count
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
