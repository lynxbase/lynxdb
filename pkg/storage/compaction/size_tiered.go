package compaction

import "sort"

// SizeTiered implements a size-tiered compaction strategy for L0→L1.
// Segments are grouped into buckets where each segment is within 2x of
// the bucket's median size. When a bucket has >= threshold segments,
// a compaction plan is produced.
type SizeTiered struct {
	Threshold int // min segments per tier to trigger (default 4)
}

// sizeTier boundaries (bytes). Segments fall into the first tier
// whose upper bound exceeds their size.
var sizeTiers = []int64{
	1 << 20,   // 0-1MB
	10 << 20,  // 1-10MB
	100 << 20, // 10-100MB
	1 << 30,   // 100MB-1GB
}

func tierIndex(sizeBytes int64) int {
	for i, bound := range sizeTiers {
		if sizeBytes <= bound {
			return i
		}
	}

	return len(sizeTiers)
}

func (st *SizeTiered) Plan(segments []*SegmentInfo) []*Plan {
	threshold := st.Threshold
	if threshold < 2 {
		threshold = L0CompactionThreshold
	}

	// Only consider L0 segments.
	var l0 []*SegmentInfo
	for _, s := range segments {
		if s.Meta.Level == L0 {
			l0 = append(l0, s)
		}
	}

	if len(l0) < threshold {
		return nil
	}

	// Group by size tier.
	tiers := make(map[int][]*SegmentInfo)
	for _, s := range l0 {
		ti := tierIndex(s.Meta.SizeBytes)
		tiers[ti] = append(tiers[ti], s)
	}

	var plans []*Plan
	for _, bucket := range tiers {
		if len(bucket) < threshold {
			continue
		}
		// Sort by MinTime within the bucket for deterministic output.
		sort.Slice(bucket, func(i, j int) bool {
			return bucket[i].Meta.MinTime.Before(bucket[j].Meta.MinTime)
		})

		// Take groups of threshold segments.
		for len(bucket) >= threshold {
			plans = append(plans, &Plan{
				InputSegments: append([]*SegmentInfo(nil), bucket[:threshold]...),
				OutputLevel:   L1,
			})
			bucket = bucket[threshold:]
		}
	}

	return plans
}
