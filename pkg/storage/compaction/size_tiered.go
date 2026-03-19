package compaction

import "sort"

// L0EmergencyThreshold is the L0 segment count that triggers emergency
// compaction (merge all L0 segments) when no tier-based plans are produced.
// This is 2x the normal L0CompactionThreshold and acts as a safety valve
// to prevent unbounded L0 growth under adversarial size distributions.
const L0EmergencyThreshold = 8

// SizeTiered implements a size-tiered compaction strategy for L0->L1.
// Segments are grouped into buckets where each segment is within 2x of
// the bucket's median size. When a bucket has >= threshold segments,
// a compaction plan is produced.
//
// Additionally, non-overlapping L0 segments that do not overlap any L1
// segment or other L0 segment are promoted via trivial move (level metadata
// change, no merge). When L0 count reaches L0EmergencyThreshold and no
// tier-based plans exist, all L0 segments are merged as an emergency measure.
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

	// Partition segments by level.
	var l0, l1 []*SegmentInfo
	for _, s := range segments {
		switch s.Meta.Level {
		case L0:
			l0 = append(l0, s)
		case L1:
			l1 = append(l1, s)
		}
	}

	if len(l0) < threshold {
		return nil
	}

	var plans []*Plan

	// Check for trivial moves: L0 segments that don't overlap any L1
	// segment and don't overlap any other L0 segment can be promoted
	// directly to L1 without a merge pass. Segments must have valid
	// time ranges (non-zero MaxTime) for overlap detection to work.
	trivialIDs := make(map[string]bool)
	for _, s := range l0 {
		if s.Meta.MaxTime.IsZero() {
			continue // cannot determine overlap without a valid time range
		}
		if !overlapsAny(s, l1) && !overlapsAny(s, l0) {
			plans = append(plans, &Plan{
				InputSegments: []*SegmentInfo{s},
				OutputLevel:   L1,
				TrivialMove:   true,
			})
			trivialIDs[s.Meta.ID] = true
		}
	}

	// Filter out trivially-moved segments from the L0 pool.
	if len(trivialIDs) > 0 {
		remaining := make([]*SegmentInfo, 0, len(l0)-len(trivialIDs))
		for _, s := range l0 {
			if !trivialIDs[s.Meta.ID] {
				remaining = append(remaining, s)
			}
		}
		l0 = remaining
	}

	// Group remaining L0 by size tier.
	tiers := make(map[int][]*SegmentInfo)
	for _, s := range l0 {
		ti := tierIndex(s.Meta.SizeBytes)
		tiers[ti] = append(tiers[ti], s)
	}

	tierPlans := 0
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
			tierPlans++
		}
	}

	// Emergency compaction: if L0 count is critical and no tier-based
	// plans were generated, merge ALL remaining L0 segments to prevent
	// unbounded L0 growth from adversarial size distributions.
	if len(l0) >= L0EmergencyThreshold && tierPlans == 0 {
		sort.Slice(l0, func(i, j int) bool {
			return l0[i].Meta.MinTime.Before(l0[j].Meta.MinTime)
		})
		plans = append(plans, &Plan{
			InputSegments: append([]*SegmentInfo(nil), l0...),
			OutputLevel:   L1,
		})
	}

	return plans
}

// overlapsAny returns true if seg's time range overlaps with any segment
// in others (excluding seg itself by ID). Two segments overlap when
// seg.MinTime < other.MaxTime AND seg.MaxTime > other.MinTime.
func overlapsAny(seg *SegmentInfo, others []*SegmentInfo) bool {
	for _, o := range others {
		if seg.Meta.ID == o.Meta.ID {
			continue
		}
		// Skip segments with zero MaxTime -- their range is unknown.
		if o.Meta.MaxTime.IsZero() {
			continue
		}
		if seg.Meta.MinTime.Before(o.Meta.MaxTime) && seg.Meta.MaxTime.After(o.Meta.MinTime) {
			return true
		}
	}

	return false
}
