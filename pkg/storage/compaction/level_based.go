package compaction

import "sort"

// LevelBased implements a level-based compaction strategy for L1→L2.
// It produces non-overlapping, time-partitioned L2 segments, each
// targeting ~1GB in size.
type LevelBased struct {
	Threshold  int   // min L1 segments to trigger (default 4)
	TargetSize int64 // target L2 segment size in bytes (default 1GB)
}

func (lb *LevelBased) Plan(segments []*SegmentInfo) []*Plan {
	threshold := lb.Threshold
	if threshold < 2 {
		threshold = L1CompactionThreshold
	}

	// Only consider L1 segments.
	var l1 []*SegmentInfo
	for _, s := range segments {
		if s.Meta.Level == L1 {
			l1 = append(l1, s)
		}
	}

	if len(l1) < threshold {
		return nil
	}

	// Sort by MinTime for time-partitioned merging.
	sort.Slice(l1, func(i, j int) bool {
		return l1[i].Meta.MinTime.Before(l1[j].Meta.MinTime)
	})

	targetSize := lb.TargetSize
	if targetSize <= 0 {
		targetSize = L2TargetSize
	}

	// Greedily group segments until the cumulative size approaches the target.
	var plans []*Plan
	var group []*SegmentInfo
	var groupSize int64

	for _, seg := range l1 {
		group = append(group, seg)
		groupSize += seg.Meta.SizeBytes

		if groupSize >= targetSize || len(group) >= threshold {
			plans = append(plans, &Plan{
				InputSegments: append([]*SegmentInfo(nil), group...),
				OutputLevel:   L2,
			})
			group = nil
			groupSize = 0
		}
	}

	// Remaining segments: only compact if they meet the threshold.
	if len(group) >= threshold {
		plans = append(plans, &Plan{
			InputSegments: append([]*SegmentInfo(nil), group...),
			OutputLevel:   L2,
		})
	}

	return plans
}
