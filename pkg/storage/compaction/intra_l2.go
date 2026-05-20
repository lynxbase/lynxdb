package compaction

import (
	"log/slog"
	"sort"
)

// IntraL2 merges under-sized L2 segments among themselves up to L2TargetSize.
// L1→L2 promotions can produce many small L2 segments when L1 trigger fires
// on count rather than size; without an intra-level cleanup, those small L2s
// accumulate forever because L2→L3 only fires on cold partitions. IntraL2
// is the LSM-style fix: greedily group small L2 parts adjacent in time and
// merge them into larger L2 parts approaching the target size.
//
// Output segments stay at L2.
type IntraL2 struct {
	// Threshold is the minimum number of small L2 segments in a group required
	// to trigger a merge. Default: L1CompactionThreshold (4).
	Threshold int

	// MinSize is the upper bound (exclusive) for an L2 segment to be
	// considered "small" and eligible for intra-L2 merging. Default: half
	// of TargetSize. Parts >= MinSize are left alone.
	MinSize int64

	// TargetSize is the desired size of the merged L2 segment. The greedy
	// grouping stops accumulating once the group's cumulative size meets or
	// exceeds TargetSize. Default: L2TargetSize (1GB).
	TargetSize int64

	Logger *slog.Logger
}

// Plan returns plans that merge small L2 segments together up to TargetSize.
// Segments are sorted by MinTime and grouped greedily; a group becomes a
// plan only when it has at least Threshold inputs. Trailing groups below
// the threshold are not emitted -- they wait for more small L2 parts to
// accumulate.
func (il *IntraL2) Plan(segments []*SegmentInfo) []*Plan {
	threshold := il.Threshold
	if threshold < 2 {
		threshold = L1CompactionThreshold
	}

	targetSize := il.TargetSize
	if targetSize <= 0 {
		targetSize = L2TargetSize
	}

	minSize := il.MinSize
	if minSize <= 0 {
		minSize = targetSize / 2
	}

	// Filter to under-sized L2 segments only. Near-full L2 parts are left
	// alone so we don't rewrite them needlessly.
	var l2 []*SegmentInfo
	for _, s := range segments {
		if s.Meta.Level == L2 && s.Meta.SizeBytes < minSize {
			l2 = append(l2, s)
		}
	}

	if il.Logger != nil {
		il.Logger.Debug("intra l2 plan",
			"l2_small_count", len(l2),
			"threshold", threshold,
			"min_size", minSize,
			"target_size", targetSize,
		)
	}

	if len(l2) < threshold {
		return nil
	}

	// Sort by MinTime so merges keep temporal locality.
	sort.Slice(l2, func(i, j int) bool {
		return l2[i].Meta.MinTime.Before(l2[j].Meta.MinTime)
	})

	var plans []*Plan
	var group []*SegmentInfo
	var groupSize int64

	flush := func() {
		if len(group) >= threshold {
			plans = append(plans, &Plan{
				InputSegments: append([]*SegmentInfo(nil), group...),
				OutputLevel:   L2,
			})
		}
		group = nil
		groupSize = 0
	}

	for _, seg := range l2 {
		group = append(group, seg)
		groupSize += seg.Meta.SizeBytes
		if groupSize >= targetSize {
			flush()
		}
	}
	// Final partial group: only emit if it already meets the threshold;
	// otherwise wait for more small parts to accumulate.
	flush()

	if il.Logger != nil && len(plans) > 0 {
		il.Logger.Debug("intra l2 plans formed",
			"plan_count", len(plans),
		)
	}

	return plans
}
