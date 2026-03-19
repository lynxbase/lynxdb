package compaction

import "time"

// TimeWindow implements a time-window compaction strategy for L2->L3.
// It consolidates multiple L2 segments within a cold partition (no writes
// for ColdThreshold duration) into a single L3 archive segment.
//
// A partition is considered "cold" when all its L2 segments have a
// CreatedAt older than ColdThreshold. This avoids archiving data that
// is still being actively compacted at lower levels.
type TimeWindow struct {
	ColdThreshold time.Duration // partition must be idle for this long (default 48h)
}

// Plan returns a single plan merging all L2 segments into one L3 segment
// if the partition is cold. Returns nil if fewer than 2 L2 segments exist
// or if any segment was created within the ColdThreshold window.
func (tw *TimeWindow) Plan(segments []*SegmentInfo) []*Plan {
	threshold := tw.ColdThreshold
	if threshold == 0 {
		threshold = 48 * time.Hour
	}

	// Only consider L2 segments.
	var l2 []*SegmentInfo
	for _, s := range segments {
		if s.Meta.Level == L2 {
			l2 = append(l2, s)
		}
	}

	// Need at least 2 L2 segments to justify consolidation.
	if len(l2) < 2 {
		return nil
	}

	// Check if partition is cold: all segments must be older than ColdThreshold.
	now := time.Now()
	for _, s := range l2 {
		if now.Sub(s.Meta.CreatedAt) < threshold {
			return nil // partition still warm
		}
	}

	return []*Plan{{
		InputSegments: append([]*SegmentInfo(nil), l2...),
		OutputLevel:   L3,
	}}
}
