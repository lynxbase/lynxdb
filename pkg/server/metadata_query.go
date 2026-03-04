package server

import (
	"context"
	"sort"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// HistogramFromMetadata computes histogram buckets using segment metadata
// (zone maps) for event count distribution. All committed data is in segments.
func (e *Engine) HistogramFromMetadata(ctx context.Context, indexName string,
	from, to time.Time, interval time.Duration, buckets []HistogramBucket) (int, error) {
	ep := e.pinEpoch()
	defer ep.unpin()
	segs := make([]*segmentHandle, len(ep.segments))
	copy(segs, ep.segments)

	bucketCount := len(buckets)
	if bucketCount == 0 || interval <= 0 {
		return 0, nil
	}

	total := 0

	// Distribute segment event counts across buckets using zone maps.
	for _, seg := range segs {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		// Skip by index name.
		if indexName != "" && seg.index != indexName {
			continue
		}

		// Skip segments outside the time range entirely.
		if seg.meta.MaxTime.Before(from) || seg.meta.MinTime.After(to) {
			continue
		}

		// Compute overlap of segment time range with the query time range.
		segStart := seg.meta.MinTime
		if segStart.Before(from) {
			segStart = from
		}

		segEnd := seg.meta.MaxTime
		if segEnd.After(to) {
			segEnd = to
		}

		segDuration := segEnd.Sub(segStart)
		if segDuration <= 0 {
			// Segment is a single point in time; put all events in one bucket.
			idx := int(segStart.Sub(from).Nanoseconds() / interval.Nanoseconds())
			if idx < 0 {
				idx = 0
			}
			if idx >= bucketCount {
				idx = bucketCount - 1
			}

			buckets[idx].Count += int(seg.meta.EventCount)
			total += int(seg.meta.EventCount)

			continue
		}

		// Distribute events proportionally across overlapping buckets.
		firstBucket := int(segStart.Sub(from).Nanoseconds() / interval.Nanoseconds())
		lastBucket := int(segEnd.Sub(from).Nanoseconds() / interval.Nanoseconds())

		if firstBucket < 0 {
			firstBucket = 0
		}
		if lastBucket >= bucketCount {
			lastBucket = bucketCount - 1
		}

		// Proportional overlap with the full segment range.
		fullSegDur := seg.meta.MaxTime.Sub(seg.meta.MinTime).Nanoseconds()
		if fullSegDur <= 0 {
			fullSegDur = 1
		}

		overlapRatio := float64(segDuration.Nanoseconds()) / float64(fullSegDur)
		overlapEvents := int(float64(seg.meta.EventCount) * overlapRatio)

		if firstBucket == lastBucket {
			buckets[firstBucket].Count += overlapEvents
			total += overlapEvents

			continue
		}

		spanBuckets := lastBucket - firstBucket + 1
		perBucket := overlapEvents / spanBuckets
		remainder := overlapEvents % spanBuckets

		for b := firstBucket; b <= lastBucket; b++ {
			n := perBucket
			if b-firstBucket < remainder {
				n++
			}

			buckets[b].Count += n
			total += n
		}
	}

	return total, nil
}

// FieldValuesFromMetadata returns top values for a field, scanning events with
// context cancellation support. Uses time bounds to limit the scan.
func (e *Engine) FieldValuesFromMetadata(ctx context.Context, fieldName string, indexName string,
	from, to time.Time, limit int) (*FieldValuesResult, error) {
	if limit <= 0 {
		limit = 10
	}

	ep := e.pinEpoch()
	defer ep.unpin()
	segs := make([]*segmentHandle, len(ep.segments))
	copy(segs, ep.segments)

	valueCounts := make(map[string]int)
	totalCount := 0

	// Scan segments.
	for _, seg := range segs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if indexName != "" && seg.index != indexName {
			continue
		}
		if !from.IsZero() && seg.meta.MaxTime.Before(from) {
			continue
		}
		if !to.IsZero() && seg.meta.MinTime.After(to) {
			continue
		}

		reader := seg.reader
		if reader == nil {
			reader = e.loadRemoteSegment(seg)
		}
		if reader == nil {
			continue
		}

		events, err := reader.ReadEventsWithColumns([]string{fieldName})
		if err != nil {
			continue
		}

		for i, ev := range events {
			if i&0x3FFF == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}

			if !from.IsZero() && ev.Time.Before(from) {
				continue
			}
			if !to.IsZero() && ev.Time.After(to) {
				continue
			}

			val := extractFieldValue(ev, fieldName)
			if val != "" {
				valueCounts[val]++
				totalCount++
			}
		}
	}

	// Sort and limit.
	sorted := make([]FieldValue, 0, len(valueCounts))
	for val, cnt := range valueCounts {
		sorted = append(sorted, FieldValue{
			Value: val,
			Count: cnt,
		})
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Count > sorted[j].Count
	})

	if len(sorted) > limit {
		sorted = sorted[:limit]
	}

	// Compute percentages.
	for i := range sorted {
		if totalCount > 0 {
			sorted[i].Percent = float64(sorted[i].Count) / float64(totalCount) * 100
		}
	}

	return &FieldValuesResult{
		Field:       fieldName,
		Values:      sorted,
		UniqueCount: len(valueCounts),
		TotalCount:  totalCount,
	}, nil
}

// ListSourcesFromMetadata returns distinct sources with context cancellation.
func (e *Engine) ListSourcesFromMetadata(ctx context.Context, indexName string,
	from, to time.Time) (*SourcesResult, error) {
	ep := e.pinEpoch()
	defer ep.unpin()
	segs := make([]*segmentHandle, len(ep.segments))
	copy(segs, ep.segments)

	sources := make(map[string]*SourceInfo)

	addEvent := func(source string, t time.Time) {
		if source == "" {
			source = "unknown"
		}

		info, ok := sources[source]
		if !ok {
			info = &SourceInfo{Name: source, FirstEvent: t, LastEvent: t}
			sources[source] = info
		}

		info.EventCount++
		if t.Before(info.FirstEvent) {
			info.FirstEvent = t
		}
		if t.After(info.LastEvent) {
			info.LastEvent = t
		}
	}

	// Scan segments.
	for _, seg := range segs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if indexName != "" && seg.index != indexName {
			continue
		}
		if !from.IsZero() && seg.meta.MaxTime.Before(from) {
			continue
		}
		if !to.IsZero() && seg.meta.MinTime.After(to) {
			continue
		}

		reader := seg.reader
		if reader == nil {
			reader = e.loadRemoteSegment(seg)
		}
		if reader == nil {
			continue
		}

		events, err := reader.ReadEventsWithColumns([]string{"_source"})
		if err != nil {
			continue
		}

		for i, ev := range events {
			if i&0x3FFF == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}

			if !from.IsZero() && ev.Time.Before(from) {
				continue
			}
			if !to.IsZero() && ev.Time.After(to) {
				continue
			}

			addEvent(ev.Source, ev.Time)
		}
	}

	result := make([]SourceInfo, 0, len(sources))
	for _, info := range sources {
		result = append(result, *info)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return &SourcesResult{Sources: result}, nil
}

// extractFieldValue extracts a field value from an event by name.
func extractFieldValue(ev *event.Event, fieldName string) string {
	switch fieldName {
	case "host":
		return ev.Host
	case "source":
		return ev.Source
	case "sourcetype":
		return ev.SourceType
	case "index":
		idx := ev.Index
		if idx == "" {
			return DefaultIndexName
		}

		return idx
	case "_raw":
		return ev.Raw
	default:
		if v, ok := ev.Fields[fieldName]; ok {
			return v.String()
		}

		return ""
	}
}
