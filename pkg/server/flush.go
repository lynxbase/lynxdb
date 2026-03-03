package server

import (
	"bytes"
	"fmt"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/model"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment/index"
)

// DefaultIndexName is the index name used when an event has no explicit index.
const DefaultIndexName = "main"

// FlushBatcher flushes buffered events to segments (in-memory mode only).
// In disk mode, the AsyncBatcher handles flushing directly to parts.
// Retained for the ephemeral in-memory engine (CLI pipe mode) and tests.
func (e *Engine) FlushBatcher() error {
	if e.batcher != nil {
		return e.batcher.Flush()
	}

	return nil
}

// flushInMemory creates in-memory segments from events, one per index.
// Used by the ephemeral CLI pipe-mode engine where no data directory is set.
func (e *Engine) flushInMemory(events []*event.Event) ([]*segmentHandle, error) {
	byIndex := make(map[string][]*event.Event)
	for _, ev := range events {
		idx := ev.Index
		if idx == "" {
			idx = DefaultIndexName
		}
		byIndex[idx] = append(byIndex[idx], ev)
	}

	var handles []*segmentHandle
	for idx, indexEvents := range byIndex {
		var buf bytes.Buffer
		sw := segment.NewWriter(&buf)

		if _, err := sw.Write(indexEvents); err != nil {
			return nil, fmt.Errorf("engine: flush: %w", err)
		}

		sr, err := segment.OpenSegment(buf.Bytes())
		if err != nil {
			return nil, fmt.Errorf("engine: open flushed segment: %w", err)
		}

		var bf *index.BloomFilter
		if b, err := sr.BloomFilter(); err != nil {
			e.logger.Warn("bloom filter unavailable after flush", "index", idx, "error", err)
		} else {
			bf = b
		}

		var ii *index.SerializedIndex
		if i, err := sr.InvertedIndex(); err != nil {
			e.logger.Warn("inverted index unavailable after flush", "index", idx, "error", err)
		} else {
			ii = i
		}

		// Compute actual min/max time from events (don't assume sorted).
		minTime, maxTime := indexEvents[0].Time, indexEvents[0].Time
		for _, ev := range indexEvents[1:] {
			if ev.Time.Before(minTime) {
				minTime = ev.Time
			}
			if ev.Time.After(maxTime) {
				maxTime = ev.Time
			}
		}

		handles = append(handles, &segmentHandle{
			reader: sr,
			meta: model.SegmentMeta{
				ID:           fmt.Sprintf("mem-%s-%d", idx, time.Now().UnixNano()),
				Index:        idx,
				MinTime:      minTime,
				MaxTime:      maxTime,
				EventCount:   int64(len(indexEvents)),
				SizeBytes:    int64(buf.Len()),
				CreatedAt:    time.Now(),
				BloomVersion: 2,
			},
			index:       idx,
			bloom:       bf,
			invertedIdx: ii,
		})
	}

	return handles, nil
}
