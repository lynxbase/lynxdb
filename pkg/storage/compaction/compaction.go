package compaction

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/model"
	segment "github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment"
)

// Level constants for compaction tiers.
const (
	L0 = 0 // Flush segments (overlapping time ranges).
	L1 = 1 // Merged, non-overlapping.
	L2 = 2 // Fully compacted (~1GB target).
)

// L0CompactionThreshold is the number of L0 segments that triggers L0→L1 compaction.
const L0CompactionThreshold = 4

// L1CompactionThreshold is the number of L1 segments that triggers L1→L2 compaction.
const L1CompactionThreshold = 4

// L2TargetSize is the target size for L2 segments (1GB).
const L2TargetSize = 1 << 30

// SegmentInfo describes a segment available for compaction.
// Input segments use Path for mmap-based disk access; Data is optional
// and used only by Execute() for in-memory segments (tests).
type SegmentInfo struct {
	Meta model.SegmentMeta
	Data []byte // raw .lsg bytes (optional, for in-memory/test use)
	Path string // filesystem path to .lsg file (preferred for disk segments)
}

// Plan determines which segments should be compacted and at what level.
type Plan struct {
	InputSegments []*SegmentInfo
	OutputLevel   int
}

// Compactor performs adaptive compaction of segments using pluggable
// strategies: SizeTiered for L0→L1 and LevelBased for L1→L2.
type Compactor struct {
	mu         sync.Mutex
	segments   map[string]*SegmentInfo // id -> info
	logger     *slog.Logger
	l0Strategy Strategy
	l1Strategy Strategy
}

// NewCompactor creates a new compactor with default strategies.
func NewCompactor(logger *slog.Logger) *Compactor {
	return &Compactor{
		segments:   make(map[string]*SegmentInfo),
		logger:     logger,
		l0Strategy: &SizeTiered{Threshold: L0CompactionThreshold},
		l1Strategy: &LevelBased{Threshold: L1CompactionThreshold, TargetSize: L2TargetSize},
	}
}

// AddSegment registers a segment for compaction tracking.
func (c *Compactor) AddSegment(info *SegmentInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.segments[info.Meta.ID] = info
}

// RemoveSegment removes a segment from compaction tracking.
func (c *Compactor) RemoveSegment(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.segments, id)
}

// Segments returns all tracked segments.
func (c *Compactor) Segments() []*SegmentInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]*SegmentInfo, 0, len(c.segments))
	for _, s := range c.segments {
		result = append(result, s)
	}

	return result
}

// SegmentsByLevel returns segments at the given compaction level for the given index.
func (c *Compactor) SegmentsByLevel(index string, level int) []*SegmentInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	var result []*SegmentInfo
	for _, s := range c.segments {
		if s.Meta.Index == index && s.Meta.Level == level {
			result = append(result, s)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Meta.MinTime.Before(result[j].Meta.MinTime)
	})

	return result
}

// segmentsForIndex returns all segments for an index (under lock).
func (c *Compactor) segmentsForIndex(index string) []*SegmentInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	var result []*SegmentInfo
	for _, s := range c.segments {
		if s.Meta.Index == index {
			result = append(result, s)
		}
	}

	return result
}

// PlanCompaction checks if compaction is needed and returns a single plan
// (highest priority first). Returns nil if no compaction is needed.
func (c *Compactor) PlanCompaction(index string) *Plan {
	segs := c.segmentsForIndex(index)

	// L0→L1 has highest priority.
	if plans := c.l0Strategy.Plan(segs); len(plans) > 0 {
		return plans[0]
	}

	// L1→L2 next.
	if plans := c.l1Strategy.Plan(segs); len(plans) > 0 {
		return plans[0]
	}

	return nil
}

// PlanAllCompactions returns all available compaction plans for an index,
// ordered by priority (L0→L1 first, then L1→L2).
func (c *Compactor) PlanAllCompactions(index string) []*Job {
	segs := c.segmentsForIndex(index)

	var jobs []*Job

	// L0→L1 plans (highest priority).
	for _, plan := range c.l0Strategy.Plan(segs) {
		jobs = append(jobs, &Job{
			Plan:     plan,
			Priority: PriorityL0ToL1,
			Index:    index,
		})
	}

	// L1→L2 plans.
	for _, plan := range c.l1Strategy.Plan(segs) {
		jobs = append(jobs, &Job{
			Plan:     plan,
			Priority: PriorityL1ToL2,
			Index:    index,
		})
	}

	return jobs
}

// MergeResult holds the output of a k-way merge without serialization.
// The caller is responsible for writing the events to disk (e.g., via part.Writer).
type MergeResult struct {
	Events  []*event.Event
	MinTime time.Time
	MaxTime time.Time
	Index   string
	Level   int
}

// Merge performs a streaming k-way merge of input segments by timestamp.
// Cursors read one row group at a time to limit peak memory.
// Segments are opened from disk (Path) when available, falling back to
// in-memory Data for tests. Mmap handles are closed after merge completes.
//
// The caller receives the merged events and is responsible for writing them
// to disk (e.g., via part.Writer for atomic rename into partition directories).
func (c *Compactor) Merge(ctx context.Context, plan *Plan) (*MergeResult, error) {
	if len(plan.InputSegments) == 0 {
		return nil, ErrNoInputSegments
	}

	c.logger.Info("starting compaction merge",
		"input_count", len(plan.InputSegments),
		"output_level", plan.OutputLevel,
	)

	index := plan.InputSegments[0].Meta.Index

	// Open all segment readers and build cursors.
	// Track mmap handles for cleanup after merge.
	var mmapHandles []*segment.MmapSegment
	defer func() {
		for _, ms := range mmapHandles {
			ms.Close()
		}
	}()

	cursors := make(mergeHeap, 0, len(plan.InputSegments))

	for _, seg := range plan.InputSegments {
		reader, ms, err := c.openSegmentReader(seg)
		if err != nil {
			return nil, fmt.Errorf("compaction: open segment %s: %w", seg.Meta.ID, err)
		}

		if ms != nil {
			mmapHandles = append(mmapHandles, ms)
		}

		cur := &segmentCursor{
			reader:  reader,
			rgCount: reader.RowGroupCount(),
		}

		// Load first row group.
		if err := cur.advance(); err != nil {
			return nil, fmt.Errorf("compaction: read segment %s: %w", seg.Meta.ID, err)
		}

		if cur.current() != nil {
			cursors = append(cursors, cur)
		}
	}

	heap.Init(&cursors)

	// Estimate total event count for pre-allocation to avoid slice growth.
	var estimatedTotal int64
	for _, seg := range plan.InputSegments {
		estimatedTotal += seg.Meta.EventCount
	}

	// Stream-merge: k-way merge into a pre-allocated slice.
	// Cursors release consumed row groups (via advance) to allow GC to
	// reclaim memory, bounding peak usage to ~(allEvents + 1 row group per cursor).
	allEvents := make([]*event.Event, 0, estimatedTotal)
	var totalEvents int64
	var minTime, maxTime time.Time

	for cursors.Len() > 0 {
		// Check context cancellation periodically.
		if totalEvents%10000 == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}

		cur := cursors[0]
		ev := cur.current()

		// Track time bounds.
		if totalEvents == 0 {
			minTime = ev.Time
			maxTime = ev.Time
		} else {
			if ev.Time.Before(minTime) {
				minTime = ev.Time
			}

			if ev.Time.After(maxTime) {
				maxTime = ev.Time
			}
		}

		allEvents = append(allEvents, ev)
		totalEvents++

		// Advance cursor.
		cur.pos++

		if cur.pos >= len(cur.events) {
			// Current row group exhausted, load next.
			// advance() replaces the events slice, allowing GC to reclaim the old one.
			if err := cur.advance(); err != nil {
				return nil, fmt.Errorf("compaction: read next row group: %w", err)
			}
		}

		if cur.current() != nil {
			heap.Fix(&cursors, 0)
		} else {
			heap.Pop(&cursors)
		}
	}

	if len(allEvents) == 0 {
		return nil, ErrEmptyMerge
	}

	c.logger.Info("compaction merge complete",
		"event_count", totalEvents,
		"output_level", plan.OutputLevel,
	)

	return &MergeResult{
		Events:  allEvents,
		MinTime: minTime,
		MaxTime: maxTime,
		Index:   index,
		Level:   plan.OutputLevel,
	}, nil
}

// openSegmentReader opens a segment from disk (Path) or in-memory (Data).
// Returns the Reader and an optional MmapSegment handle that must be closed by the caller.
func (c *Compactor) openSegmentReader(seg *SegmentInfo) (*segment.Reader, *segment.MmapSegment, error) {
	// Prefer disk-based access when a path is available.
	if seg.Path != "" {
		ms, err := segment.OpenSegmentFile(seg.Path)
		if err != nil {
			return nil, nil, err
		}

		reader := ms.Reader()

		return reader, ms, nil
	}

	// Fall back to in-memory bytes (test usage).
	if len(seg.Data) > 0 {
		reader, err := segment.OpenSegment(seg.Data)
		if err != nil {
			return nil, nil, err
		}

		return reader, nil, nil
	}

	return nil, nil, fmt.Errorf("segment %s has neither path nor data", seg.Meta.ID)
}

// Execute runs a compaction plan: k-way merge of input segments into a single
// output segment serialized to an in-memory buffer. This is a convenience
// wrapper around Merge() for tests and simple use cases where the caller
// doesn't need to control the output write path.
//
// For production use, prefer Merge() + part.Writer.Write() for atomic
// rename into partition directories.
func (c *Compactor) Execute(ctx context.Context, plan *Plan) (*SegmentInfo, error) {
	result, err := c.Merge(ctx, plan)
	if err != nil {
		return nil, err
	}

	// Write merged events to an in-memory segment.
	var buf bytes.Buffer

	sw := segment.NewWriter(&buf)

	written, err := sw.Write(result.Events)
	if err != nil {
		return nil, fmt.Errorf("compaction: write output: %w", err)
	}

	// Build output metadata.
	now := time.Now()
	outMeta := model.SegmentMeta{
		ID:         fmt.Sprintf("compact-%s-%d-%d", result.Index, result.Level, now.UnixNano()),
		Index:      result.Index,
		MinTime:    result.MinTime,
		MaxTime:    result.MaxTime,
		EventCount: int64(len(result.Events)),
		SizeBytes:  written,
		Level:      result.Level,
		CreatedAt:  now,
	}

	c.logger.Info("compaction execute complete",
		"output_id", outMeta.ID,
		"event_count", outMeta.EventCount,
		"size_bytes", outMeta.SizeBytes,
		"output_level", outMeta.Level,
	)

	return &SegmentInfo{
		Meta: outMeta,
		Data: buf.Bytes(),
	}, nil
}

// segmentCursor tracks the read position within a segment during k-way merge.
// It reads one row group at a time to limit memory usage.
type segmentCursor struct {
	reader  *segment.Reader
	rgCount int
	rgIdx   int            // next row group to read
	events  []*event.Event // current row group's events
	pos     int            // position within events
}

// current returns the current event, or nil if exhausted.
func (sc *segmentCursor) current() *event.Event {
	if sc.pos < len(sc.events) {
		return sc.events[sc.pos]
	}

	return nil
}

// advance loads the next row group. Sets events to nil when exhausted.
func (sc *segmentCursor) advance() error {
	sc.events = nil
	sc.pos = 0

	for sc.rgIdx < sc.rgCount {
		events, err := sc.reader.ReadRowGroup(sc.rgIdx)
		sc.rgIdx++
		if err != nil {
			return err
		}
		if len(events) > 0 {
			// Sort within row group by time (should already be sorted, but be safe).
			sort.SliceStable(events, func(i, j int) bool {
				return events[i].Time.Before(events[j].Time)
			})
			sc.events = events

			return nil
		}
	}

	return nil
}

// mergeHeap implements container/heap for k-way merge of segment cursors.
type mergeHeap []*segmentCursor

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	return h[i].current().Time.Before(h[j].current().Time)
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(*segmentCursor))
}

func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]

	return item
}

// ApplyCompaction executes a plan and updates the segment tracking:
// removes input segments and adds the output segment.
func (c *Compactor) ApplyCompaction(ctx context.Context, plan *Plan) (*SegmentInfo, error) {
	output, err := c.Execute(ctx, plan)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove input segments.
	for _, seg := range plan.InputSegments {
		delete(c.segments, seg.Meta.ID)
	}

	// Add output segment.
	c.segments[output.Meta.ID] = output

	return output, nil
}
