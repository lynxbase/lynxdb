package colstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"
)

// ErrPartitionNotFound reports a span with no events.
var ErrPartitionNotFound = errors.New("colstore: partition not found")

// ErrPartitionExists reports a span that already has events in the target index.
var ErrPartitionExists = errors.New("colstore: partition already exists")

// ErrInvalidPartition reports a start outside the index's partition boundaries.
var ErrInvalidPartition = errors.New("colstore: invalid partition")

// PartitionStats describes the parts in a half-open span of UTC event time.
type PartitionStats struct {
	Start  time.Time `json:"start"`
	End    time.Time `json:"end"`
	Events uint64    `json:"events"`
	Bytes  int64     `json:"bytes"`
	Parts  int       `json:"parts"`
}

func (st *PartitionStats) add(h *Handle) {
	st.Events += h.Rows
	st.Bytes += int64(h.Size())
	st.Parts++
}

func partitionID(t int64, width time.Duration) int64 {
	id := t / int64(width)
	if t%int64(width) < 0 {
		id--
	}
	return id
}

func (s *Store) span(id int64) PartitionStats {
	width := s.opts.Partition
	// The partition containing the first representable event time may
	// begin outside the range of signed Unix nanoseconds.
	var start time.Time
	if id < math.MinInt64/int64(width) {
		start = time.Unix(0, (id+1)*int64(width)).UTC().Add(-width)
	} else {
		start = time.Unix(0, id*int64(width)).UTC()
	}
	return PartitionStats{Start: start, End: start.Add(width)}
}

func (s *Store) checkPartitions(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed.Load() {
		return ErrClosed
	}
	if s.opts.Partition <= 0 {
		return fmt.Errorf("%w: index is not partitioned by time", ErrInvalidPartition)
	}
	return nil
}

// ListPartitions flushes pending rows and returns partitions in ascending order
// of start time.
func (s *Store) ListPartitions(ctx context.Context) ([]PartitionStats, error) {
	if err := s.checkPartitions(ctx); err != nil {
		return nil, err
	}
	if err := s.Sync(); err != nil {
		return nil, err
	}
	snap := s.Table().Snapshot()
	defer snap.Release()
	if s.closed.Load() {
		return nil, ErrClosed
	}
	groups := make(map[int64]*PartitionStats)
	for _, h := range snap.Parts() {
		st := groups[h.Partition]
		if st == nil {
			span := s.span(h.Partition)
			st = &span
			groups[h.Partition] = st
		}
		st.add(h)
	}
	out := make([]PartitionStats, 0, len(groups))
	for _, st := range groups {
		out = append(out, *st)
	}
	slices.SortFunc(out, func(a, b PartitionStats) int { return a.Start.Compare(b.Start) })
	return out, nil
}

// DropPartition removes the partition whose first instant is start.
func (s *Store) DropPartition(ctx context.Context, start time.Time) (PartitionStats, error) {
	return s.removePartition(ctx, start, nil)
}

// DetachPartition saves a durable copy at dest before dropping the partition.
func (s *Store) DetachPartition(ctx context.Context, start time.Time, dest string) (PartitionStats, error) {
	return s.removePartition(ctx, start, &dest)
}

func (s *Store) removePartition(ctx context.Context, start time.Time, dest *string) (PartitionStats, error) {
	s.dropping.Lock()
	defer s.dropping.Unlock()
	if err := s.checkPartitions(ctx); err != nil {
		return PartitionStats{}, err
	}
	id := partitionID(start.UnixNano(), s.opts.Partition)
	if start.Before(time.Unix(0, math.MinInt64)) {
		id = partitionID(math.MinInt64, s.opts.Partition)
	}
	st := s.span(id)
	if !st.Start.Equal(start) {
		return PartitionStats{}, fmt.Errorf("%w: %s is not a partition start", ErrInvalidPartition, start.Format(time.RFC3339Nano))
	}
	// Drop fences appends through publication. Detach takes this fence
	// only after copying the initial parts, to include any late arrivals.
	if dest == nil {
		s.appending.Lock()
		defer s.appending.Unlock()
	}
	if err := s.Sync(); err != nil {
		return PartitionStats{}, err
	}
	resume := s.pauseMerges(ctx, func(p int64) bool { return p == id })
	defer resume()
	if err := ctx.Err(); err != nil {
		return PartitionStats{}, err
	}
	first := s.partitionSnapshot(id)
	defer first.Release()
	if len(first.Parts()) == 0 {
		return PartitionStats{}, ErrPartitionNotFound
	}
	if dest == nil {
		return s.dropParts(id, first.Parts())
	}
	return s.detachParts(ctx, id, *dest, first)
}

// partitionSnapshot pins only the requested partition, so that merges in
// other partitions can reclaim their inputs throughout an archive copy.
func (s *Store) partitionSnapshot(id int64) *Snapshot {
	snap := s.Table().Snapshot()
	defer snap.Release()
	var parts []*Handle
	for _, h := range snap.Parts() {
		if h.Partition == id {
			parts = append(parts, h)
		}
	}
	return keep(parts)
}

// detachParts runs with merges paused for id. Its set of parts can only
// grow, even if other partitions are being flushed and merged.
func (s *Store) detachParts(ctx context.Context, id int64, dest string, first *Snapshot) (PartitionStats, error) {
	w, err := s.beginPartitionArchive(dest, id)
	if err != nil {
		return PartitionStats{}, err
	}
	defer w.close()
	if err := w.add(ctx, first.Parts()); err != nil {
		return PartitionStats{}, err
	}
	copied := make(map[*Handle]struct{}, len(first.Parts()))
	for _, h := range first.Parts() {
		copied[h] = struct{}{}
	}

	s.appending.Lock()
	defer s.appending.Unlock()
	if err := s.Sync(); err != nil {
		return PartitionStats{}, err
	}
	final := s.partitionSnapshot(id)
	defer final.Release()
	var arrivals []*Handle
	for _, h := range final.Parts() {
		if _, ok := copied[h]; !ok {
			arrivals = append(arrivals, h)
		}
	}
	if err := w.add(ctx, arrivals); err != nil {
		return PartitionStats{}, err
	}
	if err := w.publish(ctx); err != nil {
		return PartitionStats{}, err
	}
	return s.dropParts(id, final.Parts())
}

func (s *Store) dropParts(id int64, old []*Handle) (PartitionStats, error) {
	st := s.span(id)
	for _, h := range old {
		st.add(h)
	}
	if _, err := s.dir.Commit(nil, old); err != nil {
		return PartitionStats{}, err
	}
	s.mu.Lock()
	delete(s.arrived, id)
	s.mu.Unlock()
	s.countRuns()
	return st, nil
}

// AttachPartition copies an archive into an empty span. All of its parts
// become visible together, and the source archive remains reusable.
func (s *Store) AttachPartition(ctx context.Context, src string) (PartitionStats, error) {
	s.dropping.Lock()
	defer s.dropping.Unlock()
	if err := s.checkPartitions(ctx); err != nil {
		return PartitionStats{}, err
	}
	id, files, err := s.readPartitionArchive(ctx, src)
	if err != nil {
		return PartitionStats{}, err
	}
	defer func() {
		for _, pf := range files {
			pf.Abort()
		}
	}()
	// Copying and validation run alongside ingest. Fence new writes only
	// to check for a conflict (including buffered rows) and publish.
	s.appending.Lock()
	defer s.appending.Unlock()
	if err := s.Sync(); err != nil {
		return PartitionStats{}, err
	}
	snap := s.Table().Snapshot()
	exists := slices.ContainsFunc(snap.Parts(), func(h *Handle) bool { return h.Partition == id })
	snap.Release()
	if exists {
		return PartitionStats{}, ErrPartitionExists
	}
	if err := ctx.Err(); err != nil {
		return PartitionStats{}, err
	}
	// There are no input runs for a merge in this span. Reserve it until
	// all newly published handles have been counted.
	resume := s.pauseMerges(ctx, func(p int64) bool { return p == id })
	defer resume()
	add := files
	files = nil // Commit owns the files, including recovery after a failed write.
	handles, err := s.dir.Commit(add, nil)
	if err != nil {
		return PartitionStats{}, err
	}
	st := s.span(id)
	for _, h := range handles {
		st.add(h)
	}
	s.noteArrival(add)
	s.countRuns()
	return st, nil
}
