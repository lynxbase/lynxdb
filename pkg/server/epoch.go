package server

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// segmentEpoch is an immutable snapshot of the segment list at a point in time.
// Queries pin the current epoch to prevent retired segment mmaps from being
// closed while reads are in flight. Compaction/tiering/flush/ingest all create
// new epochs via advanceEpoch. Retired segments are cleaned up when all pinned
// readers finish.
//
// This is equivalent to RocksDB's SuperVersion pattern or Pebble's Version
// reference counting. Each epoch holds a frozen []*segmentHandle slice.
//
// IMMUTABILITY: Once created, segments and retired slices are never modified.
// Any mutation (add/remove segments) creates a new epoch.
type segmentEpoch struct {
	id       uint64
	segments []*segmentHandle
	retired  []*segmentHandle // segments removed by this epoch transition

	readers   atomic.Int64 // active query count pinning this epoch
	closeOnce sync.Once
	done      chan struct{} // closed when readers reaches 0
}

// pin increments the reader count. Must be called under e.mu.RLock()
// to prevent racing with epoch advance.
func (ep *segmentEpoch) pin() {
	ep.readers.Add(1)
}

// unpin decrements the reader count. When it reaches zero, signals done
// via sync.Once (safe against double-close race with advanceEpoch).
func (ep *segmentEpoch) unpin() {
	if ep.readers.Add(-1) == 0 {
		ep.signalDone()
	}
}

// signalDone closes the done channel exactly once. Called when readers reaches 0,
// either from the last unpin() or from advanceEpoch() when the old epoch had no readers.
func (ep *segmentEpoch) signalDone() {
	ep.closeOnce.Do(func() { close(ep.done) })
}

// drainAndClose waits for all pinned readers to finish, then decrements
// refcounts on ALL segments that were in this epoch. Segments whose refcount
// reaches 0 have their mmap closed automatically by decRef.
//
// Every epoch must drain — even epochs with 0 retired segments — because each
// epoch holds incRef'd references to its segments. Without decRef, segment
// mmaps would leak indefinitely.
//
// On timeout, logs a warning but does NOT release refs — that would cause the
// same SIGSEGV we are trying to prevent. Leaked refs are cleaned up at engine
// shutdown via force-close.
func (ep *segmentEpoch) drainAndClose(logger *slog.Logger) {
	// Always drain: even epochs with 0 retirements hold refs that must be released.
	go func() {
		// Early warning at 10s — helps operators spot hung queries before the hard deadline.
		warnTimer := time.NewTimer(10 * time.Second)
		defer warnTimer.Stop()

		select {
		case <-ep.done:
			// All readers released — safe to release refs.
		case <-warnTimer.C:
			logger.Warn("epoch drain slow — queries still pinned",
				"epoch_id", ep.id,
				"segment_count", len(ep.segments),
				"remaining_readers", ep.readers.Load())
			// Wait for the hard deadline.
			select {
			case <-ep.done:
				// Readers released after warning.
			case <-time.After(20 * time.Second): // 10s warn + 20s = 30s total
				logger.Error("epoch drain timeout — segment refs leaked",
					"epoch_id", ep.id,
					"segment_count", len(ep.segments),
					"remaining_readers", ep.readers.Load())
				// DO NOT release refs on timeout — readers may still be active.
				// Refs will be cleaned up at shutdown.
				return
			}
		}
		for _, sh := range ep.segments {
			sh.decRef()
		}
	}()
}
