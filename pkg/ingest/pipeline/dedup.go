// Package pipeline implements ingest processing stages.
//
// DedupStage provides optional event deduplication at ingest time using
// xxhash64 over the raw event text. It has two layers:
//
//  1. Intra-batch: a per-batch map catches duplicates within the same batch.
//  2. Cross-batch: a bounded LRU ring of recent hashes catches retried HTTP
//     requests that arrive in consecutive batches within a configurable window.
//
// # Collision safety
//
// With xxhash64, birthday paradox gives p(collision) ≈ n²/2⁶⁵.
// At 100K unique events: p ≈ 2.7×10⁻¹⁰ (negligible for log analytics).
// At 1B unique events:   p ≈ 2.7×10⁻² (non-negligible).
//
// Because legitimately identical log lines do occur (e.g., repeated status
// heartbeats), this stage is disabled by default (opt-in via ingest.dedup_enabled).
package pipeline

import (
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// DedupStage removes duplicate events within a batch and across recent batches.
// It is safe for concurrent use.
type DedupStage struct {
	// ring is a fixed-size circular buffer of recently seen hashes.
	// Index into ring: ring[ringPos % ringSize].
	ring    []uint64
	ringSet map[uint64]struct{} // fast lookup into ring contents
	ringPos int

	mu sync.Mutex

	// Dropped is the total number of events dropped by dedup (observable metric).
	Dropped atomic.Int64
}

// NewDedupStage creates a DedupStage with the given cross-batch LRU capacity.
// Capacity is the number of recent hashes to remember across batches.
// A reasonable default is 100,000 entries (~1.6 MB memory).
func NewDedupStage(capacity int) *DedupStage {
	if capacity <= 0 {
		capacity = 100_000
	}

	return &DedupStage{
		ring:    make([]uint64, capacity),
		ringSet: make(map[uint64]struct{}, capacity),
	}
}

// Process removes duplicate events from the batch. An event is considered a
// duplicate if its xxhash64(Raw) has been seen either earlier in the same
// batch or in a recent previous batch (within the LRU window).
func (d *DedupStage) Process(events []*event.Event) ([]*event.Event, error) {
	if len(events) == 0 {
		return events, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Intra-batch dedup set. Sized to batch length to avoid rehash.
	batchSeen := make(map[uint64]struct{}, len(events))

	var dropped int64

	// Filter in place: write pointer w <= read pointer i.
	w := 0

	for _, ev := range events {
		h := xxhash.Sum64String(ev.Raw)

		// Check intra-batch first (cheaper — no ring lookup).
		if _, dup := batchSeen[h]; dup {
			dropped++

			continue
		}

		// Check cross-batch ring.
		if _, dup := d.ringSet[h]; dup {
			dropped++

			continue
		}

		batchSeen[h] = struct{}{}
		events[w] = ev
		w++
	}

	// Add all new hashes from this batch to the cross-batch ring.
	for h := range batchSeen {
		d.addToRing(h)
	}

	if dropped > 0 {
		d.Dropped.Add(dropped)
	}

	// Nil out trailing pointers to allow GC of dropped events.
	for i := w; i < len(events); i++ {
		events[i] = nil
	}

	return events[:w], nil
}

// addToRing adds a hash to the circular buffer, evicting the oldest entry.
// Caller must hold d.mu.
func (d *DedupStage) addToRing(h uint64) {
	pos := d.ringPos % len(d.ring)

	// Evict the hash being overwritten.
	old := d.ring[pos]
	if old != 0 {
		delete(d.ringSet, old)
	}

	d.ring[pos] = h
	d.ringSet[h] = struct{}{}
	d.ringPos++
}
