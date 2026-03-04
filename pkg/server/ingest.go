package server

import (
	"fmt"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/model"
)

// Ingest buffers events in the async batcher (which flushes to parts on
// threshold), publishes to event bus, and dispatches to materialized views.
// Returns ErrShuttingDown if PrepareShutdown has been called.
func (e *Engine) Ingest(events []*event.Event) error {
	if e.closing.Load() {
		return ErrShuttingDown
	}

	// Ingest-time dedup: remove duplicate events (xxhash64-based) when enabled.
	if e.ingestDedup != nil {
		before := len(events)

		var err error

		events, err = e.ingestDedup.Process(events)
		if err != nil {
			return fmt.Errorf("server.Ingest: dedup: %w", err)
		}

		if dropped := before - len(events); dropped > 0 {
			e.metrics.IngestDedupDrops.Add(int64(dropped))
		}

		if len(events) == 0 {
			return nil
		}
	}

	// Publish to event bus BEFORE batcher (live tail sees events immediately).
	e.eventBus.Publish(events)

	// Buffer in async batcher (flushes to part on threshold).
	if e.batcher != nil {
		if err := e.batcher.Add(events); err != nil {
			return err
		}
	} else {
		// In-memory mode (no data dir): add directly to segments via flushInMemory.
		handles, err := e.flushInMemory(events)
		if err != nil {
			return err
		}

		e.mu.Lock()
		combined := make([]*segmentHandle, len(e.currentEpoch.segments)+len(handles))
		copy(combined, e.currentEpoch.segments)
		copy(combined[len(e.currentEpoch.segments):], handles)
		e.advanceEpoch(combined, nil) // no retired segments
		e.mu.Unlock()
	}

	// Auto-register any new index names so they appear in ListIndexes().
	e.ensureIndexes(events)

	// Register source names for multi-source/wildcard query resolution.
	e.ensureSources(events)

	// Bump the ingest generation so subsequent queries see a cache miss
	// for any query whose results may have changed (E3: cache invalidation).
	e.ingestGen.Add(1)

	// Update ingestion metrics.
	e.metrics.IngestEvents.Add(int64(len(events)))
	e.metrics.IngestBatches.Add(1)

	var ingestBytes int64
	for _, ev := range events {
		ingestBytes += int64(len(ev.Raw))
	}

	e.metrics.IngestBytes.Add(ingestBytes)

	e.lastIngestTime.Store(time.Now().UnixNano())

	// Dispatch to materialized views (best-effort; don't block ingest on MV errors).
	if e.mvDispatcher != nil {
		if err := e.mvDispatcher.Dispatch(events); err != nil {
			e.logger.Warn("MV dispatch failed", "error", err)
		}
	}

	return nil
}

// ensureIndexes auto-registers any new index names from the event batch so
// they appear in ListIndexes(). On the fast path (all indexes already known),
// this is a single RLock scan with no allocation. The write lock is only
// acquired when a genuinely new index name is encountered for the first time.
func (e *Engine) ensureIndexes(events []*event.Event) {
	// Fast path: check under read lock whether any event has an unknown index.
	e.mu.RLock()

	var missing []string

	seen := make(map[string]struct{}, 1) // typically 1 index per batch
	for _, ev := range events {
		idx := ev.Index
		if idx == "" || idx == DefaultIndexName {
			continue
		}
		if _, dup := seen[idx]; dup {
			continue
		}

		seen[idx] = struct{}{}
		if _, ok := e.indexes[idx]; !ok {
			missing = append(missing, idx)
		}
	}

	e.mu.RUnlock()

	if len(missing) == 0 {
		return
	}

	// Slow path: create missing indexes under write lock.
	e.mu.Lock()
	for _, name := range missing {
		if _, ok := e.indexes[name]; !ok {
			e.indexes[name] = model.DefaultIndexConfig(name)
			e.logger.Info("auto-created index", "name", name)
		}
	}
	e.mu.Unlock()
}

// ensureSources registers event index names (physical partition keys) in the
// source registry so they are available for glob pattern resolution in
// multi-source queries (FROM logs*, index=nginx, etc.).
//
// The source registry must contain INDEX names (physical partitions) because
// glob resolution matches against segment directory names (segments/hot/<INDEX>/).
// Source names (ev.Source) are also registered for backward compatibility with
// queries that reference _source values.
//
// On the fast path (all names already known), Contains uses RLock with no allocation.
func (e *Engine) ensureSources(events []*event.Event) {
	for _, ev := range events {
		// Register index names for glob/list resolution against segment dirs.
		idx := ev.Index
		if idx == "" {
			idx = DefaultIndexName
		}

		if !e.sourceRegistry.Contains(idx) {
			e.sourceRegistry.Register(idx)
		}

		// Also register source names for backward compatibility with
		// queries that reference _source values directly.
		src := ev.Source
		if src != "" && src != idx && !e.sourceRegistry.Contains(src) {
			e.sourceRegistry.Register(src)
		}
	}
}
