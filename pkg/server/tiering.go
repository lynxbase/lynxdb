package server

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

// startTiering runs background tiering evaluation when dataDir is set.
func (e *Engine) startTiering(ctx context.Context) {
	interval := e.storageCfg.TieringInterval
	if interval == 0 {
		interval = 5 * time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Bound each cycle so a hung S3 call can't block the next tick.
				cycleTimeout := interval - 5*time.Second
				if cycleTimeout < 30*time.Second {
					cycleTimeout = 30 * time.Second
				}
				cycleCtx, cancel := context.WithTimeout(ctx, cycleTimeout)
				e.runTieringCycle(cycleCtx)
				cancel()
			}
		}
	}()
}

// runTieringCycle evaluates and executes tier moves.
func (e *Engine) runTieringCycle(ctx context.Context) {
	e.mu.RLock()
	configs := make(map[string]model.IndexConfig, len(e.indexes))
	for k, v := range e.indexes {
		configs[k] = v
	}
	e.mu.RUnlock()

	result := e.tierMgr.Evaluate(configs)

	// Move to warm: upload to object store, close local mmap, delete local file.
	for _, id := range result.MovedToWarm {
		// Copy mmap data under read lock to avoid use-after-unlock race
		// with concurrent compaction or shutdown.
		e.mu.RLock()
		var sh *segmentHandle
		for _, h := range e.currentEpoch.segments {
			if h.meta.ID == id {
				sh = h

				break
			}
		}
		var data []byte
		if sh != nil && sh.mmap != nil {
			src := sh.mmap.Bytes()
			data = make([]byte, len(src))
			copy(data, src)
		}
		e.mu.RUnlock()
		if sh == nil || data == nil {
			continue
		}

		if err := e.tierMgr.MoveToWarm(ctx, id, data); err != nil {
			e.logger.Error("tiering: move to warm failed", "id", id, "error", err)

			continue
		}
		e.metrics.TieringUploads.Add(1)
		e.metrics.TieringUploadBytes.Add(int64(len(data)))

		// Create a new warm handle (nil reader/mmap) and retire the old one
		// via epoch advance. Pinned readers on the old epoch keep the mmap
		// alive until they unpin; drainAndClose handles the deferred close.
		e.mu.Lock()
		var oldSH *segmentHandle
		newSegments := make([]*segmentHandle, len(e.currentEpoch.segments))
		copy(newSegments, e.currentEpoch.segments)
		for i, h := range newSegments {
			if h.meta.ID != id {
				continue
			}

			oldSH = h
			warmMeta := h.meta
			warmMeta.Tier = "warm"
			if ts, _ := e.tierMgr.GetSegment(id); ts != nil {
				warmMeta.ObjectKey = ts.ObjectKey
			}
			warmMeta.Path = ""
			newSegments[i] = &segmentHandle{
				meta:        warmMeta,
				index:       h.index,
				bloom:       h.bloom,
				invertedIdx: h.invertedIdx,
			}

			break
		}
		var retired []*segmentHandle
		if oldSH != nil {
			if oldSH.meta.Path != "" {
				oldSH.pendingDelete = []string{oldSH.meta.Path}
			}
			retired = []*segmentHandle{oldSH}
		}
		e.advanceEpoch(newSegments, retired)
		e.mu.Unlock()
	}

	// Process retry queue: re-attempt warm uploads for previously failed segments.
	for _, id := range e.tierMgr.RetryReady() {
		e.mu.RLock()
		var sh *segmentHandle
		for _, h := range e.currentEpoch.segments {
			if h.meta.ID == id {
				sh = h

				break
			}
		}
		var data []byte
		if sh != nil && sh.mmap != nil {
			src := sh.mmap.Bytes()
			data = make([]byte, len(src))
			copy(data, src)
		}
		e.mu.RUnlock()
		if sh == nil || data == nil {
			e.tierMgr.ClearRetry(id) // segment gone, no point retrying

			continue
		}

		if err := e.tierMgr.MoveToWarm(ctx, id, data); err != nil {
			e.logger.Warn("tiering: retry move to warm failed", "id", id, "error", err)
			// MoveToWarm already re-enqueues on failure.
			continue
		}
		e.tierMgr.ClearRetry(id)
		e.logger.Info("tiering: retry move to warm succeeded", "id", id)
	}

	// Move to cold: same pattern as Hot→Warm — create a new handle and advance epoch.
	for _, id := range result.MovedToCold {
		if err := e.tierMgr.MoveToCold(ctx, id); err != nil {
			e.logger.Error("tiering: move to cold failed", "id", id, "error", err)

			continue
		}
		e.mu.Lock()
		var oldSH *segmentHandle
		newSegments := make([]*segmentHandle, len(e.currentEpoch.segments))
		copy(newSegments, e.currentEpoch.segments)
		for i, h := range newSegments {
			if h.meta.ID != id {
				continue
			}
			oldSH = h
			coldMeta := h.meta
			coldMeta.Tier = "cold"
			if ts, _ := e.tierMgr.GetSegment(id); ts != nil {
				coldMeta.ObjectKey = ts.ObjectKey
			}
			newSegments[i] = &segmentHandle{
				meta:        coldMeta,
				index:       h.index,
				bloom:       h.bloom,
				invertedIdx: h.invertedIdx,
			}

			break
		}
		var retired []*segmentHandle
		if oldSH != nil {
			retired = []*segmentHandle{oldSH}
		}
		e.advanceEpoch(newSegments, retired)
		e.mu.Unlock()
	}

	// Delete expired.
	for _, id := range result.Expired {
		if err := e.tierMgr.DeleteExpired(ctx, id); err != nil {
			e.logger.Error("tiering: delete expired failed", "id", id, "error", err)

			continue
		}
		e.mu.Lock()
		var expiredSH *segmentHandle
		newSegments := make([]*segmentHandle, 0, len(e.currentEpoch.segments))
		for _, sh := range e.currentEpoch.segments {
			if sh.meta.ID == id {
				expiredSH = sh
			} else {
				newSegments = append(newSegments, sh)
			}
		}
		var retired []*segmentHandle
		if expiredSH != nil {
			var paths []string
			if expiredSH.meta.Path != "" {
				paths = append(paths, expiredSH.meta.Path)
			}
			cachePath := filepath.Join(e.dataDir, "segment-cache", id+".lsg")
			paths = append(paths, cachePath)
			expiredSH.pendingDelete = paths
			retired = []*segmentHandle{expiredSH}
		}
		e.advanceEpoch(newSegments, retired)
		e.mu.Unlock()
	}
}

// loadRemoteSegment attempts to load a warm/cold segment from cache or object store.
// On success, the MmapSegment and reader are cached on the segment handle (under
// e.mu write lock) so subsequent queries reuse them and the mmap is properly
// closed on engine shutdown.
//
// Concurrent requests for the same segment are coalesced via singleflight to
// avoid redundant S3 downloads.
//
// Accepts a context so that cancelled queries also cancel in-flight S3 fetches.
// Falls back to a timeout derived from config when the context has no deadline.
func (e *Engine) loadRemoteSegment(ctx context.Context, sh *segmentHandle) *segment.Reader {
	if e.dataDir == "" || e.tierMgr == nil {
		return nil
	}

	cachePath := filepath.Join(e.dataDir, "segment-cache", sh.meta.ID+".lsg")

	// Check local cache first (fast path, no singleflight needed).
	if ms, err := segment.OpenSegmentFile(cachePath); err == nil {
		return e.cacheRemoteMmap(sh, ms)
	}

	// Coalesce concurrent downloads for the same segment. The singleflight
	// key is the segment ID — all concurrent callers share a single S3 fetch.
	v, err, _ := e.remoteLoadGroup.Do(sh.meta.ID, func() (any, error) {
		return e.doRemoteSegmentLoad(ctx, sh, cachePath)
	})
	if err != nil {
		e.logger.Error("failed to load remote segment", "id", sh.meta.ID, "error", err)
		return nil
	}
	if v == nil {
		return nil
	}
	return v.(*segment.Reader)
}

// doRemoteSegmentLoad performs the actual S3 download and cache write.
// Called inside singleflight.Do to ensure at most one concurrent download per segment.
func (e *Engine) doRemoteSegmentLoad(ctx context.Context, sh *segmentHandle, cachePath string) (*segment.Reader, error) {
	// Re-check cache — another goroutine in the singleflight group may have
	// completed while we were waiting.
	if ms, err := segment.OpenSegmentFile(cachePath); err == nil {
		return e.cacheRemoteMmap(sh, ms), nil
	}

	// Download from object store, using the caller's context for cancellation.
	fetchTimeout := e.storageCfg.RemoteFetchTimeout
	if fetchTimeout == 0 {
		fetchTimeout = 30 * time.Second
	}
	fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()

	data, err := e.tierMgr.ReadFromStore(fetchCtx, sh.meta.ID)
	if err != nil {
		return nil, err
	}

	// Write to cache atomically: tmp file + rename to avoid partial cache files
	// if the process crashes mid-write (P3-12).
	tmpPath := cachePath + ".tmp"
	if werr := os.WriteFile(tmpPath, data, 0o600); werr != nil {
		e.logger.Warn("failed to write segment cache temp file", "id", sh.meta.ID, "error", werr)
	} else if rerr := os.Rename(tmpPath, cachePath); rerr != nil {
		e.logger.Warn("failed to rename segment cache file", "id", sh.meta.ID, "error", rerr)
		os.Remove(tmpPath) // clean up orphan
	}

	// Open from cache.
	if ms, err := segment.OpenSegmentFile(cachePath); err == nil {
		return e.cacheRemoteMmap(sh, ms), nil
	}

	// Fallback: open from downloaded bytes directly (no mmap to cache).
	sr, err := segment.OpenSegment(data)
	if err != nil {
		return nil, nil // non-fatal: segment may be corrupt
	}

	return sr, nil
}

// cacheRemoteMmap stores a remote segment's MmapSegment and reader on the handle
// so subsequent queries reuse the cached reader and the mmap is closed on shutdown.
// Returns the valid reader (winner's cached reader if another goroutine raced).
func (e *Engine) cacheRemoteMmap(sh *segmentHandle, ms *segment.MmapSegment) *segment.Reader {
	e.mu.Lock()
	defer e.mu.Unlock()
	// Another goroutine may have raced and already cached.
	if sh.reader != nil {
		ms.Close()

		return sh.reader // return winner's cached reader
	}
	sh.mmap = ms
	sh.reader = ms.Reader()

	return sh.reader
}
