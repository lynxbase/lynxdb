package tiering

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lynxbase/lynxdb/internal/objstore"
	"github.com/lynxbase/lynxdb/pkg/model"
)

// Tier represents a storage tier.
type Tier string

const (
	TierHot       Tier = "hot"       // Local SSD.
	TierMigrating Tier = "migrating" // Upload in progress.
	TierWarm      Tier = "warm"      // Object storage (e.g., S3).
	TierCold      Tier = "cold"      // Archive storage (e.g., Glacier).
	TierFailed    Tier = "failed"    // Tiering failed after max retries; requires manual intervention.
)

const (
	localDeleteDelay = 1 * time.Minute // safety window before local file deletion

	// Retry parameters for transient upload failures.
	retryInitialBackoff = 1 * time.Minute
	retryMaxBackoff     = 1 * time.Hour
	retryMaxAttempts    = 10
)

// retryEntry tracks a segment that needs to be retried for warm-tier upload.
type retryEntry struct {
	ID       string
	Attempts int
	NextAt   time.Time
}

// nextBackoff returns the exponential backoff delay for this entry (1m, 5m, 30m, capped at 1h).
func (r *retryEntry) nextBackoff() time.Duration {
	d := retryInitialBackoff
	for i := 0; i < r.Attempts; i++ {
		d = d * 3 // 1m, 3m, 9m, 27m, ~1h, capped
		if d > retryMaxBackoff {
			d = retryMaxBackoff

			break
		}
	}

	return d
}

// TieredSegment tracks a segment and its current storage tier.
type TieredSegment struct {
	Meta              model.SegmentMeta
	Tier              Tier
	ObjectKey         string    // key in object store (empty for hot tier)
	SafeToDeleteLocal bool      // true after upload verified
	DeleteLocalAfter  time.Time // earliest time for local file deletion
}

// Manager evaluates segment ages against index config and moves segments
// between storage tiers.
type Manager struct {
	mu            sync.Mutex
	segments      map[string]*TieredSegment // id -> segment
	retryQueue    map[string]*retryEntry    // id -> retry state for failed warm uploads
	store         objstore.ObjectStore
	logger        *slog.Logger
	now           func() time.Time // injectable clock for testing
	verifyUploads bool             // when true, use CRC32 read-back verification via SafeUpload
	pipeline      *UploadPipeline  // lazy-initialized when verifyUploads is true
}

// NewManager creates a new tiering manager.
func NewManager(store objstore.ObjectStore, logger *slog.Logger) *Manager {
	return &Manager{
		segments:   make(map[string]*TieredSegment),
		retryQueue: make(map[string]*retryEntry),
		store:      store,
		logger:     logger,
		now:        time.Now,
	}
}

// SetVerifyUploads enables CRC32 read-back verification for S3 uploads.
// When enabled, uploads use UploadPipeline.SafeUpload() which reads back
// the uploaded object and verifies its CRC32 matches before marking the
// segment as safe to delete locally. This doubles S3 GET cost per upload.
func (m *Manager) SetVerifyUploads(enabled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.verifyUploads = enabled
	if enabled && m.pipeline == nil {
		m.pipeline = NewUploadPipeline(m.store, UploadConfig{})
	}
}

// SetClock sets a custom clock function (for testing).
// Must be called before any concurrent use, or externally synchronized.
func (m *Manager) SetClock(fn func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = fn
}

// AddSegment registers a segment as hot tier.
func (m *Manager) AddSegment(meta model.SegmentMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tier := TierHot
	if meta.Tier != "" {
		tier = Tier(meta.Tier)
	}

	m.segments[meta.ID] = &TieredSegment{
		Meta:      meta,
		Tier:      tier,
		ObjectKey: meta.ObjectKey,
	}
}

// GetSegment returns the tiered segment info.
func (m *Manager) GetSegment(id string) (*TieredSegment, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.segments[id]

	return s, ok
}

// RemoveSegment removes a segment from tiering tracking.
func (m *Manager) RemoveSegment(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.segments, id)
}

// SegmentsByTier returns all segments at the given tier.
func (m *Manager) SegmentsByTier(tier Tier) []*TieredSegment {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*TieredSegment
	for _, s := range m.segments {
		if s.Tier == tier {
			result = append(result, s)
		}
	}

	return result
}

// ClassifyTier determines the target tier for a segment based on its age
// and the index config.
func ClassifyTier(segCreated, now time.Time, cfg model.IndexConfig) Tier {
	age := now.Sub(segCreated)
	if age < cfg.MaxHotAge {
		return TierHot
	}
	if age < cfg.MaxWarmAge {
		return TierWarm
	}

	return TierCold
}

// MoveToWarm uploads a segment's data to the object store with safety verification.
// After upload, a HeadObject check confirms the object exists before updating the tier.
// When verifyUploads is enabled, uses UploadPipeline.SafeUpload() for CRC32
// read-back verification (doubles S3 GET cost per upload).
func (m *Manager) MoveToWarm(ctx context.Context, id string, data []byte) error {
	m.mu.Lock()
	seg, ok := m.segments[id]
	if !ok {
		m.mu.Unlock()

		return fmt.Errorf("tiering: segment not found: %s", id)
	}
	if seg.Tier == TierMigrating {
		m.mu.Unlock()

		return fmt.Errorf("tiering: segment %s already migrating", id)
	}
	if seg.Tier != TierHot {
		m.mu.Unlock()

		return fmt.Errorf("tiering: segment %s is %s, expected hot", id, seg.Tier)
	}
	// Mark as migrating.
	seg.Tier = TierMigrating
	m.mu.Unlock()

	key := fmt.Sprintf("warm/%s/%s.lsg", seg.Meta.Index, id)

	// Upload with optional CRC32 read-back verification.
	if m.verifyUploads && m.pipeline != nil {
		safe, err := m.pipeline.SafeUpload(ctx, key, data)
		if err != nil {
			m.mu.Lock()
			if s, ok := m.segments[id]; ok {
				s.Tier = TierHot
			}
			m.enqueueRetryLocked(id)
			m.mu.Unlock()

			return fmt.Errorf("tiering: safe upload to warm: %w", err)
		}
		if !safe {
			m.mu.Lock()
			if s, ok := m.segments[id]; ok {
				s.Tier = TierHot
			}
			m.mu.Unlock()

			return fmt.Errorf("tiering: CRC32 verification failed for %s", key)
		}
	} else {
		if err := m.store.Put(ctx, key, data); err != nil {
			m.mu.Lock()
			if s, ok := m.segments[id]; ok {
				s.Tier = TierHot
			}
			m.enqueueRetryLocked(id)
			m.mu.Unlock()

			return fmt.Errorf("tiering: upload to warm: %w", err)
		}

		// Verify: HeadObject check after upload.
		exists, err := m.store.Exists(ctx, key)
		if err != nil || !exists {
			m.mu.Lock()
			if s, ok := m.segments[id]; ok {
				s.Tier = TierHot
			}
			m.mu.Unlock()

			return fmt.Errorf("tiering: HeadObject verification failed for %s: exists=%v: %w", key, exists, err)
		}
	}

	m.mu.Lock()
	seg2, ok := m.segments[id]
	if !ok {
		m.mu.Unlock()
		// Segment was removed during upload — clean up orphaned S3 object.
		if derr := m.store.Delete(ctx, key); derr != nil {
			m.logger.Warn("tiering: failed to clean orphaned S3 object", "key", key, "err", derr)
		}

		return fmt.Errorf("tiering: segment %s removed during migration", id)
	}
	seg2.Tier = TierWarm
	seg2.ObjectKey = key
	seg2.SafeToDeleteLocal = true
	seg2.DeleteLocalAfter = m.now().Add(localDeleteDelay)
	m.mu.Unlock()

	m.logger.Info("moved segment to warm tier", "id", id, "key", key)

	return nil
}

// ReadyForLocalDelete returns segment IDs that have been verified in the object
// store and have passed the safety delay window for local file deletion.
func (m *Manager) ReadyForLocalDelete() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	var ready []string
	for _, seg := range m.segments {
		if seg.SafeToDeleteLocal && now.After(seg.DeleteLocalAfter) {
			ready = append(ready, seg.Meta.ID)
		}
	}

	return ready
}

// MarkLocalDeleted clears the SafeToDeleteLocal flag after the local file is removed.
func (m *Manager) MarkLocalDeleted(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if seg, ok := m.segments[id]; ok {
		seg.SafeToDeleteLocal = false
	}
}

// MoveToCold moves a segment from warm to cold tier in the object store.
func (m *Manager) MoveToCold(ctx context.Context, id string) error {
	m.mu.Lock()
	seg, ok := m.segments[id]
	if !ok {
		m.mu.Unlock()

		return fmt.Errorf("tiering: segment not found: %s", id)
	}
	warmKey := seg.ObjectKey
	index := seg.Meta.Index
	m.mu.Unlock()

	// Server-side copy from warm to cold prefix (no data transfer).
	coldKey := fmt.Sprintf("cold/%s/%s.lsg", index, id)
	if err := m.store.Copy(ctx, warmKey, coldKey); err != nil {
		return fmt.Errorf("tiering: copy to cold: %w", err)
	}

	// Remove warm copy.
	if err := m.store.Delete(ctx, warmKey); err != nil {
		m.logger.Warn("failed to delete warm copy", "key", warmKey, "error", err)
	}

	m.mu.Lock()
	seg2, ok := m.segments[id]
	if !ok {
		m.mu.Unlock()
		// Segment removed during migration — clean up cold key.
		if derr := m.store.Delete(ctx, coldKey); derr != nil {
			m.logger.Warn("tiering: failed to clean orphaned cold object", "key", coldKey, "err", derr)
		}

		return fmt.Errorf("tiering: segment %s removed during cold migration", id)
	}
	seg2.Tier = TierCold
	seg2.ObjectKey = coldKey
	m.mu.Unlock()

	m.logger.Info("moved segment to cold tier", "id", id, "key", coldKey)

	return nil
}

// ReadFromStore reads a segment's data from the object store (for warm/cold tiers).
func (m *Manager) ReadFromStore(ctx context.Context, id string) ([]byte, error) {
	m.mu.Lock()
	seg, ok := m.segments[id]
	if !ok {
		m.mu.Unlock()

		return nil, fmt.Errorf("tiering: segment not found: %s", id)
	}
	key := seg.ObjectKey
	m.mu.Unlock()

	if key == "" {
		return nil, fmt.Errorf("tiering: segment %s is on hot tier (no object key)", id)
	}

	return m.store.Get(ctx, key)
}

// ReadRangeFromStore reads a byte range from a segment in the object store.
// This is used for reading individual columns without downloading the entire segment.
func (m *Manager) ReadRangeFromStore(ctx context.Context, id string, offset, length int64) ([]byte, error) {
	m.mu.Lock()
	seg, ok := m.segments[id]
	if !ok {
		m.mu.Unlock()

		return nil, fmt.Errorf("tiering: segment not found: %s", id)
	}
	key := seg.ObjectKey
	m.mu.Unlock()

	if key == "" {
		return nil, fmt.Errorf("tiering: segment %s is on hot tier (no object key)", id)
	}

	return m.store.GetRange(ctx, key, offset, length)
}

// EvaluateResult holds the result of a tiering evaluation pass.
type EvaluateResult struct {
	MovedToWarm []string
	MovedToCold []string
	Expired     []string
}

// Evaluate checks all segments against their index configs and determines
// which segments need to move between tiers or be deleted. It does NOT
// perform the actual moves — call EvaluateAndApply for that.
func (m *Manager) Evaluate(configs map[string]model.IndexConfig) *EvaluateResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	result := &EvaluateResult{}

	for _, seg := range m.segments {
		cfg, ok := configs[seg.Meta.Index]
		if !ok {
			continue
		}

		age := now.Sub(seg.Meta.CreatedAt)

		// Check for expiration.
		if age >= cfg.RetentionPeriod {
			result.Expired = append(result.Expired, seg.Meta.ID)

			continue
		}

		// Skip segments stuck in failed or migrating state.
		if seg.Tier == TierFailed || seg.Tier == TierMigrating {
			continue
		}

		target := ClassifyTier(seg.Meta.CreatedAt, now, cfg)
		if target == TierWarm && seg.Tier == TierHot {
			result.MovedToWarm = append(result.MovedToWarm, seg.Meta.ID)
		} else if target == TierCold && seg.Tier == TierWarm {
			result.MovedToCold = append(result.MovedToCold, seg.Meta.ID)
		}
	}

	return result
}

// enqueueRetryLocked adds a segment to the retry queue with exponential backoff.
// Must be called with m.mu held.
func (m *Manager) enqueueRetryLocked(id string) {
	entry, ok := m.retryQueue[id]
	if !ok {
		entry = &retryEntry{ID: id}
		m.retryQueue[id] = entry
	}
	entry.Attempts++
	if entry.Attempts > retryMaxAttempts {
		m.logger.Error("tiering: segment exceeded max retry attempts, marking as failed",
			"id", id, "attempts", entry.Attempts)
		delete(m.retryQueue, id)
		// Mark segment as TierFailed so Evaluate() skips it.
		if seg, ok := m.segments[id]; ok {
			seg.Tier = TierFailed
		}

		return
	}
	entry.NextAt = m.now().Add(entry.nextBackoff())
	m.logger.Info("tiering: segment enqueued for retry",
		"id", id, "attempt", entry.Attempts, "next_at", entry.NextAt)
}

// RetryReady returns segment IDs that are due for retry. The caller is
// responsible for calling MoveToWarm with fresh data for each returned ID.
// On success, the retry entry is automatically cleared.
func (m *Manager) RetryReady() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	var ready []string
	for id, entry := range m.retryQueue {
		if now.After(entry.NextAt) {
			ready = append(ready, id)
		}
	}

	return ready
}

// ClearRetry removes a segment from the retry queue (called after successful upload).
func (m *Manager) ClearRetry(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.retryQueue, id)
}

// RetryQueueLen returns the number of segments pending retry.
func (m *Manager) RetryQueueLen() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.retryQueue)
}

// DeleteExpired removes expired segments from tracking and from the object store.
func (m *Manager) DeleteExpired(ctx context.Context, id string) error {
	m.mu.Lock()
	seg, ok := m.segments[id]
	if !ok {
		m.mu.Unlock()

		return nil
	}
	key := seg.ObjectKey
	delete(m.segments, id)
	m.mu.Unlock()

	if key != "" {
		if err := m.store.Delete(ctx, key); err != nil {
			return fmt.Errorf("tiering: delete expired segment: %w", err)
		}
	}

	m.logger.Info("deleted expired segment", "id", id)

	return nil
}

// FailedSegments returns segment IDs that are stuck in TierFailed state.
// Useful for status/monitoring APIs.
func (m *Manager) FailedSegments() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []string
	for _, seg := range m.segments {
		if seg.Tier == TierFailed {
			ids = append(ids, seg.Meta.ID)
		}
	}
	return ids
}

// ResetFailed moves a segment from TierFailed back to TierHot so that
// tiering evaluation will retry it. Use after resolving the underlying issue.
func (m *Manager) ResetFailed(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	seg, ok := m.segments[id]
	if !ok || seg.Tier != TierFailed {
		return false
	}
	seg.Tier = TierHot
	return true
}
