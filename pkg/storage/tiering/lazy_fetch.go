package tiering

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lynxbase/lynxdb/internal/objstore"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

const (
	footerReadSize     = 8192             // 8KB — enough for most segment footers
	negCacheTTL        = 30 * time.Second // suppress repeated S3 requests for the same key
	negCacheMaxEntries = 1000             // trigger lazy sweep when exceeded
	maxRetries         = 2
	retryBaseBackoff   = 100 * time.Millisecond
)

// LazyFetcher reads remote segments lazily: first the footer (to get the
// row group index and column offsets), then only the needed column chunks
// via range reads. Failed keys are negatively cached for 30s to avoid
// hammering the object store on persistent errors.
type LazyFetcher struct {
	store      objstore.ObjectStore
	cache      *SegmentCache // used for footer cache and as default chunk cache
	chunkCache ChunkCache    // pluggable chunk storage (default: cache itself)

	negMu    sync.Mutex
	negCache map[string]time.Time // key -> expiry time of negative entry
}

// NewLazyFetcher creates a new lazy fetcher.
func NewLazyFetcher(store objstore.ObjectStore, cache *SegmentCache) *LazyFetcher {
	return &LazyFetcher{
		store:      store,
		cache:      cache,
		chunkCache: cache, // default: disk-based LRU
		negCache:   make(map[string]time.Time),
	}
}

// NewLazyFetcherWithChunkCache creates a LazyFetcher that uses the provided
// ChunkCache instead of the default SegmentCache for column chunk storage.
// The footer cache remains on the SegmentCache (footers are small, always in-memory).
func NewLazyFetcherWithChunkCache(store objstore.ObjectStore, segCache *SegmentCache, chunkCache ChunkCache) *LazyFetcher {
	return &LazyFetcher{
		store:      store,
		cache:      segCache,
		chunkCache: chunkCache,
		negCache:   make(map[string]time.Time),
	}
}

// checkNegativeCache returns an error if the key was recently failed.
func (lf *LazyFetcher) checkNegativeCache(key string) error {
	lf.negMu.Lock()
	defer lf.negMu.Unlock()

	expiry, ok := lf.negCache[key]
	if !ok {
		return nil
	}
	if time.Now().After(expiry) {
		delete(lf.negCache, key)

		return nil
	}

	return fmt.Errorf("lazy_fetch: key %s in negative cache (retry after %s)", key, time.Until(expiry).Truncate(time.Second))
}

// addNegativeCache marks a key as failed for negCacheTTL.
// When the map exceeds negCacheMaxEntries, expired entries are swept lazily.
func (lf *LazyFetcher) addNegativeCache(key string) {
	lf.negMu.Lock()
	lf.negCache[key] = time.Now().Add(negCacheTTL)
	if len(lf.negCache) > negCacheMaxEntries {
		lf.sweepExpiredNegCacheLocked()
	}
	lf.negMu.Unlock()
}

// sweepExpiredNegCacheLocked removes all expired entries from the negative cache.
// Must be called with lf.negMu held.
func (lf *LazyFetcher) sweepExpiredNegCacheLocked() {
	now := time.Now()
	for k, expiry := range lf.negCache {
		if now.After(expiry) {
			delete(lf.negCache, k)
		}
	}
}

// SweepExpiredNegCache removes expired entries from the negative cache.
// Safe for concurrent use. Call periodically from the tiering cycle.
func (lf *LazyFetcher) SweepExpiredNegCache() {
	lf.negMu.Lock()
	defer lf.negMu.Unlock()
	lf.sweepExpiredNegCacheLocked()
}

// fetchWithRetry performs a store read with exponential backoff retries.
func (lf *LazyFetcher) fetchWithRetry(ctx context.Context, key string, fn func() ([]byte, error)) ([]byte, error) {
	if err := lf.checkNegativeCache(key); err != nil {
		return nil, err
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := retryBaseBackoff * time.Duration(1<<(attempt-1))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		data, err := fn()
		if err == nil {
			return data, nil
		}
		lastErr = err
		slog.Warn("lazy_fetch: retry", "key", key, "attempt", attempt+1, "error", err)
	}

	lf.addNegativeCache(key)

	return nil, lastErr
}

// FetchFooter retrieves the segment footer from the remote store.
// The footer is cached in-memory for future use.
func (lf *LazyFetcher) FetchFooter(ctx context.Context, key string, totalSize int64) (*segment.Footer, error) {
	if cached := lf.cache.GetFooter(key); cached != nil {
		return cached, nil
	}

	readSize := int64(footerReadSize)
	if readSize > totalSize {
		readSize = totalSize
	}
	offset := totalSize - readSize

	data, err := lf.fetchWithRetry(ctx, key, func() ([]byte, error) {
		return lf.store.GetRange(ctx, key, offset, readSize)
	})
	if err != nil {
		return nil, fmt.Errorf("lazy_fetch: read footer from %s: %w", key, err)
	}

	footer, err := segment.DecodeFooter(data)
	if err != nil {
		return nil, fmt.Errorf("lazy_fetch: decode footer from %s: %w", key, err)
	}

	lf.cache.PutFooter(key, footer)

	return footer, nil
}

// FetchColumnChunk retrieves a specific column chunk from the remote store.
// Uses the pluggable ChunkCache (pool-backed or disk-based) if available.
func (lf *LazyFetcher) FetchColumnChunk(ctx context.Context, key, segmentID string, rgIndex int, column string, offset, length int64) ([]byte, error) {
	// Check chunk cache first (pool-backed or disk-based depending on config).
	if cached, ok := lf.chunkCache.GetChunk(segmentID, rgIndex, column); ok {
		return cached, nil
	}

	chunkCacheKey := fmt.Sprintf("%s/rg%d/%s", key, rgIndex, column)
	data, err := lf.fetchWithRetry(ctx, chunkCacheKey, func() ([]byte, error) {
		return lf.store.GetRange(ctx, key, offset, length)
	})
	if err != nil {
		return nil, fmt.Errorf("lazy_fetch: read chunk %s/rg%d/%s: %w", key, rgIndex, column, err)
	}

	// Cache for reuse via the pluggable chunk cache.
	lf.chunkCache.PutChunk(segmentID, rgIndex, column, data)

	return data, nil
}
