package buffer

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"unsafe"
)

// PoolConfig configures the Pool.
type PoolConfig struct {
	// MaxPages is the total number of pages in the pool.
	// Default: computed from MaxMemoryBytes / PageSize.
	MaxPages int

	// MaxMemoryBytes is the total memory for the buffer pool.
	// 0 = auto (uses MaxPages). Takes precedence over MaxPages when > 0.
	MaxMemoryBytes int64

	// PageSize is the default page size in bytes. Default: PageSize64KB (65536).
	PageSize int

	// EnableOffHeap uses mmap for page data (recommended for production).
	// When false or on unsupported platforms, falls back to Go heap allocation.
	EnableOffHeap bool

	// CacheTargetPercent is the advisory fraction (0-100) of the pool for segment cache.
	CacheTargetPercent int

	// QueryTargetPercent is the advisory fraction (0-100) for query operators.
	QueryTargetPercent int

	// BatcherTargetPercent is the advisory fraction (0-100) for batcher.
	BatcherTargetPercent int

	// MaxPinnedPagesPerQuery is a safety limit to prevent pin leaks.
	// 0 = no limit. Default: 1024.
	MaxPinnedPagesPerQuery int

	// WriteBackFunc is called before evicting a dirty page. The function
	// receives the page (still pinned during write-back) and must persist
	// its contents. If nil, dirty pages are silently discarded on eviction.
	WriteBackFunc func(p *Page) error

	// Logger for buffer pool operations. Nil = default slog.
	Logger *slog.Logger
}

// PageWriteBack is implemented by page owner data to handle per-page
// eviction write-back. When a page's OwnerData implements this interface,
// handleEviction dispatches to it instead of the global writeback function.
// This enables operators (e.g., sort) to save their page data to a shared
// spill file on eviction, with per-page offset tracking for later retrieval.
type PageWriteBack interface {
	WriteBackPage(p *Page) error
}

// DefaultPoolConfig returns sensible defaults for a buffer pool.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxPages:               1024,
		PageSize:               PageSize64KB,
		EnableOffHeap:          true,
		CacheTargetPercent:     60,
		QueryTargetPercent:     30,
		BatcherTargetPercent:   10,
		MaxPinnedPagesPerQuery: 1024,
	}
}

// Pool is the unified memory manager for LynxDB.
// One instance per Engine. Thread-safe.
//
// All data (persistent segment cache and temporary query intermediates) is managed
// through fixed-size pages with a single eviction policy. When a consumer needs
// a page and the pool is full, the least-recently-used unpinned page from ANY
// consumer is evicted.
type Pool struct {
	mu sync.Mutex

	pages    []*Page       // all allocated pages (fixed size after init)
	freeList []*Page       // pages available for allocation
	evictor  *clockEvictor // eviction policy

	maxPages  int
	pageSize  int
	offHeap   bool // true = mmap allocation, false = Go heap
	closed    bool
	writeback func(p *Page) error
	logger    *slog.Logger

	// Advisory consumer targets (not enforced, used for rebalancing hints).
	cacheTarget   float64
	queryTarget   float64
	batcherTarget float64

	// Per-query pin tracking.
	maxPinnedPerQuery int

	// heapBacking stores Go slice references for on-heap pages to prevent GC.
	// Indexed by poolSlot. Only used when offHeap is false.
	heapBacking [][]byte

	// Stats (atomic for lock-free reads).
	hits        atomic.Int64
	misses      atomic.Int64
	evictions   atomic.Int64
	dirtyWrites atomic.Int64
	allocations atomic.Int64
	frees       atomic.Int64
}

// PoolStats is a point-in-time snapshot of Pool metrics.
type PoolStats struct {
	TotalPages      int   `json:"total_pages"`
	FreePages       int   `json:"free_pages"`
	UsedPages       int   `json:"used_pages"`
	EvictorPages    int   `json:"evictor_pages"`
	PageSizeBytes   int   `json:"page_size_bytes"`
	TotalBytes      int64 `json:"total_bytes"`
	Hits            int64 `json:"hits"`
	Misses          int64 `json:"misses"`
	Evictions       int64 `json:"evictions"`
	DirtyWritebacks int64 `json:"dirty_writebacks"`
	Allocations     int64 `json:"allocations"`
	Frees           int64 `json:"frees"`
	OffHeap         bool  `json:"off_heap"`

	// Per-owner page counts.
	CachePages    int `json:"cache_pages"`
	QueryPages    int `json:"query_pages"`
	MemtablePages int `json:"memtable_pages"`
}

// ErrAllPagesPinned is returned when AllocPage cannot find any evictable page
// because all pages are currently pinned. This indicates resource exhaustion
// or a pin leak.
var ErrAllPagesPinned = fmt.Errorf("buffer pool: all pages are pinned; cannot evict")

// NewPool creates a unified buffer pool with the given configuration.
// Pages are pre-allocated at startup (all placed on the free list).
func NewPool(cfg PoolConfig) (*Pool, error) {
	if cfg.PageSize <= 0 {
		cfg.PageSize = PageSize64KB
	}

	maxPages := cfg.MaxPages
	if cfg.MaxMemoryBytes > 0 {
		maxPages = int(cfg.MaxMemoryBytes / int64(cfg.PageSize))
	}
	if maxPages <= 0 {
		maxPages = 1024 // 1024 * 64KB = 64MB default
	}

	offHeap := cfg.EnableOffHeap && mmapAvailable()

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	bp := &Pool{
		pages:             make([]*Page, 0, maxPages),
		freeList:          make([]*Page, 0, maxPages),
		evictor:           newClockEvictor(maxPages),
		maxPages:          maxPages,
		pageSize:          cfg.PageSize,
		offHeap:           offHeap,
		writeback:         cfg.WriteBackFunc,
		logger:            logger,
		cacheTarget:       float64(cfg.CacheTargetPercent) / 100.0,
		queryTarget:       float64(cfg.QueryTargetPercent) / 100.0,
		batcherTarget:     float64(cfg.BatcherTargetPercent) / 100.0,
		maxPinnedPerQuery: cfg.MaxPinnedPagesPerQuery,
	}

	if !offHeap {
		bp.heapBacking = make([][]byte, maxPages)
	}

	// Pre-allocate all pages.
	for i := 0; i < maxPages; i++ {
		p, err := bp.newPage(PageID(i+1), i)
		if err != nil {
			_ = bp.closePages()

			return nil, fmt.Errorf("buffer.NewPool: pre-allocate page %d/%d: %w", i, maxPages, err)
		}
		bp.pages = append(bp.pages, p)
		bp.freeList = append(bp.freeList, p)
	}

	return bp, nil
}

// AllocPage allocates a page for a consumer. If the pool has no free pages,
// evicts the least-recently-used unpinned page (from ANY consumer).
//
// Returns ErrAllPagesPinned if all pages are pinned (deadlock or resource exhaustion).
// The returned page is pinned (pin count = 1). Caller must call page.Unpin()
// when done, or defer page.Unpin().
func (bp *Pool) AllocPage(owner PageOwner, ownerTag string) (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if bp.closed {
		return nil, fmt.Errorf("buffer.Pool.AllocPage: pool is closed")
	}

	// Fast path: grab from free list.
	if len(bp.freeList) > 0 {
		p := bp.freeList[len(bp.freeList)-1]
		bp.freeList = bp.freeList[:len(bp.freeList)-1]
		bp.initPage(p, owner, ownerTag)
		bp.evictor.add(p)
		bp.allocations.Add(1)
		bp.hits.Add(1)

		return p, nil
	}

	// Slow path: evict a page.
	bp.misses.Add(1)
	p := bp.evictOneLocked()
	if p == nil {
		return nil, ErrAllPagesPinned
	}

	bp.initPage(p, owner, ownerTag)
	bp.evictor.add(p)
	bp.allocations.Add(1)

	return p, nil
}

// AllocPageForOwner allocates a page, preferring to evict pages from a specific
// owner. If no pages from that owner are evictable, falls back to general eviction.
func (bp *Pool) AllocPageForOwner(owner PageOwner, ownerTag string, evictPreference PageOwner) (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if bp.closed {
		return nil, fmt.Errorf("buffer.Pool.AllocPageForOwner: pool is closed")
	}

	// Fast path: free list.
	if len(bp.freeList) > 0 {
		p := bp.freeList[len(bp.freeList)-1]
		bp.freeList = bp.freeList[:len(bp.freeList)-1]
		bp.initPage(p, owner, ownerTag)
		bp.evictor.add(p)
		bp.allocations.Add(1)
		bp.hits.Add(1)

		return p, nil
	}

	bp.misses.Add(1)

	// Try targeted eviction first.
	p := bp.evictor.evictByOwner(evictPreference)
	if p != nil {
		bp.handleEviction(p)
		bp.initPage(p, owner, ownerTag)
		bp.evictor.add(p)
		bp.allocations.Add(1)

		return p, nil
	}

	// Fallback: general eviction.
	p = bp.evictOneLocked()
	if p == nil {
		return nil, ErrAllPagesPinned
	}

	bp.initPage(p, owner, ownerTag)
	bp.evictor.add(p)
	bp.allocations.Add(1)

	return p, nil
}

// FreePage returns a page to the free list. The page's metadata is cleared for reuse.
func (bp *Pool) FreePage(p *Page) {
	if p == nil {
		return
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.evictor.remove(p)
	p.reset()
	bp.freeList = append(bp.freeList, p)
	bp.frees.Add(1)
}

// Resolve returns a byte slice for the referenced data within a page.
// The page must be pinned before calling Resolve. Returns nil if the
// reference is invalid or the page is not found.
func (bp *Pool) Resolve(ref PageRef) []byte {
	bp.mu.Lock()
	p := bp.findPageByID(ref.PageID)
	bp.mu.Unlock()

	if p == nil || p.data == nil {
		return nil
	}
	if ref.Offset < 0 || ref.Offset+ref.Length > p.size {
		return nil
	}

	return p.DataSlice()[ref.Offset : ref.Offset+ref.Length]
}

// Stats returns a point-in-time snapshot of pool metrics.
func (bp *Pool) Stats() PoolStats {
	bp.mu.Lock()
	freeCount := len(bp.freeList)
	evictorCount := bp.evictor.len()
	var cachePages, queryPages, memtablePages int
	for _, p := range bp.pages {
		if p == nil {
			continue
		}
		// Only count pages that are NOT on the free list (i.e., in use).
		switch p.owner {
		case OwnerSegmentCache:
			cachePages++
		case OwnerQueryOperator:
			queryPages++
		case OwnerMemtable:
			memtablePages++
		}
	}
	bp.mu.Unlock()

	// Subtract free pages from counts (free pages have default owner=cache).
	cachePages -= freeCount

	return PoolStats{
		TotalPages:      bp.maxPages,
		FreePages:       freeCount,
		UsedPages:       bp.maxPages - freeCount,
		EvictorPages:    evictorCount,
		PageSizeBytes:   bp.pageSize,
		TotalBytes:      int64(bp.maxPages) * int64(bp.pageSize),
		Hits:            bp.hits.Load(),
		Misses:          bp.misses.Load(),
		Evictions:       bp.evictions.Load(),
		DirtyWritebacks: bp.dirtyWrites.Load(),
		Allocations:     bp.allocations.Load(),
		Frees:           bp.frees.Load(),
		OffHeap:         bp.offHeap,
		CachePages:      cachePages,
		QueryPages:      queryPages,
		MemtablePages:   memtablePages,
	}
}

// PageSize returns the default page size in bytes.
func (bp *Pool) PageSize() int {
	return bp.pageSize
}

// MaxPages returns the total number of pages in the pool.
func (bp *Pool) MaxPages() int {
	return bp.maxPages
}

// CacheTarget returns the advisory cache target fraction (0.0–1.0).
func (bp *Pool) CacheTarget() float64 {
	return bp.cacheTarget
}

// QueryTarget returns the advisory query target fraction (0.0–1.0).
func (bp *Pool) QueryTarget() float64 {
	return bp.queryTarget
}

// BatcherTarget returns the advisory batcher target fraction (0.0–1.0).
func (bp *Pool) BatcherTarget() float64 {
	return bp.batcherTarget
}

// MaxPinnedPerQuery returns the per-query pin limit (0 = unlimited).
func (bp *Pool) MaxPinnedPerQuery() int {
	return bp.maxPinnedPerQuery
}

// Close releases all page memory and marks the pool as closed.
// Any further AllocPage calls will return an error.
func (bp *Pool) Close() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if bp.closed {
		return nil
	}
	bp.closed = true
	bp.freeList = nil

	return bp.closePages()
}

// Internal helpers

// newPage creates a Page with allocated data memory.
func (bp *Pool) newPage(id PageID, slot int) (*Page, error) {
	p := &Page{
		id:       id,
		size:     bp.pageSize,
		poolSlot: slot,
	}

	if bp.offHeap {
		ptr, err := allocateOffHeap(bp.pageSize)
		if err != nil {
			return nil, err
		}
		p.data = ptr
	} else {
		buf := make([]byte, bp.pageSize)
		p.data = unsafe.Pointer(&buf[0])
		bp.heapBacking[slot] = buf
	}

	return p, nil
}

// initPage prepares a page for use by a consumer. The page is pinned on return.
func (bp *Pool) initPage(p *Page, owner PageOwner, ownerTag string) {
	p.reset()
	p.owner = owner
	p.ownerTag = ownerTag
	p.Pin()
}

// evictOneLocked finds and evicts one page. Must be called with bp.mu held.
func (bp *Pool) evictOneLocked() *Page {
	p := bp.evictor.evict()
	if p == nil {
		return nil
	}
	bp.handleEviction(p)

	return p
}

// handleEviction performs dirty write-back and updates stats for an evicted page.
// When the page's OwnerData implements PageWriteBack, the per-page callback is
// used instead of the global writeback function. This allows operators to save
// page data to operator-specific spill files with precise offset tracking.
func (bp *Pool) handleEviction(p *Page) {
	if p.IsDirty() {
		if wb, ok := p.OwnerData().(PageWriteBack); ok {
			// Per-operator write-back via OwnerData.
			p.Pin()
			if err := wb.WriteBackPage(p); err != nil {
				bp.logger.Warn("buffer pool: per-page write-back failed",
					"page_id", p.id, "owner", p.owner.String(), "error", err)
			}
			p.Unpin()
			bp.dirtyWrites.Add(1)
		} else if bp.writeback != nil {
			// Global write-back fallback.
			p.Pin()
			if err := bp.writeback(p); err != nil {
				bp.logger.Warn("buffer pool: dirty page write-back failed",
					"page_id", p.id, "owner", p.owner.String(), "error", err)
			}
			p.Unpin()
			bp.dirtyWrites.Add(1)
		}
	}
	bp.evictions.Add(1)
}

// findPageByID looks up a page by its ID. Must be called with bp.mu held.
func (bp *Pool) findPageByID(id PageID) *Page {
	idx := int(id) - 1
	if idx < 0 || idx >= len(bp.pages) {
		return nil
	}

	return bp.pages[idx]
}

// PinPageIfOwned atomically pins a page if it is still owned by the
// expected ownerTag. Returns (page, true) on success, or (nil, false)
// if the page was evicted or reassigned. Acquires pool.mu internally.
//
// This is used by operators that store data in pool pages and unpin them
// (making them eviction candidates). When reading back, the operator tries
// PinPageIfOwned first — if the page is still in the pool, it's a cache hit.
// If evicted, the operator reads from disk (via the write-back spill file).
func (bp *Pool) PinPageIfOwned(id PageID, expectedTag string) (*Page, bool) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	p := bp.findPageByID(id)
	if p == nil || p.ownerTag != expectedTag {
		return nil, false
	}
	p.Pin()
	bp.hits.Add(1)

	return p, true
}

// closePages frees all page data memory. Must be called with bp.mu held.
func (bp *Pool) closePages() error {
	var firstErr error
	for _, p := range bp.pages {
		if p == nil || p.data == nil {
			continue
		}
		if bp.offHeap {
			if err := freeOffHeap(p.data, p.size); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		p.data = nil
	}
	bp.pages = nil
	bp.heapBacking = nil

	return firstErr
}
