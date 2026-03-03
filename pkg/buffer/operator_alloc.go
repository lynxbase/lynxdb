package buffer

import (
	"fmt"
	"sync"
)

// OperatorPageAllocator provides page-based memory for query operators.
// Each operator (sort, aggregate, dedup, join) gets its own allocator that
// draws pages from the shared Pool. When the pool is full, the buffer manager
// may evict cached segment pages to make room — automatic memory rebalancing.
//
// All pages allocated through this allocator are tagged with OwnerQueryOperator
// and the operator's jobID for diagnostics.
//
// Thread-safe. Multiple goroutines may call AllocPage concurrently, though
// typical Volcano-model operators are single-goroutine.
type OperatorPageAllocator struct {
	mu    sync.Mutex
	pool  *Pool
	jobID string
	pages []*Page // all pages allocated by this operator
}

// NewOperatorPageAllocator creates an allocator for a query operator.
// The jobID is used as the ownerTag for diagnostics and per-query pin tracking.
func NewOperatorPageAllocator(pool *Pool, jobID string) *OperatorPageAllocator {
	return &OperatorPageAllocator{
		pool:  pool,
		jobID: jobID,
	}
}

// AllocPage allocates a page for operator use. The returned page is pinned.
// If the pool is full, the buffer manager may evict cached segment pages
// to make room (automatic rebalancing). Returns ErrAllPagesPinned if eviction
// is impossible.
//
// Callers should call page.Unpin() when no longer actively reading/writing
// the page (making it an eviction candidate), or call ReleaseAll() to free
// all pages on query completion.
func (oa *OperatorPageAllocator) AllocPage() (*Page, error) {
	// Prefer to evict cache pages before evicting other query pages.
	p, err := oa.pool.AllocPageForOwner(OwnerQueryOperator, oa.jobID, OwnerSegmentCache)
	if err != nil {
		return nil, fmt.Errorf("buffer.OperatorPageAllocator.AllocPage: %w", err)
	}

	oa.mu.Lock()
	oa.pages = append(oa.pages, p)
	oa.mu.Unlock()

	return p, nil
}

// PageCount returns the number of pages currently held by this operator.
func (oa *OperatorPageAllocator) PageCount() int {
	oa.mu.Lock()
	defer oa.mu.Unlock()

	return len(oa.pages)
}

// ReleaseAll frees all pages back to the pool. Must be called when the query
// completes (success or error) to prevent memory leaks. After ReleaseAll,
// the allocator can be reused for a new query.
func (oa *OperatorPageAllocator) ReleaseAll() {
	oa.mu.Lock()
	pages := oa.pages
	oa.pages = nil
	oa.mu.Unlock()

	for _, p := range pages {
		// Ensure the page is unpinned before freeing. The pin count might be >0
		// if the operator didn't unpin all pages before calling ReleaseAll.
		for p.PinCount() > 0 {
			p.Unpin()
		}
		oa.pool.FreePage(p)
	}
}

// ReleaseLast frees the last n pages back to the pool. This is used by
// PoolAccount.Shrink() to release excess pages after a spill-to-disk
// operation frees memory. Pages are released in LIFO order (most recently
// allocated first), which is correct because spill flushes the most recently
// accumulated data.
//
// If n >= len(pages), all pages are released.
func (oa *OperatorPageAllocator) ReleaseLast(n int) {
	oa.mu.Lock()
	if n >= len(oa.pages) {
		n = len(oa.pages)
	}
	// Slice off the last n pages.
	toFree := make([]*Page, n)
	copy(toFree, oa.pages[len(oa.pages)-n:])
	oa.pages = oa.pages[:len(oa.pages)-n]
	oa.mu.Unlock()

	for _, p := range toFree {
		for p.PinCount() > 0 {
			p.Unpin()
		}
		oa.pool.FreePage(p)
	}
}

// Pool returns the underlying buffer pool.
func (oa *OperatorPageAllocator) Pool() *Pool {
	return oa.pool
}
