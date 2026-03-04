package buffer

import (
	"fmt"

	"github.com/lynxbase/lynxdb/pkg/stats"
)

// PoolAccount implements stats.MemoryAccount backed by the unified buffer
// pool. Instead of tracking memory against a fixed per-query budget, it
// allocates pages from the shared buffer pool as "memory credits."
//
// When a query operator calls Grow(n), the account calculates how many
// additional pages are needed and allocates them via OperatorPageAllocator.
// If the pool is full, the allocator evicts cache pages first (automatic
// cross-consumer rebalancing). If eviction fails (all pages pinned), Grow
// returns an error and the operator spills to disk.
//
// NOT thread-safe. Designed for single-goroutine Volcano pipeline.
type PoolAccount struct {
	alloc    *OperatorPageAllocator
	pageSize int
	jobID    string

	// usedBytes tracks how many logical bytes the operator has requested.
	// pagesHeld tracks how many pages we've allocated as credits.
	usedBytes int64
	maxUsed   int64
	pagesHeld int

	// monitor receives observation-only callbacks (ObserveGrow/ObserveShrink)
	// so that BudgetMonitor.MaxAllocated() reports accurate PeakMemoryBytes.
	// The pool handles limit enforcement; the monitor only observes for stats.
	// Nil-safe: all ObserveGrow/ObserveShrink calls are no-ops on nil monitor.
	monitor *stats.BudgetMonitor
}

// NewPoolAccount creates a memory account backed by the buffer pool.
// The jobID identifies the query for diagnostics and per-query pin tracking.
// The monitor parameter enables peak-memory reporting: Grow/Shrink/Close calls
// are mirrored to the monitor via ObserveGrow/ObserveShrink so that
// BudgetMonitor.MaxAllocated() (→ PeakMemoryBytes) is accurate. Pass nil to
// disable monitor tracking (e.g., in tests that don't need stats reporting).
// Returns nil if pool is nil (allows nil-safe chaining identical to BoundAccount).
func NewPoolAccount(pool *Pool, jobID string, monitor *stats.BudgetMonitor) *PoolAccount {
	if pool == nil {
		return nil
	}

	return &PoolAccount{
		alloc:    NewOperatorPageAllocator(pool, jobID),
		pageSize: pool.PageSize(),
		jobID:    jobID,
		monitor:  monitor,
	}
}

// Grow requests n bytes of memory. Allocates buffer pool pages as needed to
// cover the total tracked usage. Returns an error if the pool cannot provide
// enough pages (all pages pinned after eviction attempts).
//
// Nil-safe: if account is nil, always succeeds (no tracking).
func (a *PoolAccount) Grow(n int64) error {
	if a == nil || n <= 0 {
		return nil
	}

	newUsed := a.usedBytes + n

	// Calculate how many pages we need for the new total.
	pagesNeeded := int((newUsed + int64(a.pageSize) - 1) / int64(a.pageSize))

	// Allocate additional pages if needed.
	for a.pagesHeld < pagesNeeded {
		if _, err := a.alloc.AllocPage(); err != nil {
			// Wrap as *BudgetExceededError so operators trigger spill-to-disk.
			// The pool acts as the account's limit — page exhaustion means
			// "budget exceeded" from the operator's perspective. This ensures
			// stats.IsBudgetExceeded(err) and stats.IsMemoryExhausted(err) both
			// return true, matching the BoundAccount error contract.
			return &stats.BudgetExceededError{
				Monitor:   "buffer-pool",
				Account:   fmt.Sprintf("%s (page alloc: %v)", a.jobID, err),
				Requested: n,
				Current:   a.usedBytes,
				Limit:     int64(a.alloc.Pool().MaxPages()) * int64(a.pageSize),
			}
		}
		a.pagesHeld++
	}

	a.usedBytes = newUsed
	if a.usedBytes > a.maxUsed {
		a.maxUsed = a.usedBytes
	}
	a.monitor.ObserveGrow(n) // mirror to monitor for PeakMemoryBytes reporting

	return nil
}

// Shrink releases n bytes of tracked usage. Does not immediately free pages —
// pages are retained as a reserve for subsequent Grow calls within the same
// query. All pages are freed on Close().
//
// Nil-safe.
func (a *PoolAccount) Shrink(n int64) {
	if a == nil || n <= 0 {
		return
	}
	if n > a.usedBytes {
		n = a.usedBytes
	}
	a.usedBytes -= n
	a.monitor.ObserveShrink(n) // mirror to monitor

	// Release excess pages back to the pool. This is critical for spill-to-disk:
	// after an operator spills its in-memory data, Shrink() is called to reduce
	// the tracked bytes. The corresponding pool pages must actually be freed so
	// that subsequent Grow() calls (for new groups/rows) can re-allocate them.
	// Without this, the pool remains full after spill and the next Grow() fails
	// with ErrAllPagesPinned.
	pagesNeeded := int((a.usedBytes + int64(a.pageSize) - 1) / int64(a.pageSize))
	if pagesNeeded < 0 {
		pagesNeeded = 0
	}

	excess := a.pagesHeld - pagesNeeded
	if excess > 0 {
		a.alloc.ReleaseLast(excess)
		a.pagesHeld = pagesNeeded
	}
}

// Close releases all tracked bytes and returns all pages to the buffer pool.
// Must be called when the query completes to prevent memory leaks.
//
// Nil-safe.
func (a *PoolAccount) Close() {
	if a == nil {
		return
	}
	saved := a.usedBytes // save before zeroing for monitor mirror
	a.alloc.ReleaseAll()
	a.usedBytes = 0
	a.pagesHeld = 0
	a.monitor.ObserveShrink(saved) // mirror after successful release
}

// Used returns the current tracked byte count.
// Nil-safe: returns 0 if account is nil.
func (a *PoolAccount) Used() int64 {
	if a == nil {
		return 0
	}

	return a.usedBytes
}

// MaxUsed returns the peak tracked byte count.
// Nil-safe: returns 0 if account is nil.
func (a *PoolAccount) MaxUsed() int64 {
	if a == nil {
		return 0
	}

	return a.maxUsed
}

// PageCount returns the number of buffer pool pages currently held.
func (a *PoolAccount) PageCount() int {
	if a == nil {
		return 0
	}

	return a.pagesHeld
}
