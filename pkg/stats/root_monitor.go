package stats

import (
	"errors"
	"fmt"
	"sync"
)

// RootMonitor is a thread-safe global memory pool that coordinates memory
// across multiple per-query BudgetMonitors. Each query reserves chunks from
// the pool; when the pool is exhausted, new queries receive a PoolExhaustedError.
//
// When backed by a UnifiedPool, Reserve/Release delegate to
// UnifiedPool.ReserveForQuery/ReleaseQuery, enabling elastic sharing between
// query execution and segment cache. The RootMonitor still manages its own
// high-water mark and per-query child accounting.
//
// All methods are nil-safe: calling any method on a nil *RootMonitor is a no-op
// or returns zero/nil. This allows code to unconditionally use a RootMonitor
// without nil checks.
type RootMonitor struct {
	mu           sync.Mutex
	label        string
	curAllocated int64
	maxAllocated int64 // high-water mark
	limit        int64 // 0 = unlimited (tracking only)

	// unifiedPool, when non-nil, is the backing allocator. Reserve/Release
	// delegate to unifiedPool.ReserveForQuery/ReleaseQuery instead of doing
	// their own standalone limit check. The standalone limit field is still
	// respected as a secondary cap (defense in depth).
	unifiedPool *UnifiedPool
}

// NewRootMonitor creates a global memory pool with the given label and byte limit.
// A limit of 0 means unlimited (tracking only, no enforcement).
func NewRootMonitor(label string, limit int64) *RootMonitor {
	return &RootMonitor{
		label: label,
		limit: limit,
	}
}

// NewRootMonitorWithPool creates a global memory pool backed by a UnifiedPool.
// Reserve/Release delegate to the unified pool for elastic cache sharing.
// The standalone limit serves as a secondary cap (set to pool.TotalLimit()
// if you want the unified pool to be the sole arbiter).
func NewRootMonitorWithPool(label string, limit int64, pool *UnifiedPool) *RootMonitor {
	return &RootMonitor{
		label:       label,
		limit:       limit,
		unifiedPool: pool,
	}
}

// UnifiedPool returns the backing UnifiedPool, or nil if the monitor is
// standalone. Nil-safe: returns nil if monitor is nil.
func (m *RootMonitor) UnifiedPool() *UnifiedPool {
	if m == nil {
		return nil
	}

	return m.unifiedPool
}

// Reserve atomically requests n bytes from the pool.
// When backed by a UnifiedPool, delegates to ReserveForQuery (which may evict
// cache entries to make room). Falls back to standalone limit check otherwise.
// Returns a *PoolExhaustedError if the reservation would exceed the pool limit.
// Nil-safe: returns nil if monitor is nil.
func (m *RootMonitor) Reserve(n int64) error {
	if m == nil || n <= 0 {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Standalone limit check (defense in depth — always enforced).
	if m.limit > 0 && m.curAllocated+n > m.limit {
		return &PoolExhaustedError{
			Pool:      m.label,
			Requested: n,
			Current:   m.curAllocated,
			Limit:     m.limit,
		}
	}

	// Delegate to unified pool if present. The unified pool may evict cache
	// entries to satisfy the request. We must release the mu lock to avoid
	// deadlock since the evictor callback may interact with cache locking,
	// but we already checked the standalone limit above. Instead, we call
	// the unified pool while still holding mu — the UnifiedPool's own lock
	// is separate, and the evictor callback does not reacquire RootMonitor's mu.
	if m.unifiedPool != nil {
		if err := m.unifiedPool.ReserveForQuery(n); err != nil {
			return err
		}
	}

	m.curAllocated += n
	if m.curAllocated > m.maxAllocated {
		m.maxAllocated = m.curAllocated
	}

	return nil
}

// Release returns n bytes to the pool. Clamps to 0 on underflow.
// When backed by a UnifiedPool, also releases from the unified pool.
// Nil-safe: no-op if monitor is nil.
func (m *RootMonitor) Release(n int64) {
	if m == nil || n <= 0 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Release from unified pool if present.
	if m.unifiedPool != nil {
		m.unifiedPool.ReleaseQuery(n)
	}

	m.curAllocated -= n
	if m.curAllocated < 0 {
		m.curAllocated = 0
	}
}

// CurAllocated returns the current bytes reserved from the pool.
// Nil-safe: returns 0 if monitor is nil.
func (m *RootMonitor) CurAllocated() int64 {
	if m == nil {
		return 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.curAllocated
}

// MaxAllocated returns the high-water mark of bytes reserved from the pool.
// Nil-safe: returns 0 if monitor is nil.
func (m *RootMonitor) MaxAllocated() int64 {
	if m == nil {
		return 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.maxAllocated
}

// Limit returns the pool's byte limit. 0 means unlimited.
// Nil-safe: returns 0 if monitor is nil.
func (m *RootMonitor) Limit() int64 {
	if m == nil {
		return 0
	}

	return m.limit
}

// PoolExhaustedError is returned when a query cannot reserve memory from the
// global pool because all concurrent queries together have exhausted it.
// Distinct from BudgetExceededError (per-query limit) — this means the
// server-wide query memory pool is full.
type PoolExhaustedError struct {
	Pool      string
	Requested int64
	Current   int64
	Limit     int64
}

func (e *PoolExhaustedError) Error() string {
	return fmt.Sprintf(
		"query pool exhausted: %s requested %d bytes (current: %d, limit: %d)",
		e.Pool, e.Requested, e.Current, e.Limit,
	)
}

// IsPoolExhausted reports whether the error is a *PoolExhaustedError.
func IsPoolExhausted(err error) bool {
	var target *PoolExhaustedError

	return errors.As(err, &target)
}
