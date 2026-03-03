package stats

import (
	"errors"
	"fmt"
	"sync"
)

// MemoryAccount is the interface for operator-level memory tracking.
// Implementations:
//   - *BoundAccount: threshold-based tracking via BudgetMonitor.
//   - *buffer.PoolAccount: pool-based tracking via unified buffer manager.
//
// Operators store their acct as MemoryAccount so the same code works with
// either backing. When buffer_manager.enabled is false, acct is a *BoundAccount.
// When true, acct is a *buffer.PoolAccount. Both handle nil receiver gracefully
// (no-op on nil *BoundAccount, which becomes a typed-nil interface value).
type MemoryAccount interface {
	// Grow requests n bytes. Returns error if the budget/pool is exhausted.
	Grow(n int64) error
	// Shrink releases n bytes.
	Shrink(n int64)
	// Close releases all remaining bytes. Must be called on query completion.
	Close()
	// Used returns the current tracked byte count.
	Used() int64
	// MaxUsed returns the peak tracked byte count.
	MaxUsed() int64
}

// nopAccount is a MemoryAccount that does nothing.
// Used as a sentinel to avoid nil interface checks in operators.
type nopAccount struct{}

func (nopAccount) Grow(int64) error { return nil }
func (nopAccount) Shrink(int64)     {}
func (nopAccount) Close()           {}
func (nopAccount) Used() int64      { return 0 }
func (nopAccount) MaxUsed() int64   { return 0 }

// NopAccount returns a no-op MemoryAccount that accepts all Grow requests
// and tracks nothing. Intended for:
//   - Tests that don't need budget enforcement
//   - Ephemeral CLI pipe-mode pipelines (no server, no budget)
//   - Defensive fallback via EnsureAccount when a nil account is passed
//
// WARNING: NopAccount must NOT be used as a default in server-mode query
// pipelines. All server-mode operators must receive a real account from
// queryContext.newAccount().
func NopAccount() MemoryAccount {
	return nopAccount{}
}

// EnsureAccount returns acct if non-nil, otherwise returns a NopAccount.
// This allows operators to unconditionally call acct.Grow/Shrink/Close
// without nil interface checks.
func EnsureAccount(acct MemoryAccount) MemoryAccount {
	if acct == nil {
		return NopAccount()
	}

	return acct
}

// poolAllocationSize is the minimum chunk size reserved from the parent
// RootMonitor per grow call. This amortizes the cost of acquiring the
// parent's mutex — instead of locking on every small Grow, we reserve
// 64KB at a time and draw from the local reserve.
const poolAllocationSize int64 = 64 * 1024

// BudgetMonitor tracks per-query memory with enforcement.
//
// Thread-safe: protected by an internal mutex. When query parallelism is
// enabled (ConcurrentUnionIterator), multiple branch goroutines may call
// grow/shrink concurrently through their BoundAccounts. The uncontended
// mutex adds ~20-25ns overhead — negligible at one call per 1024-row batch.
//
// Each query gets one BudgetMonitor. Operators create BoundAccounts
// for their buffers (hash tables, sort buffers, etc.). The monitor
// enforces a global limit and tracks the high-water mark for
// PeakMemoryBytes reporting.
//
// When a parent RootMonitor is set, grow requests that exceed the local
// reserve are forwarded to the parent pool with amortized chunking.
// Reserved bytes are returned to the parent only in Close(), avoiding
// lock contention on every Shrink.
type BudgetMonitor struct {
	mu           sync.Mutex
	label        string
	curAllocated int64
	maxAllocated int64 // high-water mark -> becomes PeakMemoryBytes
	limit        int64 // 0 = unlimited (tracking only)

	parent   *RootMonitor // nil = standalone (no global pool)
	reserved int64        // bytes reserved from parent but not yet consumed locally
}

// NewBudgetMonitor creates a per-query memory monitor.
// Limit=0 means unlimited (tracking only, no enforcement).
func NewBudgetMonitor(label string, limit int64) *BudgetMonitor {
	return &BudgetMonitor{
		label: label,
		limit: limit,
	}
}

// NewBudgetMonitorWithParent creates a per-query memory monitor backed by a
// global RootMonitor pool. The per-query limit is enforced locally; the parent
// pool provides server-wide coordination across concurrent queries.
//
// If parent is nil, behaves identically to NewBudgetMonitor.
func NewBudgetMonitorWithParent(label string, limit int64, parent *RootMonitor) *BudgetMonitor {
	return &BudgetMonitor{
		label:  label,
		limit:  limit,
		parent: parent,
	}
}

// NewAccount creates an operator-level allocation tracker bound to this monitor.
// Returns nil if the monitor itself is nil (allows nil-safe chaining).
func (m *BudgetMonitor) NewAccount(label string) *BoundAccount {
	if m == nil {
		return nil
	}

	return &BoundAccount{
		label: label,
		mon:   m,
	}
}

// MaxAllocated returns the high-water mark of tracked allocations.
// This becomes PeakMemoryBytes in QueryStats.
func (m *BudgetMonitor) MaxAllocated() int64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.maxAllocated
}

// CurAllocated returns the current tracked allocation total.
func (m *BudgetMonitor) CurAllocated() int64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.curAllocated
}

// Limit returns the per-query byte limit. 0 means unlimited.
// Nil-safe: returns 0 if monitor is nil.
func (m *BudgetMonitor) Limit() int64 {
	if m == nil {
		return 0
	}

	return m.limit // immutable after construction, no lock needed
}

// Parent returns the backing RootMonitor, or nil if standalone.
// Nil-safe: returns nil if monitor is nil.
func (m *BudgetMonitor) Parent() *RootMonitor {
	if m == nil {
		return nil
	}

	return m.parent // immutable after construction, no lock needed
}

// ObserveGrow records n bytes of external allocation for peak-tracking.
// Unlike grow(), does NOT enforce the per-query limit or reserve from the
// parent pool. Used by buffer.PoolAccount to mirror allocations so that
// MaxAllocated() (→ PeakMemoryBytes) is reported accurately.
// Thread-safe. Nil-safe: no-op if monitor is nil.
func (m *BudgetMonitor) ObserveGrow(n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.mu.Lock()
	m.curAllocated += n
	if m.curAllocated > m.maxAllocated {
		m.maxAllocated = m.curAllocated
	}
	m.mu.Unlock()
}

// ObserveShrink reverses a previous ObserveGrow call. Panics if n exceeds
// curAllocated — this indicates unbalanced Grow/Shrink in the caller.
// Thread-safe. Nil-safe: no-op if monitor is nil.
func (m *BudgetMonitor) ObserveShrink(n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.mu.Lock()
	if n > m.curAllocated {
		m.mu.Unlock()
		panic(fmt.Sprintf("BudgetMonitor.ObserveShrink(%d) exceeds curAllocated(%d)", n, m.curAllocated))
	}
	m.curAllocated -= n
	m.mu.Unlock()
}

// Close returns all reserved bytes to the parent RootMonitor.
// Must be called when the query completes to avoid leaking pool capacity.
// Nil-safe: no-op if monitor is nil or has no parent.
func (m *BudgetMonitor) Close() {
	if m == nil || m.parent == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	// Return everything to the parent: both unconsumed reserve and
	// bytes currently tracked by accounts (which haven't been Shrunk yet).
	total := m.reserved + m.curAllocated
	if total > 0 {
		m.parent.Release(total)
	}

	m.reserved = 0
	m.curAllocated = 0
}

// grow adds n bytes to the monitor's tracked total and updates high-water mark.
// Returns *BudgetExceededError if per-query limit would be exceeded, or
// *PoolExhaustedError if the parent pool is full.
// Thread-safe: acquires mu internally.
func (m *BudgetMonitor) grow(acct *BoundAccount, n int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Per-query limit check.
	if m.limit > 0 && m.curAllocated+n > m.limit {
		return &BudgetExceededError{
			Monitor:   m.label,
			Account:   acct.label,
			Requested: n,
			Current:   m.curAllocated,
			Limit:     m.limit,
		}
	}

	// Acquire from parent pool if local reserve is insufficient.
	if m.parent != nil && n > m.reserved {
		// Request at least poolAllocationSize to amortize mutex acquisition.
		chunk := n - m.reserved
		if chunk < poolAllocationSize {
			chunk = poolAllocationSize
		}
		if err := m.parent.Reserve(chunk); err != nil {
			return err
		}
		m.reserved += chunk
	}

	// Consume from local reserve.
	if m.parent != nil {
		m.reserved -= n
	}

	// Update local tracking.
	m.curAllocated += n
	if m.curAllocated > m.maxAllocated {
		m.maxAllocated = m.curAllocated
	}

	return nil
}

// shrink returns n bytes from curAllocated back to the local reserve.
// The bytes remain reserved from the parent pool (avoiding lock contention)
// and are returned in bulk via Close(). This maintains the invariant:
//
//	bytes_reserved_from_parent == m.reserved + m.curAllocated
//
// Thread-safe: acquires mu internally.
func (m *BudgetMonitor) shrink(n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	actual := n
	if actual > m.curAllocated {
		actual = m.curAllocated
	}
	m.curAllocated -= actual
	if m.parent != nil {
		m.reserved += actual
	}
}

// BoundAccount is an operator-level allocation tracker.
// NOT thread-safe. One pipeline = one goroutine = no races.
//
// All methods are nil-safe: calling Grow/Shrink/Close on a nil
// *BoundAccount is a no-op. This allows operators to unconditionally
// call acct.Grow() without checking whether budget tracking is enabled.
type BoundAccount struct {
	label   string
	used    int64
	maxUsed int64 // high-water mark of used bytes
	mon     *BudgetMonitor
}

// Grow requests n bytes from the monitor. Returns a *BudgetExceededError
// if the monitor's limit would be exceeded, or *PoolExhaustedError if
// the parent pool is full.
// Nil-safe: if account is nil, always succeeds (no tracking).
func (a *BoundAccount) Grow(n int64) error {
	if a == nil || n <= 0 {
		return nil
	}
	if err := a.mon.grow(a, n); err != nil {
		return err
	}
	a.used += n
	if a.used > a.maxUsed {
		a.maxUsed = a.used
	}

	return nil
}

// MaxUsed returns the high-water mark of bytes tracked by this account.
// Nil-safe: returns 0 if account is nil.
func (a *BoundAccount) MaxUsed() int64 {
	if a == nil {
		return 0
	}

	return a.maxUsed
}

// Shrink returns n bytes (e.g., after spill-to-disk or buffer release).
// Nil-safe.
func (a *BoundAccount) Shrink(n int64) {
	if a == nil || n <= 0 {
		return
	}
	if n > a.used {
		n = a.used
	}
	a.used -= n
	a.mon.shrink(n)
}

// Resize atomically adjusts tracking from oldSize to newSize.
// If newSize > oldSize, grows by the delta (may return an error).
// If newSize < oldSize, shrinks by the delta.
// If equal, no-op. On error, tracking is unchanged.
// Nil-safe.
func (a *BoundAccount) Resize(oldSize, newSize int64) error {
	if a == nil {
		return nil
	}

	if newSize > oldSize {
		return a.Grow(newSize - oldSize)
	}
	if newSize < oldSize {
		a.Shrink(oldSize - newSize)
	}

	return nil
}

// Close releases all remaining bytes held by this account.
// Must be called in defer for every account.
// Nil-safe.
func (a *BoundAccount) Close() {
	if a == nil {
		return
	}
	if a.used > 0 {
		a.mon.shrink(a.used)
		a.used = 0
	}
}

// Used returns current bytes tracked by this account.
// Nil-safe: returns 0 if account is nil.
func (a *BoundAccount) Used() int64 {
	if a == nil {
		return 0
	}

	return a.used
}

// BudgetExceededError carries context for diagnostics when an operator
// exceeds the per-query memory budget.
type BudgetExceededError struct {
	Monitor   string
	Account   string
	Requested int64
	Current   int64
	Limit     int64
}

func (e *BudgetExceededError) Error() string {
	return fmt.Sprintf(
		"memory budget exceeded: %s/%s requested %d bytes (current: %d, limit: %d); "+
			"consider increasing --memory or adding filters to reduce data volume",
		e.Monitor, e.Account, e.Requested, e.Current, e.Limit,
	)
}

// IsBudgetExceeded reports whether the error is a *BudgetExceededError.
func IsBudgetExceeded(err error) bool {
	var target *BudgetExceededError

	return errors.As(err, &target)
}

// IsMemoryExhausted reports whether the error indicates any form of memory
// exhaustion: per-query budget exceeded (*BudgetExceededError), global pool
// exhausted (*PoolExhaustedError), or buffer pool pages exhausted (wrapped as
// *BudgetExceededError by PoolAccount.Grow). Operators should use this function
// to decide whether to spill to disk — it catches all memory pressure signals
// regardless of the accounting backend (BoundAccount vs PoolAccount).
func IsMemoryExhausted(err error) bool {
	return IsBudgetExceeded(err) || IsPoolExhausted(err)
}
