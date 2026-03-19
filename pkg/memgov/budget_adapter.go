package memgov

import "sync/atomic"

// BudgetAdapter wraps a QueryBudget to create AccountAdapters for operators.
// This bridges the gap between the new per-query budget system and code that
// expects to create operator accounts via BudgetAdapter.NewAccount().
//
// Thread-safe: NewAccount may be called from any goroutine. The aggregate
// allocation counters use atomics for lock-free reads from stats reporters.
//
// Usage:
//
//	budget := memgov.NewQueryBudget(gov, "query-123")
//	adapter := memgov.NewBudgetAdapterWithLimit(budget, gov, maxQueryMemory)
//	acct := adapter.NewAccount("sort")  // returns MemoryAccount
type BudgetAdapter struct {
	budget QueryBudget
	gov    Governor
	limit  int64 // per-query memory limit (0 = no per-query limit)

	// Per-query aggregate tracking (atomic for lock-free stats reads).
	allocated atomic.Int64 // current aggregate allocation across all accounts
	peak      atomic.Int64 // high-water mark of aggregate allocation
}

// NewBudgetAdapter creates a bridge that produces AccountAdapters from a QueryBudget.
func NewBudgetAdapter(budget QueryBudget, gov Governor) *BudgetAdapter {
	return &BudgetAdapter{budget: budget, gov: gov}
}

// NewBudgetAdapterWithLimit creates a BudgetAdapter with a per-query memory limit.
// The limit is used for coordinator creation and operator budget splitting.
func NewBudgetAdapterWithLimit(budget QueryBudget, gov Governor, limit int64) *BudgetAdapter {
	return &BudgetAdapter{budget: budget, gov: gov, limit: limit}
}

// NewAccount creates a new AccountAdapter for an operator. The returned
// adapter implements MemoryAccount and is backed by the governor.
// Allocations are tracked at the per-query level for PeakMemoryBytes reporting.
func (ba *BudgetAdapter) NewAccount(label string) *AccountAdapter {
	opMem := NewOperatorMemory(ba.gov, label)
	return newTrackedAccountAdapter(opMem, ba)
}

// Limit returns the per-query memory limit (0 = no per-query limit).
func (ba *BudgetAdapter) Limit() int64 {
	return ba.limit
}

// MaxAllocated returns the peak aggregate allocation across all accounts
// created by this adapter.
func (ba *BudgetAdapter) MaxAllocated() int64 {
	return ba.peak.Load()
}

// CurAllocated returns the current aggregate allocation across all accounts.
func (ba *BudgetAdapter) CurAllocated() int64 {
	return ba.allocated.Load()
}

// trackGrow records a successful allocation from an operator account.
func (ba *BudgetAdapter) trackGrow(n int64) {
	cur := ba.allocated.Add(n)
	// Update peak via CAS loop (lock-free).
	for {
		old := ba.peak.Load()
		if cur <= old {
			break
		}
		if ba.peak.CompareAndSwap(old, cur) {
			break
		}
	}
}

// trackShrink records a release from an operator account.
func (ba *BudgetAdapter) trackShrink(n int64) {
	ba.allocated.Add(-n)
}

// Governor returns the underlying governor for sub-budget creation (e.g., CTEs).
func (ba *BudgetAdapter) Governor() Governor {
	return ba.gov
}

// Close releases all outstanding leases in the underlying QueryBudget.
func (ba *BudgetAdapter) Close() {
	ba.budget.Close()
}
