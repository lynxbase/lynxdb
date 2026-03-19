package memgov

import "errors"

// ErrQueryBudgetExceeded is returned when a query's aggregate allocation
// would exceed its per-query memory limit (MaxQueryMemory).
var ErrQueryBudgetExceeded = errors.New("query memory budget exceeded")

// AccountAdapter wraps an OperatorMemory to implement the MemoryAccount
// interface. This is the migration bridge that enables incremental adoption
// of the new memory governance system: operators can continue to use their
// existing MemoryAccount-based code while backed by the new Governor.
//
// Usage:
//
//	opMem := memgov.NewOperatorMemory(gov, "sort")
//	acct := memgov.NewAccountAdapter(opMem)
//	sortOperator.SetAccount(acct) // accepts MemoryAccount
//
// NOT thread-safe (same contract as AccountAdapter — one goroutine per operator).
type AccountAdapter struct {
	opMem   OperatorMemory
	tracker *BudgetAdapter // optional, for per-query aggregate tracking
	used    int64
	maxUsed int64
}

// NewAccountAdapter wraps an OperatorMemory to satisfy the MemoryAccount
// interface. Grow calls are forwarded to OperatorMemory.Reserve (non-revocable).
// For revocable growth, use OperatorMemory.TryGrow directly.
func NewAccountAdapter(opMem OperatorMemory) *AccountAdapter {
	return &AccountAdapter{opMem: opMem}
}

// newTrackedAccountAdapter creates an AccountAdapter that reports Grow/Shrink
// to a BudgetAdapter for per-query aggregate tracking.
func newTrackedAccountAdapter(opMem OperatorMemory, tracker *BudgetAdapter) *AccountAdapter {
	return &AccountAdapter{opMem: opMem, tracker: tracker}
}

// Grow requests n bytes via the underlying OperatorMemory.
func (a *AccountAdapter) Grow(n int64) error {
	if n <= 0 {
		return nil
	}
	// Per-query limit enforcement: reject growth if it would exceed the
	// query-level budget. This prevents a single query from consuming all
	// process memory when MaxQueryMemory is configured.
	if a.tracker != nil && a.tracker.Limit() > 0 {
		if a.tracker.CurAllocated()+n > a.tracker.Limit() {
			return ErrQueryBudgetExceeded
		}
	}
	if err := a.opMem.Reserve(n); err != nil {
		return err
	}
	a.used += n
	if a.used > a.maxUsed {
		a.maxUsed = a.used
	}
	if a.tracker != nil {
		a.tracker.trackGrow(n)
	}
	return nil
}

// Shrink releases n bytes back to the governor immediately.
func (a *AccountAdapter) Shrink(n int64) {
	if n <= 0 {
		return
	}
	if n > a.used {
		n = a.used
	}
	a.used -= n
	a.opMem.Release(n)
	if a.tracker != nil {
		a.tracker.trackShrink(n)
	}
}

// Close releases all remaining bytes via OperatorMemory.Close.
func (a *AccountAdapter) Close() {
	remaining := a.used
	a.opMem.Close()
	a.used = 0
	if a.tracker != nil && remaining > 0 {
		a.tracker.trackShrink(remaining)
	}
}

// Used returns the current tracked byte count.
func (a *AccountAdapter) Used() int64 {
	return a.used
}

// MaxUsed returns the peak tracked byte count.
func (a *AccountAdapter) MaxUsed() int64 {
	return a.maxUsed
}

// OperatorMemory returns the underlying OperatorMemory for direct access
// to revocable growth and pressure callbacks.
func (a *AccountAdapter) OperatorMemory() OperatorMemory {
	return a.opMem
}
