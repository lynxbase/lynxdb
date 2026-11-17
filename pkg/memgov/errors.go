package memgov

import (
	"errors"
	"fmt"
)

// ErrMemoryPressure is returned when a reservation cannot be satisfied.
var ErrMemoryPressure = errors.New("memory pressure: unable to reserve requested bytes")

// ErrQueryBudgetExceeded is returned when a query's aggregate allocation
// would exceed its per-query memory limit.
var ErrQueryBudgetExceeded = errors.New("query memory budget exceeded")

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

// IsBudgetExceeded reports whether the error indicates a per-query memory
// budget has been exceeded. Matches both the structured *BudgetExceededError
// and the sentinel ErrQueryBudgetExceeded.
func IsBudgetExceeded(err error) bool {
	var target *BudgetExceededError

	return errors.As(err, &target) || errors.Is(err, ErrQueryBudgetExceeded)
}

// PoolExhaustedError is returned when a query cannot reserve memory from the
// global pool.
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

// IsPoolExhausted reports whether the error indicates the global memory pool
// has been exhausted. Matches both the structured *PoolExhaustedError and
// the sentinel ErrMemoryPressure.
func IsPoolExhausted(err error) bool {
	var target *PoolExhaustedError

	return errors.As(err, &target) || errors.Is(err, ErrMemoryPressure)
}

// IsMemoryExhausted reports whether the error indicates any form of memory
// exhaustion: per-query budget exceeded, global pool exhausted, or memory
// pressure.
func IsMemoryExhausted(err error) bool {
	return IsBudgetExceeded(err) || IsPoolExhausted(err) || errors.Is(err, ErrMemoryPressure) || errors.Is(err, ErrQueryBudgetExceeded)
}
