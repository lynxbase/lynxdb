package memgov

// NopGovernor is a no-op Governor for tests and CLI pipe-mode.
// All reservations succeed, all stats return zero. Safe for concurrent use.
type NopGovernor struct{}

var _ Governor = (*NopGovernor)(nil)

func (NopGovernor) Reserve(MemoryClass, int64) error            { return nil }
func (NopGovernor) TryReserve(MemoryClass, int64) bool          { return true }
func (NopGovernor) Release(MemoryClass, int64)                  {}
func (NopGovernor) ClassUsage(MemoryClass) ClassStats            { return ClassStats{} }
func (NopGovernor) TotalUsage() TotalStats                       { return TotalStats{} }
func (NopGovernor) OnPressure(MemoryClass, PressureCallback)     {}

// NopLease returns an already-released lease. Nil-safe for defer patterns.
func NopLease() *Lease {
	return &Lease{closed: true}
}

// NopQueryBudget returns a no-op QueryBudget backed by NopGovernor.
func NopQueryBudget() QueryBudget {
	return NewQueryBudget(&NopGovernor{}, "nop")
}

// NopOperatorMemory returns a no-op OperatorMemory backed by NopGovernor.
func NopOperatorMemory() OperatorMemory {
	return NewOperatorMemory(&NopGovernor{}, "nop")
}
