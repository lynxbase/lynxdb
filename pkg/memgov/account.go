package memgov

// MemoryAccount is the interface for operator-level memory tracking.
// Operators call Grow to request memory, Shrink to release it, and Close
// to release all remaining bytes on completion.
//
// Implementations:
//   - *AccountAdapter: governor-backed (new system)
//   - *CoordinatedAccount: coordinator-wrapped AccountAdapter (budget redistribution)
//   - nopAccount: no-op for tests and CLI pipe mode
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
type nopAccount struct{}

func (nopAccount) Grow(int64) error { return nil }
func (nopAccount) Shrink(int64)     {}
func (nopAccount) Close()           {}
func (nopAccount) Used() int64      { return 0 }
func (nopAccount) MaxUsed() int64   { return 0 }

// NopAccount returns a no-op MemoryAccount.
func NopAccount() MemoryAccount {
	return nopAccount{}
}

// EnsureAccount returns acct if non-nil, otherwise returns a NopAccount.
func EnsureAccount(acct MemoryAccount) MemoryAccount {
	if acct == nil {
		return NopAccount()
	}
	return acct
}
