package memgov

import (
	"log/slog"
	"sync"
)

// OperatorMemory is the per-operator interface for memory management.
// It provides the per-operator memory management contract.
//
// NOT thread-safe by default (same as AccountAdapter — one goroutine per
// Volcano pipeline). Thread-safe wrappers are available for concurrent use.
type OperatorMemory interface {
	// Reserve guarantees minimum bytes for this operator.
	Reserve(n int64) error

	// TryGrow requests additional bytes beyond reservation.
	TryGrow(n int64) (*Lease, error)

	// Revocable returns bytes that can be reclaimed via OnRevoke callback.
	Revocable() int64

	// Pinned returns bytes that cannot be reclaimed.
	Pinned() int64

	// SetOnRevoke registers callback invoked by governor under pressure.
	// Operator must free targetBytes via spill/compaction/shrink.
	SetOnRevoke(fn func(targetBytes int64) (freed int64))

	// Release returns n bytes back to the governor immediately (non-revocable class).
	// Used by AccountAdapter.Shrink so that memory freed after spill is visible
	// to the governor without waiting for Close.
	Release(n int64)

	// Close releases all memory (reserve + leases). Logs leaks in debug mode.
	Close()

	// Used returns the current tracked byte count.
	Used() int64

	// MaxUsed returns the peak tracked byte count.
	MaxUsed() int64
}

// operatorMemory is the concrete OperatorMemory implementation.
type operatorMemory struct {
	mu         sync.Mutex
	gov        Governor
	label      string
	reserved   int64 // guaranteed minimum bytes
	leases     []*Lease
	pinned     int64
	revocable  int64
	used       int64
	maxUsed    int64
	onRevoke   func(int64) int64
	closed     bool
}

// NewOperatorMemory creates a per-operator memory manager.
func NewOperatorMemory(gov Governor, label string) OperatorMemory {
	return &operatorMemory{
		gov:   gov,
		label: label,
	}
}

func (om *operatorMemory) Reserve(n int64) error {
	if n <= 0 {
		return nil
	}

	if err := om.gov.Reserve(ClassNonRevocable, n); err != nil {
		return err
	}

	om.mu.Lock()
	om.reserved += n
	om.pinned += n
	om.used += n
	if om.used > om.maxUsed {
		om.maxUsed = om.used
	}
	om.mu.Unlock()

	return nil
}

func (om *operatorMemory) TryGrow(n int64) (*Lease, error) {
	if n <= 0 {
		return &Lease{gov: om.gov, class: ClassSpillable, bytes: 0, closed: true}, nil
	}

	if err := om.gov.Reserve(ClassSpillable, n); err != nil {
		return nil, err
	}

	lease := &Lease{
		gov:   om.gov,
		class: ClassSpillable,
		bytes: n,
	}

	om.mu.Lock()
	om.leases = append(om.leases, lease)
	om.revocable += n
	om.used += n
	if om.used > om.maxUsed {
		om.maxUsed = om.used
	}
	om.mu.Unlock()

	return lease, nil
}

func (om *operatorMemory) Revocable() int64 {
	om.mu.Lock()
	defer om.mu.Unlock()

	return om.revocable
}

func (om *operatorMemory) Pinned() int64 {
	om.mu.Lock()
	defer om.mu.Unlock()

	return om.pinned
}

func (om *operatorMemory) SetOnRevoke(fn func(int64) int64) {
	om.mu.Lock()
	defer om.mu.Unlock()

	om.onRevoke = fn

	// Register with governor so it can push revocation requests.
	if fn != nil {
		om.gov.OnPressure(ClassSpillable, func(target int64) int64 {
			om.mu.Lock()
			revokeFn := om.onRevoke
			om.mu.Unlock()

			if revokeFn == nil {
				return 0
			}
			freed := revokeFn(target)
			if freed > 0 {
				om.mu.Lock()
				om.revocable -= freed
				if om.revocable < 0 {
					om.revocable = 0
				}
				om.used -= freed
				if om.used < 0 {
					om.used = 0
				}
				om.mu.Unlock()
			}

			return freed
		})
	}
}

func (om *operatorMemory) Release(n int64) {
	if n <= 0 {
		return
	}

	om.mu.Lock()
	defer om.mu.Unlock()

	// Clamp to what is actually reserved.
	if n > om.reserved {
		n = om.reserved
	}

	om.gov.Release(ClassNonRevocable, n)
	om.reserved -= n
	om.pinned -= n
	if om.pinned < 0 {
		om.pinned = 0
	}
	om.used -= n
	if om.used < 0 {
		om.used = 0
	}
}

func (om *operatorMemory) Close() {
	om.mu.Lock()
	defer om.mu.Unlock()

	if om.closed {
		return
	}
	om.closed = true

	// Release all leases.
	for _, lease := range om.leases {
		lease.Release()
	}
	om.leases = nil

	// Release reserved bytes.
	if om.reserved > 0 {
		om.gov.Release(ClassNonRevocable, om.reserved)
		om.reserved = 0
	}

	if om.used > 0 {
		slog.Debug("operatorMemory.Close: residual bytes",
			"label", om.label, "used", om.used)
	}

	om.pinned = 0
	om.revocable = 0
	om.used = 0
}

func (om *operatorMemory) Used() int64 {
	om.mu.Lock()
	defer om.mu.Unlock()

	return om.used
}

func (om *operatorMemory) MaxUsed() int64 {
	om.mu.Lock()
	defer om.mu.Unlock()

	return om.maxUsed
}
