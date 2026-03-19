package memgov

import "sync"

// PressureCallback is invoked when the governor needs to reclaim memory.
// The callback should attempt to free up to target bytes and return the
// number of bytes actually freed. Implementations must not call back into
// the governor (no Reserve/Release) to avoid deadlock.
type PressureCallback func(target int64) (freed int64)

// pressureRegistry manages callbacks registered via OnPressure.
// Callbacks are invoked in revocation priority order:
//  1. ClassRevocable (cheapest to drop)
//  2. ClassPageCache (clean re-readable pages)
//  3. ClassSpillable (triggers operator spill)
//  4. ClassMetadata (re-readable from disk)
//  5. ClassTempIO (last resort transient I/O)
//
// ClassNonRevocable is never revoked (pinned/active).
type pressureRegistry struct {
	mu        sync.Mutex
	callbacks map[MemoryClass][]PressureCallback
}

// revocationOrder defines the priority order for reclaiming memory.
// Lower index = reclaimed first (cheapest).
var revocationOrder = []MemoryClass{
	ClassRevocable,
	ClassPageCache,
	ClassSpillable,
	ClassMetadata,
	ClassTempIO,
}

func newPressureRegistry() *pressureRegistry {
	return &pressureRegistry{
		callbacks: make(map[MemoryClass][]PressureCallback),
	}
}

// register adds a pressure callback for the given class.
func (pr *pressureRegistry) register(class MemoryClass, cb PressureCallback) {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	pr.callbacks[class] = append(pr.callbacks[class], cb)
}

// invoke attempts to reclaim target bytes by calling callbacks in
// revocation priority order. Returns total bytes freed.
// Must NOT be called with the governor's main lock held.
func (pr *pressureRegistry) invoke(target int64) int64 {
	pr.mu.Lock()
	// Snapshot callbacks under lock, then release before invoking.
	type entry struct {
		class MemoryClass
		cbs   []PressureCallback
	}
	var snapshot []entry
	for _, class := range revocationOrder {
		if cbs, ok := pr.callbacks[class]; ok && len(cbs) > 0 {
			cp := make([]PressureCallback, len(cbs))
			copy(cp, cbs)
			snapshot = append(snapshot, entry{class: class, cbs: cp})
		}
	}
	pr.mu.Unlock()

	var totalFreed int64
	remaining := target

	for _, e := range snapshot {
		if remaining <= 0 {
			break
		}
		for _, cb := range e.cbs {
			if remaining <= 0 {
				break
			}
			freed := cb(remaining)
			if freed > 0 {
				totalFreed += freed
				remaining -= freed
			}
		}
	}

	return totalFreed
}
