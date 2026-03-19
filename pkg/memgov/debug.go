//go:build debug

package memgov

import (
	"log/slog"
	"runtime"
	"sync"
)

// leakDetector tracks lease creation sites for debug builds.
// When a lease is garbage collected without being released, it logs a warning
// with the creation stack trace.
var leakDetector = &detector{
	active: make(map[*Lease]string),
}

type detector struct {
	mu     sync.Mutex
	active map[*Lease]string
}

// trackLease registers a lease with the leak detector.
// Called automatically in debug builds when a lease is created.
func trackLease(l *Lease) {
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	stack := string(buf[:n])

	leakDetector.mu.Lock()
	leakDetector.active[l] = stack
	leakDetector.mu.Unlock()

	runtime.SetFinalizer(l, func(l *Lease) {
		if !l.closed {
			leakDetector.mu.Lock()
			stack := leakDetector.active[l]
			delete(leakDetector.active, l)
			leakDetector.mu.Unlock()

			slog.Warn("LEAK: Lease was garbage collected without Release()",
				"bytes", l.bytes,
				"class", l.class.String(),
				"creation_stack", stack,
			)
		} else {
			leakDetector.mu.Lock()
			delete(leakDetector.active, l)
			leakDetector.mu.Unlock()
		}
	})
}

// ActiveLeaseCount returns the number of un-released leases being tracked.
// Useful for assertions in tests.
func ActiveLeaseCount() int {
	leakDetector.mu.Lock()
	defer leakDetector.mu.Unlock()

	return len(leakDetector.active)
}
