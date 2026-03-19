package consumers

import (
	"fmt"
	"sync"

	"github.com/lynxbase/lynxdb/pkg/bufmgr"
)

// QueryOperatorAllocator provides frame-based memory for query operators.
// Each operator gets its own allocator that draws frames from the shared Manager.
//
// Thread-safe.
type QueryOperatorAllocator struct {
	mu     sync.Mutex
	mgr    bufmgr.Manager
	jobID  string
	frames []*bufmgr.Frame
}

// NewQueryOperatorAllocator creates an allocator for a query operator.
func NewQueryOperatorAllocator(mgr bufmgr.Manager, jobID string) *QueryOperatorAllocator {
	return &QueryOperatorAllocator{
		mgr:   mgr,
		jobID: jobID,
	}
}

// AllocFrame allocates a frame for operator use. The returned frame is pinned.
func (oa *QueryOperatorAllocator) AllocFrame() (*bufmgr.Frame, error) {
	// Prefer to evict seg-cache frames before evicting other query frames.
	f, err := oa.mgr.AllocFrame(bufmgr.OwnerQuery, oa.jobID)
	if err != nil {
		return nil, fmt.Errorf("bufmgr.QueryOperatorAllocator.AllocFrame: %w", err)
	}

	oa.mu.Lock()
	oa.frames = append(oa.frames, f)
	oa.mu.Unlock()

	return f, nil
}

// FrameCount returns the number of frames currently held.
func (oa *QueryOperatorAllocator) FrameCount() int {
	oa.mu.Lock()
	defer oa.mu.Unlock()

	return len(oa.frames)
}

// ReleaseAll frees all frames back to the manager.
func (oa *QueryOperatorAllocator) ReleaseAll() {
	oa.mu.Lock()
	frames := oa.frames
	oa.frames = nil
	oa.mu.Unlock()

	for _, f := range frames {
		for f.PinCount.Load() > 0 {
			f.Unpin()
		}
	}
}

// ReleaseLast frees the last n frames in LIFO order.
func (oa *QueryOperatorAllocator) ReleaseLast(n int) {
	oa.mu.Lock()
	if n >= len(oa.frames) {
		n = len(oa.frames)
	}
	toFree := make([]*bufmgr.Frame, n)
	copy(toFree, oa.frames[len(oa.frames)-n:])
	oa.frames = oa.frames[:len(oa.frames)-n]
	oa.mu.Unlock()

	for _, f := range toFree {
		for f.PinCount.Load() > 0 {
			f.Unpin()
		}
	}
}

// Manager returns the underlying buffer manager.
func (oa *QueryOperatorAllocator) Manager() bufmgr.Manager {
	return oa.mgr
}
