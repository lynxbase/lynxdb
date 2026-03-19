package bufmgr

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// WritebackScheduler runs as a background goroutine to handle batched dirty
// frame writeback and proactive eviction. When dirty frames accumulate above
// a threshold, the scheduler flushes them in batches to avoid blocking the
// allocation hot path.
type WritebackScheduler interface {
	// Start begins the background writeback loop.
	Start(ctx context.Context)

	// RequestWriteback signals that dirty frames should be flushed.
	// Non-blocking; coalesces multiple requests.
	RequestWriteback()

	// RequestEviction signals that free frames are needed.
	// The scheduler will writeback + evict in batches.
	RequestEviction(needed int)

	// SetWritebackFunc sets the callback for flushing a dirty frame.
	SetWritebackFunc(fn func(f *Frame) error)
}

// writebackScheduler is the concrete implementation.
type writebackScheduler struct {
	mu          sync.Mutex
	mgr         *manager // back-reference to buffer manager
	writebackFn func(f *Frame) error

	// Signal channels (non-blocking coalesced signals).
	writebackCh  chan struct{}
	evictionCh   chan int // carries the number of frames needed
	logger       *slog.Logger

	// Stats.
	writtenCount atomic.Int64
	evictedCount atomic.Int64
}

func newWritebackScheduler(mgr *manager, logger *slog.Logger) *writebackScheduler {
	return &writebackScheduler{
		mgr:         mgr,
		writebackCh: make(chan struct{}, 1),
		evictionCh:  make(chan int, 1),
		logger:      logger,
	}
}

func (ws *writebackScheduler) Start(ctx context.Context) {
	go ws.loop(ctx)
}

func (ws *writebackScheduler) RequestWriteback() {
	select {
	case ws.writebackCh <- struct{}{}:
	default:
		// Already signaled — coalesced.
	}
}

func (ws *writebackScheduler) RequestEviction(needed int) {
	select {
	case ws.evictionCh <- needed:
	default:
		// Already pending — coalesced.
	}
}

func (ws *writebackScheduler) SetWritebackFunc(fn func(f *Frame) error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	ws.writebackFn = fn
}

func (ws *writebackScheduler) loop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond) // periodic check
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ws.writebackCh:
			ws.flushDirtyFrames()
		case needed := <-ws.evictionCh:
			ws.flushDirtyFrames()
			ws.evictFrames(needed)
		case <-ticker.C:
			ws.flushDirtyFrames()
		}
	}
}

// flushDirtyFrames writes back all dirty frames that are not pinned.
func (ws *writebackScheduler) flushDirtyFrames() {
	ws.mu.Lock()
	fn := ws.writebackFn
	ws.mu.Unlock()

	if fn == nil {
		return
	}

	ws.mgr.mu.Lock()
	var dirty []*Frame
	for _, f := range ws.mgr.frames {
		if f == nil {
			continue
		}
		state := FrameState(f.State.Load())
		if state == StateDirty && !f.IsPinned() {
			if err := f.TransitionTo(StateWriteback); err != nil {
				continue // skip frames that cannot transition
			}
			f.Pin() // pin during writeback
			dirty = append(dirty, f)
		}
	}
	ws.mgr.mu.Unlock()

	for _, f := range dirty {
		if err := fn(f); err != nil {
			ws.logger.Warn("bufmgr: writeback failed",
				"frame_id", f.ID, "owner", f.Owner.String(), "error", err)
			// Transition back to dirty on failure.
			_ = f.TransitionTo(StateDirty)
		} else {
			_ = f.TransitionTo(StateClean)
			ws.writtenCount.Add(1)
		}
		f.Unpin()
	}
}

// evictFrames reclaims n frames by evicting from the eviction queue.
func (ws *writebackScheduler) evictFrames(n int) {
	evicted := ws.mgr.EvictBatch(n, OwnerSegCache)
	ws.evictedCount.Add(int64(evicted))
}
