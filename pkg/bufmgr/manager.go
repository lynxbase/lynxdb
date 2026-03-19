package bufmgr

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/lynxbase/lynxdb/pkg/memgov"
)

// Manager manages a pool of frames with state tracking and eviction.
// Replaces pkg/buffer/Pool with explicit state machine, batched eviction,
// and governor-integrated memory accounting.
//
// One instance per Engine. Thread-safe.
type Manager interface {
	// Start begins the background writeback scheduler. Must be called after
	// NewManager to enable dirty frame writeback and eviction requests.
	Start(ctx context.Context)

	// AllocFrame allocates a frame for the given owner.
	// May trigger eviction of lower-priority frames.
	AllocFrame(owner FrameOwner, tag string) (*Frame, error)

	// PinFrame increments pin count; pinned frames cannot be evicted.
	PinFrame(id FrameID) error

	// UnpinFrame decrements pin count.
	UnpinFrame(id FrameID)

	// MarkDirty transitions frame Clean → Dirty.
	MarkDirty(id FrameID)

	// LookupFrame returns the frame for a given ID, or nil if evicted.
	LookupFrame(id FrameID) *Frame

	// EvictBatch evicts up to n frames, preferring the given owner.
	// Returns the number of frames actually evicted.
	EvictBatch(n int, preferOwner FrameOwner) int

	// Stats returns current buffer manager statistics.
	Stats() ManagerStats

	// Close releases all frames and backing memory.
	Close() error

	// FrameSize returns the default frame size in bytes.
	FrameSize() int

	// MaxFrames returns the total number of frames in the pool.
	MaxFrames() int

	// PinFrameIfOwned pins a frame if it's still owned by the expected tag.
	PinFrameIfOwned(id FrameID, expectedTag string) (*Frame, bool)
}

// ManagerStats exposes buffer pool state for monitoring.
type ManagerStats struct {
	TotalFrames    int   `json:"total_frames"`
	FreeFrames     int   `json:"free_frames"`
	CleanFrames    int   `json:"clean_frames"`
	DirtyFrames    int   `json:"dirty_frames"`
	PinnedFrames   int   `json:"pinned_frames"`
	EvictionCount  int64 `json:"eviction_count"`
	WritebackCount int64 `json:"writeback_count"`
	HitCount       int64 `json:"hit_count"`
	MissCount      int64 `json:"miss_count"`
	SegCacheFrames int   `json:"seg_cache_frames"`
	QueryFrames    int   `json:"query_frames"`
	MemtableFrames int   `json:"memtable_frames"`
}

// ManagerConfig configures the buffer manager.
type ManagerConfig struct {
	// MaxFrames is the total number of frames. Default: 1024.
	MaxFrames int

	// MaxMemoryBytes overrides MaxFrames when > 0.
	MaxMemoryBytes int64

	// FrameSize is the frame size in bytes. Default: 65536 (64KB).
	FrameSize int

	// EnableOffHeap uses mmap for frame data. Default: true.
	EnableOffHeap bool

	// WriteBackFunc is called before evicting a dirty frame.
	WriteBackFunc func(f *Frame) error

	// Governor for memory accounting. If nil, no governor integration.
	Governor memgov.Governor

	// Logger for buffer manager operations. Nil = default slog.
	Logger *slog.Logger
}

// FrameWriteBack is implemented by frame meta to handle per-frame eviction.
type FrameWriteBack interface {
	WriteBackFrame(f *Frame) error
}

// ErrAllFramesPinned is returned when AllocFrame cannot find any evictable frame.
var ErrAllFramesPinned = fmt.Errorf("bufmgr: all frames are pinned; cannot evict")

// manager is the concrete Manager implementation.
type manager struct {
	mu sync.Mutex

	frames   []*Frame
	freeList []*Frame
	evictor  *evictionQueue
	residency ResidencyIndex

	maxFrames int
	frameSize int
	offHeap   bool
	closed    bool
	writeback func(f *Frame) error
	gov       memgov.Governor
	logger    *slog.Logger
	scheduler *writebackScheduler

	// Heap backing for non-mmap mode.
	heapBacking [][]byte

	// Stats (atomic for lock-free reads).
	hits       atomic.Int64
	misses     atomic.Int64
	evictions  atomic.Int64
	writebacks atomic.Int64
	allocs     atomic.Int64
	frees      atomic.Int64
}

// NewManager creates a buffer manager with the given configuration.
func NewManager(cfg ManagerConfig) (Manager, error) {
	if cfg.FrameSize <= 0 {
		cfg.FrameSize = FrameSize64KB
	}

	maxFrames := cfg.MaxFrames
	if cfg.MaxMemoryBytes > 0 {
		maxFrames = int(cfg.MaxMemoryBytes / int64(cfg.FrameSize))
	}
	if maxFrames <= 0 {
		maxFrames = 1024
	}

	offHeap := cfg.EnableOffHeap && mmapAvailable()

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	m := &manager{
		frames:    make([]*Frame, 0, maxFrames),
		freeList:  make([]*Frame, 0, maxFrames),
		evictor:   newEvictionQueue(maxFrames),
		residency: NewResidencyIndex(maxFrames * 2), // 2x capacity for low load factor
		maxFrames: maxFrames,
		frameSize: cfg.FrameSize,
		offHeap:   offHeap,
		writeback: cfg.WriteBackFunc,
		gov:       cfg.Governor,
		logger:    logger,
	}

	if !offHeap {
		m.heapBacking = make([][]byte, maxFrames)
	}

	m.scheduler = newWritebackScheduler(m, logger)
	if cfg.WriteBackFunc != nil {
		m.scheduler.SetWritebackFunc(cfg.WriteBackFunc)
	}

	// Pre-allocate all frames.
	for i := 0; i < maxFrames; i++ {
		f, err := m.newFrame(FrameID(i+1), i)
		if err != nil {
			_ = m.closeFrames()

			return nil, fmt.Errorf("bufmgr.NewManager: pre-allocate frame %d/%d: %w", i, maxFrames, err)
		}
		m.frames = append(m.frames, f)
		m.freeList = append(m.freeList, f)
	}

	return m, nil
}

func (m *manager) Start(ctx context.Context) {
	m.scheduler.Start(ctx)
}

func (m *manager) AllocFrame(owner FrameOwner, tag string) (*Frame, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, fmt.Errorf("bufmgr.Manager.AllocFrame: manager is closed")
	}

	// Fast path: grab from free list.
	if len(m.freeList) > 0 {
		f := m.freeList[len(m.freeList)-1]
		m.freeList = m.freeList[:len(m.freeList)-1]
		m.initFrame(f, owner, tag)
		m.evictor.add(f)
		m.residency.Insert(owner, tag, f.ID)
		m.allocs.Add(1)
		m.hits.Add(1)

		return f, nil
	}

	// Slow path: evict a frame.
	m.misses.Add(1)
	f := m.evictOneLocked()
	if f == nil {
		return nil, ErrAllFramesPinned
	}

	m.initFrame(f, owner, tag)
	m.evictor.add(f)
	m.residency.Insert(owner, tag, f.ID)
	m.allocs.Add(1)

	return f, nil
}

func (m *manager) PinFrame(id FrameID) error {
	m.mu.Lock()
	f := m.findFrame(id)
	m.mu.Unlock()

	if f == nil {
		return fmt.Errorf("bufmgr.Manager.PinFrame: frame %d not found", id)
	}

	f.Pin()

	return nil
}

func (m *manager) UnpinFrame(id FrameID) {
	m.mu.Lock()
	f := m.findFrame(id)
	m.mu.Unlock()

	if f != nil {
		f.Unpin()
	}
}

func (m *manager) MarkDirty(id FrameID) {
	m.mu.Lock()
	f := m.findFrame(id)
	m.mu.Unlock()

	if f != nil {
		state := FrameState(f.State.Load())
		if state == StateClean {
			f.State.Store(int32(StateDirty))
		}
	}
}

func (m *manager) LookupFrame(id FrameID) *Frame {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.findFrame(id)
}

func (m *manager) EvictBatch(n int, preferOwner FrameOwner) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	frames := m.evictor.evictBatch(n, preferOwner)
	for _, f := range frames {
		m.handleEviction(f)
		f.reset()
		m.freeList = append(m.freeList, f)
	}

	return len(frames)
}

func (m *manager) Stats() ManagerStats {
	m.mu.Lock()
	freeCount := len(m.freeList)
	var clean, dirty, pinned int
	var segCache, query, memtable int
	for _, f := range m.frames {
		if f == nil {
			continue
		}
		state := FrameState(f.State.Load())
		switch {
		case f.IsPinned():
			pinned++
		case state == StateClean:
			clean++
		case state == StateDirty:
			dirty++
		}
		switch f.Owner {
		case OwnerSegCache:
			segCache++
		case OwnerQuery:
			query++
		case OwnerMemtable:
			memtable++
		}
	}
	m.mu.Unlock()

	return ManagerStats{
		TotalFrames:    m.maxFrames,
		FreeFrames:     freeCount,
		CleanFrames:    clean,
		DirtyFrames:    dirty,
		PinnedFrames:   pinned,
		EvictionCount:  m.evictions.Load(),
		WritebackCount: m.writebacks.Load(),
		HitCount:       m.hits.Load(),
		MissCount:      m.misses.Load(),
		SegCacheFrames: segCache,
		QueryFrames:    query,
		MemtableFrames: memtable,
	}
}

func (m *manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true
	m.freeList = nil

	return m.closeFrames()
}

func (m *manager) FrameSize() int {
	return m.frameSize
}

func (m *manager) MaxFrames() int {
	return m.maxFrames
}

func (m *manager) PinFrameIfOwned(id FrameID, expectedTag string) (*Frame, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	f := m.findFrame(id)
	if f == nil || f.Tag != expectedTag {
		return nil, false
	}
	f.Pin()
	m.hits.Add(1)

	return f, true
}

// newFrame creates a Frame with allocated data memory.
func (m *manager) newFrame(id FrameID, slot int) (*Frame, error) {
	f := &Frame{
		ID:       id,
		size:     m.frameSize,
		PageSize: m.frameSize,
		slot:     slot,
	}

	if m.offHeap {
		ptr, err := allocateOffHeap(m.frameSize)
		if err != nil {
			return nil, err
		}
		f.data = ptr
	} else {
		buf := make([]byte, m.frameSize)
		f.data = unsafe.Pointer(&buf[0])
		m.heapBacking[slot] = buf
	}

	return f, nil
}

// initFrame prepares a frame for use by a consumer.
func (m *manager) initFrame(f *Frame, owner FrameOwner, tag string) {
	f.reset()
	f.Owner = owner
	f.Tag = tag
	f.State.Store(int32(StateLoading))
	f.Pin()
	// Consumer is expected to write data into the frame and then the frame
	// transitions to Clean. For immediate-use allocations (no I/O), we
	// transition to Clean here so the frame is ready for use.
	f.State.Store(int32(StateClean))
}

// evictOneLocked finds and evicts one frame. Must be called with m.mu held.
func (m *manager) evictOneLocked() *Frame {
	f := m.evictor.evictOne()
	if f == nil {
		return nil
	}
	m.handleEviction(f)

	return f
}

// handleEviction performs dirty writeback and updates stats.
func (m *manager) handleEviction(f *Frame) {
	// Remove from residency index.
	m.residency.Remove(f.Owner, f.Tag)

	// Check dirty status before transitioning to evicting.
	wasDirty := f.IsDirty()

	// Mark as evicting before any writeback I/O.
	f.State.Store(int32(StateEvicting))

	if wasDirty {
		// Try per-frame writeback via meta.
		if wb, ok := f.Meta().(FrameWriteBack); ok {
			f.Pin()
			if err := wb.WriteBackFrame(f); err != nil {
				m.logger.Warn("bufmgr: per-frame writeback failed",
					"frame_id", f.ID, "owner", f.Owner.String(), "error", err)
			}
			f.Unpin()
			m.writebacks.Add(1)
		} else if m.writeback != nil {
			f.Pin()
			if err := m.writeback(f); err != nil {
				m.logger.Warn("bufmgr: dirty frame writeback failed",
					"frame_id", f.ID, "owner", f.Owner.String(), "error", err)
			}
			f.Unpin()
			m.writebacks.Add(1)
		}
	}

	// Release memory from governor if tracked.
	if m.gov != nil {
		m.gov.Release(memgov.ClassPageCache, int64(m.frameSize))
	}

	m.evictions.Add(1)
}

// findFrame looks up a frame by its ID. Must be called with m.mu held.
func (m *manager) findFrame(id FrameID) *Frame {
	idx := int(id) - 1
	if idx < 0 || idx >= len(m.frames) {
		return nil
	}

	return m.frames[idx]
}

// closeFrames frees all frame data memory. Must be called with m.mu held.
func (m *manager) closeFrames() error {
	var firstErr error
	for _, f := range m.frames {
		if f == nil || f.data == nil {
			continue
		}
		if m.offHeap {
			if err := freeOffHeap(f.data, f.size); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		f.data = nil
	}
	m.frames = nil
	m.heapBacking = nil

	return firstErr
}
