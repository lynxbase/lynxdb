package bufmgr

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

// Default frame sizes.
const (
	FrameSize64KB  = 65536
	FrameSize256KB = 262144
)

// FrameState represents the lifecycle state of a buffer frame.
type FrameState int32

const (
	StateFree      FrameState = iota // available for allocation
	StateLoading                     // I/O in progress (read from disk/S3)
	StateClean                       // resident, unmodified
	StateDirty                       // resident, modified (needs writeback)
	StateWriteback                   // writeback I/O in progress
	StateEvicting                    // being reclaimed
)

// String returns a human-readable frame state name.
func (s FrameState) String() string {
	switch s {
	case StateFree:
		return "free"
	case StateLoading:
		return "loading"
	case StateClean:
		return "clean"
	case StateDirty:
		return "dirty"
	case StateWriteback:
		return "writeback"
	case StateEvicting:
		return "evicting"
	default:
		return "unknown"
	}
}

// FrameID is a unique identifier for a buffer frame.
type FrameID uint32

// FrameOwner identifies the consumer category.
type FrameOwner int

const (
	OwnerFree       FrameOwner = iota
	OwnerSegCache              // segment column cache
	OwnerQuery                 // query operator working memory
	OwnerMemtable              // ingestion buffer
	OwnerCompaction            // compaction scratch
)

// String returns a human-readable owner name.
func (o FrameOwner) String() string {
	switch o {
	case OwnerFree:
		return "free"
	case OwnerSegCache:
		return "seg-cache"
	case OwnerQuery:
		return "query"
	case OwnerMemtable:
		return "memtable"
	case OwnerCompaction:
		return "compaction"
	default:
		return "unknown"
	}
}

// Frame is the physical unit of buffer management.
// Frame descriptors live on the Go heap (~128 bytes). Frame data lives off-heap
// via mmap to avoid GC pressure.
type Frame struct {
	ID       FrameID
	data     unsafe.Pointer // backing memory (off-heap mmap or Go heap)
	size     int
	State    atomic.Int32 // FrameState
	PinCount atomic.Int32 // >0 = cannot evict
	RefBit   atomic.Bool  // reference bit for Clock eviction (second chance)
	Owner    FrameOwner
	Tag      string      // diagnostic: segment ID, operator ID, etc.
	PageSize int         // 64KB or 256KB
	slot     int         // index in manager's frames slice
	meta     interface{} // owner-specific metadata
}

// DataSlice returns the frame data as a byte slice. The frame must be
// pinned before calling this. The returned slice is valid only while pinned.
func (f *Frame) DataSlice() []byte {
	if f.data == nil {
		return nil
	}

	return unsafe.Slice((*byte)(f.data), f.size)
}

// WriteAt writes data into the frame at the given offset.
// Automatically marks the frame as dirty.
func (f *Frame) WriteAt(src []byte, offset int) error {
	if offset < 0 || offset+len(src) > f.size {
		return fmt.Errorf("bufmgr.Frame.WriteAt: offset %d + len %d exceeds frame size %d",
			offset, len(src), f.size)
	}

	dst := f.DataSlice()
	copy(dst[offset:], src)
	f.State.Store(int32(StateDirty))

	return nil
}

// ReadAt reads data from the frame at the given offset.
func (f *Frame) ReadAt(dst []byte, offset int) error {
	if offset < 0 || offset+len(dst) > f.size {
		return fmt.Errorf("bufmgr.Frame.ReadAt: offset %d + len %d exceeds frame size %d",
			offset, len(dst), f.size)
	}

	src := f.DataSlice()
	copy(dst, src[offset:])

	return nil
}

// Pin marks the frame as actively in use. Increments pin count.
// Also sets the reference bit for Clock eviction (second chance).
func (f *Frame) Pin() {
	f.PinCount.Add(1)
	f.RefBit.Store(true)
}

// Unpin decrements pin count. When pin count reaches 0, the frame becomes
// an eviction candidate.
func (f *Frame) Unpin() {
	if v := f.PinCount.Add(-1); v < 0 {
		f.PinCount.Store(0)
	}
}

// IsPinned returns true if the frame is currently pinned.
func (f *Frame) IsPinned() bool {
	return f.PinCount.Load() > 0
}

// IsDirty returns true if the frame has been modified since last writeback.
func (f *Frame) IsDirty() bool {
	return FrameState(f.State.Load()) == StateDirty
}

// Size returns the frame's data size in bytes.
func (f *Frame) Size() int {
	return f.size
}

// SetMeta sets owner-specific metadata on the frame.
func (f *Frame) SetMeta(meta interface{}) {
	f.meta = meta
}

// Meta returns owner-specific metadata stored on the frame.
func (f *Frame) Meta() interface{} {
	return f.meta
}

// ErrInvalidTransition is returned when a frame state transition is not allowed.
var ErrInvalidTransition = fmt.Errorf("bufmgr: invalid state transition")

// TransitionTo atomically transitions the frame to a new state, validating
// that the transition is allowed by the state machine.
//
// Valid transitions:
//
//	Free → Loading
//	Loading → Clean
//	Clean → Dirty
//	Clean → Evicting
//	Dirty → Writeback
//	Dirty → Evicting (forced eviction)
//	Writeback → Clean
//	Writeback → Dirty (writeback failure)
//	Evicting → Free
func (f *Frame) TransitionTo(newState FrameState) error {
	old := FrameState(f.State.Load())
	if !validTransition(old, newState) {
		return fmt.Errorf("%w: %s → %s", ErrInvalidTransition, old, newState)
	}
	f.State.Store(int32(newState))
	return nil
}

// validTransition checks if a state transition is allowed.
func validTransition(from, to FrameState) bool {
	switch from {
	case StateFree:
		return to == StateLoading
	case StateLoading:
		return to == StateClean
	case StateClean:
		return to == StateDirty || to == StateEvicting
	case StateDirty:
		return to == StateWriteback || to == StateEvicting
	case StateWriteback:
		return to == StateClean || to == StateDirty
	case StateEvicting:
		return to == StateFree
	default:
		return false
	}
}

// reset clears frame metadata for reuse.
func (f *Frame) reset() {
	f.PinCount.Store(0)
	f.State.Store(int32(StateFree))
	f.RefBit.Store(false)
	f.Owner = OwnerFree
	f.Tag = ""
	f.meta = nil
}

// FrameRef is a stable reference to data within a frame.
type FrameRef struct {
	FrameID FrameID
	Offset  int
	Length  int
}

// IsValid returns true if this is a non-zero reference.
func (r FrameRef) IsValid() bool {
	return r.Length > 0
}
