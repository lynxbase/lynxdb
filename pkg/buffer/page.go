package buffer

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

// Page size constants. Pages are the unit of memory allocation in the buffer
// manager. All data — cached segments, query intermediates, memtable batches —
// lives in pages.
const (
	// PageSize64KB is the default data page size (64KB).
	// Suitable for most data: column chunks, hash table buckets, sort buffers.
	PageSize64KB = 65536

	// PageSize256KB is used for large data (row groups, large sort runs).
	PageSize256KB = 262144
)

// PageOwner identifies which subsystem owns a page.
type PageOwner int

const (
	// OwnerSegmentCache indicates the page holds cached segment data.
	OwnerSegmentCache PageOwner = iota
	// OwnerQueryOperator indicates the page holds query intermediate data.
	OwnerQueryOperator
	// OwnerMemtable indicates the page holds memtable ingestion data.
	OwnerMemtable
)

// String returns a human-readable owner name.
func (o PageOwner) String() string {
	switch o {
	case OwnerSegmentCache:
		return "cache"
	case OwnerQueryOperator:
		return "query"
	case OwnerMemtable:
		return "memtable"
	default:
		return "unknown"
	}
}

// PageID is a globally unique identifier for a page within the buffer pool.
type PageID uint64

// Page represents a fixed-size memory block managed by the buffer pool.
// The page descriptor (this struct) lives on the Go heap (~128 bytes).
// The page data lives off-heap via mmap (when enabled) to avoid GC pressure.
//
// Thread-safety: pinCount, dirty, and refBit are accessed atomically.
// The data pointer is immutable after creation (but data content changes
// while pinned).
type Page struct {
	id   PageID
	data unsafe.Pointer // off-heap memory (mmap'd or Go heap fallback)
	size int            // PageSize64KB or PageSize256KB

	pinCount atomic.Int32 // >0 means page is in active use, cannot be evicted
	dirty    atomic.Bool  // true if modified since last write-back
	refBit   atomic.Bool  // reference bit for Clock eviction (second chance)

	owner     PageOwner   // which consumer owns this page
	ownerTag  string      // consumer-specific tag (segment ID, operator ID, etc.)
	ownerData interface{} // consumer-specific metadata

	// poolSlot is the index of this page in the pool's pages slice.
	// Set once during allocation and never changed.
	poolSlot int
}

// ID returns the page's unique identifier.
func (p *Page) ID() PageID {
	return p.id
}

// Size returns the page's data size in bytes.
func (p *Page) Size() int {
	return p.size
}

// Owner returns which subsystem owns this page.
func (p *Page) Owner() PageOwner {
	return p.owner
}

// OwnerTag returns the consumer-specific tag (e.g., segment ID, operator ID).
func (p *Page) OwnerTag() string {
	return p.ownerTag
}

// OwnerData returns consumer-specific metadata stored on the page.
func (p *Page) OwnerData() interface{} {
	return p.ownerData
}

// SetOwnerData sets consumer-specific metadata on the page.
// Must only be called while the page is pinned.
func (p *Page) SetOwnerData(data interface{}) {
	p.ownerData = data
}

// Pin marks the page as actively in use. Must be called before reading/writing.
// Increments pin count. Multiple concurrent pins are allowed (shared access).
// Also sets the reference bit for Clock eviction (second chance).
func (p *Page) Pin() {
	p.pinCount.Add(1)
	p.refBit.Store(true)
}

// Unpin marks the page as no longer actively in use. Decrements pin count.
// When pin count reaches 0, the page becomes an eviction candidate.
//
// Critical invariant: Every Pin() must have a matching Unpin().
// A leaked pin = memory leak (page never evicted). Use defer page.Unpin().
func (p *Page) Unpin() {
	if v := p.pinCount.Add(-1); v < 0 {
		// Defensive: clamp at 0 to avoid negative pin counts from double-unpin.
		p.pinCount.Store(0)
	}
}

// IsPinned returns true if the page is currently pinned (in active use).
func (p *Page) IsPinned() bool {
	return p.pinCount.Load() > 0
}

// PinCount returns the current pin count.
func (p *Page) PinCount() int32 {
	return p.pinCount.Load()
}

// MarkDirty marks the page as modified. Dirty pages require write-back before
// eviction.
func (p *Page) MarkDirty() {
	p.dirty.Store(true)
}

// ClearDirty clears the dirty flag (after write-back).
func (p *Page) ClearDirty() {
	p.dirty.Store(false)
}

// IsDirty returns true if the page has been modified since last write-back.
func (p *Page) IsDirty() bool {
	return p.dirty.Load()
}

// DataSlice returns the page data as a byte slice. The page must be pinned
// before calling this method. The returned slice is valid only while the page
// is pinned — do not retain it after Unpin().
//
// Bounds checking: the returned slice is exactly p.size bytes.
func (p *Page) DataSlice() []byte {
	if p.data == nil {
		return nil
	}

	return unsafe.Slice((*byte)(p.data), p.size)
}

// WriteAt writes data into the page at the given offset. The page must be
// pinned and the write must not exceed the page boundary.
// Automatically marks the page as dirty.
func (p *Page) WriteAt(src []byte, offset int) error {
	if offset < 0 || offset+len(src) > p.size {
		return fmt.Errorf("buffer.Page.WriteAt: offset %d + len %d exceeds page size %d", offset, len(src), p.size)
	}

	dst := p.DataSlice()
	copy(dst[offset:], src)
	p.MarkDirty()

	return nil
}

// ReadAt reads data from the page at the given offset. The page must be
// pinned and the read must not exceed the page boundary.
func (p *Page) ReadAt(dst []byte, offset int) error {
	if offset < 0 || offset+len(dst) > p.size {
		return fmt.Errorf("buffer.Page.ReadAt: offset %d + len %d exceeds page size %d", offset, len(dst), p.size)
	}

	src := p.DataSlice()
	copy(dst, src[offset:])

	return nil
}

// reset clears all page metadata for reuse. Called when a page is returned
// to the free list. Does NOT zero the data (caller may do so if needed).
func (p *Page) reset() {
	p.pinCount.Store(0)
	p.dirty.Store(false)
	p.refBit.Store(false)
	p.owner = OwnerSegmentCache
	p.ownerTag = ""
	p.ownerData = nil
}

// PageRef is a stable reference to data within a page.
// Valid across eviction/reload cycles because it uses offset-based addressing
// instead of Go pointers. When a page is evicted and reloaded at a different
// memory address, only the base address changes — offsets remain valid.
type PageRef struct {
	PageID PageID
	Offset int
	Length int
}

// IsValid returns true if this is a non-zero reference.
func (r PageRef) IsValid() bool {
	return r.Length > 0
}
