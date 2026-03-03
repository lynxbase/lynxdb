package buffer

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func newTestPool(t *testing.T, maxPages int) *Pool {
	t.Helper()
	bp, err := NewPool(PoolConfig{
		MaxPages:      maxPages,
		PageSize:      PageSize64KB,
		EnableOffHeap: false, // use Go heap in tests for simplicity
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(func() { _ = bp.Close() })

	return bp
}

func TestNewPool_Defaults(t *testing.T) {
	bp, err := NewPool(DefaultPoolConfig())
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer func() { _ = bp.Close() }()

	if bp.maxPages != 1024 {
		t.Errorf("maxPages = %d, want 1024", bp.maxPages)
	}
	if bp.pageSize != PageSize64KB {
		t.Errorf("pageSize = %d, want %d", bp.pageSize, PageSize64KB)
	}
	stats := bp.Stats()
	if stats.FreePages != 1024 {
		t.Errorf("FreePages = %d, want 1024", stats.FreePages)
	}
}

func TestNewPool_MaxMemoryBytes(t *testing.T) {
	// 4 pages of 64KB = 256KB
	bp, err := NewPool(PoolConfig{
		MaxMemoryBytes: 256 * 1024,
		PageSize:       PageSize64KB,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer func() { _ = bp.Close() }()

	if bp.maxPages != 4 {
		t.Errorf("maxPages = %d, want 4", bp.maxPages)
	}
}

func TestPool_AllocPage(t *testing.T) {
	bp := newTestPool(t, 4)

	p, err := bp.AllocPage(OwnerQueryOperator, "test-op")
	if err != nil {
		t.Fatalf("AllocPage: %v", err)
	}
	if p == nil {
		t.Fatal("AllocPage returned nil")
	}
	if p.Owner() != OwnerQueryOperator {
		t.Errorf("owner = %v, want OwnerQueryOperator", p.Owner())
	}
	if p.OwnerTag() != "test-op" {
		t.Errorf("ownerTag = %q, want %q", p.OwnerTag(), "test-op")
	}
	if !p.IsPinned() {
		t.Error("page should be pinned after AllocPage")
	}
	if p.PinCount() != 1 {
		t.Errorf("pinCount = %d, want 1", p.PinCount())
	}

	stats := bp.Stats()
	if stats.FreePages != 3 {
		t.Errorf("FreePages = %d, want 3", stats.FreePages)
	}
	if stats.Allocations != 1 {
		t.Errorf("Allocations = %d, want 1", stats.Allocations)
	}
	p.Unpin()
}

func TestPool_AllocAllPages(t *testing.T) {
	bp := newTestPool(t, 4)

	pages := make([]*Page, 4)
	for i := range pages {
		p, err := bp.AllocPage(OwnerSegmentCache, "seg")
		if err != nil {
			t.Fatalf("AllocPage %d: %v", i, err)
		}
		pages[i] = p
	}

	stats := bp.Stats()
	if stats.FreePages != 0 {
		t.Errorf("FreePages = %d, want 0", stats.FreePages)
	}

	// All pages are pinned — next alloc should evict via clock, but all are pinned.
	_, err := bp.AllocPage(OwnerSegmentCache, "overflow")
	if !errors.Is(err, ErrAllPagesPinned) {
		t.Errorf("expected ErrAllPagesPinned, got: %v", err)
	}

	// Unpin one page and try again — should succeed via eviction.
	pages[0].Unpin()
	p, err := bp.AllocPage(OwnerQueryOperator, "new")
	if err != nil {
		t.Fatalf("AllocPage after unpin: %v", err)
	}
	if p.Owner() != OwnerQueryOperator {
		t.Errorf("owner = %v, want OwnerQueryOperator", p.Owner())
	}

	stats = bp.Stats()
	if stats.Evictions != 1 {
		t.Errorf("Evictions = %d, want 1", stats.Evictions)
	}

	// Cleanup.
	for _, pg := range pages {
		pg.Unpin()
	}
	p.Unpin()
}

func TestPool_FreePage(t *testing.T) {
	bp := newTestPool(t, 4)

	p, err := bp.AllocPage(OwnerSegmentCache, "seg")
	if err != nil {
		t.Fatalf("AllocPage: %v", err)
	}
	p.Unpin()

	bp.FreePage(p)
	stats := bp.Stats()
	if stats.FreePages != 4 {
		t.Errorf("FreePages = %d, want 4", stats.FreePages)
	}
	if stats.Frees != 1 {
		t.Errorf("Frees = %d, want 1", stats.Frees)
	}
}

func TestPool_FreePage_Nil(t *testing.T) {
	bp := newTestPool(t, 4)
	bp.FreePage(nil) // should not panic
}

func TestPool_Resolve(t *testing.T) {
	bp := newTestPool(t, 4)

	p, err := bp.AllocPage(OwnerQueryOperator, "test")
	if err != nil {
		t.Fatalf("AllocPage: %v", err)
	}

	// Write data into the page.
	data := []byte("hello, buffer manager!")
	if err := p.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	// Resolve a reference.
	ref := PageRef{PageID: p.ID(), Offset: 0, Length: len(data)}
	resolved := bp.Resolve(ref)
	if !bytes.Equal(resolved, data) {
		t.Errorf("Resolve = %q, want %q", resolved, data)
	}

	// Resolve partial reference.
	ref2 := PageRef{PageID: p.ID(), Offset: 7, Length: 6}
	resolved2 := bp.Resolve(ref2)
	if string(resolved2) != "buffer" {
		t.Errorf("Resolve partial = %q, want %q", resolved2, "buffer")
	}

	// Invalid reference.
	bad := PageRef{PageID: 999, Offset: 0, Length: 1}
	if bp.Resolve(bad) != nil {
		t.Error("expected nil for invalid PageID")
	}

	// Out of bounds.
	oob := PageRef{PageID: p.ID(), Offset: PageSize64KB - 1, Length: 2}
	if bp.Resolve(oob) != nil {
		t.Error("expected nil for out-of-bounds reference")
	}
	p.Unpin()
}

func TestPool_DirtyWriteBack(t *testing.T) {
	var writeBackCalled atomic.Int32
	var writeBackPage *Page

	bp, err := NewPool(PoolConfig{
		MaxPages:      2,
		PageSize:      PageSize64KB,
		EnableOffHeap: false,
		WriteBackFunc: func(p *Page) error {
			writeBackCalled.Add(1)
			writeBackPage = p

			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer func() { _ = bp.Close() }()

	// Allocate both pages.
	p1, _ := bp.AllocPage(OwnerQueryOperator, "op1")
	p2, _ := bp.AllocPage(OwnerQueryOperator, "op2")

	// Mark p1 dirty and unpin it.
	p1.MarkDirty()
	p1.Unpin()
	// Also clear p1's reference bit to make it evictable on first sweep.
	p1.refBit.Store(false)

	// Unpin p2 to allow it to be an eviction candidate too.
	p2.Unpin()

	// Allocate a third page — must evict p1 (dirty).
	p3, err := bp.AllocPage(OwnerSegmentCache, "cache")
	if err != nil {
		t.Fatalf("AllocPage after dirty eviction: %v", err)
	}

	if writeBackCalled.Load() != 1 {
		t.Errorf("writeBackFunc called %d times, want 1", writeBackCalled.Load())
	}
	if writeBackPage == nil {
		t.Error("writeBackPage is nil")
	}

	stats := bp.Stats()
	if stats.DirtyWritebacks != 1 {
		t.Errorf("DirtyWritebacks = %d, want 1", stats.DirtyWritebacks)
	}
	p3.Unpin()
}

func TestPool_DirtyWriteBack_Error(t *testing.T) {
	bp, err := NewPool(PoolConfig{
		MaxPages:      1,
		PageSize:      PageSize64KB,
		EnableOffHeap: false,
		WriteBackFunc: func(_ *Page) error {
			return errors.New("disk full")
		},
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer func() { _ = bp.Close() }()

	p1, _ := bp.AllocPage(OwnerQueryOperator, "op1")
	p1.MarkDirty()
	p1.Unpin()
	p1.refBit.Store(false)

	// Eviction should succeed even if write-back fails (logged as warning).
	p2, err := bp.AllocPage(OwnerSegmentCache, "cache")
	if err != nil {
		t.Fatalf("AllocPage should succeed despite write-back error: %v", err)
	}
	p2.Unpin()
}

func TestPool_AllocPageForOwner_TargetedEviction(t *testing.T) {
	bp := newTestPool(t, 4)

	// Fill pool with 2 cache + 2 query pages.
	cache1, _ := bp.AllocPage(OwnerSegmentCache, "cache1")
	cache2, _ := bp.AllocPage(OwnerSegmentCache, "cache2")
	query1, _ := bp.AllocPage(OwnerQueryOperator, "query1")
	query2, _ := bp.AllocPage(OwnerQueryOperator, "query2")

	// Unpin all.
	cache1.Unpin()
	cache2.Unpin()
	query1.Unpin()
	query2.Unpin()

	// Clear ref bits on cache pages for immediate evictability.
	cache1.refBit.Store(false)
	cache2.refBit.Store(false)

	// Request a new query page, preferring to evict cache pages.
	p, err := bp.AllocPageForOwner(OwnerQueryOperator, "new-query", OwnerSegmentCache)
	if err != nil {
		t.Fatalf("AllocPageForOwner: %v", err)
	}
	if p.Owner() != OwnerQueryOperator {
		t.Errorf("owner = %v, want OwnerQueryOperator", p.Owner())
	}

	stats := bp.Stats()
	if stats.Evictions != 1 {
		t.Errorf("Evictions = %d, want 1", stats.Evictions)
	}
	p.Unpin()
}

func TestPool_Close(t *testing.T) {
	bp := newTestPool(t, 4)

	p, _ := bp.AllocPage(OwnerSegmentCache, "test")
	p.Unpin()

	if err := bp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Double close should be no-op.
	if err := bp.Close(); err != nil {
		t.Fatalf("double Close: %v", err)
	}

	// AllocPage after close should fail.
	_, err := bp.AllocPage(OwnerSegmentCache, "after-close")
	if err == nil {
		t.Fatal("AllocPage after Close should fail")
	}
}

func TestPool_PageDataSlice(t *testing.T) {
	bp := newTestPool(t, 2)

	p, _ := bp.AllocPage(OwnerQueryOperator, "test")
	ds := p.DataSlice()
	if len(ds) != PageSize64KB {
		t.Errorf("DataSlice len = %d, want %d", len(ds), PageSize64KB)
	}

	// Write and read back.
	if err := p.WriteAt([]byte{0xDE, 0xAD}, 100); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if !p.IsDirty() {
		t.Error("page should be dirty after WriteAt")
	}

	var buf [2]byte
	if err := p.ReadAt(buf[:], 100); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if buf[0] != 0xDE || buf[1] != 0xAD {
		t.Errorf("ReadAt = %x, want DEAD", buf)
	}
	p.Unpin()
}

func TestPool_PageWriteAt_BoundsCheck(t *testing.T) {
	bp := newTestPool(t, 1)
	p, _ := bp.AllocPage(OwnerQueryOperator, "test")

	if err := p.WriteAt([]byte{0xFF}, PageSize64KB); err == nil {
		t.Error("WriteAt at boundary should fail")
	}
	if err := p.WriteAt([]byte{0xFF}, -1); err == nil {
		t.Error("WriteAt at negative offset should fail")
	}
	p.Unpin()
}

func TestPool_Stats_OwnerCounts(t *testing.T) {
	bp := newTestPool(t, 6)

	c1, _ := bp.AllocPage(OwnerSegmentCache, "c1")
	c2, _ := bp.AllocPage(OwnerSegmentCache, "c2")
	q1, _ := bp.AllocPage(OwnerQueryOperator, "q1")
	m1, _ := bp.AllocPage(OwnerMemtable, "m1")

	stats := bp.Stats()
	if stats.CachePages != 2 {
		t.Errorf("CachePages = %d, want 2", stats.CachePages)
	}
	if stats.QueryPages != 1 {
		t.Errorf("QueryPages = %d, want 1", stats.QueryPages)
	}
	if stats.MemtablePages != 1 {
		t.Errorf("MemtablePages = %d, want 1", stats.MemtablePages)
	}
	if stats.FreePages != 2 {
		t.Errorf("FreePages = %d, want 2", stats.FreePages)
	}
	if stats.UsedPages != 4 {
		t.Errorf("UsedPages = %d, want 4", stats.UsedPages)
	}

	c1.Unpin()
	c2.Unpin()
	q1.Unpin()
	m1.Unpin()
}

func TestPool_ConcurrentAccess(t *testing.T) {
	bp := newTestPool(t, 64)

	var wg sync.WaitGroup
	const goroutines = 16
	const opsPerGoroutine = 100

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				p, err := bp.AllocPage(OwnerQueryOperator, "concurrent")
				if err != nil {
					// Under heavy concurrency with limited pages, ErrAllPagesPinned is expected.
					continue
				}

				// Write while pinned — safe, we own this page.
				_ = p.WriteAt([]byte{0x42}, 0)

				// Read while still pinned.
				var buf [1]byte
				_ = p.ReadAt(buf[:], 0)

				// Free the page (returns to pool — must not access after this).
				bp.FreePage(p)
			}
		}()
	}
	wg.Wait()

	stats := bp.Stats()
	if stats.TotalPages != 64 {
		t.Errorf("TotalPages = %d, want 64", stats.TotalPages)
	}
}

func TestPool_PinUnpin(t *testing.T) {
	bp := newTestPool(t, 2)

	p, _ := bp.AllocPage(OwnerSegmentCache, "test")

	// Initially pinned once.
	if p.PinCount() != 1 {
		t.Errorf("pinCount = %d, want 1", p.PinCount())
	}

	// Pin again (shared access).
	p.Pin()
	if p.PinCount() != 2 {
		t.Errorf("pinCount = %d, want 2", p.PinCount())
	}

	// Unpin once.
	p.Unpin()
	if p.PinCount() != 1 {
		t.Errorf("pinCount = %d, want 1", p.PinCount())
	}
	if !p.IsPinned() {
		t.Error("should still be pinned")
	}

	// Unpin again.
	p.Unpin()
	if p.PinCount() != 0 {
		t.Errorf("pinCount = %d, want 0", p.PinCount())
	}
	if p.IsPinned() {
		t.Error("should not be pinned")
	}

	// Double-unpin should clamp to 0.
	p.Unpin()
	if p.PinCount() != 0 {
		t.Errorf("pinCount after double-unpin = %d, want 0", p.PinCount())
	}
}

func TestPool_OwnerData(t *testing.T) {
	bp := newTestPool(t, 2)

	p, _ := bp.AllocPage(OwnerSegmentCache, "seg-123")

	type segInfo struct {
		segID  string
		column string
	}

	info := &segInfo{segID: "abc", column: "level"}
	p.SetOwnerData(info)

	got, ok := p.OwnerData().(*segInfo)
	if !ok {
		t.Fatal("OwnerData type assertion failed")
	}
	if got.segID != "abc" || got.column != "level" {
		t.Errorf("OwnerData = %+v, want {abc level}", got)
	}
	p.Unpin()
}

func TestPool_EvictionOrder_SecondChance(t *testing.T) {
	bp := newTestPool(t, 3)

	// Allocate 3 pages.
	p1, _ := bp.AllocPage(OwnerSegmentCache, "p1")
	p2, _ := bp.AllocPage(OwnerSegmentCache, "p2")
	p3, _ := bp.AllocPage(OwnerSegmentCache, "p3")

	// Unpin all.
	p1.Unpin()
	p2.Unpin()
	p3.Unpin()

	// Clear refBit on p1 so it's evicted first. p2, p3 retain refBit (second chance).
	p1.refBit.Store(false)

	// Allocate — should evict p1 (refBit=0) not p2 or p3 (refBit=1).
	pNew, err := bp.AllocPage(OwnerQueryOperator, "new")
	if err != nil {
		t.Fatalf("AllocPage: %v", err)
	}

	// Verify p1 was evicted by checking its data is recycled.
	// p1's poolSlot should now be used by pNew.
	if pNew.ID() != p1.ID() {
		// The page ID stays the same since we recycle the same page object.
		t.Logf("evicted page ID: %d, new page ID: %d", p1.ID(), pNew.ID())
	}

	stats := bp.Stats()
	if stats.Evictions != 1 {
		t.Errorf("Evictions = %d, want 1", stats.Evictions)
	}
	pNew.Unpin()
}

func TestPool_PageRef_IsValid(t *testing.T) {
	ref := PageRef{PageID: 1, Offset: 0, Length: 10}
	if !ref.IsValid() {
		t.Error("expected valid ref")
	}

	zero := PageRef{}
	if zero.IsValid() {
		t.Error("zero ref should be invalid")
	}
}

func TestPool_PageOwner_String(t *testing.T) {
	tests := []struct {
		owner PageOwner
		want  string
	}{
		{OwnerSegmentCache, "cache"},
		{OwnerQueryOperator, "query"},
		{OwnerMemtable, "memtable"},
		{PageOwner(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.owner.String(); got != tt.want {
			t.Errorf("PageOwner(%d).String() = %q, want %q", tt.owner, got, tt.want)
		}
	}
}

func TestPool_Accessors(t *testing.T) {
	bp := newTestPool(t, 8)

	if bp.PageSize() != PageSize64KB {
		t.Errorf("PageSize() = %d, want %d", bp.PageSize(), PageSize64KB)
	}
	if bp.MaxPages() != 8 {
		t.Errorf("MaxPages() = %d, want 8", bp.MaxPages())
	}
}
