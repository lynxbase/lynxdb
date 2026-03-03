package buffer

import (
	"testing"
)

func TestOperatorPageAllocator_AllocPage(t *testing.T) {
	bp := newTestPool(t, 8)
	alloc := NewOperatorPageAllocator(bp, "job-123")

	p, err := alloc.AllocPage()
	if err != nil {
		t.Fatalf("AllocPage: %v", err)
	}
	if p.Owner() != OwnerQueryOperator {
		t.Errorf("owner = %v, want OwnerQueryOperator", p.Owner())
	}
	if p.OwnerTag() != "job-123" {
		t.Errorf("ownerTag = %q, want %q", p.OwnerTag(), "job-123")
	}
	if !p.IsPinned() {
		t.Error("page should be pinned after AllocPage")
	}
	if alloc.PageCount() != 1 {
		t.Errorf("PageCount = %d, want 1", alloc.PageCount())
	}
	p.Unpin()
}

func TestOperatorPageAllocator_MultiplePages(t *testing.T) {
	bp := newTestPool(t, 8)
	alloc := NewOperatorPageAllocator(bp, "job-456")

	const n = 5
	for i := 0; i < n; i++ {
		p, err := alloc.AllocPage()
		if err != nil {
			t.Fatalf("AllocPage %d: %v", i, err)
		}
		p.Unpin()
	}

	if alloc.PageCount() != n {
		t.Errorf("PageCount = %d, want %d", alloc.PageCount(), n)
	}

	stats := bp.Stats()
	if stats.QueryPages != n {
		t.Errorf("QueryPages = %d, want %d", stats.QueryPages, n)
	}
}

func TestOperatorPageAllocator_ReleaseAll(t *testing.T) {
	bp := newTestPool(t, 8)
	alloc := NewOperatorPageAllocator(bp, "job-789")

	// Allocate 3 pages and keep them pinned.
	for i := 0; i < 3; i++ {
		_, err := alloc.AllocPage()
		if err != nil {
			t.Fatalf("AllocPage %d: %v", i, err)
		}
		// Don't unpin — ReleaseAll should handle it.
	}

	if alloc.PageCount() != 3 {
		t.Errorf("PageCount before release = %d, want 3", alloc.PageCount())
	}

	alloc.ReleaseAll()

	if alloc.PageCount() != 0 {
		t.Errorf("PageCount after release = %d, want 0", alloc.PageCount())
	}

	stats := bp.Stats()
	if stats.FreePages != 8 {
		t.Errorf("FreePages after release = %d, want 8", stats.FreePages)
	}
}

func TestOperatorPageAllocator_EvictsCache(t *testing.T) {
	bp := newTestPool(t, 4)

	// Fill pool with cache pages.
	cachePages := make([]*Page, 4)
	for i := range cachePages {
		p, err := bp.AllocPage(OwnerSegmentCache, "cache")
		if err != nil {
			t.Fatalf("AllocPage cache %d: %v", i, err)
		}
		p.Unpin()
		p.refBit.Store(false) // make immediately evictable
		cachePages[i] = p
	}

	// Now allocate via operator — should evict cache pages.
	alloc := NewOperatorPageAllocator(bp, "job-evict")
	p, err := alloc.AllocPage()
	if err != nil {
		t.Fatalf("AllocPage should evict cache: %v", err)
	}

	stats := bp.Stats()
	if stats.Evictions < 1 {
		t.Errorf("Evictions = %d, want >= 1", stats.Evictions)
	}
	if p.Owner() != OwnerQueryOperator {
		t.Errorf("owner = %v, want OwnerQueryOperator", p.Owner())
	}
	alloc.ReleaseAll()
}

func TestOperatorPageAllocator_Pool(t *testing.T) {
	bp := newTestPool(t, 4)
	alloc := NewOperatorPageAllocator(bp, "job")

	if alloc.Pool() != bp {
		t.Error("Pool() should return the underlying pool")
	}
}
