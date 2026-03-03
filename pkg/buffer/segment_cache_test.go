package buffer

import (
	"bytes"
	"testing"
)

func TestSegmentCacheConsumer_PutGet(t *testing.T) {
	bp := newTestPool(t, 16)
	sc := NewSegmentCacheConsumer(bp)

	key := SegmentCacheKey{SegmentID: "seg-001", Column: "level", RowGroup: 0}
	data := []byte("hello, segment cache!")

	if err := sc.Put(key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if sc.EntryCount() != 1 {
		t.Errorf("EntryCount = %d, want 1", sc.EntryCount())
	}

	pages, ok := sc.Get(key)
	if !ok {
		t.Fatal("Get returned miss for a cached key")
	}
	if len(pages) == 0 {
		t.Fatal("Get returned empty pages")
	}

	// Read back data from pages.
	var got []byte
	for _, p := range pages {
		ds := p.DataSlice()
		// Only read up to original data length from first page.
		remaining := len(data) - len(got)
		if remaining > len(ds) {
			remaining = len(ds)
		}
		got = append(got, ds[:remaining]...)
		p.Unpin() // MUST unpin after Get
	}

	if !bytes.Equal(got, data) {
		t.Errorf("cached data = %q, want %q", got, data)
	}
}

func TestSegmentCacheConsumer_Miss(t *testing.T) {
	bp := newTestPool(t, 8)
	sc := NewSegmentCacheConsumer(bp)

	key := SegmentCacheKey{SegmentID: "nonexistent", Column: "x", RowGroup: 0}
	pages, ok := sc.Get(key)
	if ok {
		t.Error("Get should return false for missing key")
	}
	if pages != nil {
		t.Error("pages should be nil on miss")
	}
}

func TestSegmentCacheConsumer_Overwrite(t *testing.T) {
	bp := newTestPool(t, 16)
	sc := NewSegmentCacheConsumer(bp)

	key := SegmentCacheKey{SegmentID: "seg-001", Column: "level", RowGroup: 0}

	if err := sc.Put(key, []byte("version-1")); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := sc.Put(key, []byte("version-2")); err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	if sc.EntryCount() != 1 {
		t.Errorf("EntryCount = %d, want 1 (overwrite)", sc.EntryCount())
	}

	pages, ok := sc.Get(key)
	if !ok {
		t.Fatal("Get returned miss after overwrite")
	}

	ds := pages[0].DataSlice()
	if !bytes.HasPrefix(ds, []byte("version-2")) {
		t.Errorf("cached data does not start with version-2")
	}
	for _, p := range pages {
		p.Unpin()
	}
}

func TestSegmentCacheConsumer_LargeData(t *testing.T) {
	bp := newTestPool(t, 16)
	sc := NewSegmentCacheConsumer(bp)

	key := SegmentCacheKey{SegmentID: "seg-002", Column: "raw", RowGroup: 0}

	// Data larger than one page — requires multiple pages.
	data := make([]byte, PageSize64KB+1000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	if err := sc.Put(key, data); err != nil {
		t.Fatalf("Put large data: %v", err)
	}

	pages, ok := sc.Get(key)
	if !ok {
		t.Fatal("Get returned miss for large data")
	}
	if len(pages) != 2 {
		t.Errorf("expected 2 pages for data spanning page boundary, got %d", len(pages))
	}

	// Read back and verify.
	var got []byte
	remaining := len(data)
	for _, p := range pages {
		ds := p.DataSlice()
		take := remaining
		if take > len(ds) {
			take = len(ds)
		}
		got = append(got, ds[:take]...)
		remaining -= take
		p.Unpin()
	}

	if !bytes.Equal(got, data) {
		t.Errorf("large data mismatch: got %d bytes, want %d bytes", len(got), len(data))
	}
}

func TestSegmentCacheConsumer_Invalidate(t *testing.T) {
	bp := newTestPool(t, 16)
	sc := NewSegmentCacheConsumer(bp)

	key := SegmentCacheKey{SegmentID: "seg-001", Column: "level", RowGroup: 0}
	if err := sc.Put(key, []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	sc.Invalidate(key)

	if sc.EntryCount() != 0 {
		t.Errorf("EntryCount = %d, want 0 after invalidate", sc.EntryCount())
	}
	_, ok := sc.Get(key)
	if ok {
		t.Error("Get should return miss after invalidate")
	}
}

func TestSegmentCacheConsumer_InvalidateSegment(t *testing.T) {
	bp := newTestPool(t, 32)
	sc := NewSegmentCacheConsumer(bp)

	// Put 3 entries for seg-001, 1 entry for seg-002.
	for i := 0; i < 3; i++ {
		key := SegmentCacheKey{SegmentID: "seg-001", Column: "col", RowGroup: i}
		if err := sc.Put(key, []byte("data")); err != nil {
			t.Fatalf("Put seg-001 rg%d: %v", i, err)
		}
	}
	key2 := SegmentCacheKey{SegmentID: "seg-002", Column: "col", RowGroup: 0}
	if err := sc.Put(key2, []byte("other")); err != nil {
		t.Fatalf("Put seg-002: %v", err)
	}

	if sc.EntryCount() != 4 {
		t.Errorf("EntryCount = %d, want 4", sc.EntryCount())
	}

	sc.InvalidateSegment("seg-001")

	if sc.EntryCount() != 1 {
		t.Errorf("EntryCount after invalidate = %d, want 1", sc.EntryCount())
	}

	// seg-002 should still be cached.
	_, ok := sc.Get(key2)
	if !ok {
		t.Error("seg-002 should still be cached")
	} else {
		// Unpin pages from Get.
		pages, _ := sc.Get(key2)
		for _, p := range pages {
			p.Unpin()
		}
	}
}

func TestSegmentCacheConsumer_Clear(t *testing.T) {
	bp := newTestPool(t, 16)
	sc := NewSegmentCacheConsumer(bp)

	for i := 0; i < 3; i++ {
		key := SegmentCacheKey{SegmentID: "seg", Column: "col", RowGroup: i}
		if err := sc.Put(key, []byte("data")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	sc.Clear()

	if sc.EntryCount() != 0 {
		t.Errorf("EntryCount = %d, want 0 after Clear", sc.EntryCount())
	}

	stats := bp.Stats()
	if stats.FreePages != 16 {
		t.Errorf("FreePages = %d, want 16 after Clear (all pages returned)", stats.FreePages)
	}
}
