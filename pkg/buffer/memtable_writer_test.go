package buffer

import (
	"bytes"
	"testing"
)

func TestMemtablePageWriter_Append(t *testing.T) {
	bp := newTestPool(t, 8)
	mw := NewMemtablePageWriter(bp)

	data := []byte("event-1: user logged in")
	ref, err := mw.Append(data)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if !ref.IsValid() {
		t.Fatal("Append returned invalid ref")
	}
	if ref.Length != len(data) {
		t.Errorf("ref.Length = %d, want %d", ref.Length, len(data))
	}

	// Resolve and verify.
	resolved := bp.Resolve(ref)
	if !bytes.Equal(resolved, data) {
		t.Errorf("resolved = %q, want %q", resolved, data)
	}

	if mw.PageCount() != 1 {
		t.Errorf("PageCount = %d, want 1", mw.PageCount())
	}
}

func TestMemtablePageWriter_MultipleAppends(t *testing.T) {
	bp := newTestPool(t, 8)
	mw := NewMemtablePageWriter(bp)

	events := []string{
		"event-1: login",
		"event-2: query",
		"event-3: logout",
	}

	refs := make([]PageRef, len(events))
	for i, ev := range events {
		ref, err := mw.Append([]byte(ev))
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		refs[i] = ref
	}

	// All should be on the same page (they're small).
	if mw.PageCount() != 1 {
		t.Errorf("PageCount = %d, want 1", mw.PageCount())
	}

	// Verify all refs resolve correctly.
	for i, ref := range refs {
		resolved := bp.Resolve(ref)
		if !bytes.Equal(resolved, []byte(events[i])) {
			t.Errorf("event %d: resolved = %q, want %q", i, resolved, events[i])
		}
	}
}

func TestMemtablePageWriter_PageBoundary(t *testing.T) {
	bp := newTestPool(t, 8)
	mw := NewMemtablePageWriter(bp)

	// Write enough data to span multiple pages.
	// Each page is 64KB. Write 100 events of ~700 bytes each (~70KB total).
	eventData := make([]byte, 700)
	for i := range eventData {
		eventData[i] = byte('A' + i%26)
	}

	var refs []PageRef
	for i := 0; i < 100; i++ {
		ref, err := mw.Append(eventData)
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		refs = append(refs, ref)
	}

	// Should have used multiple pages (70KB > 64KB).
	if mw.PageCount() < 2 {
		t.Errorf("PageCount = %d, want >= 2 (data spans page boundary)", mw.PageCount())
	}

	// Verify all refs still resolve correctly.
	for i, ref := range refs {
		resolved := bp.Resolve(ref)
		if !bytes.Equal(resolved, eventData) {
			t.Errorf("event %d: data mismatch", i)
		}
	}
}

func TestMemtablePageWriter_EmptyAppend(t *testing.T) {
	bp := newTestPool(t, 4)
	mw := NewMemtablePageWriter(bp)

	ref, err := mw.Append(nil)
	if err != nil {
		t.Fatalf("Append nil: %v", err)
	}
	if ref.IsValid() {
		t.Error("empty append should return invalid ref")
	}

	ref2, err := mw.Append([]byte{})
	if err != nil {
		t.Fatalf("Append empty: %v", err)
	}
	if ref2.IsValid() {
		t.Error("empty append should return invalid ref")
	}

	if mw.PageCount() != 0 {
		t.Errorf("PageCount = %d, want 0 (no pages allocated for empty appends)", mw.PageCount())
	}
}

func TestMemtablePageWriter_PagesAreDirty(t *testing.T) {
	bp := newTestPool(t, 4)
	mw := NewMemtablePageWriter(bp)

	_, err := mw.Append([]byte("dirty data"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Check that the memtable page is dirty and owned by memtable.
	stats := bp.Stats()
	if stats.MemtablePages != 1 {
		t.Errorf("MemtablePages = %d, want 1", stats.MemtablePages)
	}
}

func TestMemtablePageWriter_BytesWritten(t *testing.T) {
	bp := newTestPool(t, 8)
	mw := NewMemtablePageWriter(bp)

	if mw.BytesWritten() != 0 {
		t.Errorf("BytesWritten = %d, want 0", mw.BytesWritten())
	}

	data := []byte("0123456789") // 10 bytes
	for i := 0; i < 5; i++ {
		if _, err := mw.Append(data); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	if mw.BytesWritten() != 50 {
		t.Errorf("BytesWritten = %d, want 50", mw.BytesWritten())
	}
}

func TestMemtablePageWriter_ReleaseAll(t *testing.T) {
	bp := newTestPool(t, 8)
	mw := NewMemtablePageWriter(bp)

	for i := 0; i < 10; i++ {
		if _, err := mw.Append([]byte("event data here")); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	if mw.PageCount() == 0 {
		t.Fatal("expected some pages allocated")
	}

	mw.ReleaseAll()

	if mw.PageCount() != 0 {
		t.Errorf("PageCount = %d, want 0 after ReleaseAll", mw.PageCount())
	}
	if mw.BytesWritten() != 0 {
		t.Errorf("BytesWritten = %d, want 0 after ReleaseAll", mw.BytesWritten())
	}

	stats := bp.Stats()
	if stats.FreePages != 8 {
		t.Errorf("FreePages = %d, want 8 after ReleaseAll", stats.FreePages)
	}
}

func TestMemtablePageWriter_OversizedAppend(t *testing.T) {
	bp := newTestPool(t, 4)
	mw := NewMemtablePageWriter(bp)

	// Try to append data larger than a single page.
	oversized := make([]byte, PageSize64KB+1)
	_, err := mw.Append(oversized)
	if err == nil {
		t.Fatal("Append of oversized data should fail")
	}
}
