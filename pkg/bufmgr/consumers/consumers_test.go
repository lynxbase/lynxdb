package consumers

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/bufmgr"
)

// newTestManager creates a heap-backed Manager for test portability.
// Uses a small frame size (4096) to make multi-frame scenarios testable
// without requiring large allocations.
func newTestManager(t *testing.T, maxFrames int) bufmgr.Manager {
	t.Helper()

	mgr, err := bufmgr.NewManager(bufmgr.ManagerConfig{
		MaxFrames:     maxFrames,
		FrameSize:     4096,
		EnableOffHeap: false,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	return mgr
}

// readFrameData reads all data from a slice of frames and returns the
// concatenated bytes, truncated to totalSize.
func readFrameData(t *testing.T, frames []*bufmgr.Frame, totalSize int) []byte {
	t.Helper()

	var result []byte
	for _, f := range frames {
		ds := f.DataSlice()
		if ds == nil {
			t.Fatal("frame DataSlice is nil; frame may have been evicted")
		}
		result = append(result, ds...)
	}

	if len(result) < totalSize {
		t.Fatalf("concatenated frame data length %d < expected size %d", len(result), totalSize)
	}

	return result[:totalSize]
}

// SegmentCacheConsumer tests

func TestUnit_SegmentCacheConsumer_PutGet_SmallData_RoundTrips(t *testing.T) {
	mgr := newTestManager(t, 16)
	sc := NewSegmentCacheConsumer(mgr)

	key := SegmentCacheKey{SegmentID: "seg-001", Column: "message", RowGroup: 0}
	data := []byte("hello segment cache")

	if err := sc.Put(key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	frames, ok := sc.Get(key)
	if !ok {
		t.Fatal("Get returned false for a key that was just Put")
	}
	if len(frames) == 0 {
		t.Fatal("Get returned empty frames slice")
	}

	got := readFrameData(t, frames, len(data))
	if !bytes.Equal(got, data) {
		t.Fatalf("Get returned %q; want %q", got, data)
	}

	// Caller must unpin; verify frames are pinned.
	for i, f := range frames {
		if !f.IsPinned() {
			t.Errorf("frame[%d] is not pinned after Get; caller needs pinned frames", i)
		}
		f.Unpin()
	}
}

func TestUnit_SegmentCacheConsumer_PutGet_MultiFrame_SplitsCorrectly(t *testing.T) {
	mgr := newTestManager(t, 32)
	sc := NewSegmentCacheConsumer(mgr)
	frameSize := mgr.FrameSize() // 4096

	dataSize := frameSize*2 + 500
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 251) // prime modulus for non-repeating pattern
	}

	key := SegmentCacheKey{SegmentID: "seg-002", Column: "raw", RowGroup: 1}
	if err := sc.Put(key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	frames, ok := sc.Get(key)
	if !ok {
		t.Fatal("Get returned false for multi-frame entry")
	}
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames for %d bytes in %d-byte frames; got %d frames",
			dataSize, frameSize, len(frames))
	}

	got := readFrameData(t, frames, dataSize)
	if !bytes.Equal(got, data) {
		t.Fatal("multi-frame round-trip data mismatch")
	}

	for _, f := range frames {
		f.Unpin()
	}
}

func TestUnit_SegmentCacheConsumer_GetWithSize_ReturnsOriginalSize(t *testing.T) {
	mgr := newTestManager(t, 16)
	sc := NewSegmentCacheConsumer(mgr)

	key := SegmentCacheKey{SegmentID: "seg-003", Column: "timestamp", RowGroup: 0}
	data := []byte("some data of known length")

	if err := sc.Put(key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	frames, size, ok := sc.GetWithSize(key)
	if !ok {
		t.Fatal("GetWithSize returned false")
	}
	if size != len(data) {
		t.Fatalf("GetWithSize size = %d; want %d", size, len(data))
	}

	for _, f := range frames {
		f.Unpin()
	}
}

func TestUnit_SegmentCacheConsumer_Get_Miss_ReturnsFalse(t *testing.T) {
	mgr := newTestManager(t, 8)
	sc := NewSegmentCacheConsumer(mgr)

	key := SegmentCacheKey{SegmentID: "nonexistent", Column: "col", RowGroup: 0}

	frames, ok := sc.Get(key)
	if ok {
		t.Fatal("Get returned true for a key that was never Put")
	}
	if frames != nil {
		t.Fatal("Get returned non-nil frames for a miss")
	}
}

func TestUnit_SegmentCacheConsumer_Invalidate_RemovesEntry(t *testing.T) {
	mgr := newTestManager(t, 16)
	sc := NewSegmentCacheConsumer(mgr)

	key := SegmentCacheKey{SegmentID: "seg-004", Column: "level", RowGroup: 0}
	if err := sc.Put(key, []byte("cached data")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Verify it exists first.
	_, ok := sc.Get(key)
	if !ok {
		t.Fatal("entry should exist before invalidation")
	}
	// Unpin the frames from the Get above.
	frames, _ := sc.Get(key)
	for _, f := range frames {
		f.Unpin()
		f.Unpin() // unpin for first Get too
	}

	sc.Invalidate(key)

	_, ok = sc.Get(key)
	if ok {
		t.Fatal("Get returned true after Invalidate; entry should be removed")
	}
}

func TestUnit_SegmentCacheConsumer_Invalidate_NonexistentKey_NoPanic(t *testing.T) {
	mgr := newTestManager(t, 8)
	sc := NewSegmentCacheConsumer(mgr)

	// Should not panic when invalidating a key that does not exist.
	key := SegmentCacheKey{SegmentID: "does-not-exist", Column: "col", RowGroup: 0}
	sc.Invalidate(key)
}

func TestUnit_SegmentCacheConsumer_InvalidateSegment_RemovesAllColumnsForSegment(t *testing.T) {
	mgr := newTestManager(t, 32)
	sc := NewSegmentCacheConsumer(mgr)

	segID := "seg-005"
	columns := []string{"message", "level", "timestamp", "source"}

	for i, col := range columns {
		key := SegmentCacheKey{SegmentID: segID, Column: col, RowGroup: 0}
		if err := sc.Put(key, []byte(fmt.Sprintf("data-%d", i))); err != nil {
			t.Fatalf("Put(%s): %v", col, err)
		}
	}

	// Also put an entry for a different segment to confirm it survives.
	otherKey := SegmentCacheKey{SegmentID: "seg-other", Column: "message", RowGroup: 0}
	if err := sc.Put(otherKey, []byte("other segment data")); err != nil {
		t.Fatalf("Put(other): %v", err)
	}

	if sc.EntryCount() != 5 {
		t.Fatalf("EntryCount before InvalidateSegment = %d; want 5", sc.EntryCount())
	}

	sc.InvalidateSegment(segID)

	// All entries for segID should be gone.
	for _, col := range columns {
		key := SegmentCacheKey{SegmentID: segID, Column: col, RowGroup: 0}
		if _, ok := sc.Get(key); ok {
			t.Errorf("Get(%s/%s) returned true after InvalidateSegment", segID, col)
		}
	}

	// Entry for other segment should still be present.
	frames, ok := sc.Get(otherKey)
	if !ok {
		t.Fatal("Get for other segment returned false; it should survive InvalidateSegment")
	}
	for _, f := range frames {
		f.Unpin()
	}
}

func TestUnit_SegmentCacheConsumer_EntryCount_TracksCorrectly(t *testing.T) {
	mgr := newTestManager(t, 16)
	sc := NewSegmentCacheConsumer(mgr)

	if sc.EntryCount() != 0 {
		t.Fatalf("initial EntryCount = %d; want 0", sc.EntryCount())
	}

	for i := 0; i < 3; i++ {
		key := SegmentCacheKey{SegmentID: "seg", Column: fmt.Sprintf("col-%d", i), RowGroup: 0}
		if err := sc.Put(key, []byte("x")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	if sc.EntryCount() != 3 {
		t.Fatalf("EntryCount after 3 puts = %d; want 3", sc.EntryCount())
	}
}

func TestUnit_SegmentCacheConsumer_Clear_RemovesAllEntries(t *testing.T) {
	mgr := newTestManager(t, 16)
	sc := NewSegmentCacheConsumer(mgr)

	for i := 0; i < 5; i++ {
		key := SegmentCacheKey{SegmentID: fmt.Sprintf("seg-%d", i), Column: "col", RowGroup: 0}
		if err := sc.Put(key, []byte("data")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	if sc.EntryCount() != 5 {
		t.Fatalf("EntryCount before Clear = %d; want 5", sc.EntryCount())
	}

	sc.Clear()

	if sc.EntryCount() != 0 {
		t.Fatalf("EntryCount after Clear = %d; want 0", sc.EntryCount())
	}
}

func TestUnit_SegmentCacheConsumer_Put_OverwritesExistingKey(t *testing.T) {
	mgr := newTestManager(t, 16)
	sc := NewSegmentCacheConsumer(mgr)

	key := SegmentCacheKey{SegmentID: "seg-006", Column: "message", RowGroup: 0}

	if err := sc.Put(key, []byte("first")); err != nil {
		t.Fatalf("Put first: %v", err)
	}
	if err := sc.Put(key, []byte("second")); err != nil {
		t.Fatalf("Put second: %v", err)
	}

	if sc.EntryCount() != 1 {
		t.Fatalf("EntryCount after overwrite = %d; want 1", sc.EntryCount())
	}

	frames, ok := sc.Get(key)
	if !ok {
		t.Fatal("Get returned false after overwrite")
	}
	got := readFrameData(t, frames, 6) // "second" = 6 bytes
	if string(got) != "second" {
		t.Fatalf("Get after overwrite returned %q; want %q", got, "second")
	}

	for _, f := range frames {
		f.Unpin()
	}
}

func TestUnit_SegmentCacheConsumer_Get_EvictedFrame_ReturnsFalse(t *testing.T) {
	// The SegmentCacheConsumer detects eviction by checking DataSlice() == nil.
	// With heap-backed frames, data is never nil'd during normal eviction+reuse
	// (only on Manager.Close()). This test verifies eviction detection after the
	// manager is closed, which nils all frame data pointers.
	mgr, err := bufmgr.NewManager(bufmgr.ManagerConfig{
		MaxFrames:     4,
		FrameSize:     4096,
		EnableOffHeap: false,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	sc := NewSegmentCacheConsumer(mgr)
	key := SegmentCacheKey{SegmentID: "seg-evict", Column: "col", RowGroup: 0}
	if err := sc.Put(key, []byte("will be evicted")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Verify it is retrievable before close.
	frames, ok := sc.Get(key)
	if !ok {
		t.Fatal("Get before close should succeed")
	}
	for _, f := range frames {
		f.Unpin()
	}

	// Close the manager, which nils all frame data pointers.
	if err := mgr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Now DataSlice() returns nil for all frames, so Get should detect the miss.
	_, ok = sc.Get(key)
	if ok {
		t.Fatal("Get returned true after manager Close; expected cache miss due to nil DataSlice")
	}
}

func TestConcurrent_SegmentCacheConsumer_PutGet_NoRace(t *testing.T) {
	mgr := newTestManager(t, 128)
	sc := NewSegmentCacheConsumer(mgr)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				key := SegmentCacheKey{
					SegmentID: fmt.Sprintf("seg-%d", gid),
					Column:    fmt.Sprintf("col-%d", i),
					RowGroup:  0,
				}
				data := []byte(fmt.Sprintf("goroutine-%d-iter-%d", gid, i))
				_ = sc.Put(key, data)

				frames, ok := sc.Get(key)
				if ok {
					for _, f := range frames {
						f.Unpin()
					}
				}
			}
		}(g)
	}
	wg.Wait()
}

// MemtableFrameWriter tests

func TestUnit_MemtableFrameWriter_Append_SmallData_WritesCorrectly(t *testing.T) {
	mgr := newTestManager(t, 16)
	mw := NewMemtableFrameWriter(mgr)
	t.Cleanup(func() { mw.ReleaseAll() })

	data := []byte("event-line-001")
	ref, err := mw.Append(data)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if !ref.IsValid() {
		t.Fatal("Append returned invalid FrameRef")
	}
	if ref.Length != len(data) {
		t.Fatalf("FrameRef.Length = %d; want %d", ref.Length, len(data))
	}
	if ref.Offset != 0 {
		t.Fatalf("FrameRef.Offset = %d; want 0 (first write)", ref.Offset)
	}
}

func TestUnit_MemtableFrameWriter_Append_MultipleSmall_PacksIntoOneFrame(t *testing.T) {
	mgr := newTestManager(t, 16)
	mw := NewMemtableFrameWriter(mgr)
	t.Cleanup(func() { mw.ReleaseAll() })

	items := []string{"alpha", "bravo", "charlie", "delta"}
	expectedOffset := 0
	for _, item := range items {
		ref, err := mw.Append([]byte(item))
		if err != nil {
			t.Fatalf("Append(%q): %v", item, err)
		}
		if ref.Offset != expectedOffset {
			t.Errorf("Append(%q): offset = %d; want %d", item, ref.Offset, expectedOffset)
		}
		expectedOffset += len(item)
	}

	if mw.FrameCount() != 1 {
		t.Fatalf("FrameCount = %d; want 1 (all items fit in one frame)", mw.FrameCount())
	}
}

func TestUnit_MemtableFrameWriter_Append_ExceedsFrameCapacity_AllocatesNewFrame(t *testing.T) {
	mgr := newTestManager(t, 16)
	mw := NewMemtableFrameWriter(mgr)
	t.Cleanup(func() { mw.ReleaseAll() })

	frameSize := mgr.FrameSize() // 4096

	// Fill the first frame almost completely.
	filler := make([]byte, frameSize-100)
	if _, err := mw.Append(filler); err != nil {
		t.Fatalf("Append filler: %v", err)
	}

	if mw.FrameCount() != 1 {
		t.Fatalf("FrameCount after filler = %d; want 1", mw.FrameCount())
	}

	// This write exceeds the remaining 100 bytes, so a new frame must be allocated.
	overflow := make([]byte, 200)
	ref, err := mw.Append(overflow)
	if err != nil {
		t.Fatalf("Append overflow: %v", err)
	}

	if mw.FrameCount() != 2 {
		t.Fatalf("FrameCount after overflow = %d; want 2", mw.FrameCount())
	}

	// The overflow data should start at offset 0 in the new frame.
	if ref.Offset != 0 {
		t.Fatalf("overflow FrameRef.Offset = %d; want 0", ref.Offset)
	}
}

func TestUnit_MemtableFrameWriter_Append_DataLargerThanFrame_ReturnsError(t *testing.T) {
	mgr := newTestManager(t, 16)
	mw := NewMemtableFrameWriter(mgr)
	t.Cleanup(func() { mw.ReleaseAll() })

	frameSize := mgr.FrameSize() // 4096
	huge := make([]byte, frameSize+1)

	_, err := mw.Append(huge)
	if err == nil {
		t.Fatal("Append with data larger than frame size should return error")
	}
}

func TestUnit_MemtableFrameWriter_Append_EmptyData_ReturnsZeroRef(t *testing.T) {
	mgr := newTestManager(t, 8)
	mw := NewMemtableFrameWriter(mgr)
	t.Cleanup(func() { mw.ReleaseAll() })

	ref, err := mw.Append([]byte{})
	if err != nil {
		t.Fatalf("Append(empty): %v", err)
	}
	if ref.IsValid() {
		t.Fatal("Append of empty data should return invalid (zero) FrameRef")
	}
	if mw.FrameCount() != 0 {
		t.Fatalf("FrameCount after empty append = %d; want 0 (no frame allocated)", mw.FrameCount())
	}
}

func TestUnit_MemtableFrameWriter_FrameCount_IncrementsCorrectly(t *testing.T) {
	mgr := newTestManager(t, 16)
	mw := NewMemtableFrameWriter(mgr)
	t.Cleanup(func() { mw.ReleaseAll() })

	if mw.FrameCount() != 0 {
		t.Fatalf("initial FrameCount = %d; want 0", mw.FrameCount())
	}

	frameSize := mgr.FrameSize()
	full := make([]byte, frameSize)

	// Each full-frame write should allocate exactly one new frame.
	for i := 1; i <= 3; i++ {
		if _, err := mw.Append(full); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if mw.FrameCount() != i {
			t.Fatalf("FrameCount after %d full-frame appends = %d; want %d", i, mw.FrameCount(), i)
		}
	}
}

func TestUnit_MemtableFrameWriter_BytesWritten_TracksTotal(t *testing.T) {
	mgr := newTestManager(t, 16)
	mw := NewMemtableFrameWriter(mgr)
	t.Cleanup(func() { mw.ReleaseAll() })

	if mw.BytesWritten() != 0 {
		t.Fatalf("initial BytesWritten = %d; want 0", mw.BytesWritten())
	}

	frameSize := mgr.FrameSize() // 4096

	// Write a full frame.
	full := make([]byte, frameSize)
	if _, err := mw.Append(full); err != nil {
		t.Fatalf("Append full: %v", err)
	}
	if mw.BytesWritten() != int64(frameSize) {
		t.Fatalf("BytesWritten after full frame = %d; want %d", mw.BytesWritten(), frameSize)
	}

	// Write a partial frame.
	partial := make([]byte, 100)
	if _, err := mw.Append(partial); err != nil {
		t.Fatalf("Append partial: %v", err)
	}
	// BytesWritten = (numFrames-1)*frameSize + currentOffset
	// = 1*4096 + 100 = 4196
	want := int64(frameSize) + 100
	if mw.BytesWritten() != want {
		t.Fatalf("BytesWritten after full+partial = %d; want %d", mw.BytesWritten(), want)
	}
}

func TestUnit_MemtableFrameWriter_ReleaseAll_ResetsState(t *testing.T) {
	mgr := newTestManager(t, 16)
	mw := NewMemtableFrameWriter(mgr)

	for i := 0; i < 3; i++ {
		if _, err := mw.Append([]byte("event data")); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	if mw.FrameCount() == 0 {
		t.Fatal("FrameCount should be > 0 before ReleaseAll")
	}

	mw.ReleaseAll()

	if mw.FrameCount() != 0 {
		t.Fatalf("FrameCount after ReleaseAll = %d; want 0", mw.FrameCount())
	}
	if mw.BytesWritten() != 0 {
		t.Fatalf("BytesWritten after ReleaseAll = %d; want 0", mw.BytesWritten())
	}
}

func TestUnit_MemtableFrameWriter_ReleaseAll_CanAppendAgain(t *testing.T) {
	mgr := newTestManager(t, 16)
	mw := NewMemtableFrameWriter(mgr)

	if _, err := mw.Append([]byte("before release")); err != nil {
		t.Fatalf("Append before: %v", err)
	}
	mw.ReleaseAll()

	// Should be able to append after release.
	ref, err := mw.Append([]byte("after release"))
	if err != nil {
		t.Fatalf("Append after ReleaseAll: %v", err)
	}
	if !ref.IsValid() {
		t.Fatal("Append after ReleaseAll returned invalid FrameRef")
	}
	if ref.Offset != 0 {
		t.Fatalf("Append after ReleaseAll offset = %d; want 0 (fresh frame)", ref.Offset)
	}
	mw.ReleaseAll()
}

func TestUnit_MemtableFrameWriter_Append_FrameMarkedDirty(t *testing.T) {
	mgr := newTestManager(t, 16)
	mw := NewMemtableFrameWriter(mgr)
	t.Cleanup(func() { mw.ReleaseAll() })

	ref, err := mw.Append([]byte("dirty data"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Look up the frame via the manager to check its state.
	f := mgr.LookupFrame(ref.FrameID)
	if f == nil {
		t.Fatal("LookupFrame returned nil for the frame used by Append")
	}
	if !f.IsDirty() {
		t.Fatal("memtable frame should be marked dirty after allocation")
	}
}

func TestUnit_MemtableFrameWriter_Append_ReturnsValidFrameRef(t *testing.T) {
	mgr := newTestManager(t, 16)
	mw := NewMemtableFrameWriter(mgr)
	t.Cleanup(func() { mw.ReleaseAll() })

	data := []byte("verify-ref")
	ref, err := mw.Append(data)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// The FrameRef should allow reading back the data from the frame.
	f := mgr.LookupFrame(ref.FrameID)
	if f == nil {
		t.Fatal("LookupFrame returned nil")
	}

	ds := f.DataSlice()
	if ds == nil {
		t.Fatal("DataSlice is nil")
	}

	got := ds[ref.Offset : ref.Offset+ref.Length]
	if !bytes.Equal(got, data) {
		t.Fatalf("data at FrameRef = %q; want %q", got, data)
	}
}

func TestConcurrent_MemtableFrameWriter_Append_NoRace(t *testing.T) {
	mgr := newTestManager(t, 128)
	mw := NewMemtableFrameWriter(mgr)
	t.Cleanup(func() { mw.ReleaseAll() })

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				data := []byte(fmt.Sprintf("g%d-i%d", gid, i))
				_, _ = mw.Append(data)
			}
		}(g)
	}
	wg.Wait()

	// All 160 appends should have been tracked.
	if mw.BytesWritten() <= 0 {
		t.Fatal("BytesWritten is 0 after concurrent appends")
	}
}

// QueryOperatorAllocator tests

func TestUnit_QueryOperatorAllocator_AllocFrame_ReturnsPinnedFrame(t *testing.T) {
	mgr := newTestManager(t, 16)
	alloc := NewQueryOperatorAllocator(mgr, "job-001")
	t.Cleanup(func() { alloc.ReleaseAll() })

	f, err := alloc.AllocFrame()
	if err != nil {
		t.Fatalf("AllocFrame: %v", err)
	}
	if f == nil {
		t.Fatal("AllocFrame returned nil")
	}
	if !f.IsPinned() {
		t.Fatal("AllocFrame returned unpinned frame")
	}
	if f.Owner != bufmgr.OwnerQuery {
		t.Fatalf("frame owner = %s; want query", f.Owner)
	}
}

func TestUnit_QueryOperatorAllocator_FrameCount_TracksAllocations(t *testing.T) {
	mgr := newTestManager(t, 16)
	alloc := NewQueryOperatorAllocator(mgr, "job-002")
	t.Cleanup(func() { alloc.ReleaseAll() })

	if alloc.FrameCount() != 0 {
		t.Fatalf("initial FrameCount = %d; want 0", alloc.FrameCount())
	}

	for i := 1; i <= 5; i++ {
		if _, err := alloc.AllocFrame(); err != nil {
			t.Fatalf("AllocFrame %d: %v", i, err)
		}
		if alloc.FrameCount() != i {
			t.Fatalf("FrameCount after %d allocs = %d; want %d", i, alloc.FrameCount(), i)
		}
	}
}

func TestUnit_QueryOperatorAllocator_ReleaseAll_FreesAllFrames(t *testing.T) {
	mgr := newTestManager(t, 16)
	alloc := NewQueryOperatorAllocator(mgr, "job-003")

	frames := make([]*bufmgr.Frame, 4)
	for i := 0; i < 4; i++ {
		f, err := alloc.AllocFrame()
		if err != nil {
			t.Fatalf("AllocFrame: %v", err)
		}
		frames[i] = f
	}

	alloc.ReleaseAll()

	if alloc.FrameCount() != 0 {
		t.Fatalf("FrameCount after ReleaseAll = %d; want 0", alloc.FrameCount())
	}

	// All frames should be unpinned after release.
	for i, f := range frames {
		if f.IsPinned() {
			t.Errorf("frame[%d] is still pinned after ReleaseAll", i)
		}
	}
}

func TestUnit_QueryOperatorAllocator_ReleaseLast_LIFO_Order(t *testing.T) {
	mgr := newTestManager(t, 16)
	alloc := NewQueryOperatorAllocator(mgr, "job-004")
	t.Cleanup(func() { alloc.ReleaseAll() })

	frames := make([]*bufmgr.Frame, 5)
	for i := 0; i < 5; i++ {
		f, err := alloc.AllocFrame()
		if err != nil {
			t.Fatalf("AllocFrame %d: %v", i, err)
		}
		frames[i] = f
	}

	// Release the last 2.
	alloc.ReleaseLast(2)

	if alloc.FrameCount() != 3 {
		t.Fatalf("FrameCount after ReleaseLast(2) = %d; want 3", alloc.FrameCount())
	}

	// The last 2 frames should be unpinned.
	if frames[3].IsPinned() {
		t.Error("frames[3] (released) should not be pinned")
	}
	if frames[4].IsPinned() {
		t.Error("frames[4] (released) should not be pinned")
	}

	// The first 3 frames should still be pinned.
	for i := 0; i < 3; i++ {
		if !frames[i].IsPinned() {
			t.Errorf("frames[%d] (kept) should still be pinned", i)
		}
	}
}

func TestUnit_QueryOperatorAllocator_ReleaseLast_MoreThanAvailable_ReleasesAll(t *testing.T) {
	mgr := newTestManager(t, 16)
	alloc := NewQueryOperatorAllocator(mgr, "job-005")

	for i := 0; i < 3; i++ {
		if _, err := alloc.AllocFrame(); err != nil {
			t.Fatalf("AllocFrame: %v", err)
		}
	}

	// Releasing more than we have should release all without panicking.
	alloc.ReleaseLast(100)

	if alloc.FrameCount() != 0 {
		t.Fatalf("FrameCount after ReleaseLast(100) from 3 = %d; want 0", alloc.FrameCount())
	}
}

func TestUnit_QueryOperatorAllocator_ReleaseLast_Zero_NoOp(t *testing.T) {
	mgr := newTestManager(t, 16)
	alloc := NewQueryOperatorAllocator(mgr, "job-006")
	t.Cleanup(func() { alloc.ReleaseAll() })

	if _, err := alloc.AllocFrame(); err != nil {
		t.Fatalf("AllocFrame: %v", err)
	}

	alloc.ReleaseLast(0)

	if alloc.FrameCount() != 1 {
		t.Fatalf("FrameCount after ReleaseLast(0) = %d; want 1", alloc.FrameCount())
	}
}

func TestUnit_QueryOperatorAllocator_Manager_ReturnsUnderlying(t *testing.T) {
	mgr := newTestManager(t, 8)
	alloc := NewQueryOperatorAllocator(mgr, "job-007")

	if alloc.Manager() != mgr {
		t.Fatal("Manager() returned a different manager instance")
	}
}

func TestConcurrent_QueryOperatorAllocator_AllocAndRelease_NoRace(t *testing.T) {
	mgr := newTestManager(t, 128)
	alloc := NewQueryOperatorAllocator(mgr, "job-concurrent")

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				_, _ = alloc.AllocFrame()
			}
		}()
	}
	wg.Wait()

	alloc.ReleaseAll()

	if alloc.FrameCount() != 0 {
		t.Fatalf("FrameCount after concurrent alloc + ReleaseAll = %d; want 0", alloc.FrameCount())
	}
}

// FrameHashTable tests

func TestUnit_FrameHashTable_PutGet_BasicKeyValue(t *testing.T) {
	mgr := newTestManager(t, 32)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-001")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	key := []byte("status")
	value := []byte("active")
	hash := uint64(42)

	ref, err := ht.Put(hash, key, value)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !ref.IsValid() {
		t.Fatal("Put returned invalid FrameRef")
	}
	if ref.Length != len(value) {
		t.Fatalf("Put ref.Length = %d; want %d", ref.Length, len(value))
	}

	gotRef, ok := ht.Get(hash, key)
	if !ok {
		t.Fatal("Get returned false for a key that was just Put")
	}
	if gotRef.Length != len(value) {
		t.Fatalf("Get ref.Length = %d; want %d", gotRef.Length, len(value))
	}

	// Read back the actual value from the frame.
	f := mgr.LookupFrame(gotRef.FrameID)
	if f == nil {
		t.Fatal("LookupFrame returned nil")
	}
	ds := f.DataSlice()
	got := ds[gotRef.Offset : gotRef.Offset+gotRef.Length]
	if !bytes.Equal(got, value) {
		t.Fatalf("value at FrameRef = %q; want %q", got, value)
	}
}

func TestUnit_FrameHashTable_PutGet_MultipleEntries_SameHash_CollisionHandling(t *testing.T) {
	mgr := newTestManager(t, 32)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-002")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	// Use the same hash for different keys to force collision handling.
	hash := uint64(99)
	entries := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key-alpha"), []byte("val-alpha")},
		{[]byte("key-bravo"), []byte("val-bravo")},
		{[]byte("key-charlie"), []byte("val-charlie")},
	}

	for _, e := range entries {
		if _, err := ht.Put(hash, e.key, e.value); err != nil {
			t.Fatalf("Put(%s): %v", e.key, err)
		}
	}

	if ht.Len() != 3 {
		t.Fatalf("Len after 3 colliding puts = %d; want 3", ht.Len())
	}

	// All keys should be retrievable.
	for _, e := range entries {
		ref, ok := ht.Get(hash, e.key)
		if !ok {
			t.Errorf("Get(%s) returned false; want true", e.key)
			continue
		}
		f := mgr.LookupFrame(ref.FrameID)
		ds := f.DataSlice()
		got := ds[ref.Offset : ref.Offset+ref.Length]
		if !bytes.Equal(got, e.value) {
			t.Errorf("Get(%s) value = %q; want %q", e.key, got, e.value)
		}
	}
}

func TestUnit_FrameHashTable_Put_UpdateValue_SameSize_Succeeds(t *testing.T) {
	mgr := newTestManager(t, 32)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-003")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	key := []byte("counter")
	hash := uint64(7)
	original := []byte("0001")
	updated := []byte("0002")

	if _, err := ht.Put(hash, key, original); err != nil {
		t.Fatalf("Put original: %v", err)
	}

	ref, err := ht.Put(hash, key, updated)
	if err != nil {
		t.Fatalf("Put update: %v", err)
	}

	// Entry count should not increase; this is an update, not an insert.
	if ht.Len() != 1 {
		t.Fatalf("Len after update = %d; want 1", ht.Len())
	}

	// Value should be updated.
	f := mgr.LookupFrame(ref.FrameID)
	ds := f.DataSlice()
	got := ds[ref.Offset : ref.Offset+ref.Length]
	if !bytes.Equal(got, updated) {
		t.Fatalf("value after update = %q; want %q", got, updated)
	}
}

func TestUnit_FrameHashTable_Put_UpdateValue_DifferentSize_ReturnsError(t *testing.T) {
	mgr := newTestManager(t, 32)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-004")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	key := []byte("counter")
	hash := uint64(7)

	if _, err := ht.Put(hash, key, []byte("short")); err != nil {
		t.Fatalf("Put original: %v", err)
	}

	_, err := ht.Put(hash, key, []byte("much-longer-value"))
	if err == nil {
		t.Fatal("Put with different-size value should return error")
	}
}

func TestUnit_FrameHashTable_Get_Miss_ReturnsFalse(t *testing.T) {
	mgr := newTestManager(t, 16)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-005")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	// Miss on an empty table.
	_, ok := ht.Get(uint64(1), []byte("nonexistent"))
	if ok {
		t.Fatal("Get on empty table returned true")
	}

	// Miss on a populated table with wrong hash.
	if _, err := ht.Put(uint64(10), []byte("exists"), []byte("val")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	_, ok = ht.Get(uint64(11), []byte("exists"))
	if ok {
		t.Fatal("Get with wrong hash returned true")
	}

	// Miss on a populated table with correct hash but wrong key.
	_, ok = ht.Get(uint64(10), []byte("wrong-key"))
	if ok {
		t.Fatal("Get with correct hash but wrong key returned true")
	}
}

func TestUnit_FrameHashTable_Len_CountsEntries(t *testing.T) {
	mgr := newTestManager(t, 32)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-006")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	if ht.Len() != 0 {
		t.Fatalf("initial Len = %d; want 0", ht.Len())
	}

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		val := []byte(fmt.Sprintf("val-%d", i))
		if _, err := ht.Put(uint64(i), key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	if ht.Len() != 10 {
		t.Fatalf("Len after 10 puts = %d; want 10", ht.Len())
	}
}

func TestUnit_FrameHashTable_FrameCount_TracksUsage(t *testing.T) {
	mgr := newTestManager(t, 32)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-007")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	if ht.FrameCount() != 0 {
		t.Fatalf("initial FrameCount = %d; want 0", ht.FrameCount())
	}

	// First put should allocate a frame.
	if _, err := ht.Put(uint64(1), []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if ht.FrameCount() < 1 {
		t.Fatalf("FrameCount after first Put = %d; want >= 1", ht.FrameCount())
	}
}

func TestUnit_FrameHashTable_FrameCount_GrowsWithData(t *testing.T) {
	mgr := newTestManager(t, 64)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-008")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	frameSize := mgr.FrameSize() // 4096
	// Each entry has 16 bytes header + key + value.
	// Use large values to force frame allocation.
	largeVal := make([]byte, frameSize/2) // ~2KB per value

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		if _, err := ht.Put(uint64(i), key, largeVal); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	if ht.FrameCount() < 2 {
		t.Fatalf("FrameCount with large values = %d; expected > 1", ht.FrameCount())
	}
}

func TestUnit_FrameHashTable_ForEach_IteratesAllEntries(t *testing.T) {
	mgr := newTestManager(t, 32)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-009")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	expected := map[string]string{
		"alpha":   "val-a",
		"bravo":   "val-b",
		"charlie": "val-c",
	}

	for k, v := range expected {
		hash := uint64(len(k)) // simple hash for test
		if _, err := ht.Put(hash, []byte(k), []byte(v)); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}

	visited := make(map[string]string)
	ht.ForEach(func(hash uint64, key []byte, valRef bufmgr.FrameRef) bool {
		f := mgr.LookupFrame(valRef.FrameID)
		ds := f.DataSlice()
		val := string(ds[valRef.Offset : valRef.Offset+valRef.Length])
		visited[string(key)] = val
		return true
	})

	if len(visited) != len(expected) {
		t.Fatalf("ForEach visited %d entries; want %d", len(visited), len(expected))
	}
	for k, v := range expected {
		got, ok := visited[k]
		if !ok {
			t.Errorf("ForEach did not visit key %q", k)
			continue
		}
		if got != v {
			t.Errorf("ForEach value for %q = %q; want %q", k, got, v)
		}
	}
}

func TestUnit_FrameHashTable_ForEach_StopsOnFalse(t *testing.T) {
	mgr := newTestManager(t, 32)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-010")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	// Use different hashes so iteration order is determined by map iteration.
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("k%d", i))
		if _, err := ht.Put(uint64(i*1000), key, []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	count := 0
	ht.ForEach(func(hash uint64, key []byte, valRef bufmgr.FrameRef) bool {
		count++
		return count < 3 // stop after 3
	})

	if count != 3 {
		t.Fatalf("ForEach with early stop visited %d entries; want 3", count)
	}
}

func TestUnit_FrameHashTable_ForEach_EmptyTable_DoesNothing(t *testing.T) {
	mgr := newTestManager(t, 16)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-011")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	count := 0
	ht.ForEach(func(hash uint64, key []byte, valRef bufmgr.FrameRef) bool {
		count++
		return true
	})

	if count != 0 {
		t.Fatalf("ForEach on empty table visited %d entries; want 0", count)
	}
}

func TestUnit_FrameHashTable_Clear_ResetsAllState(t *testing.T) {
	mgr := newTestManager(t, 32)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-012")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("k%d", i))
		if _, err := ht.Put(uint64(i), key, []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	if ht.Len() != 5 {
		t.Fatalf("Len before Clear = %d; want 5", ht.Len())
	}

	ht.Clear()

	if ht.Len() != 0 {
		t.Fatalf("Len after Clear = %d; want 0", ht.Len())
	}
	if ht.FrameCount() != 0 {
		t.Fatalf("FrameCount after Clear = %d; want 0", ht.FrameCount())
	}

	// Should be able to Put again after Clear.
	_, err := ht.Put(uint64(1), []byte("new-key"), []byte("new-val"))
	if err != nil {
		t.Fatalf("Put after Clear: %v", err)
	}
	if ht.Len() != 1 {
		t.Fatalf("Len after Put-after-Clear = %d; want 1", ht.Len())
	}
}

func TestUnit_FrameHashTable_Get_AfterClear_ReturnsFalse(t *testing.T) {
	mgr := newTestManager(t, 16)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-013")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	hash := uint64(42)
	key := []byte("ephemeral")
	if _, err := ht.Put(hash, key, []byte("val")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	ht.Clear()

	_, ok := ht.Get(hash, key)
	if ok {
		t.Fatal("Get returned true after Clear")
	}
}

func TestUnit_FrameHashTable_Put_DifferentHashesSameKey_CreatesDistinctEntries(t *testing.T) {
	mgr := newTestManager(t, 32)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-014")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	key := []byte("same-key")

	if _, err := ht.Put(uint64(1), key, []byte("val-h1")); err != nil {
		t.Fatalf("Put hash 1: %v", err)
	}
	if _, err := ht.Put(uint64(2), key, []byte("val-h2")); err != nil {
		t.Fatalf("Put hash 2: %v", err)
	}

	// Each hash bucket has its own entry for the same key bytes.
	if ht.Len() != 2 {
		t.Fatalf("Len = %d; want 2 (different hashes create distinct entries)", ht.Len())
	}

	ref1, ok1 := ht.Get(uint64(1), key)
	ref2, ok2 := ht.Get(uint64(2), key)
	if !ok1 || !ok2 {
		t.Fatal("Get failed for one of the hash-distinct entries")
	}

	f1 := mgr.LookupFrame(ref1.FrameID)
	ds1 := f1.DataSlice()
	got1 := string(ds1[ref1.Offset : ref1.Offset+ref1.Length])

	f2 := mgr.LookupFrame(ref2.FrameID)
	ds2 := f2.DataSlice()
	got2 := string(ds2[ref2.Offset : ref2.Offset+ref2.Length])

	if got1 != "val-h1" {
		t.Errorf("value for hash 1 = %q; want %q", got1, "val-h1")
	}
	if got2 != "val-h2" {
		t.Errorf("value for hash 2 = %q; want %q", got2, "val-h2")
	}
}

func TestUnit_FrameHashTable_Put_EntryExceedsFrameSize_ReturnsError(t *testing.T) {
	mgr := newTestManager(t, 16)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-015")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	frameSize := mgr.FrameSize() // 4096
	// entryHeaderSize = 16, so key+value must exceed 4096-16 = 4080
	hugeVal := make([]byte, frameSize)

	_, err := ht.Put(uint64(1), []byte("k"), hugeVal)
	if err == nil {
		t.Fatal("Put with entry larger than frame should return error")
	}
}

func TestUnit_FrameHashTable_Put_EmptyKeyAndValue(t *testing.T) {
	mgr := newTestManager(t, 16)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-016")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	// Empty key and empty value should work (the entry is just a header).
	ref, err := ht.Put(uint64(0), []byte{}, []byte{})
	if err != nil {
		t.Fatalf("Put with empty key/value: %v", err)
	}

	// The ref length is 0, so IsValid returns false, but the entry still exists.
	_ = ref
	if ht.Len() != 1 {
		t.Fatalf("Len after put with empty key/value = %d; want 1", ht.Len())
	}

	_, ok := ht.Get(uint64(0), []byte{})
	if !ok {
		t.Fatal("Get with empty key returned false")
	}
}

func TestUnit_FrameHashTable_Put_ManyEntries_SpansMultipleFrames(t *testing.T) {
	mgr := newTestManager(t, 64)
	alloc := NewQueryOperatorAllocator(mgr, "ht-job-017")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	// entryHeaderSize(16) + key(~6) + value(100) = ~122 bytes per entry
	// 4096 / 122 ~= 33 entries per frame.
	// 100 entries should require at least 3 frames.
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		val := make([]byte, 100)
		val[0] = byte(i)
		if _, err := ht.Put(uint64(i), key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	if ht.Len() != 100 {
		t.Fatalf("Len = %d; want 100", ht.Len())
	}
	if ht.FrameCount() < 3 {
		t.Fatalf("FrameCount = %d; expected >= 3 for 100 entries", ht.FrameCount())
	}

	// Verify all entries are retrievable.
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		ref, ok := ht.Get(uint64(i), key)
		if !ok {
			t.Fatalf("Get(%s) returned false", key)
		}
		f := mgr.LookupFrame(ref.FrameID)
		ds := f.DataSlice()
		if ds[ref.Offset] != byte(i) {
			t.Fatalf("Get(%s) value[0] = %d; want %d", key, ds[ref.Offset], byte(i))
		}
	}
}

// Integration: FrameHashTable uses QueryOperatorAllocator

func TestIntegration_FrameHashTable_OwnerIsQuery(t *testing.T) {
	mgr := newTestManager(t, 32)
	alloc := NewQueryOperatorAllocator(mgr, "ht-integration")
	t.Cleanup(func() { alloc.ReleaseAll() })
	ht := NewFrameHashTable(alloc)

	if _, err := ht.Put(uint64(1), []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Verify that the allocator tracked the frame.
	if alloc.FrameCount() != ht.FrameCount() {
		t.Fatalf("allocator FrameCount %d != hash table FrameCount %d",
			alloc.FrameCount(), ht.FrameCount())
	}

	// Verify that the underlying manager sees the frames as OwnerQuery.
	stats := mgr.Stats()
	if stats.QueryFrames < ht.FrameCount() {
		t.Fatalf("Manager stats QueryFrames %d < ht FrameCount %d",
			stats.QueryFrames, ht.FrameCount())
	}
}

// Integration: SegmentCacheConsumer + QueryOperatorAllocator memory pressure

func TestIntegration_SegmentCache_UnderMemoryPressure_QueryAllocSucceeds(t *testing.T) {
	// Small pool: 8 frames total. Fill it with cache entries (unpinned),
	// then allocate query frames to force eviction of cache frames.
	// This verifies that segment cache frames can be reclaimed under pressure.
	mgr := newTestManager(t, 8)
	sc := NewSegmentCacheConsumer(mgr)
	alloc := NewQueryOperatorAllocator(mgr, "query-pressure")
	t.Cleanup(func() { alloc.ReleaseAll() })

	// Fill all 8 frames with cache entries (Put unpins them, making them evictable).
	for i := 0; i < 8; i++ {
		key := SegmentCacheKey{SegmentID: "seg", Column: fmt.Sprintf("col-%d", i), RowGroup: 0}
		if err := sc.Put(key, []byte("cached")); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if sc.EntryCount() != 8 {
		t.Fatalf("cache EntryCount = %d; want 8", sc.EntryCount())
	}

	// All 8 frames are used by cache but unpinned. The pool has zero free frames.
	stats := mgr.Stats()
	if stats.FreeFrames != 0 {
		t.Fatalf("FreeFrames = %d; want 0 (all used by cache)", stats.FreeFrames)
	}

	// Allocating query frames must succeed by evicting cache frames.
	for i := 0; i < 4; i++ {
		f, err := alloc.AllocFrame()
		if err != nil {
			t.Fatalf("AllocFrame under pressure %d: %v -- cache frames should be evictable", i, err)
		}
		if f.Owner != bufmgr.OwnerQuery {
			t.Errorf("frame[%d] owner = %s; want query", i, f.Owner)
		}
	}

	// Manager should report evictions occurred.
	stats = mgr.Stats()
	if stats.EvictionCount < 4 {
		t.Fatalf("EvictionCount = %d; want >= 4 (evicted cache frames)", stats.EvictionCount)
	}
	if stats.QueryFrames < 4 {
		t.Fatalf("QueryFrames = %d; want >= 4", stats.QueryFrames)
	}
}

func TestIntegration_SegmentCache_PutAfterInvalidate_WorksCleanly(t *testing.T) {
	mgr := newTestManager(t, 16)
	sc := NewSegmentCacheConsumer(mgr)

	key := SegmentCacheKey{SegmentID: "seg-reuse", Column: "message", RowGroup: 0}

	// Put -> Invalidate -> Put cycle should work without leaking frames.
	for cycle := 0; cycle < 5; cycle++ {
		data := []byte(fmt.Sprintf("cycle-%d", cycle))
		if err := sc.Put(key, data); err != nil {
			t.Fatalf("Put cycle %d: %v", cycle, err)
		}
		sc.Invalidate(key)
	}

	// Final put.
	finalData := []byte("final")
	if err := sc.Put(key, finalData); err != nil {
		t.Fatalf("Put final: %v", err)
	}
	frames, ok := sc.Get(key)
	if !ok {
		t.Fatal("Get after final Put returned false")
	}
	got := readFrameData(t, frames, len(finalData))
	if !bytes.Equal(got, finalData) {
		t.Fatalf("final data = %q; want %q", got, finalData)
	}
	for _, f := range frames {
		f.Unpin()
	}
}
