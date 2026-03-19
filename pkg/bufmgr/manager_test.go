package bufmgr

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// newTestManager creates a Manager with heap-backed frames for test portability.
func newTestManager(t *testing.T, maxFrames int) Manager {
	t.Helper()

	m, err := NewManager(ManagerConfig{
		MaxFrames:     maxFrames,
		FrameSize:     4096,
		EnableOffHeap: false,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	return m
}

func TestUnit_Manager_AllocFrame_ReturnsPinnedCleanFrame(t *testing.T) {
	m := newTestManager(t, 8)

	f, err := m.AllocFrame(OwnerSegCache, "seg-001")
	if err != nil {
		t.Fatalf("AllocFrame: %v", err)
	}

	if f == nil {
		t.Fatal("AllocFrame returned nil frame")
	}
	if !f.IsPinned() {
		t.Error("allocated frame is not pinned")
	}
	if FrameState(f.State.Load()) != StateClean {
		t.Errorf("allocated frame state = %s; want clean", FrameState(f.State.Load()))
	}
	if f.Owner != OwnerSegCache {
		t.Errorf("allocated frame owner = %s; want seg-cache", f.Owner)
	}
	if f.Tag != "seg-001" {
		t.Errorf("allocated frame tag = %q; want %q", f.Tag, "seg-001")
	}
	if f.ID == 0 {
		t.Error("allocated frame has zero ID")
	}
}

func TestUnit_Manager_AllocFrame_ExhaustsFreelist_TriggersEviction(t *testing.T) {
	m := newTestManager(t, 4)

	// Allocate all 4 frames and unpin them so they can be evicted.
	for i := 0; i < 4; i++ {
		f, err := m.AllocFrame(OwnerSegCache, "seg")
		if err != nil {
			t.Fatalf("AllocFrame %d: %v", i, err)
		}
		m.UnpinFrame(f.ID)
	}

	// Free list is now empty. The next alloc must evict.
	f, err := m.AllocFrame(OwnerQuery, "query-1")
	if err != nil {
		t.Fatalf("AllocFrame after freelist exhausted: %v", err)
	}
	if f == nil {
		t.Fatal("AllocFrame returned nil after eviction")
	}
	if f.Owner != OwnerQuery {
		t.Errorf("evicted+reallocated frame owner = %s; want query", f.Owner)
	}
}

func TestUnit_Manager_AllocFrame_AllPinned_ReturnsError(t *testing.T) {
	m := newTestManager(t, 4)

	// Allocate all frames and keep them pinned.
	for i := 0; i < 4; i++ {
		_, err := m.AllocFrame(OwnerSegCache, "seg")
		if err != nil {
			t.Fatalf("AllocFrame %d: %v", i, err)
		}
		// Do not unpin — all frames remain pinned.
	}

	// The next alloc must fail because nothing is evictable.
	_, err := m.AllocFrame(OwnerQuery, "query-1")
	if err == nil {
		t.Fatal("AllocFrame succeeded when all frames are pinned; want ErrAllFramesPinned")
	}
	if !errors.Is(err, ErrAllFramesPinned) {
		t.Fatalf("error = %v; want ErrAllFramesPinned", err)
	}
}

func TestUnit_Manager_AllocFrame_AfterClose_ReturnsError(t *testing.T) {
	m := newTestManager(t, 4)
	_ = m.Close()

	_, err := m.AllocFrame(OwnerSegCache, "seg-001")
	if err == nil {
		t.Fatal("AllocFrame on closed manager should return error")
	}
}

func TestUnit_Manager_PinFrame_IncrementsPinCount(t *testing.T) {
	m := newTestManager(t, 4)

	f, _ := m.AllocFrame(OwnerSegCache, "seg-001")
	// AllocFrame already pins once, so PinCount = 1.
	initialPin := f.PinCount.Load()

	err := m.PinFrame(f.ID)
	if err != nil {
		t.Fatalf("PinFrame: %v", err)
	}

	if f.PinCount.Load() != initialPin+1 {
		t.Fatalf("PinCount after PinFrame = %d; want %d", f.PinCount.Load(), initialPin+1)
	}
}

func TestUnit_Manager_PinFrame_InvalidID_ReturnsError(t *testing.T) {
	m := newTestManager(t, 4)

	err := m.PinFrame(FrameID(9999))
	if err == nil {
		t.Fatal("PinFrame with invalid ID should return error")
	}
}

func TestUnit_Manager_UnpinFrame_DecrementsPinCount(t *testing.T) {
	m := newTestManager(t, 4)

	f, _ := m.AllocFrame(OwnerSegCache, "seg-001")

	if !f.IsPinned() {
		t.Fatal("frame should be pinned after AllocFrame")
	}

	m.UnpinFrame(f.ID)

	if f.IsPinned() {
		t.Fatal("frame should not be pinned after UnpinFrame")
	}
}

func TestUnit_Manager_MarkDirty_CleanToDirty(t *testing.T) {
	m := newTestManager(t, 4)

	f, _ := m.AllocFrame(OwnerSegCache, "seg-001")

	if FrameState(f.State.Load()) != StateClean {
		t.Fatalf("initial state = %s; want clean", FrameState(f.State.Load()))
	}

	m.MarkDirty(f.ID)

	if FrameState(f.State.Load()) != StateDirty {
		t.Fatalf("state after MarkDirty = %s; want dirty", FrameState(f.State.Load()))
	}
}

func TestUnit_Manager_MarkDirty_NonCleanFrame_NoChange(t *testing.T) {
	m := newTestManager(t, 4)

	f, _ := m.AllocFrame(OwnerSegCache, "seg-001")

	// Manually put frame into StateFree. MarkDirty should not transition it.
	f.State.Store(int32(StateFree))
	m.MarkDirty(f.ID)
	if FrameState(f.State.Load()) != StateFree {
		t.Fatalf("MarkDirty changed non-clean frame state to %s; want free (unchanged)", FrameState(f.State.Load()))
	}

	// StateDirty -> MarkDirty should be a no-op (stays dirty).
	f.State.Store(int32(StateDirty))
	m.MarkDirty(f.ID)
	if FrameState(f.State.Load()) != StateDirty {
		t.Fatalf("MarkDirty changed dirty frame state; expected no change")
	}
}

func TestUnit_Manager_EvictBatch_ReturnsEvictedCount(t *testing.T) {
	m := newTestManager(t, 8)

	for i := 0; i < 8; i++ {
		f, _ := m.AllocFrame(OwnerSegCache, "seg")
		m.UnpinFrame(f.ID)
	}

	evicted := m.EvictBatch(3, OwnerFree)
	if evicted != 3 {
		t.Fatalf("EvictBatch(3) = %d; want 3", evicted)
	}
}

func TestUnit_Manager_EvictBatch_WritesBackDirtyFrames(t *testing.T) {
	var writebackCalls atomic.Int32

	mgr, err := NewManager(ManagerConfig{
		MaxFrames:     4,
		FrameSize:     4096,
		EnableOffHeap: false,
		WriteBackFunc: func(f *Frame) error {
			writebackCalls.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	// Allocate, mark dirty, unpin.
	for i := 0; i < 4; i++ {
		f, _ := mgr.AllocFrame(OwnerSegCache, "seg")
		mgr.MarkDirty(f.ID)
		mgr.UnpinFrame(f.ID)
	}

	evicted := mgr.EvictBatch(4, OwnerFree)
	if evicted != 4 {
		t.Fatalf("EvictBatch = %d; want 4", evicted)
	}
	if writebackCalls.Load() != 4 {
		t.Fatalf("writeback called %d times; want 4", writebackCalls.Load())
	}
}

func TestUnit_Manager_Stats_ReflectsAllocAndEvict(t *testing.T) {
	m := newTestManager(t, 8)

	// Initially all frames are free.
	stats := m.Stats()
	if stats.TotalFrames != 8 {
		t.Fatalf("TotalFrames = %d; want 8", stats.TotalFrames)
	}
	if stats.FreeFrames != 8 {
		t.Fatalf("initial FreeFrames = %d; want 8", stats.FreeFrames)
	}

	// Allocate 3 frames.
	frames := make([]*Frame, 3)
	for i := 0; i < 3; i++ {
		f, _ := m.AllocFrame(OwnerQuery, "query")
		frames[i] = f
	}

	stats = m.Stats()
	if stats.FreeFrames != 5 {
		t.Fatalf("FreeFrames after 3 allocs = %d; want 5", stats.FreeFrames)
	}
	if stats.HitCount != 3 {
		t.Fatalf("HitCount = %d; want 3", stats.HitCount)
	}

	// Unpin and evict 2.
	for i := 0; i < 2; i++ {
		m.UnpinFrame(frames[i].ID)
	}
	evicted := m.EvictBatch(2, OwnerFree)
	if evicted != 2 {
		t.Fatalf("EvictBatch = %d; want 2", evicted)
	}

	stats = m.Stats()
	if stats.EvictionCount != 2 {
		t.Fatalf("EvictionCount = %d; want 2", stats.EvictionCount)
	}
	if stats.FreeFrames != 7 {
		t.Fatalf("FreeFrames after eviction = %d; want 7", stats.FreeFrames)
	}
}

func TestUnit_Manager_Stats_OwnerCounts(t *testing.T) {
	m := newTestManager(t, 16)

	// Allocate frames for different owners.
	for i := 0; i < 3; i++ {
		m.AllocFrame(OwnerSegCache, "seg")
	}
	for i := 0; i < 2; i++ {
		m.AllocFrame(OwnerQuery, "q")
	}
	m.AllocFrame(OwnerMemtable, "mt")

	stats := m.Stats()
	if stats.SegCacheFrames != 3 {
		t.Errorf("SegCacheFrames = %d; want 3", stats.SegCacheFrames)
	}
	if stats.QueryFrames != 2 {
		t.Errorf("QueryFrames = %d; want 2", stats.QueryFrames)
	}
	if stats.MemtableFrames != 1 {
		t.Errorf("MemtableFrames = %d; want 1", stats.MemtableFrames)
	}
}

func TestUnit_Manager_Close_ReleasesMemory(t *testing.T) {
	mgr, err := NewManager(ManagerConfig{
		MaxFrames:     4,
		FrameSize:     4096,
		EnableOffHeap: false,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	err = mgr.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After close, AllocFrame should fail.
	_, err = mgr.AllocFrame(OwnerSegCache, "seg")
	if err == nil {
		t.Fatal("AllocFrame after Close should fail")
	}
}

func TestUnit_Manager_Close_Idempotent(t *testing.T) {
	m := newTestManager(t, 4)

	err1 := m.Close()
	err2 := m.Close()

	if err1 != nil {
		t.Fatalf("first Close: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("second Close: %v", err2)
	}
}

func TestUnit_Manager_PinFrameIfOwned_MatchingTag_PinsAndReturns(t *testing.T) {
	m := newTestManager(t, 4)

	f, _ := m.AllocFrame(OwnerSegCache, "seg-xyz")
	m.UnpinFrame(f.ID)

	got, ok := m.PinFrameIfOwned(f.ID, "seg-xyz")
	if !ok {
		t.Fatal("PinFrameIfOwned with matching tag returned false")
	}
	if got == nil {
		t.Fatal("PinFrameIfOwned returned nil frame")
	}
	if !got.IsPinned() {
		t.Fatal("frame is not pinned after PinFrameIfOwned")
	}
	if got.ID != f.ID {
		t.Fatalf("returned frame ID = %d; want %d", got.ID, f.ID)
	}
}

func TestUnit_Manager_PinFrameIfOwned_WrongTag_ReturnsFalse(t *testing.T) {
	m := newTestManager(t, 4)

	f, _ := m.AllocFrame(OwnerSegCache, "seg-abc")
	m.UnpinFrame(f.ID)

	_, ok := m.PinFrameIfOwned(f.ID, "seg-wrong")
	if ok {
		t.Fatal("PinFrameIfOwned with wrong tag returned true; want false")
	}
}

func TestUnit_Manager_PinFrameIfOwned_InvalidID_ReturnsFalse(t *testing.T) {
	m := newTestManager(t, 4)

	_, ok := m.PinFrameIfOwned(FrameID(9999), "anything")
	if ok {
		t.Fatal("PinFrameIfOwned with invalid ID returned true; want false")
	}
}

func TestUnit_Manager_LookupFrame_ValidID_ReturnsFrame(t *testing.T) {
	m := newTestManager(t, 4)

	f, _ := m.AllocFrame(OwnerSegCache, "seg-001")

	got := m.LookupFrame(f.ID)
	if got == nil {
		t.Fatal("LookupFrame returned nil for valid ID")
	}
	if got.ID != f.ID {
		t.Fatalf("LookupFrame returned frame %d; want %d", got.ID, f.ID)
	}
}

func TestUnit_Manager_LookupFrame_InvalidID_ReturnsNil(t *testing.T) {
	m := newTestManager(t, 4)

	got := m.LookupFrame(FrameID(9999))
	if got != nil {
		t.Fatalf("LookupFrame returned non-nil for invalid ID %d", got.ID)
	}

	got = m.LookupFrame(FrameID(0))
	if got != nil {
		t.Fatalf("LookupFrame returned non-nil for zero ID")
	}
}

func TestUnit_Manager_FrameSize_ReturnsConfigured(t *testing.T) {
	m := newTestManager(t, 4)

	if m.FrameSize() != 4096 {
		t.Fatalf("FrameSize() = %d; want 4096", m.FrameSize())
	}
}

func TestUnit_Manager_MaxFrames_ReturnsConfigured(t *testing.T) {
	m := newTestManager(t, 16)

	if m.MaxFrames() != 16 {
		t.Fatalf("MaxFrames() = %d; want 16", m.MaxFrames())
	}
}

func TestUnit_Manager_AllocFrame_DataSliceIsUsable(t *testing.T) {
	m := newTestManager(t, 4)

	f, err := m.AllocFrame(OwnerSegCache, "seg-001")
	if err != nil {
		t.Fatalf("AllocFrame: %v", err)
	}

	data := f.DataSlice()
	if data == nil {
		t.Fatal("DataSlice is nil")
	}
	if len(data) != 4096 {
		t.Fatalf("DataSlice length = %d; want 4096", len(data))
	}

	// Write and read back data to verify the backing memory works.
	payload := []byte("hello bufmgr")
	err = f.WriteAt(payload, 0)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	readBuf := make([]byte, len(payload))
	err = f.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	if string(readBuf) != string(payload) {
		t.Fatalf("ReadAt returned %q; want %q", readBuf, payload)
	}
}

func TestUnit_Manager_Defaults_NoConfig(t *testing.T) {
	// Test that NewManager works with zero-value config (all defaults).
	mgr, err := NewManager(ManagerConfig{})
	if err != nil {
		t.Fatalf("NewManager with defaults: %v", err)
	}
	defer mgr.Close()

	if mgr.MaxFrames() != 1024 {
		t.Fatalf("default MaxFrames = %d; want 1024", mgr.MaxFrames())
	}
	if mgr.FrameSize() != FrameSize64KB {
		t.Fatalf("default FrameSize = %d; want %d", mgr.FrameSize(), FrameSize64KB)
	}
}

func TestUnit_Manager_MaxMemoryBytes_OverridesMaxFrames(t *testing.T) {
	mgr, err := NewManager(ManagerConfig{
		MaxMemoryBytes: 4 * 4096, // 4 frames of 4096 bytes
		FrameSize:      4096,
		EnableOffHeap:  false,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer mgr.Close()

	if mgr.MaxFrames() != 4 {
		t.Fatalf("MaxFrames with MaxMemoryBytes = %d; want 4", mgr.MaxFrames())
	}
}

func TestUnit_Manager_EvictBatch_OwnerPreference(t *testing.T) {
	m := newTestManager(t, 8)

	// Allocate 4 seg-cache and 4 query frames, unpin all.
	for i := 0; i < 4; i++ {
		f, _ := m.AllocFrame(OwnerSegCache, "seg")
		m.UnpinFrame(f.ID)
	}
	for i := 0; i < 4; i++ {
		f, _ := m.AllocFrame(OwnerQuery, "q")
		m.UnpinFrame(f.ID)
	}

	// Evict 2 with preference for seg-cache.
	evicted := m.EvictBatch(2, OwnerSegCache)
	if evicted != 2 {
		t.Fatalf("EvictBatch = %d; want 2", evicted)
	}

	// Verify free count increased.
	stats := m.Stats()
	if stats.FreeFrames != 2 {
		t.Fatalf("FreeFrames after EvictBatch = %d; want 2", stats.FreeFrames)
	}
}

func TestUnit_Manager_AllocFrame_ReusesEvictedFrame(t *testing.T) {
	m := newTestManager(t, 2)

	// Allocate both frames, unpin one.
	f1, _ := m.AllocFrame(OwnerSegCache, "seg-1")
	f2, _ := m.AllocFrame(OwnerSegCache, "seg-2")
	m.UnpinFrame(f1.ID)
	_ = f2 // keep pinned

	// The next alloc must evict f1 and reuse it.
	f3, err := m.AllocFrame(OwnerQuery, "query-1")
	if err != nil {
		t.Fatalf("AllocFrame: %v", err)
	}
	// The reused frame should have the new owner and tag.
	if f3.Owner != OwnerQuery {
		t.Errorf("reused frame owner = %s; want query", f3.Owner)
	}
	if f3.Tag != "query-1" {
		t.Errorf("reused frame tag = %q; want %q", f3.Tag, "query-1")
	}
}

func TestUnit_Manager_MissCount_IncrementedOnEviction(t *testing.T) {
	m := newTestManager(t, 2)

	// Fill the pool.
	f1, _ := m.AllocFrame(OwnerSegCache, "seg-1")
	f2, _ := m.AllocFrame(OwnerSegCache, "seg-2")
	m.UnpinFrame(f1.ID)
	m.UnpinFrame(f2.ID)

	// This alloc requires eviction (free list is empty).
	_, err := m.AllocFrame(OwnerQuery, "q-1")
	if err != nil {
		t.Fatalf("AllocFrame: %v", err)
	}

	stats := m.Stats()
	if stats.MissCount != 1 {
		t.Fatalf("MissCount = %d; want 1 (eviction path)", stats.MissCount)
	}
}

func TestUnit_Manager_WritebackOnEviction_ErrorDoesNotPreventEviction(t *testing.T) {
	writebackErr := errors.New("disk full")

	mgr, err := NewManager(ManagerConfig{
		MaxFrames:     2,
		FrameSize:     4096,
		EnableOffHeap: false,
		WriteBackFunc: func(f *Frame) error {
			return writebackErr
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	// Allocate, dirty, unpin.
	f, _ := mgr.AllocFrame(OwnerSegCache, "seg-1")
	mgr.MarkDirty(f.ID)
	mgr.UnpinFrame(f.ID)

	f2, _ := mgr.AllocFrame(OwnerSegCache, "seg-2")
	mgr.UnpinFrame(f2.ID)

	// Force eviction: despite writeback error, a new alloc should succeed.
	f3, err := mgr.AllocFrame(OwnerQuery, "q-1")
	if err != nil {
		t.Fatalf("AllocFrame should succeed even when writeback fails: %v", err)
	}
	if f3 == nil {
		t.Fatal("AllocFrame returned nil")
	}
}

func TestConcurrent_Manager_AllocAndUnpin_NoRace(t *testing.T) {
	m := newTestManager(t, 64)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				f, err := m.AllocFrame(OwnerQuery, "concurrent")
				if err != nil {
					continue
				}
				m.UnpinFrame(f.ID)
			}
		}()
	}
	wg.Wait()

	// Verify stats consistency.
	stats := m.Stats()
	if stats.TotalFrames != 64 {
		t.Fatalf("TotalFrames after concurrent access = %d; want 64", stats.TotalFrames)
	}
}

func TestConcurrent_Manager_PinFrameIfOwned_NoRace(t *testing.T) {
	m := newTestManager(t, 16)

	// Allocate some frames.
	var ids []FrameID
	for i := 0; i < 8; i++ {
		f, err := m.AllocFrame(OwnerSegCache, "seg-shared")
		if err != nil {
			t.Fatalf("AllocFrame: %v", err)
		}
		m.UnpinFrame(f.ID)
		ids = append(ids, f.ID)
	}

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, id := range ids {
				f, ok := m.PinFrameIfOwned(id, "seg-shared")
				if ok && f != nil {
					m.UnpinFrame(f.ID)
				}
			}
		}()
	}
	wg.Wait()
}

func TestUnit_Manager_PerFrameWriteBack_CalledOnDirtyEviction(t *testing.T) {
	var called atomic.Bool

	mgr, err := NewManager(ManagerConfig{
		MaxFrames:     1,
		FrameSize:     4096,
		EnableOffHeap: false,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	// Allocate the only frame, set meta that implements FrameWriteBack, dirty it, unpin.
	f1, _ := mgr.AllocFrame(OwnerSegCache, "seg-1")
	f1.SetMeta(&testWriteback{called: &called})
	mgr.MarkDirty(f1.ID)
	mgr.UnpinFrame(f1.ID)

	// Force eviction: pool is full, must evict f1 (the only frame).
	// handleEviction should call FrameWriteBack on the dirty frame's meta.
	_, err = mgr.AllocFrame(OwnerQuery, "q-1")
	if err != nil {
		t.Fatalf("AllocFrame: %v", err)
	}

	if !called.Load() {
		t.Fatal("FrameWriteBack.WriteBackFrame was not called during dirty frame eviction")
	}
}

// testWriteback implements FrameWriteBack for testing.
type testWriteback struct {
	called *atomic.Bool
}

func (tw *testWriteback) WriteBackFrame(f *Frame) error {
	tw.called.Store(true)
	return nil
}
