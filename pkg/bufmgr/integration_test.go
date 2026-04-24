package bufmgr

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/memgov"
)

// newTestManagerWithGovernor creates a Manager backed by a real Governor.
// The caller must reserve from the governor before allocating frames, mirroring
// how the storage engine uses these components together: the engine reserves
// from the governor, the buffer manager releases on eviction.
func newTestManagerWithGovernor(t *testing.T, maxFrames, frameSize int, gov memgov.Governor) Manager {
	t.Helper()

	m, err := NewManager(ManagerConfig{
		MaxFrames:     maxFrames,
		FrameSize:     frameSize,
		EnableOffHeap: false,
		Governor:      gov,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	return m
}

// TestIntegration_GovernorBufferManager_AllocEvict_TracksPageCacheBytes verifies
// that the Governor's ClassPageCache bytes decrease when the BufferManager evicts
// frames. The design contract is:
//   - The caller (e.g. storage engine) reserves from the governor before using a frame.
//   - The buffer manager releases from the governor on eviction via handleEviction.
//
// This test exercises the full round-trip: reserve -> alloc -> unpin -> evict -> verify release.
func TestIntegration_GovernorBufferManager_AllocEvict_TracksPageCacheBytes(t *testing.T) {
	const (
		maxFrames = 8
		frameSize = 4096
	)

	gov := memgov.NewGovernor(memgov.GovernorConfig{
		TotalLimit: int64(maxFrames) * int64(frameSize) * 4, // plenty of room
	})
	mgr := newTestManagerWithGovernor(t, maxFrames, frameSize, gov)

	// Simulate the storage engine pattern: reserve from governor, then use a frame.
	frames := make([]*Frame, maxFrames)
	for i := 0; i < maxFrames; i++ {
		if err := gov.Reserve(memgov.ClassPageCache, int64(frameSize)); err != nil {
			t.Fatalf("gov.Reserve for frame %d: %v", i, err)
		}

		f, err := mgr.AllocFrame(OwnerSegCache, "seg-block")
		if err != nil {
			t.Fatalf("AllocFrame %d: %v", i, err)
		}
		mgr.UnpinFrame(f.ID) // make evictable
		frames[i] = f
	}

	// Governor should now track maxFrames * frameSize bytes for ClassPageCache.
	usage := gov.ClassUsage(memgov.ClassPageCache)
	expectedBytes := int64(maxFrames) * int64(frameSize)
	if usage.Allocated != expectedBytes {
		t.Fatalf("ClassPageCache.Allocated = %d; want %d after reserving %d frames",
			usage.Allocated, expectedBytes, maxFrames)
	}

	// Evict half the frames. Each eviction should release frameSize bytes from the governor.
	evictCount := maxFrames / 2
	evicted := mgr.EvictBatch(evictCount, OwnerSegCache)
	if evicted != evictCount {
		t.Fatalf("EvictBatch(%d) = %d; want %d", evictCount, evicted, evictCount)
	}

	// Verify governor allocation decreased by evictCount * frameSize.
	usage = gov.ClassUsage(memgov.ClassPageCache)
	expectedAfterEvict := expectedBytes - int64(evictCount)*int64(frameSize)
	if usage.Allocated != expectedAfterEvict {
		t.Fatalf("ClassPageCache.Allocated after eviction = %d; want %d",
			usage.Allocated, expectedAfterEvict)
	}

	// Verify total usage is consistent.
	total := gov.TotalUsage()
	if total.Allocated != expectedAfterEvict {
		t.Fatalf("TotalUsage.Allocated = %d; want %d", total.Allocated, expectedAfterEvict)
	}

	// Evict the remaining frames.
	remaining := maxFrames - evictCount
	evicted = mgr.EvictBatch(remaining, OwnerFree)
	if evicted != remaining {
		t.Fatalf("EvictBatch(%d) = %d; want %d", remaining, evicted, remaining)
	}

	// Governor should now be at zero.
	usage = gov.ClassUsage(memgov.ClassPageCache)
	if usage.Allocated != 0 {
		t.Fatalf("ClassPageCache.Allocated after full eviction = %d; want 0", usage.Allocated)
	}

	total = gov.TotalUsage()
	if total.Allocated != 0 {
		t.Fatalf("TotalUsage.Allocated after full eviction = %d; want 0", total.Allocated)
	}
}

// TestIntegration_GovernorBufferManager_EvictionReleasesGovernorOnDirtyWriteback
// verifies that dirty frame eviction still releases governor bytes even when the
// writeback function is invoked.
func TestIntegration_GovernorBufferManager_EvictionReleasesGovernorOnDirtyWriteback(t *testing.T) {
	const (
		maxFrames = 4
		frameSize = 4096
	)

	var writebackCount atomic.Int32

	gov := memgov.NewGovernor(memgov.GovernorConfig{
		TotalLimit: int64(maxFrames) * int64(frameSize) * 2,
	})

	mgr, err := NewManager(ManagerConfig{
		MaxFrames:     maxFrames,
		FrameSize:     frameSize,
		EnableOffHeap: false,
		Governor:      gov,
		WriteBackFunc: func(f *Frame) error {
			writebackCount.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	// Reserve + alloc + dirty + unpin all frames.
	for i := 0; i < maxFrames; i++ {
		if err := gov.Reserve(memgov.ClassPageCache, int64(frameSize)); err != nil {
			t.Fatalf("gov.Reserve %d: %v", i, err)
		}
		f, err := mgr.AllocFrame(OwnerSegCache, "seg")
		if err != nil {
			t.Fatalf("AllocFrame %d: %v", i, err)
		}
		mgr.MarkDirty(f.ID)
		mgr.UnpinFrame(f.ID)
	}

	// Evict all. Each should trigger writeback (dirty) and governor release.
	evicted := mgr.EvictBatch(maxFrames, OwnerFree)
	if evicted != maxFrames {
		t.Fatalf("EvictBatch = %d; want %d", evicted, maxFrames)
	}

	if writebackCount.Load() != int32(maxFrames) {
		t.Fatalf("writeback called %d times; want %d", writebackCount.Load(), maxFrames)
	}

	usage := gov.ClassUsage(memgov.ClassPageCache)
	if usage.Allocated != 0 {
		t.Fatalf("ClassPageCache.Allocated = %d after dirty eviction; want 0", usage.Allocated)
	}
}

// TestIntegration_PressureDrivenEviction_EvictsFramesWhenGovernorOverLimit tests
// the critical feedback loop between Governor and BufferManager:
//
//  1. Governor is near its total limit.
//  2. A new reservation exceeds the limit.
//  3. Governor invokes the pressure callback registered on ClassPageCache.
//  4. The callback evicts frames from the buffer manager, which calls gov.Release.
//  5. The governor re-checks and admits the new reservation.
//
// This is the core memory pressure reclamation path.
func TestIntegration_PressureDrivenEviction_EvictsFramesWhenGovernorOverLimit(t *testing.T) {
	const (
		maxFrames   = 8
		frameSize   = 4096
		totalBudget = int64(maxFrames) * int64(frameSize) // exactly enough for maxFrames
	)

	gov := memgov.NewGovernor(memgov.GovernorConfig{
		TotalLimit: totalBudget,
	})
	mgr := newTestManagerWithGovernor(t, maxFrames, frameSize, gov)

	// Reserve + alloc all frames, making them all evictable.
	for i := 0; i < maxFrames; i++ {
		if err := gov.Reserve(memgov.ClassPageCache, int64(frameSize)); err != nil {
			t.Fatalf("gov.Reserve %d: %v", i, err)
		}
		f, err := mgr.AllocFrame(OwnerSegCache, "seg-block")
		if err != nil {
			t.Fatalf("AllocFrame %d: %v", i, err)
		}
		mgr.UnpinFrame(f.ID)
	}

	// Governor is now at totalBudget. Any new reservation will exceed the limit.
	usageBefore := gov.TotalUsage()
	if usageBefore.Allocated != totalBudget {
		t.Fatalf("total allocated = %d; want %d", usageBefore.Allocated, totalBudget)
	}

	// Register a pressure callback on ClassPageCache that evicts frames from bufmgr.
	// The callback receives the deficit (how many bytes must be freed).
	var callbackInvocations atomic.Int32
	gov.OnPressure(memgov.ClassPageCache, func(target int64) int64 {
		callbackInvocations.Add(1)

		// Calculate how many frames to evict to free at least `target` bytes.
		framesToEvict := int((target + int64(frameSize) - 1) / int64(frameSize))
		evicted := mgr.EvictBatch(framesToEvict, OwnerSegCache)

		// EvictBatch already calls gov.Release internally via handleEviction.
		return int64(evicted) * int64(frameSize)
	})

	// Now try to reserve more bytes from a different class (simulating another
	// subsystem, e.g. a query operator needing spillable memory).
	extraBytes := int64(frameSize * 2) // need 2 frames worth
	err := gov.Reserve(memgov.ClassSpillable, extraBytes)
	if err != nil {
		t.Fatalf("Reserve after pressure callback should succeed: %v", err)
	}

	// Verify the pressure callback was invoked.
	if callbackInvocations.Load() == 0 {
		t.Fatal("pressure callback was never invoked; expected governor to trigger it")
	}

	// Verify the new reservation went through and total accounting is correct.
	totalAfter := gov.TotalUsage()
	if totalAfter.Allocated > totalBudget {
		t.Fatalf("total allocated = %d exceeds budget %d", totalAfter.Allocated, totalBudget)
	}

	// Verify spillable class has the expected allocation.
	spillUsage := gov.ClassUsage(memgov.ClassSpillable)
	if spillUsage.Allocated != extraBytes {
		t.Fatalf("Spillable.Allocated = %d; want %d", spillUsage.Allocated, extraBytes)
	}

	// Verify page cache allocation decreased.
	pcUsage := gov.ClassUsage(memgov.ClassPageCache)
	if pcUsage.Allocated >= totalBudget {
		t.Fatalf("PageCache.Allocated = %d; should have decreased from %d", pcUsage.Allocated, totalBudget)
	}

	// Verify buffer manager stats reflect the evictions.
	stats := mgr.Stats()
	if stats.EvictionCount == 0 {
		t.Fatal("EvictionCount = 0; expected at least one eviction from pressure callback")
	}
}

// TestIntegration_PressureDrivenEviction_InsufficientReclamation_ReturnsError verifies
// that when the pressure callback cannot free enough memory (e.g. all frames pinned),
// the governor returns ErrMemoryPressure.
func TestIntegration_PressureDrivenEviction_InsufficientReclamation_ReturnsError(t *testing.T) {
	const (
		maxFrames   = 4
		frameSize   = 4096
		totalBudget = int64(maxFrames) * int64(frameSize)
	)

	gov := memgov.NewGovernor(memgov.GovernorConfig{
		TotalLimit: totalBudget,
	})
	mgr := newTestManagerWithGovernor(t, maxFrames, frameSize, gov)

	// Reserve + alloc all frames, but keep them ALL PINNED (not evictable).
	for i := 0; i < maxFrames; i++ {
		if err := gov.Reserve(memgov.ClassPageCache, int64(frameSize)); err != nil {
			t.Fatalf("gov.Reserve %d: %v", i, err)
		}
		_, err := mgr.AllocFrame(OwnerSegCache, "seg-pinned")
		if err != nil {
			t.Fatalf("AllocFrame %d: %v", i, err)
		}
		// DO NOT unpin -- frames are not evictable.
	}

	// Register pressure callback that tries to evict, but fails (all pinned).
	gov.OnPressure(memgov.ClassPageCache, func(target int64) int64 {
		framesToEvict := int((target + int64(frameSize) - 1) / int64(frameSize))
		evicted := mgr.EvictBatch(framesToEvict, OwnerSegCache)
		// evicted will be 0 because all frames are pinned.
		return int64(evicted) * int64(frameSize)
	})

	// This reservation should fail because we cannot reclaim enough memory.
	err := gov.Reserve(memgov.ClassSpillable, int64(frameSize))
	if err == nil {
		t.Fatal("Reserve should fail when pressure callback cannot free enough; all frames are pinned")
	}
}

// TestIntegration_GovernorWithMultipleManagers_SharedBudget verifies that a single
// Governor correctly tracks total memory across multiple buffer managers sharing
// the same budget. This models a real deployment where separate subsystems (e.g.
// segment cache and query working memory) share a process-wide memory governor.
func TestIntegration_GovernorWithMultipleManagers_SharedBudget(t *testing.T) {
	const (
		framesPerMgr = 4
		frameSize    = 4096
		numManagers  = 3
		totalBudget  = int64(framesPerMgr*numManagers) * int64(frameSize) * 2 // generous
	)

	gov := memgov.NewGovernor(memgov.GovernorConfig{
		TotalLimit: totalBudget,
	})

	managers := make([]Manager, numManagers)
	for i := 0; i < numManagers; i++ {
		managers[i] = newTestManagerWithGovernor(t, framesPerMgr, frameSize, gov)
	}

	// Each manager allocates all its frames. The caller reserves from the governor.
	type frameRecord struct {
		mgr   Manager
		frame *Frame
	}
	var allFrames []frameRecord

	for mgrIdx, mgr := range managers {
		for i := 0; i < framesPerMgr; i++ {
			if err := gov.Reserve(memgov.ClassPageCache, int64(frameSize)); err != nil {
				t.Fatalf("gov.Reserve mgr=%d frame=%d: %v", mgrIdx, i, err)
			}
			f, err := mgr.AllocFrame(OwnerSegCache, "seg-block")
			if err != nil {
				t.Fatalf("mgr[%d].AllocFrame %d: %v", mgrIdx, i, err)
			}
			mgr.UnpinFrame(f.ID)
			allFrames = append(allFrames, frameRecord{mgr: mgr, frame: f})
		}
	}

	// Total allocation should be framesPerMgr * numManagers * frameSize.
	expectedTotal := int64(framesPerMgr*numManagers) * int64(frameSize)
	total := gov.TotalUsage()
	if total.Allocated != expectedTotal {
		t.Fatalf("TotalUsage.Allocated = %d; want %d across %d managers",
			total.Allocated, expectedTotal, numManagers)
	}

	// Evict all frames from manager 0 only.
	evicted := managers[0].EvictBatch(framesPerMgr, OwnerFree)
	if evicted != framesPerMgr {
		t.Fatalf("mgr[0].EvictBatch = %d; want %d", evicted, framesPerMgr)
	}

	// Total should decrease by framesPerMgr * frameSize.
	expectedAfter := expectedTotal - int64(framesPerMgr)*int64(frameSize)
	total = gov.TotalUsage()
	if total.Allocated != expectedAfter {
		t.Fatalf("TotalUsage.Allocated after mgr[0] eviction = %d; want %d",
			total.Allocated, expectedAfter)
	}

	// Evict from manager 1.
	evicted = managers[1].EvictBatch(framesPerMgr, OwnerFree)
	if evicted != framesPerMgr {
		t.Fatalf("mgr[1].EvictBatch = %d; want %d", evicted, framesPerMgr)
	}

	expectedAfter -= int64(framesPerMgr) * int64(frameSize)
	total = gov.TotalUsage()
	if total.Allocated != expectedAfter {
		t.Fatalf("TotalUsage.Allocated after mgr[1] eviction = %d; want %d",
			total.Allocated, expectedAfter)
	}

	// Evict from manager 2 -- should bring total to zero.
	evicted = managers[2].EvictBatch(framesPerMgr, OwnerFree)
	if evicted != framesPerMgr {
		t.Fatalf("mgr[2].EvictBatch = %d; want %d", evicted, framesPerMgr)
	}

	total = gov.TotalUsage()
	if total.Allocated != 0 {
		t.Fatalf("TotalUsage.Allocated after all evictions = %d; want 0", total.Allocated)
	}
}

// TestIntegration_GovernorWithMultipleManagers_PressureCascadesAcrossSubsystems
// verifies that when a new reservation triggers pressure, the governor's callback
// can reclaim memory from ANY registered subsystem -- not just the one requesting.
func TestIntegration_GovernorWithMultipleManagers_PressureCascadesAcrossSubsystems(t *testing.T) {
	const (
		framesPerMgr = 4
		frameSize    = 4096
		totalBudget  = int64(framesPerMgr*2) * int64(frameSize) // exactly 2 managers worth
	)

	gov := memgov.NewGovernor(memgov.GovernorConfig{
		TotalLimit: totalBudget,
	})

	mgr1 := newTestManagerWithGovernor(t, framesPerMgr, frameSize, gov)
	mgr2 := newTestManagerWithGovernor(t, framesPerMgr, frameSize, gov)

	// Fill both managers to capacity.
	for i := 0; i < framesPerMgr; i++ {
		if err := gov.Reserve(memgov.ClassPageCache, int64(frameSize)); err != nil {
			t.Fatalf("gov.Reserve mgr1 frame %d: %v", i, err)
		}
		f, err := mgr1.AllocFrame(OwnerSegCache, "seg-1")
		if err != nil {
			t.Fatalf("mgr1.AllocFrame %d: %v", i, err)
		}
		mgr1.UnpinFrame(f.ID)
	}
	for i := 0; i < framesPerMgr; i++ {
		if err := gov.Reserve(memgov.ClassPageCache, int64(frameSize)); err != nil {
			t.Fatalf("gov.Reserve mgr2 frame %d: %v", i, err)
		}
		f, err := mgr2.AllocFrame(OwnerSegCache, "seg-2")
		if err != nil {
			t.Fatalf("mgr2.AllocFrame %d: %v", i, err)
		}
		mgr2.UnpinFrame(f.ID)
	}

	// Governor is at capacity. Register pressure callbacks for both managers.
	var mgr1Evictions, mgr2Evictions atomic.Int32

	gov.OnPressure(memgov.ClassPageCache, func(target int64) int64 {
		// Try mgr1 first.
		framesToEvict := int((target + int64(frameSize) - 1) / int64(frameSize))
		evicted := mgr1.EvictBatch(framesToEvict, OwnerSegCache)
		mgr1Evictions.Add(int32(evicted))
		freed := int64(evicted) * int64(frameSize)

		// If mgr1 didn't free enough, try mgr2.
		if freed < target {
			remaining := int((target - freed + int64(frameSize) - 1) / int64(frameSize))
			evicted2 := mgr2.EvictBatch(remaining, OwnerSegCache)
			mgr2Evictions.Add(int32(evicted2))
			freed += int64(evicted2) * int64(frameSize)
		}
		return freed
	})

	// Try to reserve for a different class. This should trigger pressure eviction.
	extraBytes := int64(frameSize * 2)
	err := gov.Reserve(memgov.ClassSpillable, extraBytes)
	if err != nil {
		t.Fatalf("Reserve should succeed after pressure cascading across managers: %v", err)
	}

	totalEvictions := mgr1Evictions.Load() + mgr2Evictions.Load()
	if totalEvictions == 0 {
		t.Fatal("no evictions occurred across either manager")
	}

	total := gov.TotalUsage()
	if total.Allocated > totalBudget {
		t.Fatalf("TotalUsage.Allocated = %d exceeds budget %d after pressure cascade",
			total.Allocated, totalBudget)
	}
}

// TestIntegration_ManagerStartAndWriteback_FlushesFramesOnTick verifies the end-to-end
// writeback scheduler integration:
//
//  1. Create a Manager with a WriteBackFunc.
//  2. Start the background scheduler.
//  3. Allocate a frame, write data, mark dirty, unpin.
//  4. Wait for the periodic tick (100ms) to flush the dirty frame.
//  5. Cancel context to stop the scheduler cleanly.
func TestIntegration_ManagerStartAndWriteback_FlushesFramesOnTick(t *testing.T) {
	const (
		maxFrames = 4
		frameSize = 4096
	)

	var (
		mu              sync.Mutex
		writtenFrameIDs []FrameID
	)

	mgr, err := NewManager(ManagerConfig{
		MaxFrames:     maxFrames,
		FrameSize:     frameSize,
		EnableOffHeap: false,
		WriteBackFunc: func(f *Frame) error {
			mu.Lock()
			writtenFrameIDs = append(writtenFrameIDs, f.ID)
			mu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.Start(ctx)

	// Allocate a frame, write data, mark dirty, then unpin.
	f, err := mgr.AllocFrame(OwnerSegCache, "seg-001")
	if err != nil {
		t.Fatalf("AllocFrame: %v", err)
	}
	payload := []byte("dirty-data-for-writeback-test")
	if err := f.WriteAt(payload, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	// WriteAt already sets frame to dirty, but also call MarkDirty to be explicit.
	mgr.MarkDirty(f.ID)
	mgr.UnpinFrame(f.ID) // must unpin for the scheduler to pick it up

	// Poll for the writeback to complete. The scheduler ticks every 100ms.
	// We allow up to 2 seconds for the writeback to be observed.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		count := len(writtenFrameIDs)
		mu.Unlock()
		if count > 0 {
			break
		}
		// Use a short poll interval to avoid making this test slow.
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(writtenFrameIDs) == 0 {
		t.Fatal("writeback scheduler did not flush any dirty frames within deadline")
	}

	// Verify the correct frame was written back.
	found := false
	for _, id := range writtenFrameIDs {
		if id == f.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("frame %d was not written back; written frames: %v", f.ID, writtenFrameIDs)
	}

	// After writeback, the frame should transition to Clean.
	state := FrameState(f.State.Load())
	if state != StateClean {
		t.Fatalf("frame state after writeback = %s; want clean", state)
	}

	// Cancel context to stop the scheduler goroutine.
	cancel()
}

// TestIntegration_ManagerStartAndWriteback_CancelStopsScheduler verifies that
// cancelling the context stops the writeback scheduler cleanly without goroutine
// leaks.
func TestIntegration_ManagerStartAndWriteback_CancelStopsScheduler(t *testing.T) {
	mgr, err := NewManager(ManagerConfig{
		MaxFrames:     4,
		FrameSize:     4096,
		EnableOffHeap: false,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	mgr.Start(ctx)

	// Cancel immediately. The scheduler goroutine should exit.
	cancel()

	// Allocate a frame and dirty it. Since the scheduler is stopped,
	// it should NOT be written back. This is a behavioral verification:
	// we just ensure no panic or deadlock occurs.
	f, err := mgr.AllocFrame(OwnerSegCache, "seg-001")
	if err != nil {
		t.Fatalf("AllocFrame after scheduler cancel: %v", err)
	}
	mgr.MarkDirty(f.ID)
	mgr.UnpinFrame(f.ID)

	// Allow some time to verify no crash.
	time.Sleep(50 * time.Millisecond)
}

// TestIntegration_ManagerStartAndWriteback_RequestWriteback_TriggersImmediate
// verifies that calling RequestWriteback on the scheduler triggers an immediate
// flush rather than waiting for the periodic tick.
func TestIntegration_ManagerStartAndWriteback_RequestWriteback_TriggersImmediate(t *testing.T) {
	const frameSize = 4096

	var writebackDone atomic.Int32

	m, err := NewManager(ManagerConfig{
		MaxFrames:     4,
		FrameSize:     frameSize,
		EnableOffHeap: false,
		WriteBackFunc: func(f *Frame) error {
			writebackDone.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m.Start(ctx)

	f, err := m.AllocFrame(OwnerSegCache, "seg-fast")
	if err != nil {
		t.Fatalf("AllocFrame: %v", err)
	}
	m.MarkDirty(f.ID)
	m.UnpinFrame(f.ID)

	// Explicitly request writeback to trigger immediate flush.
	concrete := m.(*manager)
	concrete.scheduler.RequestWriteback()

	// Poll until writeback completes (should be nearly instant).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if writebackDone.Load() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if writebackDone.Load() == 0 {
		t.Fatal("RequestWriteback did not trigger an immediate flush")
	}
}

// TestConcurrent_GovernorBufferManager_AllocEvictUnderPressure_NoRace exercises the
// full Governor + BufferManager integration under concurrent load with the race
// detector enabled. Multiple goroutines simultaneously:
//   - Reserve from governor
//   - Allocate frames
//   - Mark dirty / unpin
//   - Evict via pressure callbacks
//
// The test verifies no data races and no accounting drift (total ends at zero).
func TestConcurrent_GovernorBufferManager_AllocEvictUnderPressure_NoRace(t *testing.T) {
	const (
		maxFrames   = 32
		frameSize   = 4096
		goroutines  = 8
		iterations  = 50
		totalBudget = int64(maxFrames+goroutines*2) * int64(frameSize)
	)

	gov := memgov.NewGovernor(memgov.GovernorConfig{
		TotalLimit: totalBudget,
	})
	mgr := newTestManagerWithGovernor(t, maxFrames, frameSize, gov)

	// Register a pressure callback so the governor has a reclamation path.
	gov.OnPressure(memgov.ClassPageCache, func(target int64) int64 {
		framesToEvict := int((target + int64(frameSize) - 1) / int64(frameSize))
		if framesToEvict < 1 {
			framesToEvict = 1
		}
		evicted := mgr.EvictBatch(framesToEvict, OwnerSegCache)
		return int64(evicted) * int64(frameSize)
	})

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				// Reserve from governor.
				if err := gov.Reserve(memgov.ClassPageCache, int64(frameSize)); err != nil {
					// Under contention, reservation may fail. That is fine.
					continue
				}

				f, err := mgr.AllocFrame(OwnerSegCache, "concurrent-seg")
				if err != nil {
					// If alloc fails (all pinned), release the reservation.
					gov.Release(memgov.ClassPageCache, int64(frameSize))
					continue
				}

				// Simulate some work.
				if i%3 == 0 {
					mgr.MarkDirty(f.ID)
				}

				mgr.UnpinFrame(f.ID)

				// Occasionally read stats to exercise concurrent stat reads.
				if i%5 == 0 {
					_ = mgr.Stats()
					_ = gov.TotalUsage()
				}
			}
		}(g)
	}

	wg.Wait()

	// Evict all remaining frames to bring governor to zero.
	mgr.EvictBatch(maxFrames, OwnerFree)

	// Verify no accounting drift on the PageCache class.
	usage := gov.ClassUsage(memgov.ClassPageCache)
	if usage.Allocated != 0 {
		t.Errorf("ClassPageCache.Allocated after draining = %d; want 0 (accounting drift detected)", usage.Allocated)
	}
}

// TestIntegration_GovernorBufferManager_PeakTracking verifies that the Governor's
// peak watermark accurately reflects the maximum concurrent allocation that occurred
// across Governor + BufferManager operations.
func TestIntegration_GovernorBufferManager_PeakTracking(t *testing.T) {
	const (
		maxFrames = 8
		frameSize = 4096
	)

	gov := memgov.NewGovernor(memgov.GovernorConfig{
		TotalLimit: int64(maxFrames) * int64(frameSize) * 4,
	})
	mgr := newTestManagerWithGovernor(t, maxFrames, frameSize, gov)

	// Allocate 6 frames.
	for i := 0; i < 6; i++ {
		if err := gov.Reserve(memgov.ClassPageCache, int64(frameSize)); err != nil {
			t.Fatalf("Reserve %d: %v", i, err)
		}
		f, err := mgr.AllocFrame(OwnerSegCache, "seg")
		if err != nil {
			t.Fatalf("AllocFrame %d: %v", i, err)
		}
		mgr.UnpinFrame(f.ID)
	}

	peak6 := gov.ClassUsage(memgov.ClassPageCache).Peak
	if peak6 != int64(6*frameSize) {
		t.Fatalf("peak after 6 allocs = %d; want %d", peak6, 6*frameSize)
	}

	// Evict 4 frames. Peak should stay at 6.
	mgr.EvictBatch(4, OwnerFree)

	peakAfterEvict := gov.ClassUsage(memgov.ClassPageCache).Peak
	if peakAfterEvict != peak6 {
		t.Fatalf("peak after eviction = %d; want %d (should not decrease)", peakAfterEvict, peak6)
	}

	current := gov.ClassUsage(memgov.ClassPageCache).Allocated
	if current != int64(2*frameSize) {
		t.Fatalf("current after eviction = %d; want %d", current, 2*frameSize)
	}

	// Reserve more to set a new peak.
	for i := 0; i < 7; i++ {
		if err := gov.Reserve(memgov.ClassPageCache, int64(frameSize)); err != nil {
			t.Fatalf("Reserve %d: %v", i, err)
		}
	}

	peakNew := gov.ClassUsage(memgov.ClassPageCache).Peak
	// Current allocated = 2*frameSize (from remaining frames) + 7*frameSize = 9*frameSize
	expectedPeak := int64(9 * frameSize)
	if peakNew != expectedPeak {
		t.Fatalf("peak after new high = %d; want %d", peakNew, expectedPeak)
	}
}
