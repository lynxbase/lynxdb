package server

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/model"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment"
)

// makeTestSegmentHandle creates a segmentHandle with the given ID.
// For epoch tests we don't need real mmap data — just handles with identifiable IDs.
func makeTestSegmentHandle(id string) *segmentHandle {
	return &segmentHandle{
		meta: model.SegmentMeta{
			ID:    id,
			Index: "main",
		},
		index: "main",
	}
}

// makeTestSegmentHandleWithReader creates a segmentHandle with a real in-memory
// segment reader (no mmap). Useful for tests that need to read segment data.
func makeTestSegmentHandleWithReader(t *testing.T, id string, count int) *segmentHandle {
	t.Helper()

	events := make([]*event.Event, count)
	now := time.Now()
	for i := 0; i < count; i++ {
		events[i] = &event.Event{
			Time: now.Add(time.Duration(i) * time.Millisecond),
			Raw:  fmt.Sprintf("event %d from segment %s", i, id),
			Host: "test-host",
		}
	}

	var buf bytes.Buffer
	w := segment.NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("write test segment: %v", err)
	}

	sr, err := segment.OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("open test segment: %v", err)
	}

	return &segmentHandle{
		reader: sr,
		meta: model.SegmentMeta{
			ID:         id,
			Index:      "main",
			MinTime:    events[0].Time,
			MaxTime:    events[count-1].Time,
			EventCount: int64(count),
			SizeBytes:  int64(buf.Len()),
		},
		index: "main",
	}
}

// TestEpochPinUnpin verifies the basic pin/unpin lifecycle: pin increments readers,
// unpin decrements, and done is signaled when readers reaches zero.
func TestEpochPinUnpin(t *testing.T) {
	ep := &segmentEpoch{
		done: make(chan struct{}),
	}

	ep.pin()
	if got := ep.readers.Load(); got != 1 {
		t.Fatalf("after pin: readers = %d, want 1", got)
	}

	ep.pin()
	if got := ep.readers.Load(); got != 2 {
		t.Fatalf("after second pin: readers = %d, want 2", got)
	}

	ep.unpin()
	if got := ep.readers.Load(); got != 1 {
		t.Fatalf("after first unpin: readers = %d, want 1", got)
	}

	// Done channel should still be open.
	select {
	case <-ep.done:
		t.Fatal("done signaled too early — reader still pinned")
	default:
		// OK
	}

	ep.unpin()
	if got := ep.readers.Load(); got != 0 {
		t.Fatalf("after second unpin: readers = %d, want 0", got)
	}

	// Done should be signaled now.
	select {
	case <-ep.done:
		// OK
	case <-time.After(time.Second):
		t.Fatal("done not signaled after all readers unpinned")
	}
}

// TestEpochImmutability verifies that adding segments via advanceEpoch does not
// affect a previously pinned epoch's segment list.
func TestEpochImmutability(t *testing.T) {
	e := newTestEngine(t)

	// Seed with 3 segments.
	sh1 := makeTestSegmentHandle("seg-1")
	sh2 := makeTestSegmentHandle("seg-2")
	sh3 := makeTestSegmentHandle("seg-3")

	e.mu.Lock()
	e.advanceEpoch([]*segmentHandle{sh1, sh2, sh3}, nil)
	e.mu.Unlock()

	// Pin the current epoch — this is what a query does.
	ep := e.pinEpoch()
	if len(ep.segments) != 3 {
		t.Fatalf("pinned epoch segments = %d, want 3", len(ep.segments))
	}

	// Capture the segment IDs from the pinned epoch.
	pinnedIDs := make(map[string]bool)
	for _, sh := range ep.segments {
		pinnedIDs[sh.meta.ID] = true
	}

	// Add 2 more segments via advanceEpoch (simulates ingest flush).
	sh4 := makeTestSegmentHandle("seg-4")
	sh5 := makeTestSegmentHandle("seg-5")

	e.mu.Lock()
	combined := make([]*segmentHandle, len(e.currentEpoch.segments)+2)
	copy(combined, e.currentEpoch.segments)
	combined[len(combined)-2] = sh4
	combined[len(combined)-1] = sh5
	e.advanceEpoch(combined, nil)
	e.mu.Unlock()

	// Verify: the current epoch should have 5 segments.
	e.mu.RLock()
	currentCount := len(e.currentEpoch.segments)
	e.mu.RUnlock()
	if currentCount != 5 {
		t.Fatalf("current epoch segments = %d, want 5", currentCount)
	}

	// Verify: the pinned epoch should still have exactly 3 segments.
	if len(ep.segments) != 3 {
		t.Fatalf("pinned epoch segments changed: got %d, want 3", len(ep.segments))
	}

	for _, sh := range ep.segments {
		if !pinnedIDs[sh.meta.ID] {
			t.Errorf("pinned epoch contains unexpected segment %q", sh.meta.ID)
		}
	}

	// Verify: seg-4 and seg-5 are NOT in the pinned epoch.
	for _, sh := range ep.segments {
		if sh.meta.ID == "seg-4" || sh.meta.ID == "seg-5" {
			t.Errorf("pinned epoch should not contain %q", sh.meta.ID)
		}
	}

	ep.unpin()
}

// TestEpochRetiredCleanup verifies that retired segment mmaps are closed only
// after the last reader unpins, not before.
func TestEpochRetiredCleanup(t *testing.T) {
	e := newTestEngine(t)

	// Create a segment handle with a trackable close (using a temp file + mmap).
	// For simplicity, we use a nil mmap and verify the drain lifecycle instead.
	// The key invariant: drainAndClose waits for done before closing.

	sh := makeTestSegmentHandle("to-retire")

	e.mu.Lock()
	e.advanceEpoch([]*segmentHandle{sh}, nil)
	e.mu.Unlock()

	// Pin the epoch (simulates a query in flight).
	ep := e.pinEpoch()
	if len(ep.segments) != 1 {
		t.Fatalf("pinned epoch segments = %d, want 1", len(ep.segments))
	}

	// Advance epoch, retiring the segment (simulates compaction).
	e.mu.Lock()
	e.advanceEpoch(make([]*segmentHandle, 0), []*segmentHandle{sh})
	e.mu.Unlock()

	// The old epoch (ep) is now retired. Verify the done channel is NOT
	// signaled — the pinned reader is still active.
	select {
	case <-ep.done:
		t.Fatal("done signaled while reader still pinned")
	default:
		// Expected: reader still holding pin.
	}

	// Verify retired is set on the old epoch.
	if len(ep.retired) != 1 {
		t.Fatalf("retired count = %d, want 1", len(ep.retired))
	}
	if ep.retired[0].meta.ID != "to-retire" {
		t.Fatalf("retired segment ID = %q, want %q", ep.retired[0].meta.ID, "to-retire")
	}

	// Unpin — this should signal done, allowing drainAndClose to proceed.
	ep.unpin()

	select {
	case <-ep.done:
		// Good: done signaled after last unpin.
	case <-time.After(time.Second):
		t.Fatal("done not signaled after unpin")
	}
}

// TestEpochDrainDuringQuery simulates the exact race condition that caused SIGSEGV:
// a query pins an epoch, background compaction retires segments via advanceEpoch,
// and the query continues to access segment data safely through the pinned epoch.
func TestEpochDrainDuringQuery(t *testing.T) {
	e := newTestEngine(t)

	// Create 5 segments with real readers so we can verify data access.
	segs := make([]*segmentHandle, 5)
	for i := 0; i < 5; i++ {
		segs[i] = makeTestSegmentHandleWithReader(t, fmt.Sprintf("seg-%d", i), 100)
	}

	e.mu.Lock()
	e.advanceEpoch(segs, nil)
	e.mu.Unlock()

	// Pin the epoch (simulates query start).
	ep := e.pinEpoch()
	if len(ep.segments) != 5 {
		t.Fatalf("pinned epoch segments = %d, want 5", len(ep.segments))
	}

	// Compaction retires seg-0, seg-1, seg-2 (simulates compaction merge).
	retired := make([]*segmentHandle, 3)
	copy(retired, ep.segments[:3])
	kept := make([]*segmentHandle, 2)
	copy(kept, ep.segments[3:]) // seg-3, seg-4
	newCompacted := makeTestSegmentHandleWithReader(t, "compacted-0-1-2", 300)
	combined := append(kept, newCompacted)

	e.mu.Lock()
	e.advanceEpoch(combined, retired)
	e.mu.Unlock()

	// Verify the query can still access all 5 original segments — the
	// mmap'd data must remain valid because the epoch pin prevents drainAndClose.
	for _, sh := range ep.segments {
		if sh.reader == nil {
			t.Errorf("segment %q has nil reader", sh.meta.ID)

			continue
		}
		count := sh.reader.EventCount()
		if count != 100 {
			t.Errorf("segment %q: EventCount = %d, want 100", sh.meta.ID, count)
		}
	}

	// Verify the new epoch has the compacted view.
	newEp := e.pinEpoch()
	if len(newEp.segments) != 3 { // seg-3, seg-4, compacted-0-1-2
		t.Fatalf("new epoch segments = %d, want 3", len(newEp.segments))
	}
	newEp.unpin()

	// Unpin the old epoch — drainAndClose should now proceed.
	ep.unpin()

	// Wait for done signal.
	select {
	case <-ep.done:
		// Good.
	case <-time.After(time.Second):
		t.Fatal("old epoch done not signaled after unpin")
	}
}

// TestEpochConcurrentPinUnpin verifies that concurrent pin/unpin operations
// are safe and don't lose or double-count readers.
func TestEpochConcurrentPinUnpin(t *testing.T) {
	e := newTestEngine(t)

	sh := makeTestSegmentHandle("seg-1")
	e.mu.Lock()
	e.advanceEpoch([]*segmentHandle{sh}, nil)
	e.mu.Unlock()

	const goroutines = 100
	var wg sync.WaitGroup

	// All goroutines will pin, hold for a bit, then unpin.
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ep := e.pinEpoch()

			// Simulate some work.
			if len(ep.segments) != 1 {
				t.Errorf("expected 1 segment, got %d", len(ep.segments))
			}

			ep.unpin()
		}()
	}

	wg.Wait()

	// After all goroutines finish, reader count should be 0.
	e.mu.RLock()
	readers := e.currentEpoch.readers.Load()
	e.mu.RUnlock()

	if readers != 0 {
		t.Fatalf("after all unpins: readers = %d, want 0", readers)
	}
}

// TestEpochAdvanceDuringConcurrentPins verifies that advancing the epoch while
// multiple goroutines hold pins on the current epoch is safe. Each pinned
// goroutine should see a consistent snapshot.
func TestEpochAdvanceDuringConcurrentPins(t *testing.T) {
	e := newTestEngine(t)

	// Start with 3 segments.
	initialSegs := make([]*segmentHandle, 3)
	for i := 0; i < 3; i++ {
		initialSegs[i] = makeTestSegmentHandle(fmt.Sprintf("seg-%d", i))
	}
	e.mu.Lock()
	e.advanceEpoch(initialSegs, nil)
	e.mu.Unlock()

	const numReaders = 50
	var wg sync.WaitGroup
	startGate := make(chan struct{}) // synchronize goroutine start

	// Launch readers that pin the epoch.
	pinnedEpochs := make([]*segmentEpoch, numReaders)
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			<-startGate
			pinnedEpochs[idx] = e.pinEpoch()
		}()
	}

	// Release all readers at once.
	close(startGate)
	wg.Wait()

	// Now advance the epoch with an additional segment.
	sh4 := makeTestSegmentHandle("seg-new")
	e.mu.Lock()
	oldEpoch := e.currentEpoch
	combined := append(make([]*segmentHandle, 0, 4), oldEpoch.segments...)
	combined = append(combined, sh4)
	e.advanceEpoch(combined, nil)
	e.mu.Unlock()

	// All pinned epochs should have exactly 3 segments (the original count).
	for i, ep := range pinnedEpochs {
		if ep == nil {
			t.Fatalf("pinnedEpochs[%d] is nil", i)
		}
		if len(ep.segments) != 3 {
			t.Errorf("pinnedEpochs[%d] has %d segments, want 3", i, len(ep.segments))
		}
	}

	// A new pin should see 4 segments.
	newEp := e.pinEpoch()
	if len(newEp.segments) != 4 {
		t.Errorf("new pin has %d segments, want 4", len(newEp.segments))
	}
	newEp.unpin()

	// Unpin all old pins.
	for _, ep := range pinnedEpochs {
		ep.unpin()
	}
}

// TestEpochSignalDoneIdempotent verifies that signalDone is safe to call
// multiple times (sync.Once prevents double close panic).
func TestEpochSignalDoneIdempotent(t *testing.T) {
	ep := &segmentEpoch{
		done: make(chan struct{}),
	}

	// Should not panic on multiple calls.
	ep.signalDone()
	ep.signalDone()
	ep.signalDone()

	select {
	case <-ep.done:
		// OK
	default:
		t.Fatal("done channel not closed")
	}
}

// TestAdvanceEpochNoReadersSignalsImmediately verifies that when the old epoch
// has no pinned readers, signalDone is called immediately during advanceEpoch
// (not deferred to a drain goroutine).
func TestAdvanceEpochNoReadersSignalsImmediately(t *testing.T) {
	e := newTestEngine(t)

	sh := makeTestSegmentHandle("seg-1")
	e.mu.Lock()
	e.advanceEpoch([]*segmentHandle{sh}, nil)
	oldEpoch := e.currentEpoch
	e.mu.Unlock()

	// advanceEpoch with retirement, old epoch has 0 readers.
	e.mu.Lock()
	e.advanceEpoch(make([]*segmentHandle, 0), []*segmentHandle{sh})
	e.mu.Unlock()

	// Old epoch's done should be signaled immediately.
	select {
	case <-oldEpoch.done:
		// Good: no readers means immediate signal.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("old epoch done not signaled immediately with 0 readers")
	}
}

// TestCacheRemoteMmapRace verifies that when two goroutines race to cache the
// same warm segment, both get a valid reader and the loser's MmapSegment is
// properly closed.
func TestCacheRemoteMmapRace(t *testing.T) {
	e := newTestEngine(t)

	// Create a warm segment handle (nil reader/mmap — simulates warm tier).
	sh := &segmentHandle{
		meta: model.SegmentMeta{
			ID:    "warm-seg-1",
			Index: "main",
			Tier:  "warm",
		},
		index: "main",
	}

	// Create two real in-memory segments to simulate two downloads.
	events := make([]*event.Event, 10)
	now := time.Now()
	for i := 0; i < 10; i++ {
		events[i] = &event.Event{
			Time: now.Add(time.Duration(i) * time.Millisecond),
			Raw:  fmt.Sprintf("event %d", i),
			Host: "test-host",
		}
	}

	makeSegmentFile := func(t *testing.T) *segment.MmapSegment {
		t.Helper()
		dir := t.TempDir()
		path := dir + "/test.lsg"

		var buf bytes.Buffer
		w := segment.NewWriter(&buf)
		if _, err := w.Write(events); err != nil {
			t.Fatalf("write: %v", err)
		}

		if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
			t.Fatalf("write file: %v", err)
		}

		ms, err := segment.OpenSegmentFile(path)
		if err != nil {
			t.Fatalf("open: %v", err)
		}

		return ms
	}

	// Create two MmapSegments (simulating two goroutines downloading the same data).
	ms1 := makeSegmentFile(t)
	ms2 := makeSegmentFile(t)

	// Race: both goroutines call cacheRemoteMmap concurrently.
	var wg sync.WaitGroup
	var reader1, reader2 *segment.Reader

	wg.Add(2)
	go func() {
		defer wg.Done()
		reader1 = e.cacheRemoteMmap(sh, ms1)
	}()
	go func() {
		defer wg.Done()
		reader2 = e.cacheRemoteMmap(sh, ms2)
	}()
	wg.Wait()

	// Both readers must be non-nil.
	if reader1 == nil {
		t.Fatal("reader1 is nil")
	}
	if reader2 == nil {
		t.Fatal("reader2 is nil")
	}

	// Both readers must return valid data.
	if reader1.EventCount() != 10 {
		t.Errorf("reader1 EventCount = %d, want 10", reader1.EventCount())
	}
	if reader2.EventCount() != 10 {
		t.Errorf("reader2 EventCount = %d, want 10", reader2.EventCount())
	}

	// The handle should have exactly one mmap — the winner's.
	if sh.mmap == nil {
		t.Fatal("sh.mmap should be set by the winner")
	}
	if sh.reader == nil {
		t.Fatal("sh.reader should be set by the winner")
	}
}

// TestEpochDrainAndCloseSkipsNilMmap verifies that drainAndClose handles
// segments with nil mmaps (in-memory segments) without panicking.
// With per-segment refcounting, drainAndClose calls decRef on all segments
// in the epoch (not just retired), so segments need refs initialized.
func TestEpochDrainAndCloseSkipsNilMmap(t *testing.T) {
	sh1 := makeTestSegmentHandle("no-mmap-1")
	sh2 := makeTestSegmentHandle("no-mmap-2")

	// Simulate the refs that advanceEpoch would have set (1 ref each).
	sh1.incRef()
	sh2.incRef()

	ep := &segmentEpoch{
		id:       1,
		segments: []*segmentHandle{sh1, sh2},
		retired:  []*segmentHandle{sh1, sh2},
		done:     make(chan struct{}),
	}

	// Signal done immediately (no readers).
	ep.signalDone()

	// drainAndClose should not panic on nil mmaps.
	ep.drainAndClose(slog.Default())

	// Give the goroutine time to run.
	time.Sleep(50 * time.Millisecond)

	// Refs should be 0 after drain.
	if got := sh1.refs.Load(); got != 0 {
		t.Errorf("sh1.refs = %d, want 0", got)
	}
	if got := sh2.refs.Load(); got != 0 {
		t.Errorf("sh2.refs = %d, want 0", got)
	}
}

// TestEpochDrainAndCloseNoSegments verifies that drainAndClose with no segments
// still spawns a drain goroutine (to release the done channel) but decRefs nothing.
func TestEpochDrainAndCloseNoSegments(t *testing.T) {
	ep := &segmentEpoch{
		id:   1,
		done: make(chan struct{}),
	}

	ep.signalDone()

	// drainAndClose with 0 segments — should not panic.
	ep.drainAndClose(slog.Default())

	// Give the goroutine time to run.
	time.Sleep(50 * time.Millisecond)
}

// TestEngineIngestAdvancesEpoch verifies that in-memory ingest creates a new
// epoch with the ingested segments.
func TestEngineIngestAdvancesEpoch(t *testing.T) {
	e := newTestEngine(t)

	e.mu.RLock()
	epochBefore := e.currentEpoch.id
	segsBefore := len(e.currentEpoch.segments)
	e.mu.RUnlock()

	if segsBefore != 0 {
		t.Fatalf("expected 0 segments before ingest, got %d", segsBefore)
	}

	// Ingest events in-memory mode (no data dir).
	events := make([]*event.Event, 10)
	now := time.Now()
	for i := 0; i < 10; i++ {
		events[i] = &event.Event{
			Time: now.Add(time.Duration(i) * time.Millisecond),
			Raw:  fmt.Sprintf("test event %d", i),
			Host: "test-host",
		}
	}

	if err := e.Ingest(events); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	e.mu.RLock()
	epochAfter := e.currentEpoch.id
	segsAfter := len(e.currentEpoch.segments)
	e.mu.RUnlock()

	if epochAfter <= epochBefore {
		t.Errorf("epoch did not advance: before=%d, after=%d", epochBefore, epochAfter)
	}
	if segsAfter == 0 {
		t.Fatal("no segments after ingest")
	}
}

// TestEpochMultipleAdvances verifies that epoch IDs increase monotonically
// across multiple advance calls.
func TestEpochMultipleAdvances(t *testing.T) {
	e := newTestEngine(t)

	var lastID uint64
	for i := 0; i < 10; i++ {
		sh := makeTestSegmentHandle(fmt.Sprintf("seg-%d", i))
		e.mu.Lock()
		combined := make([]*segmentHandle, len(e.currentEpoch.segments)+1)
		copy(combined, e.currentEpoch.segments)
		combined[len(combined)-1] = sh
		e.advanceEpoch(combined, nil)
		currentID := e.currentEpoch.id
		e.mu.Unlock()

		if currentID <= lastID {
			t.Errorf("epoch ID not monotonic: prev=%d, current=%d", lastID, currentID)
		}
		lastID = currentID
	}

	// Should have 10 segments.
	e.mu.RLock()
	count := len(e.currentEpoch.segments)
	e.mu.RUnlock()
	if count != 10 {
		t.Fatalf("segment count = %d, want 10", count)
	}
}

// TestPinEpochRaceWithAdvance uses the race detector to verify that pinEpoch
// and advanceEpoch don't race. Run with -race.
func TestPinEpochRaceWithAdvance(t *testing.T) {
	e := newTestEngine(t)

	sh := makeTestSegmentHandle("seg-1")
	e.mu.Lock()
	e.advanceEpoch([]*segmentHandle{sh}, nil)
	e.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Concurrent readers.
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				ep := e.pinEpoch()
				_ = len(ep.segments) // read the segment list
				ep.unpin()
			}
		}()
	}

	// Concurrent writers.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			counter := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				counter++
				newSH := makeTestSegmentHandle(fmt.Sprintf("dyn-%d", counter))
				e.mu.Lock()
				combined := make([]*segmentHandle, len(e.currentEpoch.segments)+1)
				copy(combined, e.currentEpoch.segments)
				combined[len(combined)-1] = newSH
				e.advanceEpoch(combined, nil)
				e.mu.Unlock()
			}
		}()
	}

	wg.Wait()
}

// TestSegmentHandleRefCounting verifies the incRef/decRef lifecycle:
// refs increment, decrement, mmap closes exactly when refs reaches 0,
// and double-decRef past 0 doesn't panic.
func TestSegmentHandleRefCounting(t *testing.T) {
	t.Run("basic lifecycle", func(t *testing.T) {
		sh := makeTestSegmentHandle("ref-test")
		if got := sh.refs.Load(); got != 0 {
			t.Fatalf("initial refs = %d, want 0", got)
		}

		sh.incRef()
		if got := sh.refs.Load(); got != 1 {
			t.Fatalf("after incRef: refs = %d, want 1", got)
		}

		sh.incRef()
		if got := sh.refs.Load(); got != 2 {
			t.Fatalf("after second incRef: refs = %d, want 2", got)
		}

		closed := sh.decRef()
		if closed {
			t.Fatal("decRef returned true at refs=1, want false")
		}
		if got := sh.refs.Load(); got != 1 {
			t.Fatalf("after first decRef: refs = %d, want 1", got)
		}

		closed = sh.decRef()
		if !closed {
			t.Fatal("decRef returned false at refs=0, want true")
		}
		if got := sh.refs.Load(); got != 0 {
			t.Fatalf("after second decRef: refs = %d, want 0", got)
		}
	})

	t.Run("mmap closed at zero", func(t *testing.T) {
		// Create a segment handle with a real mmap.
		dir := t.TempDir()
		path := dir + "/test.lsg"

		events := make([]*event.Event, 5)
		now := time.Now()
		for i := range events {
			events[i] = &event.Event{
				Time: now.Add(time.Duration(i) * time.Millisecond),
				Raw:  fmt.Sprintf("event %d", i),
				Host: "test-host",
			}
		}

		var buf bytes.Buffer
		w := segment.NewWriter(&buf)
		if _, err := w.Write(events); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
			t.Fatalf("write file: %v", err)
		}

		ms, err := segment.OpenSegmentFile(path)
		if err != nil {
			t.Fatalf("open: %v", err)
		}

		sh := &segmentHandle{
			reader: ms.Reader(),
			mmap:   ms,
			meta: model.SegmentMeta{
				ID:    "mmap-ref-test",
				Index: "main",
			},
			index: "main",
		}

		sh.incRef()
		sh.incRef()

		// First decRef: refs=1, mmap should still be open.
		sh.decRef()

		// Reader should still be valid.
		if count := sh.reader.EventCount(); count != 5 {
			t.Fatalf("reader.EventCount = %d, want 5", count)
		}

		// Second decRef: refs=0, mmap should be closed.
		closed := sh.decRef()
		if !closed {
			t.Fatal("expected decRef to return true at refs=0")
		}
	})

	t.Run("decRef past zero is defensive", func(t *testing.T) {
		sh := makeTestSegmentHandle("negative-ref-test")
		sh.incRef()
		sh.decRef()

		// This should not panic — it logs an error and returns false.
		closed := sh.decRef()
		if closed {
			t.Fatal("decRef past 0 returned true, want false")
		}
	})
}

// TestEpochDrainDecrefsAllSegments verifies that drainAndClose decrements refs
// on ALL segments in the epoch (not just retired), and that shared segments
// survive across epoch transitions.
func TestEpochDrainDecrefsAllSegments(t *testing.T) {
	e := newTestEngine(t)

	// Create 3 segments.
	sh1 := makeTestSegmentHandle("seg-A")
	sh2 := makeTestSegmentHandle("seg-B")
	sh3 := makeTestSegmentHandle("seg-C")

	// E(1): [A, B, C]
	e.mu.Lock()
	e.advanceEpoch([]*segmentHandle{sh1, sh2, sh3}, nil)
	e.mu.Unlock()

	// After E(1), each segment has refs=1 (from the E(1) incRef).
	// The initial epoch (E(0)) had 0 segments so its drain is a no-op.
	// Wait briefly for E(0)'s drain goroutine to complete.
	time.Sleep(50 * time.Millisecond)

	if got := sh1.refs.Load(); got != 1 {
		t.Fatalf("sh1.refs after E(1) = %d, want 1", got)
	}
	if got := sh2.refs.Load(); got != 1 {
		t.Fatalf("sh2.refs after E(1) = %d, want 1", got)
	}

	// E(2): [A, B, C, D] — simulates flush adding segment D.
	sh4 := makeTestSegmentHandle("seg-D")
	e.mu.Lock()
	e.advanceEpoch([]*segmentHandle{sh1, sh2, sh3, sh4}, nil)
	e.mu.Unlock()

	// After E(2), A/B/C have refs=2 (one from E(1), one from E(2)). D has refs=1.
	// E(1) had 0 readers so it drains immediately and decRefs A/B/C back to 1.
	time.Sleep(50 * time.Millisecond)

	if got := sh1.refs.Load(); got != 1 {
		t.Fatalf("sh1.refs after E(1) drain = %d, want 1", got)
	}
	if got := sh4.refs.Load(); got != 1 {
		t.Fatalf("sh4.refs after E(2) = %d, want 1", got)
	}

	// E(3): [A, D], retired=[B, C] — simulates compaction removing B and C.
	e.mu.Lock()
	e.advanceEpoch([]*segmentHandle{sh1, sh4}, []*segmentHandle{sh2, sh3})
	e.mu.Unlock()

	// E(2) had 0 readers, drains immediately: decRefs A/B/C/D.
	// After E(2) drain: A.refs=1 (from E(3)), B.refs=0, C.refs=0, D.refs=1 (from E(3)).
	time.Sleep(50 * time.Millisecond)

	if got := sh1.refs.Load(); got != 1 {
		t.Fatalf("sh1.refs after E(2) drain = %d, want 1", got)
	}
	if got := sh2.refs.Load(); got != 0 {
		t.Fatalf("sh2.refs after E(2) drain = %d, want 0 (retired, no longer in any epoch)", got)
	}
	if got := sh3.refs.Load(); got != 0 {
		t.Fatalf("sh3.refs after E(2) drain = %d, want 0 (retired, no longer in any epoch)", got)
	}
	if got := sh4.refs.Load(); got != 1 {
		t.Fatalf("sh4.refs after E(2) drain = %d, want 1", got)
	}
}

// TestEpochMultiEpochRetirement directly reproduces the SIGSEGV scenario from
// the bug report: a long-running query pins E(1), multiple intermediate epochs
// are created with 0 readers, and retired segments must stay alive until the
// original query unpins.
//
// Timeline:
//
//	T0: E(1) has segments [A,B,C,D,E]. Long query Q pins E(1).
//	T1: Flush → advanceEpoch([A,B,C,D,E,L0], nil) → E(2).
//	T2: Compaction loadPartAsSegment → advanceEpoch([A,B,C,D,E,L0,F], nil) → E(3).
//	T3: Compaction remove → advanceEpoch([A,D,E,L0,F], [B,C]) → E(4).
//	T4: Q reads B and C through E(1) — must not SIGSEGV.
//	T5: Q unpins E(1) → B and C refs reach 0 → mmaps closed.
func TestEpochMultiEpochRetirement(t *testing.T) {
	e := newTestEngine(t)

	// Create 5 segments with real readers so we can verify data access.
	segA := makeTestSegmentHandleWithReader(t, "seg-A", 100)
	segB := makeTestSegmentHandleWithReader(t, "seg-B", 100)
	segC := makeTestSegmentHandleWithReader(t, "seg-C", 100)
	segD := makeTestSegmentHandleWithReader(t, "seg-D", 100)
	segE := makeTestSegmentHandleWithReader(t, "seg-E", 100)

	// T0: E(1) = [A, B, C, D, E].
	e.mu.Lock()
	e.advanceEpoch([]*segmentHandle{segA, segB, segC, segD, segE}, nil)
	e.mu.Unlock()

	// Long query Q pins E(1).
	ep1 := e.pinEpoch()
	if len(ep1.segments) != 5 {
		t.Fatalf("E(1) segments = %d, want 5", len(ep1.segments))
	}

	// T1: Flush adds L0 → E(2) = [A, B, C, D, E, L0].
	segL0 := makeTestSegmentHandleWithReader(t, "seg-L0", 50)
	e.mu.Lock()
	e.advanceEpoch([]*segmentHandle{segA, segB, segC, segD, segE, segL0}, nil)
	e.mu.Unlock()

	// T2: Compaction loadPartAsSegment adds F → E(3) = [A, B, C, D, E, L0, F].
	segF := makeTestSegmentHandleWithReader(t, "seg-F", 200)
	e.mu.Lock()
	e.advanceEpoch([]*segmentHandle{segA, segB, segC, segD, segE, segL0, segF}, nil)
	e.mu.Unlock()

	// T3: Compaction removes B, C → E(4) = [A, D, E, L0, F], retired=[B, C].
	e.mu.Lock()
	e.advanceEpoch(
		[]*segmentHandle{segA, segD, segE, segL0, segF},
		[]*segmentHandle{segB, segC},
	)
	e.mu.Unlock()

	// Give intermediate epoch drain goroutines time to run.
	// E(2) and E(3) have 0 readers so they drain immediately.
	time.Sleep(100 * time.Millisecond)

	// T4: THE CRITICAL CHECK — Q on E(1) reads B and C. In the old code,
	// B and C would be unmapped here because E(3) drained immediately and
	// closed their mmaps. With per-segment refcounting, B and C are still
	// alive because E(1) holds refs to them.
	for _, sh := range ep1.segments {
		if sh.reader == nil {
			t.Errorf("segment %q has nil reader", sh.meta.ID)
			continue
		}
		count := sh.reader.EventCount()
		if count != 100 {
			t.Errorf("segment %q: EventCount = %d, want 100", sh.meta.ID, count)
		}
	}

	// Verify B and C refs are still > 0 (held by E(1)).
	if got := segB.refs.Load(); got <= 0 {
		t.Fatalf("segB.refs = %d, want > 0 (E(1) still pinned)", got)
	}
	if got := segC.refs.Load(); got <= 0 {
		t.Fatalf("segC.refs = %d, want > 0 (E(1) still pinned)", got)
	}

	// T5: Q unpins E(1) → E(1) drains → decRefs all of [A,B,C,D,E].
	ep1.unpin()

	// Wait for E(1)'s drain goroutine.
	select {
	case <-ep1.done:
		// Good.
	case <-time.After(2 * time.Second):
		t.Fatal("E(1) done not signaled after unpin")
	}

	// Give drain goroutine time to decRef.
	time.Sleep(50 * time.Millisecond)

	// B and C should now have refs=0 (no epoch references them anymore).
	if got := segB.refs.Load(); got != 0 {
		t.Fatalf("segB.refs after E(1) drain = %d, want 0", got)
	}
	if got := segC.refs.Load(); got != 0 {
		t.Fatalf("segC.refs after E(1) drain = %d, want 0", got)
	}

	// A, D, E should still have refs=1 (from E(4), the current epoch).
	if got := segA.refs.Load(); got != 1 {
		t.Fatalf("segA.refs = %d, want 1 (still in current epoch)", got)
	}
	if got := segD.refs.Load(); got != 1 {
		t.Fatalf("segD.refs = %d, want 1 (still in current epoch)", got)
	}
	if got := segE.refs.Load(); got != 1 {
		t.Fatalf("segE.refs = %d, want 1 (still in current epoch)", got)
	}
}
