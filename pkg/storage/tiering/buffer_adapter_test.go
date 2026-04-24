package tiering

import (
	"bytes"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/bufmgr"
	"github.com/lynxbase/lynxdb/pkg/bufmgr/consumers"
)

func newTestBufMgr(t *testing.T, maxFrames int) bufmgr.Manager {
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

func TestBufferPoolChunkAdapter_PutGet(t *testing.T) {
	mgr := newTestBufMgr(t, 16)
	segCache := consumers.NewSegmentCacheConsumer(mgr)
	diskCache := NewSegmentCache(t.TempDir(), 1<<20)
	adapter := NewBufferPoolChunkAdapter(segCache, diskCache, nil)

	data := []byte("hello, buffer pool chunk adapter")
	adapter.PutChunk("seg-1", 0, "_raw", data)

	got, ok := adapter.GetChunk("seg-1", 0, "_raw")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}
}

func TestBufferPoolChunkAdapter_Miss(t *testing.T) {
	mgr := newTestBufMgr(t, 8)
	segCache := consumers.NewSegmentCacheConsumer(mgr)
	diskCache := NewSegmentCache(t.TempDir(), 1<<20)
	adapter := NewBufferPoolChunkAdapter(segCache, diskCache, nil)

	// No data stored — should miss.
	_, ok := adapter.GetChunk("seg-missing", 0, "_raw")
	if ok {
		t.Fatal("expected cache miss")
	}
}

func TestBufferPoolChunkAdapter_EvictionFallback(t *testing.T) {
	// Small pool: 4 frames. Put data, then invalidate pool entries to
	// simulate eviction, then verify disk fallback.
	mgr := newTestBufMgr(t, 4)
	segCache := consumers.NewSegmentCacheConsumer(mgr)
	diskCache := NewSegmentCache(t.TempDir(), 1<<20)
	adapter := NewBufferPoolChunkAdapter(segCache, diskCache, nil)

	data := []byte("important chunk data for eviction test")
	adapter.PutChunk("seg-evict", 0, "_raw", data)

	// Verify it's cached.
	got, ok := adapter.GetChunk("seg-evict", 0, "_raw")
	if !ok {
		t.Fatal("expected cache hit before eviction")
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}

	// Invalidate pool cache entries to simulate eviction.
	segCache.InvalidateSegment("seg-evict")

	// Pool cache miss, but disk cache should have the data.
	got, ok = adapter.GetChunk("seg-evict", 0, "_raw")
	if !ok {
		t.Fatal("expected disk fallback hit after pool eviction")
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}
}

func TestBufferPoolChunkAdapter_PoolExhaustedFallback(t *testing.T) {
	// Pool with 2 frames — exhaust them with pinned allocations, then
	// verify Put falls back to disk-only gracefully.
	mgr := newTestBufMgr(t, 2)
	segCache := consumers.NewSegmentCacheConsumer(mgr)
	diskCache := NewSegmentCache(t.TempDir(), 1<<20)
	adapter := NewBufferPoolChunkAdapter(segCache, diskCache, nil)

	// Pin all frames so the pool is exhausted.
	var pinnedFrames []*bufmgr.Frame
	for i := 0; i < 2; i++ {
		f, err := mgr.AllocFrame(bufmgr.OwnerQuery, "blocker")
		if err != nil {
			t.Fatalf("AllocFrame %d: %v", i, err)
		}
		pinnedFrames = append(pinnedFrames, f)
	}

	// Put should not panic — falls back to disk.
	data := []byte("disk-only chunk")
	adapter.PutChunk("seg-full", 0, "_raw", data)

	// Pool miss expected, but disk should have it.
	got, ok := adapter.GetChunk("seg-full", 0, "_raw")
	if !ok {
		t.Fatal("expected disk fallback hit when pool exhausted")
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}

	// Release pinned frames.
	for _, f := range pinnedFrames {
		f.Unpin()
	}
}

func TestBufferPoolChunkAdapter_SatisfiesChunkCache(t *testing.T) {
	mgr := newTestBufMgr(t, 4)
	segCache := consumers.NewSegmentCacheConsumer(mgr)
	diskCache := NewSegmentCache(t.TempDir(), 1<<20)

	// Verify the adapter satisfies ChunkCache at compile time and runtime.
	var cc ChunkCache = NewBufferPoolChunkAdapter(segCache, diskCache, nil)
	cc.PutChunk("seg-iface", 0, "ts", []byte{1, 2, 3})
	got, ok := cc.GetChunk("seg-iface", 0, "ts")
	if !ok {
		t.Fatal("expected hit via interface")
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 bytes, got %d", len(got))
	}
}
