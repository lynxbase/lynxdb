package tiering

import (
	"bytes"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/buffer"
)

func newTestBufferPool(t *testing.T, maxPages int) *buffer.Pool {
	t.Helper()
	bp, err := buffer.NewPool(buffer.PoolConfig{
		MaxPages:      maxPages,
		PageSize:      buffer.PageSize64KB,
		EnableOffHeap: false,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(func() { _ = bp.Close() })

	return bp
}

func TestBufferPoolChunkAdapter_PutGet(t *testing.T) {
	pool := newTestBufferPool(t, 16)
	poolCache := buffer.NewSegmentCacheConsumer(pool)
	diskCache := NewSegmentCache(t.TempDir(), 1<<20)
	adapter := NewBufferPoolChunkAdapter(poolCache, diskCache, nil)

	data := []byte("hello, buffer pool chunk adapter")
	adapter.PutChunk("seg-1", 0, "_raw", data)

	// Get should return the data.
	got, ok := adapter.GetChunk("seg-1", 0, "_raw")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}
}

func TestBufferPoolChunkAdapter_Miss(t *testing.T) {
	pool := newTestBufferPool(t, 8)
	poolCache := buffer.NewSegmentCacheConsumer(pool)
	diskCache := NewSegmentCache(t.TempDir(), 1<<20)
	adapter := NewBufferPoolChunkAdapter(poolCache, diskCache, nil)

	// No data stored — should miss.
	_, ok := adapter.GetChunk("seg-missing", 0, "_raw")
	if ok {
		t.Fatal("expected cache miss")
	}
}

func TestBufferPoolChunkAdapter_EvictionFallback(t *testing.T) {
	// Very small pool: 2 pages. Put data, then evict pool pages by
	// allocating all pages for something else, then verify disk fallback.
	pool := newTestBufferPool(t, 4)
	poolCache := buffer.NewSegmentCacheConsumer(pool)
	diskCache := NewSegmentCache(t.TempDir(), 1<<20)
	adapter := NewBufferPoolChunkAdapter(poolCache, diskCache, nil)

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
	poolCache.InvalidateSegment("seg-evict")

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
	// Pool with 2 pages — exhaust them with pinned allocations, then
	// verify Put falls back to disk-only gracefully.
	pool := newTestBufferPool(t, 2)
	poolCache := buffer.NewSegmentCacheConsumer(pool)
	diskCache := NewSegmentCache(t.TempDir(), 1<<20)
	adapter := NewBufferPoolChunkAdapter(poolCache, diskCache, nil)

	// Pin all pages so the pool is exhausted.
	alloc := buffer.NewOperatorPageAllocator(pool, "blocker")
	for i := 0; i < 2; i++ {
		_, err := alloc.AllocPage()
		if err != nil {
			t.Fatalf("AllocPage %d: %v", i, err)
		}
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

	alloc.ReleaseAll()
}

func TestBufferPoolChunkAdapter_SatisfiesChunkCache(t *testing.T) {
	pool := newTestBufferPool(t, 4)
	poolCache := buffer.NewSegmentCacheConsumer(pool)
	diskCache := NewSegmentCache(t.TempDir(), 1<<20)

	// Verify the adapter satisfies ChunkCache at compile time and runtime.
	var cc ChunkCache = NewBufferPoolChunkAdapter(poolCache, diskCache, nil)
	cc.PutChunk("seg-iface", 0, "ts", []byte{1, 2, 3})
	got, ok := cc.GetChunk("seg-iface", 0, "ts")
	if !ok {
		t.Fatal("expected hit via interface")
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 bytes, got %d", len(got))
	}
}
