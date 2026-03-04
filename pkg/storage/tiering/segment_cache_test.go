package tiering

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/internal/objstore"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/model"
	segment "github.com/lynxbase/lynxdb/pkg/storage/segment"
)

// SegmentCache tests

func TestSegmentCache_FooterCaching(t *testing.T) {
	sc := NewSegmentCache("", 1<<30)

	footer := &segment.Footer{EventCount: 42}
	sc.PutFooter("key1", footer)

	cached := sc.GetFooter("key1")
	if cached == nil {
		t.Fatal("expected cached footer")
	}
	if cached.EventCount != 42 {
		t.Errorf("EventCount: got %d, want 42", cached.EventCount)
	}

	if sc.GetFooter("nonexistent") != nil {
		t.Error("expected nil for missing key")
	}
}

func TestSegmentCache_FooterEviction(t *testing.T) {
	sc := NewSegmentCache("", 1<<30)
	sc.maxFooters = 3

	for i := 0; i < 5; i++ {
		sc.PutFooter(fmt.Sprintf("key%d", i), &segment.Footer{EventCount: int64(i)})
	}

	if sc.FooterCount() != 3 {
		t.Errorf("footer count: got %d, want 3", sc.FooterCount())
	}
}

func TestSegmentCache_ChunkCaching(t *testing.T) {
	dir := t.TempDir()
	sc := NewSegmentCache(dir, 1<<30)

	data := []byte("column chunk data here")
	sc.PutChunk("seg-001", 0, "_raw", data)

	cached, ok := sc.GetChunk("seg-001", 0, "_raw")
	if !ok {
		t.Fatal("expected cached chunk")
	}
	if !bytes.Equal(cached, data) {
		t.Errorf("data mismatch: %q", cached)
	}

	// Miss.
	_, ok = sc.GetChunk("seg-001", 1, "_raw")
	if ok {
		t.Error("expected miss for non-cached chunk")
	}
}

func TestSegmentCache_DiskEviction(t *testing.T) {
	dir := t.TempDir()
	sc := NewSegmentCache(dir, 100) // 100 bytes max

	// Insert chunks that exceed the limit.
	for i := 0; i < 5; i++ {
		data := make([]byte, 30) // 30 bytes each → 150 bytes > 100
		sc.PutChunk(fmt.Sprintf("seg-%d", i), 0, "_raw", data)
	}

	if sc.DiskBytes() > 100 {
		t.Errorf("disk bytes should be <= 100, got %d", sc.DiskBytes())
	}

	// Some old chunks should have been evicted.
	if sc.ChunkCount() >= 5 {
		t.Errorf("expected eviction, got %d chunks", sc.ChunkCount())
	}
}

func TestSegmentCache_Clear(t *testing.T) {
	dir := t.TempDir()
	sc := NewSegmentCache(dir, 1<<30)

	sc.PutFooter("k1", &segment.Footer{})
	sc.PutChunk("s1", 0, "col", []byte("data"))

	sc.Clear()

	if sc.FooterCount() != 0 {
		t.Errorf("footers: %d", sc.FooterCount())
	}
	if sc.ChunkCount() != 0 {
		t.Errorf("chunks: %d", sc.ChunkCount())
	}
}

// LazyFetcher tests

func makeTestSegment(t *testing.T, eventCount int) []byte {
	t.Helper()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := make([]*event.Event, eventCount)
	for i := range events {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			fmt.Sprintf("host=web-%02d level=INFO msg=\"event %d\"", i%5, i))
		e.Host = fmt.Sprintf("web-%02d", i%5)
		e.Index = "main"
		events[i] = e
	}

	var buf bytes.Buffer
	sw := segment.NewWriter(&buf)
	if _, err := sw.Write(events); err != nil {
		t.Fatal(err)
	}

	return buf.Bytes()
}

func TestLazyFetcher_FooterFetch(t *testing.T) {
	store := objstore.NewMemStore()
	cache := NewSegmentCache("", 1<<30)
	fetcher := NewLazyFetcher(store, cache)
	ctx := context.Background()

	// Create a real segment and upload it.
	data := makeTestSegment(t, 100)
	key := "warm/main/seg-001.lsg"
	store.Put(ctx, key, data)

	// Fetch footer.
	footer, err := fetcher.FetchFooter(ctx, key, int64(len(data)))
	if err != nil {
		t.Fatalf("FetchFooter: %v", err)
	}
	if footer.EventCount != 100 {
		t.Errorf("EventCount: got %d, want 100", footer.EventCount)
	}

	// Second fetch should come from cache.
	footer2, err := fetcher.FetchFooter(ctx, key, int64(len(data)))
	if err != nil {
		t.Fatalf("FetchFooter cached: %v", err)
	}
	if footer2.EventCount != 100 {
		t.Errorf("cached EventCount: got %d, want 100", footer2.EventCount)
	}
	if cache.FooterCount() != 1 {
		t.Errorf("footer cache count: %d", cache.FooterCount())
	}
}

func TestLazyFetcher_ColumnChunkFetch(t *testing.T) {
	store := &countingStore{ObjectStore: objstore.NewMemStore()}
	dir := t.TempDir()
	cache := NewSegmentCache(dir, 1<<30)
	fetcher := NewLazyFetcher(store, cache)
	ctx := context.Background()

	// Upload segment data.
	data := makeTestSegment(t, 100)
	key := "warm/main/seg-002.lsg"
	store.Put(ctx, key, data)

	// Fetch a column chunk (simulated by range read).
	chunk, err := fetcher.FetchColumnChunk(ctx, key, "seg-002", 0, "_raw", 100, 200)
	if err != nil {
		t.Fatalf("FetchColumnChunk: %v", err)
	}
	if len(chunk) != 200 {
		t.Errorf("chunk length: got %d, want 200", len(chunk))
	}

	rangeCount1 := store.getRangeCount.Load()

	// Second fetch should come from disk cache.
	chunk2, err := fetcher.FetchColumnChunk(ctx, key, "seg-002", 0, "_raw", 100, 200)
	if err != nil {
		t.Fatalf("FetchColumnChunk cached: %v", err)
	}
	if !bytes.Equal(chunk, chunk2) {
		t.Error("cached chunk mismatch")
	}

	rangeCount2 := store.getRangeCount.Load()
	if rangeCount2 != rangeCount1 {
		t.Errorf("expected no additional GetRange calls, got %d -> %d", rangeCount1, rangeCount2)
	}
}

// UploadPipeline tests

func TestUploadPipeline_SmallSegment(t *testing.T) {
	store := objstore.NewMemStore()
	pipeline := NewUploadPipeline(store, UploadConfig{PartSize: 64 << 20})
	ctx := context.Background()

	data := []byte("small segment data")
	result, err := pipeline.Upload(ctx, "segments/seg-001.lsg", data)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if result.Parts != 1 {
		t.Errorf("parts: got %d, want 1", result.Parts)
	}
	if result.TotalSize != int64(len(data)) {
		t.Errorf("total size: got %d", result.TotalSize)
	}

	// Verify data in store.
	got, err := store.Get(ctx, "segments/seg-001.lsg")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Error("data mismatch")
	}
}

func TestUploadPipeline_Upload(t *testing.T) {
	store := objstore.NewMemStore()
	pipeline := NewUploadPipeline(store, UploadConfig{
		ConcurrentUploads: 2,
	})
	ctx := context.Background()

	// Create a 1000-byte segment.
	data := make([]byte, 1000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	result, err := pipeline.Upload(ctx, "segments/big.lsg", data)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if result.Parts != 1 {
		t.Errorf("parts: got %d, want 1", result.Parts)
	}

	// Verify data.
	got, err := store.Get(ctx, "segments/big.lsg")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Error("data mismatch")
	}

	// No part keys should exist.
	keys, _ := store.List(ctx, "segments/big.lsg.part.")
	if len(keys) != 0 {
		t.Errorf("unexpected part keys: %v", keys)
	}
}

func TestUploadPipeline_SafeUpload(t *testing.T) {
	store := objstore.NewMemStore()
	pipeline := NewUploadPipeline(store, UploadConfig{})
	ctx := context.Background()

	data := []byte("important data")
	safe, err := pipeline.SafeUpload(ctx, "segments/safe.lsg", data)
	if err != nil {
		t.Fatalf("SafeUpload: %v", err)
	}
	if !safe {
		t.Error("expected safeToDeleteLocal=true")
	}
}

func TestUploadPipeline_FailureSafety(t *testing.T) {
	store := &failingStore{ObjectStore: objstore.NewMemStore(), failOnPut: true}
	pipeline := NewUploadPipeline(store, UploadConfig{})
	ctx := context.Background()

	data := []byte("data that should not be lost")
	safe, err := pipeline.SafeUpload(ctx, "segments/fail.lsg", data)
	if err == nil {
		t.Error("expected error from failing store")
	}
	if safe {
		t.Error("should not be safe to delete local file on failure")
	}
}

// Helper types

// countingStore wraps an ObjectStore and counts GetRange calls.
type countingStore struct {
	objstore.ObjectStore
	getRangeCount atomic.Int64
}

func (cs *countingStore) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	cs.getRangeCount.Add(1)

	return cs.ObjectStore.GetRange(ctx, key, offset, length)
}

// failingStore wraps an ObjectStore and fails on Put.
type failingStore struct {
	objstore.ObjectStore
	failOnPut bool
}

func (fs *failingStore) Put(ctx context.Context, key string, data []byte) error {
	if fs.failOnPut {
		return fmt.Errorf("simulated put failure")
	}

	return fs.ObjectStore.Put(ctx, key, data)
}

// Integration test: lazy fetch + cache reuse

func TestIntegration_LazyFetchCacheReuse(t *testing.T) {
	store := &countingStore{ObjectStore: objstore.NewMemStore()}
	dir := t.TempDir()
	cache := NewSegmentCache(dir, 1<<30)
	fetcher := NewLazyFetcher(store, cache)
	ctx := context.Background()

	// Upload a real segment.
	data := makeTestSegment(t, 500)
	key := "warm/main/seg-int.lsg"
	store.Put(ctx, key, data)

	// First footer fetch: 1 GetRange call.
	_, err := fetcher.FetchFooter(ctx, key, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	calls1 := store.getRangeCount.Load()
	if calls1 != 1 {
		t.Errorf("after first footer: %d calls, want 1", calls1)
	}

	// Second footer fetch: cached, no new calls.
	_, err = fetcher.FetchFooter(ctx, key, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if store.getRangeCount.Load() != calls1 {
		t.Error("footer should be cached")
	}

	// Fetch a chunk: 1 new GetRange call.
	_, err = fetcher.FetchColumnChunk(ctx, key, "seg-int", 0, "_raw", 16, 100)
	if err != nil {
		t.Fatal(err)
	}
	calls2 := store.getRangeCount.Load()
	if calls2 != calls1+1 {
		t.Errorf("after chunk fetch: %d calls, want %d", calls2, calls1+1)
	}

	// Fetch same chunk again: cached, no new calls.
	_, err = fetcher.FetchColumnChunk(ctx, key, "seg-int", 0, "_raw", 16, 100)
	if err != nil {
		t.Fatal(err)
	}
	if store.getRangeCount.Load() != calls2 {
		t.Error("chunk should be cached")
	}
}

func TestManager_MoveToWarmWithPipeline(t *testing.T) {
	store := objstore.NewMemStore()
	mgr := NewManager(store, testLogger())
	ctx := context.Background()

	pipeline := NewUploadPipeline(store, UploadConfig{})

	meta := model.SegmentMeta{ID: "seg-pipe", Index: "main"}
	mgr.AddSegment(meta)

	data := make([]byte, 200)
	for i := range data {
		data[i] = byte(i)
	}

	key := fmt.Sprintf("warm/%s/%s.lsg", meta.Index, meta.ID)
	result, err := pipeline.Upload(ctx, key, data)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if result.Parts != 1 {
		t.Errorf("parts: got %d, want 1", result.Parts)
	}

	// Verify data.
	got, err := store.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Error("data mismatch")
	}
}

func BenchmarkSegmentCacheChunkReadWrite(b *testing.B) {
	dir := b.TempDir()
	sc := NewSegmentCache(dir, 1<<30)

	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("seg-%d", i%100)
		sc.PutChunk(key, 0, "_raw", data)
		sc.GetChunk(key, 0, "_raw")
	}
}

func BenchmarkUploadPipeline(b *testing.B) {
	store := objstore.NewMemStore()
	pipeline := NewUploadPipeline(store, UploadConfig{PartSize: 1 << 20})
	ctx := context.Background()

	data := make([]byte, 10<<20) // 10MB
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pipeline.Upload(ctx, fmt.Sprintf("bench/seg-%d.lsg", i), data)
	}
}

func TestSegmentCache_ChunkDeletedExternally(t *testing.T) {
	dir := t.TempDir()
	sc := NewSegmentCache(dir, 1<<30)

	data := []byte("chunk data")
	sc.PutChunk("seg-ext", 0, "_raw", data)

	// Delete the file externally.
	path := sc.chunkPath("seg-ext", 0, "_raw")
	os.Remove(path)

	// Should return miss and clean up index.
	_, ok := sc.GetChunk("seg-ext", 0, "_raw")
	if ok {
		t.Error("expected miss after external delete")
	}
	if sc.ChunkCount() != 0 {
		t.Errorf("chunk count: got %d, want 0", sc.ChunkCount())
	}
}
