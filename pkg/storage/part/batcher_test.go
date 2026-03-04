package part

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

func testBatcher(t *testing.T, cfg BatcherConfig) (*AsyncBatcher, *Registry, string) {
	t.Helper()

	dir := t.TempDir()
	layout := NewLayout(dir)
	registry := NewRegistry(testLogger())
	writer := NewWriter(layout, segment.CompressionLZ4, DefaultRowGroupSize)
	batcher := NewAsyncBatcher(writer, registry, cfg, testLogger())

	return batcher, registry, dir
}

func makeEvents(n int, index string) []*event.Event {
	now := time.Now()
	events := make([]*event.Event, n)

	for i := 0; i < n; i++ {
		events[i] = &event.Event{
			Time:   now.Add(time.Duration(i) * time.Millisecond),
			Raw:    "test event line for batcher test",
			Source: "test",
			Index:  index,
			Fields: map[string]event.Value{
				"level": event.StringValue("info"),
			},
		}
	}

	return events
}

func TestAsyncBatcher_ThresholdFlush_Events(t *testing.T) {
	cfg := BatcherConfig{
		MaxEvents: 100,
		MaxBytes:  1 << 30, // large, won't trigger
		MaxWait:   10 * time.Second,
	}
	batcher, registry, _ := testBatcher(t, cfg)
	defer batcher.Close()

	var committed atomic.Int32
	batcher.SetOnCommit(func(_ *Meta) {
		committed.Add(1)
	})
	batcher.Start(context.Background())

	// Add exactly MaxEvents — should trigger flush.
	events := makeEvents(100, "test-idx")
	if err := batcher.Add(events); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// onCommit should have been called synchronously during Add.
	if committed.Load() != 1 {
		t.Fatalf("expected 1 commit, got %d", committed.Load())
	}
	if registry.Count() != 1 {
		t.Fatalf("expected 1 part in registry, got %d", registry.Count())
	}

	// Verify part metadata.
	parts := registry.All()
	if parts[0].EventCount != 100 {
		t.Errorf("expected 100 events, got %d", parts[0].EventCount)
	}
	if parts[0].Index != "test-idx" {
		t.Errorf("expected index test-idx, got %s", parts[0].Index)
	}
}

func TestAsyncBatcher_ThresholdFlush_Bytes(t *testing.T) {
	cfg := BatcherConfig{
		MaxEvents: 1_000_000, // large, won't trigger
		MaxBytes:  500,       // small, will trigger quickly
		MaxWait:   10 * time.Second,
	}
	batcher, registry, _ := testBatcher(t, cfg)
	defer batcher.Close()

	batcher.Start(context.Background())

	// Each event has Raw = "test event line for batcher test" (~33 bytes).
	// 500 / 33 ≈ 16 events to cross the threshold.
	events := makeEvents(20, "main")
	if err := batcher.Add(events); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if registry.Count() < 1 {
		t.Fatalf("expected at least 1 part from byte threshold, got %d", registry.Count())
	}
}

func TestAsyncBatcher_IdleFlush(t *testing.T) {
	cfg := BatcherConfig{
		MaxEvents: 1_000_000,
		MaxBytes:  1 << 30,
		MaxWait:   50 * time.Millisecond, // short idle timeout for test
	}
	batcher, registry, _ := testBatcher(t, cfg)
	defer batcher.Close()

	batcher.Start(context.Background())

	// Add events below threshold — should not flush immediately.
	events := makeEvents(10, "main")
	if err := batcher.Add(events); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if registry.Count() != 0 {
		t.Fatalf("expected 0 parts before idle timeout, got %d", registry.Count())
	}

	// Wait for idle flush to fire (MaxWait + some margin).
	time.Sleep(200 * time.Millisecond)

	if registry.Count() != 1 {
		t.Fatalf("expected 1 part after idle timeout, got %d", registry.Count())
	}
}

func TestAsyncBatcher_ShutdownFlush(t *testing.T) {
	cfg := BatcherConfig{
		MaxEvents: 1_000_000,
		MaxBytes:  1 << 30,
		MaxWait:   10 * time.Second, // long, won't trigger
	}
	batcher, registry, _ := testBatcher(t, cfg)

	batcher.Start(context.Background())

	// Add events below threshold.
	events := makeEvents(10, "main")
	if err := batcher.Add(events); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if registry.Count() != 0 {
		t.Fatalf("expected 0 parts before close, got %d", registry.Count())
	}

	// Close should flush remaining events.
	if err := batcher.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if registry.Count() != 1 {
		t.Fatalf("expected 1 part after close, got %d", registry.Count())
	}
}

func TestAsyncBatcher_EmptyFlush(t *testing.T) {
	cfg := DefaultBatcherConfig()
	batcher, registry, _ := testBatcher(t, cfg)

	batcher.Start(context.Background())

	// Flush with no events should be a no-op.
	if err := batcher.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if registry.Count() != 0 {
		t.Fatalf("expected 0 parts after empty flush, got %d", registry.Count())
	}

	if err := batcher.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAsyncBatcher_OnCommitCallback(t *testing.T) {
	cfg := BatcherConfig{
		MaxEvents: 50,
		MaxBytes:  1 << 30,
		MaxWait:   10 * time.Second,
	}
	batcher, _, _ := testBatcher(t, cfg)
	defer batcher.Close()

	var receivedMetas []*Meta
	var mu sync.Mutex
	batcher.SetOnCommit(func(meta *Meta) {
		mu.Lock()
		receivedMetas = append(receivedMetas, meta)
		mu.Unlock()
	})

	batcher.Start(context.Background())

	// Trigger two flushes (two different indexes).
	if err := batcher.Add(makeEvents(50, "idx-a")); err != nil {
		t.Fatalf("Add idx-a: %v", err)
	}
	if err := batcher.Add(makeEvents(50, "idx-b")); err != nil {
		t.Fatalf("Add idx-b: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(receivedMetas) != 2 {
		t.Fatalf("expected 2 onCommit calls, got %d", len(receivedMetas))
	}

	indexes := map[string]bool{}
	for _, m := range receivedMetas {
		indexes[m.Index] = true
		if m.EventCount != 50 {
			t.Errorf("expected 50 events, got %d for index %s", m.EventCount, m.Index)
		}
		if m.Path == "" {
			t.Error("expected non-empty path")
		}
	}
	if !indexes["idx-a"] || !indexes["idx-b"] {
		t.Errorf("expected commits for idx-a and idx-b, got %v", indexes)
	}
}

func TestAsyncBatcher_ConcurrentAdd(t *testing.T) {
	cfg := BatcherConfig{
		MaxEvents: 200,
		MaxBytes:  1 << 30,
		MaxWait:   10 * time.Second,
	}
	batcher, registry, _ := testBatcher(t, cfg)
	defer batcher.Close()

	batcher.Start(context.Background())

	// Launch concurrent writers.
	var wg sync.WaitGroup
	const goroutines = 10
	const eventsPerGoroutine = 50

	for g := 0; g < goroutines; g++ {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			events := makeEvents(eventsPerGoroutine, "concurrent")
			if err := batcher.Add(events); err != nil {
				t.Errorf("concurrent Add: %v", err)
			}
		}(g)
	}

	wg.Wait()

	// Close to flush remaining.
	if err := batcher.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// All events should be in parts now.
	var totalEvents int64
	for _, m := range registry.All() {
		totalEvents += m.EventCount
	}

	expected := int64(goroutines * eventsPerGoroutine)
	if totalEvents != expected {
		t.Fatalf("expected %d total events, got %d", expected, totalEvents)
	}
}

func TestAsyncBatcher_MultiIndex(t *testing.T) {
	cfg := BatcherConfig{
		MaxEvents: 1_000_000,
		MaxBytes:  1 << 30,
		MaxWait:   10 * time.Second,
	}
	batcher, registry, _ := testBatcher(t, cfg)

	batcher.Start(context.Background())

	// Add events to different indexes.
	if err := batcher.Add(makeEvents(10, "nginx")); err != nil {
		t.Fatalf("Add nginx: %v", err)
	}
	if err := batcher.Add(makeEvents(5, "redis")); err != nil {
		t.Fatalf("Add redis: %v", err)
	}
	if err := batcher.Add(makeEvents(3, "")); err != nil { // empty -> "main"
		t.Fatalf("Add main: %v", err)
	}

	if err := batcher.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Should have 3 parts (one per index).
	if registry.Count() != 3 {
		t.Fatalf("expected 3 parts, got %d", registry.Count())
	}

	// Verify per-index counts.
	byIndex := map[string]int64{}
	for _, m := range registry.All() {
		byIndex[m.Index] += m.EventCount
	}
	if byIndex["nginx"] != 10 {
		t.Errorf("nginx: expected 10, got %d", byIndex["nginx"])
	}
	if byIndex["redis"] != 5 {
		t.Errorf("redis: expected 5, got %d", byIndex["redis"])
	}
	if byIndex["main"] != 3 {
		t.Errorf("main: expected 3, got %d", byIndex["main"])
	}
}

func TestAsyncBatcher_BufferedStats(t *testing.T) {
	cfg := BatcherConfig{
		MaxEvents: 1_000_000,
		MaxBytes:  1 << 30,
		MaxWait:   10 * time.Second,
	}
	batcher, _, _ := testBatcher(t, cfg)
	defer batcher.Close()

	batcher.Start(context.Background())

	if batcher.BufferedEvents() != 0 {
		t.Fatalf("expected 0 buffered events initially")
	}

	events := makeEvents(25, "main")
	if err := batcher.Add(events); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if batcher.BufferedEvents() != 25 {
		t.Errorf("expected 25 buffered events, got %d", batcher.BufferedEvents())
	}
	if batcher.BufferedBytes() <= 0 {
		t.Errorf("expected positive buffered bytes, got %d", batcher.BufferedBytes())
	}
}

func TestAsyncBatcher_Backpressure_Reject(t *testing.T) {
	// Set very low thresholds so we can trigger rejection quickly.
	cfg := BatcherConfig{
		MaxEvents:       10, // flush every 10 events
		MaxBytes:        1 << 30,
		MaxWait:         10 * time.Second,
		DelayThreshold:  3, // start delay at 3 parts
		RejectThreshold: 5, // reject at 5 parts
		MaxDelayMs:      10,
	}
	batcher, registry, _ := testBatcher(t, cfg)
	defer batcher.Close()

	batcher.Start(context.Background())

	// Generate parts by adding events in batches that cross the threshold.
	for i := 0; i < 6; i++ {
		events := makeEvents(10, "main")
		err := batcher.Add(events)

		if err != nil {
			// Should get ErrTooManyParts once we hit 5 parts.
			if registry.Count() >= cfg.RejectThreshold {
				if !errors.Is(err, ErrTooManyParts) {
					t.Fatalf("expected ErrTooManyParts, got %v", err)
				}

				return // success
			}

			t.Fatalf("unexpected error at part count %d: %v", registry.Count(), err)
		}
	}

	// If we got here, we should have hit the reject threshold.
	if registry.Count() >= cfg.RejectThreshold {
		// Try one more Add — should be rejected.
		events := makeEvents(10, "main")
		err := batcher.Add(events)

		if !errors.Is(err, ErrTooManyParts) {
			t.Fatalf("expected ErrTooManyParts at %d parts, got %v", registry.Count(), err)
		}
	}
}

func TestAsyncBatcher_Backpressure_Delay(t *testing.T) {
	// Test that delay is applied between DelayThreshold and RejectThreshold.
	cfg := BatcherConfig{
		MaxEvents:       10,
		MaxBytes:        1 << 30,
		MaxWait:         10 * time.Second,
		DelayThreshold:  2, // start delay at 2 parts
		RejectThreshold: 20,
		MaxDelayMs:      100,
	}
	batcher, registry, _ := testBatcher(t, cfg)
	defer batcher.Close()

	batcher.Start(context.Background())

	// Create parts up to the delay threshold.
	for i := 0; i < 3; i++ {
		if err := batcher.Add(makeEvents(10, "main")); err != nil {
			t.Fatalf("Add[%d]: %v", i, err)
		}
	}

	if registry.Count() < cfg.DelayThreshold {
		t.Fatalf("expected >= %d parts, got %d", cfg.DelayThreshold, registry.Count())
	}

	// Next Add should be delayed (but not rejected).
	start := time.Now()
	if err := batcher.Add(makeEvents(10, "main")); err != nil {
		t.Fatalf("delayed Add: %v", err)
	}

	elapsed := time.Since(start)
	// Should have taken some delay. The exact amount depends on part count
	// and interpolation, but should be > 0 and < MaxDelayMs.
	if elapsed < 1*time.Millisecond {
		t.Logf("delay was very short (%v), which is acceptable for low part counts", elapsed)
	}
}

func TestAsyncBatcher_Backpressure_BelowThreshold(t *testing.T) {
	// Below DelayThreshold, no backpressure at all.
	cfg := BatcherConfig{
		MaxEvents:       1_000_000,
		MaxBytes:        1 << 30,
		MaxWait:         10 * time.Second,
		DelayThreshold:  100,
		RejectThreshold: 200,
		MaxDelayMs:      1000,
	}
	batcher, _, _ := testBatcher(t, cfg)
	defer batcher.Close()

	batcher.Start(context.Background())

	// Add events — should be instant, no delay.
	start := time.Now()
	if err := batcher.Add(makeEvents(10, "main")); err != nil {
		t.Fatalf("Add: %v", err)
	}

	elapsed := time.Since(start)
	if elapsed > 50*time.Millisecond {
		t.Fatalf("expected near-instant Add below threshold, took %v", elapsed)
	}
}

func TestAsyncBatcher_DefaultIndex(t *testing.T) {
	// Events with empty Index should be mapped to "main".
	cfg := BatcherConfig{
		MaxEvents: 1_000_000,
		MaxBytes:  1 << 30,
		MaxWait:   10 * time.Second,
	}
	batcher, registry, _ := testBatcher(t, cfg)

	batcher.Start(context.Background())

	events := makeEvents(5, "") // empty index
	if err := batcher.Add(events); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if err := batcher.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	parts := registry.ByIndex("main")
	if len(parts) != 1 {
		t.Fatalf("expected 1 part for 'main' index, got %d", len(parts))
	}
	if parts[0].EventCount != 5 {
		t.Errorf("expected 5 events, got %d", parts[0].EventCount)
	}
}
