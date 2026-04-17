package part

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// Default backpressure thresholds (ClickHouse-style).
const (
	DefaultDelayThreshold  = 150 // start adding progressive delay
	DefaultRejectThreshold = 300 // reject ingests entirely
	DefaultMaxDelayMs      = 1000
)

// BatcherConfig controls flush thresholds for the AsyncBatcher.
type BatcherConfig struct {
	// MaxEvents is the maximum number of events per index before a flush
	// is triggered. Default: 50,000.
	MaxEvents int

	// MaxBytes is the maximum total raw byte size per index before a flush
	// is triggered. Default: 64MB.
	MaxBytes int64

	// MaxWait is the maximum time events can sit in the buffer before being
	// flushed. Default: 200ms.
	MaxWait time.Duration

	// DelayThreshold: when total part count exceeds this, Add() sleeps with
	// progressive delay before returning. This gives compaction time to catch
	// up. Default: 150.
	DelayThreshold int

	// RejectThreshold: when total part count exceeds this, Add() returns
	// ErrTooManyParts. This prevents unbounded part accumulation when
	// compaction falls far behind ingest. Default: 300.
	RejectThreshold int

	// MaxDelayMs: maximum sleep duration in milliseconds at the delay
	// threshold. The actual delay is linearly interpolated between 0
	// (at DelayThreshold) and MaxDelayMs (at RejectThreshold). Default: 1000.
	MaxDelayMs int
}

// DefaultBatcherConfig returns a BatcherConfig with production-ready defaults.
func DefaultBatcherConfig() BatcherConfig {
	return BatcherConfig{
		MaxEvents:       50_000,
		MaxBytes:        64 * 1024 * 1024, // 64MB
		MaxWait:         200 * time.Millisecond,
		DelayThreshold:  DefaultDelayThreshold,
		RejectThreshold: DefaultRejectThreshold,
		MaxDelayMs:      DefaultMaxDelayMs,
	}
}

func (c BatcherConfig) withDefaults() BatcherConfig {
	if c.MaxEvents <= 0 {
		c.MaxEvents = 50_000
	}

	if c.MaxBytes <= 0 {
		c.MaxBytes = 64 * 1024 * 1024
	}

	if c.MaxWait <= 0 {
		c.MaxWait = 200 * time.Millisecond
	}

	if c.DelayThreshold <= 0 {
		c.DelayThreshold = DefaultDelayThreshold
	}

	if c.RejectThreshold <= 0 {
		c.RejectThreshold = DefaultRejectThreshold
	}

	if c.MaxDelayMs <= 0 {
		c.MaxDelayMs = DefaultMaxDelayMs
	}

	// Ensure reject > delay to avoid division by zero in interpolation.
	if c.RejectThreshold <= c.DelayThreshold {
		c.RejectThreshold = c.DelayThreshold + 1
	}

	return c
}

// AsyncBatcher buffers incoming events per-index and flushes them to immutable
// parts on disk when any threshold (event count, byte size, idle timeout) is
// reached. It replaces the WAL+memtable+flush pipeline with a simpler
// direct-to-disk model following the ClickHouse/VictoriaLogs pattern.
//
// AsyncBatcher is safe for concurrent use from multiple goroutines.
type AsyncBatcher struct {
	mu       sync.Mutex
	shards   map[string]*batchShard // index -> shard
	writer   *Writer
	registry *Registry
	cfg      BatcherConfig
	logger   *slog.Logger
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	// onCommit is called after each part is committed to disk.
	// Engine uses this to open the mmap'd reader and register for queries.
	onCommit func(meta *Meta)
	runCtx   context.Context
}

// batchShard holds buffered events for a single index.
type batchShard struct {
	events    []*event.Event
	sizeBytes int64
	lastAdd   time.Time
}

// NewAsyncBatcher creates an AsyncBatcher that writes parts using the given
// Writer and registers them in the given Registry.
func NewAsyncBatcher(writer *Writer, registry *Registry, cfg BatcherConfig, logger *slog.Logger) *AsyncBatcher {
	cfg = cfg.withDefaults()

	return &AsyncBatcher{
		shards:   make(map[string]*batchShard),
		writer:   writer,
		registry: registry,
		cfg:      cfg,
		logger:   logger,
	}
}

// SetOnCommit sets the callback invoked after each part is committed to disk.
// Must be called before Start.
func (b *AsyncBatcher) SetOnCommit(fn func(meta *Meta)) {
	b.onCommit = fn
}

// Start starts the background goroutine that flushes idle shards.
// The goroutine exits when ctx is canceled or Close is called.
func (b *AsyncBatcher) Start(ctx context.Context) {
	ctx, b.cancel = context.WithCancel(ctx)
	b.runCtx = ctx
	b.wg.Add(1)
	go b.idleFlushLoop(ctx)
}

// Add buffers events for later flush. Events are grouped by their Index field.
// If any per-index shard crosses a threshold (MaxEvents or MaxBytes), that
// shard is flushed immediately (I/O happens outside the lock).
//
// Backpressure: when the total part count exceeds DelayThreshold, Add()
// sleeps progressively before returning. When the count exceeds
// RejectThreshold, Add() returns ErrTooManyParts immediately.
//
// Add is safe for concurrent use.
func (b *AsyncBatcher) Add(events []*event.Event) error {
	return b.AddContext(context.Background(), events)
}

// AddContext buffers events for later flush while honoring cancellation during
// pre-admission backpressure delays.
func (b *AsyncBatcher) AddContext(ctx context.Context, events []*event.Event) error {
	if len(events) == 0 {
		return nil
	}

	// Check backpressure BEFORE buffering to avoid accepting data we can't
	// compact fast enough.
	if err := b.checkBackpressure(ctx); err != nil {
		return err
	}

	// Group events by index under lock, check thresholds.
	b.mu.Lock()

	type flushTarget struct {
		index  string
		events []*event.Event
	}
	var toFlush []flushTarget

	for _, ev := range events {
		idx := ev.Index
		if idx == "" {
			idx = "main"
		}

		shard, ok := b.shards[idx]
		if !ok {
			shard = &batchShard{}
			b.shards[idx] = shard
		}

		shard.events = append(shard.events, ev)
		shard.sizeBytes += int64(len(ev.Raw))
		shard.lastAdd = time.Now()

		// Check thresholds.
		if len(shard.events) >= b.cfg.MaxEvents || shard.sizeBytes >= b.cfg.MaxBytes {
			trigger := "MaxEvents"
			if shard.sizeBytes >= b.cfg.MaxBytes {
				trigger = "MaxBytes"
			}
			b.logger.Debug("batcher threshold crossed",
				"index", idx,
				"trigger", trigger,
				"events", len(shard.events),
				"size_bytes", shard.sizeBytes,
			)
			// Snapshot and clear shard under lock; flush happens below without lock.
			toFlush = append(toFlush, flushTarget{
				index:  idx,
				events: shard.events,
			})
			b.shards[idx] = &batchShard{lastAdd: time.Now()}
		}
	}
	b.mu.Unlock()

	// Flush threshold-crossing shards outside the lock.
	for _, ft := range toFlush {
		if err := b.flushEvents(b.writeContext(), ft.index, ft.events); err != nil {
			return fmt.Errorf("part.AsyncBatcher.Add: flush %s: %w", ft.index, err)
		}
	}

	return nil
}

// checkBackpressure applies ClickHouse-style backpressure based on total part
// count. When parts accumulate faster than compaction can merge them:
//   - Below DelayThreshold: no delay (fast path).
//   - Between DelayThreshold and RejectThreshold: progressive sleep (linear
//     interpolation from 0ms to MaxDelayMs). This gives compaction time to
//     catch up while still accepting data.
//   - At or above RejectThreshold: return ErrTooManyParts immediately.
func (b *AsyncBatcher) checkBackpressure(ctx context.Context) error {
	partCount := b.registry.Count()

	b.logger.Debug("backpressure check",
		"part_count", partCount,
		"delay_threshold", b.cfg.DelayThreshold,
		"reject_threshold", b.cfg.RejectThreshold,
	)

	if partCount >= b.cfg.RejectThreshold {
		b.logger.Warn("backpressure: rejecting ingest",
			"part_count", partCount,
			"reject_threshold", b.cfg.RejectThreshold,
		)

		return ErrTooManyParts
	}

	if partCount >= b.cfg.DelayThreshold {
		// Linear interpolation: 0ms at DelayThreshold, MaxDelayMs at RejectThreshold.
		fraction := float64(partCount-b.cfg.DelayThreshold) /
			float64(b.cfg.RejectThreshold-b.cfg.DelayThreshold)
		delayMs := int(fraction * float64(b.cfg.MaxDelayMs))

		if delayMs > 0 {
			b.logger.Debug("backpressure: delaying ingest",
				"part_count", partCount,
				"delay_ms", delayMs,
			)
			timer := time.NewTimer(time.Duration(delayMs) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
	}

	return nil
}

// Flush flushes all buffered shards to disk. Called on shutdown.
func (b *AsyncBatcher) Flush() error {
	return b.FlushContext(context.Background())
}

// FlushContext flushes all buffered shards to disk using the provided context.
func (b *AsyncBatcher) FlushContext(ctx context.Context) error {
	b.mu.Lock()
	type flushTarget struct {
		index  string
		events []*event.Event
	}
	var toFlush []flushTarget
	for idx, shard := range b.shards {
		if len(shard.events) > 0 {
			toFlush = append(toFlush, flushTarget{
				index:  idx,
				events: shard.events,
			})
		}
	}
	// Clear all shards.
	b.shards = make(map[string]*batchShard)
	b.mu.Unlock()

	var firstErr error
	for _, ft := range toFlush {
		if err := b.flushEvents(ctx, ft.index, ft.events); err != nil {
			b.logger.Error("batcher flush failed", "index", ft.index, "error", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("part.AsyncBatcher.Flush: %s: %w", ft.index, err)
			}
		}
	}

	return firstErr
}

// Close flushes remaining events and stops the background goroutine.
func (b *AsyncBatcher) Close() error {
	return b.CloseContext(context.Background())
}

// CloseContext flushes remaining events and stops the background goroutine.
func (b *AsyncBatcher) CloseContext(ctx context.Context) error {
	// Stop the idle flush loop.
	if b.cancel != nil {
		b.cancel()
	}
	b.wg.Wait()

	// Final flush.
	return b.FlushContext(ctx)
}

// BufferedEvents returns the total number of events currently buffered
// across all shards. Used for status reporting.
func (b *AsyncBatcher) BufferedEvents() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var total int64
	for _, shard := range b.shards {
		total += int64(len(shard.events))
	}

	return total
}

// BufferedBytes returns the total raw bytes currently buffered
// across all shards. Used for status reporting.
func (b *AsyncBatcher) BufferedBytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	var total int64
	for _, shard := range b.shards {
		total += shard.sizeBytes
	}

	return total
}

// idleFlushLoop runs in a background goroutine and flushes any shard that
// has been idle for longer than MaxWait.
func (b *AsyncBatcher) idleFlushLoop(ctx context.Context) {
	defer b.wg.Done()

	// Tick at 1/4 of MaxWait for responsive idle detection.
	tickInterval := b.cfg.MaxWait / 4
	if tickInterval < 10*time.Millisecond {
		tickInterval = 10 * time.Millisecond
	}
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.flushIdleShards()
		}
	}
}

// flushIdleShards flushes any shard that has been idle for longer than MaxWait.
func (b *AsyncBatcher) flushIdleShards() {
	b.flushIdleShardsWithContext(b.writeContext())
}

func (b *AsyncBatcher) flushIdleShardsWithContext(ctx context.Context) {
	now := time.Now()

	b.mu.Lock()
	type flushTarget struct {
		index  string
		events []*event.Event
	}
	var toFlush []flushTarget
	for idx, shard := range b.shards {
		if len(shard.events) > 0 && now.Sub(shard.lastAdd) >= b.cfg.MaxWait {
			toFlush = append(toFlush, flushTarget{
				index:  idx,
				events: shard.events,
			})
			b.shards[idx] = &batchShard{}
		}
	}
	b.mu.Unlock()

	if len(toFlush) > 0 {
		b.logger.Debug("idle shards detected",
			"count", len(toFlush),
		)
	}

	for _, ft := range toFlush {
		if err := b.flushEvents(ctx, ft.index, ft.events); err != nil {
			b.logger.Error("idle flush failed", "index", ft.index,
				"events", len(ft.events), "error", err)
		}
	}
}

// flushEvents sorts events by timestamp and writes them to a part file.
func (b *AsyncBatcher) flushEvents(ctx context.Context, index string, events []*event.Event) error {
	if len(events) == 0 {
		return nil
	}

	// Sort: primary by timestamp (required for delta-varint encoding),
	// secondary by _source for block clustering. Same-source events
	// cluster into the same row groups, improving dictionary encoding
	// and constColumn detection (e.g., a row group where all events
	// have source="nginx" stores the value once, not per-row).
	sortStart := time.Now()
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].Time.Equal(events[j].Time) {
			return events[i].Source < events[j].Source
		}

		return events[i].Time.Before(events[j].Time)
	})
	sortElapsed := time.Since(sortStart)

	b.logger.Debug("flush started",
		"index", index,
		"events", len(events),
		"sort_ms", sortElapsed.Milliseconds(),
	)

	meta, err := b.writer.Write(ctx, index, events, 0)
	if err != nil {
		return err
	}

	b.registry.Add(meta)

	b.logger.Info("part committed",
		"index", index,
		"events", meta.EventCount,
		"size_bytes", meta.SizeBytes,
		"id", meta.ID,
	)

	if b.onCommit != nil {
		b.onCommit(meta)
	}

	return nil
}

func (b *AsyncBatcher) writeContext() context.Context {
	if b.runCtx != nil {
		return b.runCtx
	}

	return context.Background()
}
