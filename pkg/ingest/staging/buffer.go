package staging

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

var (
	ErrBufferOverflow = errors.New("staging buffer overflow")
	ErrClosed         = errors.New("staging buffer closed")
)

type Sink func(context.Context, []*event.Event) error

type Config struct {
	Enabled           bool
	MaxBytes          int64
	MaxAge            time.Duration
	MaxInflightEvents int
	FlushRetries      int
	FlushBackoffMax   time.Duration
}

type Metrics interface {
	SetState(bytes int64, events int, ageSeconds float64)
	RecordFlush(trigger string, bytes int64)
	RecordOverflow()
	RecordDropped(reason string, events int)
}

type Buffer struct {
	cfg     Config
	sink    Sink
	account memgov.MemoryAccount
	metrics Metrics

	mu      sync.Mutex
	pending []*event.Event
	bytes   int64
	oldest  time.Time
	closed  bool

	stop chan struct{}
	done chan struct{}
}

func NewBuffer(cfg Config, sink Sink, account memgov.MemoryAccount, metrics Metrics) *Buffer {
	cfg = normalizeConfig(cfg)
	b := &Buffer{
		cfg:     cfg,
		sink:    sink,
		account: memgov.EnsureAccount(account),
		metrics: metrics,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	if cfg.Enabled {
		go b.drainLoop()
	} else {
		close(b.done)
	}
	b.recordStateLocked(time.Now())
	return b
}

func (b *Buffer) Add(ctx context.Context, events []*event.Event) error {
	if len(events) == 0 {
		return nil
	}
	if !b.cfg.Enabled {
		return b.sink(ctx, events)
	}

	batchBytes := estimateBytes(events)
	if b.exceedsSingleBatch(batchBytes, len(events)) {
		b.recordOverflow()
		return ErrBufferOverflow
	}

	for attempts := 0; attempts < 2; attempts++ {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return ErrClosed
		}
		if b.canFitLocked(batchBytes, len(events)) {
			if err := b.account.Grow(batchBytes); err != nil {
				b.mu.Unlock()
				return err
			}
			if len(b.pending) == 0 {
				b.oldest = time.Now()
			}
			b.pending = append(b.pending, events...)
			b.bytes += batchBytes
			shouldFlush := b.bytes >= b.cfg.MaxBytes/2
			b.recordStateLocked(time.Now())
			b.mu.Unlock()
			if shouldFlush {
				return b.flush(ctx, "size")
			}
			return nil
		}
		b.mu.Unlock()

		if err := b.flush(ctx, "size"); err != nil {
			return err
		}
	}

	b.recordOverflow()
	return ErrBufferOverflow
}

func (b *Buffer) Flush(ctx context.Context) error {
	return b.flush(ctx, "sync")
}

func (b *Buffer) Close(ctx context.Context) error {
	if b.cfg.Enabled {
		b.mu.Lock()
		if !b.closed {
			b.closed = true
			close(b.stop)
		}
		b.mu.Unlock()

		select {
		case <-b.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	err := b.flush(ctx, "shutdown")
	b.account.Close()
	return err
}

func (b *Buffer) flush(ctx context.Context, trigger string) error {
	b.mu.Lock()
	if len(b.pending) == 0 {
		b.recordStateLocked(time.Now())
		b.mu.Unlock()
		return nil
	}
	batch := append([]*event.Event(nil), b.pending...)
	bytes := b.bytes
	b.pending = nil
	b.bytes = 0
	b.oldest = time.Time{}
	b.recordStateLocked(time.Now())
	b.mu.Unlock()

	err := b.callSinkWithRetries(ctx, batch)
	b.account.Shrink(bytes)
	if err != nil {
		b.recordDropped("sink_error", len(batch))
		return err
	}
	b.recordFlush(trigger, bytes)
	return nil
}

func (b *Buffer) callSinkWithRetries(ctx context.Context, batch []*event.Event) error {
	var err error
	attempts := b.cfg.FlushRetries + 1
	for i := 0; i < attempts; i++ {
		if err = b.sink(ctx, batch); err == nil {
			return nil
		}
		if i == attempts-1 || b.cfg.FlushBackoffMax <= 0 {
			continue
		}
		timer := time.NewTimer(backoffForAttempt(i, b.cfg.FlushBackoffMax))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
	return err
}

func (b *Buffer) drainLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	defer close(b.done)

	for {
		select {
		case <-ticker.C:
			trigger := b.flushTrigger(time.Now())
			if trigger != "" {
				_ = b.flush(context.Background(), trigger)
			}
		case <-b.stop:
			return
		}
	}
}

func (b *Buffer) flushTrigger(now time.Time) string {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.pending) == 0 {
		b.recordStateLocked(now)
		return ""
	}
	if b.bytes >= b.cfg.MaxBytes/2 {
		return "size"
	}
	if !b.oldest.IsZero() && now.Sub(b.oldest) >= b.cfg.MaxAge {
		return "age"
	}
	b.recordStateLocked(now)
	return ""
}

func (b *Buffer) canFitLocked(bytes int64, events int) bool {
	if b.bytes+bytes > b.cfg.MaxBytes {
		return false
	}
	return len(b.pending)+events <= b.cfg.MaxInflightEvents
}

func (b *Buffer) exceedsSingleBatch(bytes int64, events int) bool {
	return bytes > b.cfg.MaxBytes || events > b.cfg.MaxInflightEvents
}

func (b *Buffer) recordStateLocked(now time.Time) {
	if b.metrics == nil {
		return
	}
	age := 0.0
	if !b.oldest.IsZero() && len(b.pending) > 0 {
		age = now.Sub(b.oldest).Seconds()
	}
	b.metrics.SetState(b.bytes, len(b.pending), age)
}

func (b *Buffer) recordFlush(trigger string, bytes int64) {
	if b.metrics != nil {
		b.metrics.RecordFlush(trigger, bytes)
	}
}

func (b *Buffer) recordOverflow() {
	if b.metrics != nil {
		b.metrics.RecordOverflow()
	}
}

func (b *Buffer) recordDropped(reason string, events int) {
	if b.metrics != nil {
		b.metrics.RecordDropped(reason, events)
	}
}

func normalizeConfig(cfg Config) Config {
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 64 << 20
	}
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = 5 * time.Second
	}
	if cfg.MaxInflightEvents <= 0 {
		cfg.MaxInflightEvents = 1_000_000
	}
	if cfg.FlushBackoffMax <= 0 {
		cfg.FlushBackoffMax = 5 * time.Second
	}
	return cfg
}

func estimateBytes(events []*event.Event) int64 {
	var total int64
	for _, ev := range events {
		if ev == nil {
			continue
		}
		total += int64(len(ev.Raw) + len(ev.Source) + len(ev.SourceType) + len(ev.Host) + len(ev.Index))
		for k, v := range ev.Fields {
			total += int64(len(k) + len(v.String()))
		}
	}
	if total == 0 {
		return int64(len(events))
	}
	return total
}

func backoffForAttempt(attempt int, max time.Duration) time.Duration {
	d := time.Duration(1<<attempt) * 100 * time.Millisecond
	if d > max {
		return max
	}
	return d
}
