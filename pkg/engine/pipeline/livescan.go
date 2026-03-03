package pipeline

import (
	"context"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

// LiveScanIterator is a pipeline source that reads from an EventBus channel.
// It micro-batches incoming events for efficient pipeline processing.
//
// When skipBefore is set (non-zero), events with timestamps at or before that
// time are silently discarded. This is used to deduplicate events during the
// overlap window between a catchup read and the live subscription.
type LiveScanIterator struct {
	ch            <-chan *event.Event
	batchSize     int
	flushInterval time.Duration
	skipBefore    time.Time   // events with Time <= skipBefore are discarded
	flushTimer    *time.Timer // reused across Next() calls to avoid allocation per batch
}

// NewLiveScanIterator creates a LiveScanIterator reading from the given channel.
// Events are micro-batched: up to batchSize events or flushInterval timeout,
// whichever comes first.
func NewLiveScanIterator(ch <-chan *event.Event, batchSize int, flushInterval time.Duration) *LiveScanIterator {
	if batchSize <= 0 {
		batchSize = 64
	}
	if flushInterval <= 0 {
		flushInterval = 100 * time.Millisecond
	}

	return &LiveScanIterator{
		ch:            ch,
		batchSize:     batchSize,
		flushInterval: flushInterval,
	}
}

// SetSkipBefore configures the dedup cursor. Events with timestamps at or
// before t are silently skipped. This must be called before the first Next().
func (l *LiveScanIterator) SetSkipBefore(t time.Time) {
	l.skipBefore = t
}

func (l *LiveScanIterator) Init(ctx context.Context) error { return nil }

func (l *LiveScanIterator) Next(ctx context.Context) (*Batch, error) {
	hasSkip := !l.skipBefore.IsZero()

	for {
		// Wait for at least one event or termination.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case ev, ok := <-l.ch:
			if !ok {
				return nil, nil // channel closed — subscription ended
			}

			// Skip events already covered by the catchup phase.
			if hasSkip && !ev.Time.After(l.skipBefore) {
				continue
			}

			events := make([]*event.Event, 1, l.batchSize)
			events[0] = ev

			// Reuse the flush timer across Next() calls to avoid allocation per batch.
			if l.flushTimer == nil {
				l.flushTimer = time.NewTimer(l.flushInterval)
			} else {
				l.flushTimer.Reset(l.flushInterval)
			}
			count := l.drainBatch(ctx, events, l.flushTimer, hasSkip)
			l.flushTimer.Stop()

			if count == 0 {
				// All drained events were skipped — loop again.
				continue
			}

			return BatchFromEvents(events[:count]), nil
		}
	}
}

// drainBatch reads additional events from the channel up to batchSize or until
// the flush timer fires. It writes accepted events into buf starting at index 1
// and returns the total count of accepted events (including the initial one at
// index 0). When hasSkip is true, events at or before skipBefore are discarded.
func (l *LiveScanIterator) drainBatch(ctx context.Context, buf []*event.Event, timer *time.Timer, hasSkip bool) int {
	count := 1 // buf[0] already accepted by caller
	for count < l.batchSize {
		select {
		case <-ctx.Done():
			return count
		case ev, ok := <-l.ch:
			if !ok {
				return count
			}
			if hasSkip && !ev.Time.After(l.skipBefore) {
				continue
			}
			if count < len(buf) {
				buf[count] = ev
			} else {
				buf = append(buf, ev)
			}
			count++
		case <-timer.C:
			return count
		}
	}

	return count
}

func (l *LiveScanIterator) Close() error {
	if l.flushTimer != nil {
		l.flushTimer.Stop()
	}

	return nil
}
func (l *LiveScanIterator) Schema() []FieldInfo { return nil }
