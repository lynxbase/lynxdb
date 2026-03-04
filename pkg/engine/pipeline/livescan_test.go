package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestLiveScanIterator_MicroBatch(t *testing.T) {
	ch := make(chan *event.Event, 10)

	// Send 5 events in quick succession.
	for i := 0; i < 5; i++ {
		ch <- &event.Event{
			Time: time.Now(),
			Raw:  "event",
		}
	}

	iter := NewLiveScanIterator(ch, 10, 50*time.Millisecond)
	ctx := context.Background()

	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil {
		t.Fatal("expected batch, got nil")
	}
	// Should get all 5 events in one batch (flush interval).
	if batch.Len != 5 {
		t.Fatalf("expected 5 rows, got %d", batch.Len)
	}
}

func TestLiveScanIterator_BatchSizeLimit(t *testing.T) {
	ch := make(chan *event.Event, 20)

	// Send 10 events.
	for i := 0; i < 10; i++ {
		ch <- &event.Event{
			Time: time.Now(),
			Raw:  "event",
		}
	}

	iter := NewLiveScanIterator(ch, 4, time.Second) // batchSize=4
	ctx := context.Background()

	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len != 4 {
		t.Fatalf("expected 4 rows (batch limit), got %d", batch.Len)
	}
}

func TestLiveScanIterator_FlushInterval(t *testing.T) {
	ch := make(chan *event.Event, 10)

	// Send 1 event.
	ch <- &event.Event{Time: time.Now(), Raw: "solo"}

	iter := NewLiveScanIterator(ch, 100, 50*time.Millisecond)
	ctx := context.Background()

	start := time.Now()
	batch, err := iter.Next(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatal(err)
	}
	if batch.Len != 1 {
		t.Fatalf("expected 1 row, got %d", batch.Len)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("flush took too long: %v", elapsed)
	}
}

func TestLiveScanIterator_ChannelClose(t *testing.T) {
	ch := make(chan *event.Event, 10)
	close(ch)

	iter := NewLiveScanIterator(ch, 10, 50*time.Millisecond)
	ctx := context.Background()

	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch != nil {
		t.Fatalf("expected nil batch on closed channel, got %+v", batch)
	}
}

func TestLiveScanIterator_ContextCancel(t *testing.T) {
	ch := make(chan *event.Event) // unbuffered, will block

	iter := NewLiveScanIterator(ch, 10, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := iter.Next(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestLiveScanIterator_SkipBefore(t *testing.T) {
	ch := make(chan *event.Event, 10)

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Send 5 events: 3 old (at or before cursor), 2 new (after cursor).
	ch <- &event.Event{Time: base.Add(-2 * time.Second), Raw: "old-1"}
	ch <- &event.Event{Time: base.Add(-1 * time.Second), Raw: "old-2"}
	ch <- &event.Event{Time: base, Raw: "old-3-at-cursor"}
	ch <- &event.Event{Time: base.Add(1 * time.Second), Raw: "new-1"}
	ch <- &event.Event{Time: base.Add(2 * time.Second), Raw: "new-2"}

	iter := NewLiveScanIterator(ch, 10, 50*time.Millisecond)
	iter.SetSkipBefore(base) // skip events at or before base
	ctx := context.Background()

	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil {
		t.Fatal("expected batch, got nil")
	}
	if batch.Len != 2 {
		t.Fatalf("expected 2 events (after cursor), got %d", batch.Len)
	}

	// Verify the events are the new ones.
	row0 := batch.Row(0)
	row1 := batch.Row(1)
	if raw := row0["_raw"].String(); raw != "new-1" {
		t.Errorf("row0._raw = %q, want new-1", raw)
	}
	if raw := row1["_raw"].String(); raw != "new-2" {
		t.Errorf("row1._raw = %q, want new-2", raw)
	}
}

func TestLiveScanIterator_SkipBeforeAllFiltered(t *testing.T) {
	ch := make(chan *event.Event, 10)

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Send 2 old events, then 1 new event.
	ch <- &event.Event{Time: base.Add(-1 * time.Second), Raw: "old-1"}
	ch <- &event.Event{Time: base, Raw: "old-2"}
	ch <- &event.Event{Time: base.Add(1 * time.Second), Raw: "new-1"}

	iter := NewLiveScanIterator(ch, 10, 50*time.Millisecond)
	iter.SetSkipBefore(base)
	ctx := context.Background()

	// The first call should skip the old events and return the new one.
	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil {
		t.Fatal("expected batch, got nil")
	}
	if batch.Len != 1 {
		t.Fatalf("expected 1 event, got %d", batch.Len)
	}
	if raw := batch.Row(0)["_raw"].String(); raw != "new-1" {
		t.Errorf("_raw = %q, want new-1", raw)
	}
}

func TestLiveScanIterator_NoSkipByDefault(t *testing.T) {
	ch := make(chan *event.Event, 10)

	// Without SetSkipBefore, all events pass through.
	ch <- &event.Event{Time: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), Raw: "old"}
	ch <- &event.Event{Time: time.Now(), Raw: "new"}

	iter := NewLiveScanIterator(ch, 10, 50*time.Millisecond)
	// No SetSkipBefore call.
	ctx := context.Background()

	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len != 2 {
		t.Fatalf("expected 2 events (no filter), got %d", batch.Len)
	}
}
