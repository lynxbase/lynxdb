package storage

import (
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestEventBusSubscribePublish(t *testing.T) {
	bus := NewEventBus()

	id, ch, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer bus.Unsubscribe(id)

	if bus.SubscriberCount() != 1 {
		t.Fatalf("expected 1 subscriber, got %d", bus.SubscriberCount())
	}

	events := []*event.Event{
		event.NewEvent(time.Now(), `{"level":"error"}`),
		event.NewEvent(time.Now(), `{"level":"info"}`),
	}

	bus.Publish(events)

	// Read published events.
	for i := 0; i < 2; i++ {
		select {
		case ev := <-ch:
			if ev == nil {
				t.Fatal("received nil event")
			}
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for event")
		}
	}
}

func TestEventBusMultipleSubscribers(t *testing.T) {
	bus := NewEventBus()

	id1, ch1, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	id2, ch2, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer bus.Unsubscribe(id1)
	defer bus.Unsubscribe(id2)

	if bus.SubscriberCount() != 2 {
		t.Fatalf("expected 2 subscribers, got %d", bus.SubscriberCount())
	}

	ev := event.NewEvent(time.Now(), `{"msg":"test"}`)
	bus.Publish([]*event.Event{ev})

	// Both subscribers should receive the event.
	for _, ch := range []<-chan *event.Event{ch1, ch2} {
		select {
		case got := <-ch:
			if got.Raw != ev.Raw {
				t.Fatalf("got %q, want %q", got.Raw, ev.Raw)
			}
		case <-time.After(time.Second):
			t.Fatal("timeout")
		}
	}
}

func TestEventBusUnsubscribe(t *testing.T) {
	bus := NewEventBus()

	id, ch, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	bus.Unsubscribe(id)

	if bus.SubscriberCount() != 0 {
		t.Fatalf("expected 0 subscribers, got %d", bus.SubscriberCount())
	}

	// Channel should be closed.
	_, ok := <-ch
	if ok {
		t.Fatal("expected channel to be closed")
	}
}

func TestEventBusDropOnFull(t *testing.T) {
	bus := NewEventBus()

	id, ch, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer bus.Unsubscribe(id)

	// Fill the channel buffer (1024).
	events := make([]*event.Event, 1100)
	for i := range events {
		events[i] = event.NewEvent(time.Now(), `{"i":"test"}`)
	}

	// This should not block even though buffer will be exceeded.
	bus.Publish(events)

	// Should have 1024 events in channel (buffer size).
	count := 0
	for {
		select {
		case <-ch:
			count++
		default:
			goto done
		}
	}
done:
	if count != 1024 {
		t.Fatalf("expected 1024 buffered events, got %d", count)
	}
}

func TestEventBusMaxSubscribers(t *testing.T) {
	bus := NewEventBus()
	bus.maxSubscribers = 2

	id1, _, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe 1: %v", err)
	}

	id2, _, err := bus.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe 2: %v", err)
	}

	// Third should fail.
	_, _, err = bus.Subscribe()
	if err == nil {
		t.Fatal("expected error on third subscribe")
	}

	// After unsubscribing, a new one should succeed.
	bus.Unsubscribe(id1)

	_, _, err = bus.Subscribe()
	if err != nil {
		t.Fatalf("Subscribe after unsubscribe: %v", err)
	}

	bus.Unsubscribe(id2)
}
