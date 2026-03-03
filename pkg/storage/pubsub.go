package storage

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

const defaultMaxSubscribers = 1024

// EventBus provides a publish-subscribe mechanism for real-time event delivery.
// Subscribers receive events on buffered channels. If a subscriber's channel is
// full, events are dropped for that subscriber (non-blocking send).
type EventBus struct {
	mu             sync.RWMutex
	subscribers    map[uint64]*subscriber
	nextID         atomic.Uint64
	droppedEvents  atomic.Int64
	maxSubscribers int
}

type subscriber struct {
	ch      chan<- *event.Event
	dropped atomic.Int64
}

// NewEventBus creates a new event bus with the default subscriber limit.
func NewEventBus() *EventBus {
	return &EventBus{
		subscribers:    make(map[uint64]*subscriber),
		maxSubscribers: defaultMaxSubscribers,
	}
}

// Subscribe registers a new subscriber and returns a subscription ID and
// a receive-only channel for events. The channel is buffered (1024 events).
// Returns an error if the maximum subscriber limit has been reached.
func (b *EventBus) Subscribe() (uint64, <-chan *event.Event, error) {
	id := b.nextID.Add(1)
	ch := make(chan *event.Event, 1024)

	b.mu.Lock()
	if len(b.subscribers) >= b.maxSubscribers {
		b.mu.Unlock()

		return 0, nil, fmt.Errorf("eventbus: max subscribers (%d) reached", b.maxSubscribers)
	}
	b.subscribers[id] = &subscriber{ch: ch}
	b.mu.Unlock()

	return id, ch, nil
}

// Unsubscribe removes a subscriber by ID and closes its channel.
func (b *EventBus) Unsubscribe(id uint64) {
	b.mu.Lock()
	if sub, ok := b.subscribers[id]; ok {
		delete(b.subscribers, id)
		close(sub.ch)
	}
	b.mu.Unlock()
}

// Publish sends events to all subscribers. Events are sent non-blocking;
// if a subscriber's channel is full, events are dropped for that subscriber.
func (b *EventBus) Publish(events []*event.Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, ev := range events {
		for _, sub := range b.subscribers {
			select {
			case sub.ch <- ev:
			default:
				sub.dropped.Add(1)
				b.droppedEvents.Add(1)
			}
		}
	}
}

// DroppedEvents returns the total number of events dropped due to slow subscribers.
func (b *EventBus) DroppedEvents() int64 {
	return b.droppedEvents.Load()
}

// DroppedEventsForSubscriber returns the number of events dropped for a specific subscriber.
func (b *EventBus) DroppedEventsForSubscriber(id uint64) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if sub, ok := b.subscribers[id]; ok {
		return sub.dropped.Load()
	}

	return 0
}

// SubscriberCount returns the number of active subscribers.
func (b *EventBus) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.subscribers)
}
