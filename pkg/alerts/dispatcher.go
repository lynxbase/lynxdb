package alerts

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Notifier sends notifications for a specific channel type.
type Notifier interface {
	Send(ctx context.Context, alert Alert, result map[string]interface{}) error
	Test(ctx context.Context, alert Alert) (latency time.Duration, err error)
}

// NotifierFactory creates a Notifier from a channel type and config.
type NotifierFactory func(chType ChannelType, config map[string]interface{}) (Notifier, error)

const (
	channelStatusOK    = string(StatusOK)
	channelStatusError = string(StatusError)
)

// ChannelResult reports the outcome of a single channel notification.
type ChannelResult struct {
	Type      ChannelType `json:"type"`
	Name      string      `json:"name"`
	Status    string      `json:"status"` // "ok" or "error"
	Error     string      `json:"error,omitempty"`
	LatencyMs int64       `json:"latency_ms"`
}

const defaultMaxConcurrentNotifications = 10

// Dispatcher sends notifications to all enabled channels on an alert.
type Dispatcher struct {
	factory NotifierFactory
	logger  *slog.Logger
	maxConc int // max concurrent notification goroutines per Dispatch call
}

// NewDispatcher creates a Dispatcher with the given notifier factory.
func NewDispatcher(factory NotifierFactory, logger *slog.Logger) *Dispatcher {
	return &Dispatcher{factory: factory, logger: logger, maxConc: defaultMaxConcurrentNotifications}
}

// NewDispatcherWithConcurrency creates a Dispatcher with a configurable concurrency limit.
func NewDispatcherWithConcurrency(factory NotifierFactory, logger *slog.Logger, maxConcurrent int) *Dispatcher {
	if maxConcurrent <= 0 {
		maxConcurrent = defaultMaxConcurrentNotifications
	}

	return &Dispatcher{factory: factory, logger: logger, maxConc: maxConcurrent}
}

// Dispatch sends a notification to all enabled channels concurrently.
// Concurrency is bounded by the dispatcher's maxConc limit to prevent
// goroutine storms when alerts have many channels.
func (d *Dispatcher) Dispatch(ctx context.Context, alert Alert, result map[string]interface{}) []ChannelResult {
	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		results []ChannelResult
	)

	sem := make(chan struct{}, d.maxConc)

	for _, ch := range alert.Channels {
		if !ch.IsEnabled() {
			continue
		}

		wg.Add(1)
		go func(ch NotificationChannel) {
			defer wg.Done()
			sem <- struct{}{}        // acquire semaphore
			defer func() { <-sem }() // release semaphore

			chCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			cr := ChannelResult{Type: ch.Type, Name: ch.Name, Status: channelStatusOK}
			start := time.Now()

			notifier, err := d.factory(ch.Type, ch.Config)
			if err != nil {
				cr.Status = channelStatusError
				cr.Error = err.Error()
			} else if err := notifier.Send(chCtx, alert, result); err != nil {
				cr.Status = channelStatusError
				cr.Error = err.Error()
				d.logger.Warn("channel send failed", "alert", alert.Name, "channel", ch.Name, "type", ch.Type, "error", err)
			}

			cr.LatencyMs = time.Since(start).Milliseconds()
			mu.Lock()
			results = append(results, cr)
			mu.Unlock()
		}(ch)
	}

	wg.Wait()

	return results
}

// TestChannels sends a test message to each enabled channel.
func (d *Dispatcher) TestChannels(ctx context.Context, alert Alert) []ChannelResult {
	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		results []ChannelResult
	)

	sem := make(chan struct{}, d.maxConc)

	for _, ch := range alert.Channels {
		if !ch.IsEnabled() {
			continue
		}

		wg.Add(1)
		go func(ch NotificationChannel) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			chCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			cr := ChannelResult{Type: ch.Type, Name: ch.Name, Status: channelStatusOK}

			notifier, err := d.factory(ch.Type, ch.Config)
			if err != nil {
				cr.Status = channelStatusError
				cr.Error = err.Error()
			} else {
				latency, err := notifier.Test(chCtx, alert)
				cr.LatencyMs = latency.Milliseconds()
				if err != nil {
					cr.Status = channelStatusError
					cr.Error = err.Error()
				}
			}

			mu.Lock()
			results = append(results, cr)
			mu.Unlock()
		}(ch)
	}

	wg.Wait()

	return results
}
