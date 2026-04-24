package alerts

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestSchedulerStartStop(t *testing.T) {
	store := OpenInMemory()
	a := testAlert("sched-test")
	a.Interval = "10s"
	store.Create(a)

	var queryCount atomic.Int32
	queryFn := func(ctx context.Context, query string) ([]map[string]interface{}, error) {
		queryCount.Add(1)

		return nil, nil
	}

	d := NewDispatcher(testWebhookFactory(), slog.Default())
	sched := NewScheduler(store, d, queryFn, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx)

	// Wait for at least one check to run (jitter is 0–1s for 10s interval).
	time.Sleep(1500 * time.Millisecond)
	cancel()
	sched.Stop()

	if c := queryCount.Load(); c < 1 {
		t.Fatalf("expected at least 1 query execution, got %d", c)
	}
}

func TestSchedulerTriggersNotification(t *testing.T) {
	store := OpenInMemory()
	a := testAlert("trigger-test")
	a.Interval = "10s"
	store.Create(a)

	var dispatched atomic.Int32
	factory := func(chType ChannelType, config map[string]interface{}) (Notifier, error) {
		return &countingNotifier{count: &dispatched}, nil
	}

	queryFn := func(ctx context.Context, query string) ([]map[string]interface{}, error) {
		return []map[string]interface{}{{"count": 5}}, nil
	}

	d := NewDispatcher(factory, slog.Default())
	sched := NewScheduler(store, d, queryFn, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx)
	time.Sleep(1500 * time.Millisecond)
	cancel()
	sched.Stop()

	if c := dispatched.Load(); c < 1 {
		t.Fatalf("expected at least 1 dispatch, got %d", c)
	}

	got, _ := store.Get(a.ID)
	if got.Status != StatusTriggered {
		t.Errorf("status = %v, want %v", got.Status, StatusTriggered)
	}
}

func TestSchedulerQueryError(t *testing.T) {
	store := OpenInMemory()
	a := testAlert("error-test")
	a.Interval = "10s"
	store.Create(a)

	queryFn := func(ctx context.Context, query string) ([]map[string]interface{}, error) {
		return nil, fmt.Errorf("connection refused")
	}

	d := NewDispatcher(testWebhookFactory(), slog.Default())
	sched := NewScheduler(store, d, queryFn, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx)
	time.Sleep(1500 * time.Millisecond)
	cancel()
	sched.Stop()

	got, _ := store.Get(a.ID)
	if got.Status != StatusError {
		t.Errorf("status = %v, want %v", got.Status, StatusError)
	}
}

func TestSchedulerAddRemove(t *testing.T) {
	store := OpenInMemory()

	var queryCount atomic.Int32
	queryFn := func(ctx context.Context, query string) ([]map[string]interface{}, error) {
		queryCount.Add(1)

		return nil, nil
	}

	d := NewDispatcher(testWebhookFactory(), slog.Default())
	sched := NewScheduler(store, d, queryFn, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx)

	// Add alert dynamically.
	a := testAlert("dynamic")
	a.Interval = "10s"
	store.Create(a)
	sched.ScheduleAlert(*a)

	time.Sleep(1500 * time.Millisecond)

	// Unschedule.
	sched.UnscheduleAlert(a.ID)
	countAfterRemove := queryCount.Load()

	time.Sleep(500 * time.Millisecond)
	countAfterWait := queryCount.Load()

	if countAfterWait != countAfterRemove {
		t.Errorf("query ran after unschedule: before=%d after=%d", countAfterRemove, countAfterWait)
	}

	cancel()
	sched.Stop()
}

func TestSchedulerDisabledAlert(t *testing.T) {
	store := OpenInMemory()
	a := testAlert("disabled")
	a.Interval = "10s"
	a.Enabled = false
	store.Create(a)

	var queryCount atomic.Int32
	queryFn := func(ctx context.Context, query string) ([]map[string]interface{}, error) {
		queryCount.Add(1)

		return nil, nil
	}

	d := NewDispatcher(testWebhookFactory(), slog.Default())
	sched := NewScheduler(store, d, queryFn, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	cancel()
	sched.Stop()

	if c := queryCount.Load(); c != 0 {
		t.Fatalf("disabled alert should not run, got %d queries", c)
	}
}

type countingNotifier struct {
	count *atomic.Int32
}

func (n *countingNotifier) Send(ctx context.Context, alert Alert, result map[string]interface{}) error {
	n.count.Add(1)

	return nil
}

func (n *countingNotifier) Test(ctx context.Context, alert Alert) (time.Duration, error) {
	return 0, nil
}
