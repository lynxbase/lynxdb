package alerts

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// QueryFunc executes an SPL2 query and returns rows.
type QueryFunc func(ctx context.Context, query string) ([]map[string]interface{}, error)

// ClusterGuard determines whether this node should evaluate an alert in
// cluster mode. In single-node mode, this is nil and all alerts are evaluated.
type ClusterGuard interface {
	// ShouldEvaluate returns true if this node is responsible for the alert.
	ShouldEvaluate(alertID string) bool
	// OnFired reports an alert fire event to the cluster for dedup tracking.
	OnFired(ctx context.Context, alertID string)
}

// Scheduler manages per-alert timer goroutines.
type Scheduler struct {
	store        *AlertStore
	dispatcher   *Dispatcher
	queryFn      QueryFunc
	logger       *slog.Logger
	mu           sync.Mutex
	timers       map[string]context.CancelFunc
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	sem          chan struct{} // concurrency limiter
	clusterGuard ClusterGuard // nil in single-node mode
}

// NewScheduler creates a Scheduler.
func NewScheduler(store *AlertStore, dispatcher *Dispatcher, queryFn QueryFunc, logger *slog.Logger) *Scheduler {
	return &Scheduler{
		store:      store,
		dispatcher: dispatcher,
		queryFn:    queryFn,
		logger:     logger,
		timers:     make(map[string]context.CancelFunc),
		sem:        make(chan struct{}, 10),
	}
}

// SetClusterGuard sets the cluster guard for distributed alert evaluation.
// When set, only alerts assigned to this node via rendezvous hashing are
// scheduled. Must be called before Start().
func (s *Scheduler) SetClusterGuard(guard ClusterGuard) {
	s.clusterGuard = guard
}

// Start loads all enabled alerts and starts their timer goroutines.
func (s *Scheduler) Start(ctx context.Context) {
	s.ctx, s.cancel = context.WithCancel(ctx)

	for _, alert := range s.store.List() {
		if alert.Enabled {
			s.ScheduleAlert(alert)
		}
	}
	s.logger.Info("alert scheduler started", "alerts", len(s.timers))
}

// Stop cancels all timer goroutines and waits for in-flight checks.
func (s *Scheduler) Stop() {
	s.cancel()
	s.wg.Wait()
}

// ScheduleAlert starts or restarts the timer goroutine for an alert.
// In cluster mode, only schedules if this node is assigned to evaluate the alert.
// Holds the lock across cancel + reschedule to prevent duplicate goroutines.
func (s *Scheduler) ScheduleAlert(alert Alert) {
	if !alert.Enabled {
		s.UnscheduleAlert(alert.ID)

		return
	}

	// In cluster mode, skip scheduling if this node is not the evaluator.
	if s.clusterGuard != nil && !s.clusterGuard.ShouldEvaluate(alert.ID) {
		return
	}

	interval, err := time.ParseDuration(alert.Interval)
	if err != nil {
		s.logger.Error("invalid alert interval", "alert", alert.Name, "interval", alert.Interval, "error", err)

		return
	}

	alertCtx, alertCancel := context.WithCancel(s.ctx)

	s.mu.Lock()
	if cancel, ok := s.timers[alert.ID]; ok {
		cancel()
	}
	s.timers[alert.ID] = alertCancel
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			s.mu.Lock()
			delete(s.timers, alert.ID)
			s.mu.Unlock()
		}()

		jitter := time.Duration(rand.Int64N(int64(interval) / 10))
		timer := time.NewTimer(jitter)
		defer timer.Stop()

		for {
			select {
			case <-alertCtx.Done():
				return
			case <-timer.C:
				s.executeCheck(alertCtx, alert.ID)
				timer.Reset(interval)
			}
		}
	}()
}

// UnscheduleAlert stops the timer goroutine for an alert.
func (s *Scheduler) UnscheduleAlert(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cancel, ok := s.timers[id]; ok {
		cancel()
		delete(s.timers, id)
	}
}

func (s *Scheduler) executeCheck(ctx context.Context, alertID string) {
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-ctx.Done():
		return
	}

	alert, err := s.store.Get(alertID)
	if err != nil {
		return // alert was deleted
	}

	now := time.Now()
	rows, err := s.queryFn(ctx, alert.Query)
	if err != nil {
		s.logger.Warn("alert query failed", "alert", alert.Name, "error", err)
		if updateErr := s.store.UpdateStatus(alertID, StatusError, now, nil); updateErr != nil {
			s.logger.Warn("alert status update failed", "alert", alert.Name, "error", updateErr)
		}

		return
	}

	if len(rows) > 0 {
		result := map[string]interface{}{
			"rows":  rows,
			"count": len(rows),
		}
		s.dispatcher.Dispatch(ctx, *alert, result)
		if updateErr := s.store.UpdateStatus(alertID, StatusTriggered, now, &now); updateErr != nil {
			s.logger.Warn("alert status update failed", "alert", alert.Name, "error", updateErr)
		}

		// Report fire to cluster for dedup tracking during failover.
		if s.clusterGuard != nil {
			s.clusterGuard.OnFired(ctx, alertID)
		}

		s.logger.Info("alert triggered", "alert", alert.Name, "rows", len(rows))
	} else {
		if updateErr := s.store.UpdateStatus(alertID, StatusOK, now, nil); updateErr != nil {
			s.logger.Warn("alert status update failed", "alert", alert.Name, "error", updateErr)
		}
	}
}
