package views

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/stats"
)

const (
	backfillBatchSize = 1000
	backfillRateLimit = 10 * time.Millisecond // pause between batches

	// DefaultBackfillBudgetFraction — fraction of the global pool for a single backfill.
	defaultBackfillBudgetFraction = 10 // 10% of global pool

	// DefaultBackfillMinBudget — minimum backfill budget (256MB).
	defaultBackfillMinBudget int64 = 256 << 20

	// DefaultBackpressureWait — wait between retries when the pool is under pressure.
	defaultBackpressureWait = 5 * time.Second

	// DefaultMaxRetries — maximum backpressure retries before failing (5 min at 5s).
	defaultMaxRetries = 60
)

// BackfillConfig holds tunable parameters for backfill budget and backpressure.
type BackfillConfig struct {
	// MaxMemoryBytes is the dedicated memory budget for a single backfill.
	// 0 means auto-compute: min(10% of global pool, 2 * per-query limit).
	MaxMemoryBytes int64

	// BackpressureWait is the delay between retries when the pool is exhausted.
	// 0 means use defaultBackpressureWait (5s).
	BackpressureWait time.Duration

	// MaxRetries is the maximum number of backpressure retries before failing.
	// 0 means use defaultMaxRetries (60).
	MaxRetries int

	// BatchSize is the number of events to scan per batch.
	// 0 means use default (1000).
	BatchSize int

	// RateLimit is the pause between batches to avoid overwhelming the system.
	// 0 means use default (10ms).
	RateLimit time.Duration
}

// EventSource provides events for backfill.
type EventSource interface {
	// ScanEvents returns events starting from the given cursor.
	// Returns events, next cursor, and whether there are more events.
	ScanEvents(cursor string, limit int) ([]*event.Event, string, bool, error)
}

// Backfiller processes historical data through a view's pipeline.
type Backfiller struct {
	registry    *ViewRegistry
	rootMonitor *stats.RootMonitor
	config      BackfillConfig
	logger      *slog.Logger
}

// NewBackfiller creates a new backfiller.
func NewBackfiller(registry *ViewRegistry, logger *slog.Logger) *Backfiller {
	return &Backfiller{
		registry: registry,
		logger:   logger,
	}
}

// NewBackfillerWithBudget creates a new backfiller with a dedicated memory budget
// backed by the global RootMonitor pool. The backfill gets its own BudgetMonitor
// as a child of the RootMonitor, with backpressure when the pool is under pressure
// from interactive queries.
func NewBackfillerWithBudget(
	registry *ViewRegistry,
	rootMonitor *stats.RootMonitor,
	cfg BackfillConfig,
	logger *slog.Logger,
) *Backfiller {
	return &Backfiller{
		registry:    registry,
		rootMonitor: rootMonitor,
		config:      cfg,
		logger:      logger,
	}
}

// Run executes backfill for a view, reading from source and dispatching to the view.
// It is non-blocking (run in a goroutine), cursor-based, and rate-limited.
//
// When a RootMonitor is configured, the backfill creates a dedicated BudgetMonitor
// as a child. If the global pool is under pressure, the backfill pauses and retries
// with backpressure instead of failing immediately.
func (b *Backfiller) Run(ctx context.Context, name string, source EventSource, dispatch func([]*event.Event) error) error {
	def, err := b.registry.Get(name)
	if err != nil {
		return err
	}

	if def.Status != ViewStatusBackfill {
		return fmt.Errorf("views backfill: view %s is not in backfill state", name)
	}

	filter, err := Compile(def.Filter)
	if err != nil {
		return fmt.Errorf("views backfill: compile filter: %w", err)
	}

	// Create dedicated budget monitor for this backfill.
	monitor := b.createBudgetMonitor(name)
	if monitor != nil {
		defer monitor.Close()
	}

	batchSize := b.config.BatchSize
	if batchSize <= 0 {
		batchSize = backfillBatchSize
	}
	rateLimit := b.config.RateLimit
	if rateLimit <= 0 {
		rateLimit = backfillRateLimit
	}

	cursor := def.Cursor
	// Validate cursor is not obviously corrupt. A valid cursor is either
	// empty (start from beginning) or a non-empty string from ScanEvents.
	// Deep format validation is the EventSource's responsibility; this
	// rejects cursors that are clearly invalid.
	if len(cursor) > 4096 {
		b.logger.Warn("backfill: cursor too long, resetting to start",
			"view", name, "cursor_len", len(cursor))
		cursor = ""
	}
	total := 0

	for {
		select {
		case <-ctx.Done():
			// Save progress.
			b.saveCursor(name, cursor)

			return ctx.Err()
		default:
		}

		events, nextCursor, more, scanErr := b.scanWithBackpressure(ctx, source, cursor, batchSize)
		if scanErr != nil {
			return fmt.Errorf("views backfill: scan: %w", scanErr)
		}

		// Filter matching events.
		var matching []*event.Event
		for _, e := range events {
			if filter.Match(e) {
				matching = append(matching, e)
			}
		}

		if len(matching) > 0 {
			if dispatchErr := dispatch(matching); dispatchErr != nil {
				return fmt.Errorf("views backfill: dispatch: %w", dispatchErr)
			}
			total += len(matching)
		}

		cursor = nextCursor
		if !more {
			break
		}

		// Rate limiting.
		time.Sleep(rateLimit)
	}

	// Mark backfill complete.
	def, err = b.registry.Get(name)
	if err != nil {
		return fmt.Errorf("views backfill: reload definition: %w", err)
	}
	def.Status = ViewStatusActive
	def.Cursor = cursor
	def.UpdatedAt = time.Now()
	if err := b.registry.Update(def); err != nil {
		return fmt.Errorf("views backfill: update status: %w", err)
	}

	b.logger.Info("views backfill: complete", "view", name, "events", total)

	return nil
}

// createBudgetMonitor creates a dedicated BudgetMonitor for this backfill as
// a child of the global RootMonitor. The budget is limited to a fraction of
// the global pool to avoid starving interactive queries.
//
// Returns nil if no RootMonitor is configured (standalone/ephemeral mode).
func (b *Backfiller) createBudgetMonitor(viewName string) *stats.BudgetMonitor {
	if b.rootMonitor == nil {
		return nil
	}

	budget := b.config.MaxMemoryBytes
	if budget <= 0 {
		// Auto-compute: 10% of global pool, minimum 256MB.
		poolLimit := b.rootMonitor.Limit()
		if poolLimit > 0 {
			budget = poolLimit / int64(defaultBackfillBudgetFraction)
		}
		if budget < defaultBackfillMinBudget {
			budget = defaultBackfillMinBudget
		}
	}

	label := "mv-backfill:" + viewName

	return stats.NewBudgetMonitorWithParent(label, budget, b.rootMonitor)
}

// scanWithBackpressure wraps the event source scan with backpressure retry logic.
// When the global pool is under pressure (PoolExhaustedError from the monitor),
// the backfill pauses and retries instead of failing immediately. Interactive
// queries fail fast with 503, while backfills wait — this is the correct priority.
func (b *Backfiller) scanWithBackpressure(
	ctx context.Context,
	source EventSource,
	cursor string,
	batchSize int,
) ([]*event.Event, string, bool, error) {
	backpressureWait := b.config.BackpressureWait
	if backpressureWait <= 0 {
		backpressureWait = defaultBackpressureWait
	}
	maxRetries := b.config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = defaultMaxRetries
	}

	for retry := 0; ; retry++ {
		events, nextCursor, more, err := source.ScanEvents(cursor, batchSize)
		if err == nil {
			return events, nextCursor, more, nil
		}

		if !stats.IsPoolExhausted(err) {
			return nil, "", false, err
		}

		// Pool exhausted — backpressure.
		if retry >= maxRetries {
			return nil, "", false, fmt.Errorf(
				"views backfill: timed out waiting for memory pool after %d retries (%v): %w",
				retry, time.Duration(retry)*backpressureWait, err)
		}

		b.logger.Info("mv backfill paused: global pool under pressure",
			"view", cursor,
			"retry", retry+1,
			"max_retries", maxRetries,
			"wait", backpressureWait)

		select {
		case <-ctx.Done():
			return nil, "", false, ctx.Err()
		case <-time.After(backpressureWait):
			continue
		}
	}
}

func (b *Backfiller) saveCursor(name, cursor string) {
	def, err := b.registry.Get(name)
	if err != nil {
		return
	}
	def.Cursor = cursor
	def.UpdatedAt = time.Now()
	if updateErr := b.registry.Update(def); updateErr != nil {
		b.logger.Warn("backfill cursor save failed", "view", name, "error", updateErr)
	}
}
