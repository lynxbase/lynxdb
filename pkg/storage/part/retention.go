package part

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Default retention settings.
const (
	DefaultRetentionMaxAge   = 90 * 24 * time.Hour // 90 days
	DefaultRetentionInterval = 1 * time.Hour
)

// RetentionConfig controls partition-based retention.
type RetentionConfig struct {
	// MaxAge is the maximum age of a partition before it is deleted.
	// Partitions older than this are removed on the next retention pass.
	// Default: 90 days.
	MaxAge time.Duration

	// Interval is how often the retention manager checks for expired
	// partitions. Default: 1 hour.
	Interval time.Duration
}

// RetentionConfigWithDefaults returns a config with defaults applied.
func (c RetentionConfig) withDefaults() RetentionConfig {
	if c.MaxAge <= 0 {
		c.MaxAge = DefaultRetentionMaxAge
	}

	if c.Interval <= 0 {
		c.Interval = DefaultRetentionInterval
	}

	return c
}

// RetentionManager periodically deletes partitions older than MaxAge.
// File deletion is deferred to the engine (via onDelete callback) which
// schedules removal after all mmap references are released.
//
// RetentionManager is safe for concurrent use.
type RetentionManager struct {
	layout   *Layout
	registry *Registry
	cfg      RetentionConfig
	logger   *slog.Logger

	// onDelete is called for each deleted partition with the index name,
	// partition key, the list of part IDs that were removed, and the
	// partition directory path. Engine uses this to close mmap handles,
	// remove segment handles, and defer file deletion until mmap close.
	onDelete func(index, partition string, removedIDs []string, partitionDir string)
}

// NewRetentionManager creates a RetentionManager.
func NewRetentionManager(
	layout *Layout,
	registry *Registry,
	cfg RetentionConfig,
	logger *slog.Logger,
) *RetentionManager {
	cfg = cfg.withDefaults()

	return &RetentionManager{
		layout:   layout,
		registry: registry,
		cfg:      cfg,
		logger:   logger,
	}
}

// SetOnDelete sets the callback invoked after each partition is deleted.
// Must be called before Start.
func (rm *RetentionManager) SetOnDelete(fn func(index, partition string, removedIDs []string, partitionDir string)) {
	rm.onDelete = fn
}

// Start runs the retention loop in a background goroutine.
// The goroutine exits when ctx is canceled.
func (rm *RetentionManager) Start(ctx context.Context) {
	go rm.loop(ctx)
}

func (rm *RetentionManager) loop(ctx context.Context) {
	ticker := time.NewTicker(rm.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			deleted, err := rm.RunOnce()
			if err != nil {
				rm.logger.Error("retention pass failed", "error", err)
			} else if deleted > 0 {
				rm.logger.Info("retention pass complete", "partitions_deleted", deleted)
			}
		}
	}
}

// RunOnce checks all partitions across all indexes and deletes those older
// than MaxAge. Returns the number of partitions deleted.
func (rm *RetentionManager) RunOnce() (int, error) {
	indexes, err := rm.layout.ListIndexes()
	if err != nil {
		return 0, fmt.Errorf("part.RetentionManager.RunOnce: list indexes: %w", err)
	}

	var totalDeleted int

	for _, index := range indexes {
		deleted, err := rm.retainIndex(index)
		if err != nil {
			rm.logger.Error("retention failed for index", "index", index, "error", err)

			continue
		}

		totalDeleted += deleted
	}

	return totalDeleted, nil
}

// retainIndex checks partitions for a single index and deletes expired ones.
func (rm *RetentionManager) retainIndex(index string) (int, error) {
	partitions, err := rm.layout.ListPartitions(index)
	if err != nil {
		return 0, fmt.Errorf("list partitions for %s: %w", index, err)
	}

	now := time.Now().UTC()
	cutoff := now.Add(-rm.cfg.MaxAge)
	var deleted int

	for _, partition := range partitions {
		partTime, err := parsePartitionTime(partition, rm.layout.Granularity())
		if err != nil {
			rm.logger.Warn("retention: cannot parse partition key, skipping",
				"index", index, "partition", partition, "error", err)

			continue
		}

		if partTime.Before(cutoff) {
			if err := rm.deletePartition(index, partition); err != nil {
				rm.logger.Error("retention: delete partition failed",
					"index", index, "partition", partition, "error", err)

				continue
			}

			deleted++
		}
	}

	return deleted, nil
}

// deletePartition removes a partition's parts from the registry and notifies
// the engine. File deletion is deferred: individual .lsg files are deleted
// when the segment's mmap ref count reaches 0 (via pendingDelete in decRef),
// and the empty partition directory is cleaned up at the same time.
func (rm *RetentionManager) deletePartition(index, partition string) error {
	// Collect part IDs in this partition from the registry before deletion.
	allParts := rm.registry.ByIndex(index)
	var removedIDs []string

	for _, m := range allParts {
		if m.Partition == partition {
			removedIDs = append(removedIDs, m.ID)
		}
	}

	dir := rm.layout.PartitionDirByKey(index, partition)

	for _, id := range removedIDs {
		rm.registry.Remove(id)
	}

	rm.logger.Info("retention: partition deleted",
		"index", index,
		"partition", partition,
		"parts_removed", len(removedIDs),
	)

	// Notify engine to close mmap handles, remove segment handles, and
	// defer file deletion until mmap close.
	if rm.onDelete != nil {
		rm.onDelete(index, partition, removedIDs, dir)
	}

	return nil
}

// parsePartitionTime parses a partition key string into a time.Time.
// Supports daily (YYYY-MM-DD), hourly (YYYY-MM-DD-HH), weekly (YYYY-Www),
// and monthly (YYYY-MM) formats. GranularityNone partitions ("all") return
// time.Time{} which is always before any cutoff, so they are never deleted
// by age-based retention.
func parsePartitionTime(key string, granularity PartitionGranularity) (time.Time, error) {
	switch granularity {
	case GranularityHourly:
		t, err := time.Parse("2006-01-02-15", key)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse hourly partition %q: %w", key, err)
		}

		return t, nil

	case GranularityWeekly:
		var year, week int
		if _, err := fmt.Sscanf(key, "%d-W%d", &year, &week); err != nil {
			return time.Time{}, fmt.Errorf("parse weekly partition %q: %w", key, err)
		}
		// ISO 8601: week 1 contains January 4th. Approximate by computing
		// the Monday of the given ISO week.
		jan4 := time.Date(year, time.January, 4, 0, 0, 0, 0, time.UTC)
		// ISOWeek of Jan 4 is always week 1.
		_, jan4Week := jan4.ISOWeek()
		weekDiff := week - jan4Week

		return jan4.AddDate(0, 0, weekDiff*7-int(jan4.Weekday()-time.Monday)), nil

	case GranularityMonthly:
		t, err := time.Parse("2006-01", key)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse monthly partition %q: %w", key, err)
		}

		return t, nil

	case GranularityNone:
		// "all" partition has no time semantics — return zero time so
		// age-based retention never matches.
		return time.Time{}, nil

	default:
		t, err := time.Parse("2006-01-02", key)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse daily partition %q: %w", key, err)
		}

		return t, nil
	}
}
