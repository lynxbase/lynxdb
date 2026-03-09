package rebalance

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// SplitterConfig controls the hot-partition splitter.
type SplitterConfig struct {
	// HotPartitionThresholdEPS is the ingest rate (events/sec) above which
	// a partition is considered hot and eligible for splitting.
	HotPartitionThresholdEPS int64
	// CheckInterval is how often the splitter evaluates partition load.
	CheckInterval time.Duration
	// CooldownPeriod is the minimum time between consecutive splits.
	CooldownPeriod time.Duration
	// MaxSplitDepth limits the number of chained splits from the original
	// partition count. This caps the split bit at this depth.
	MaxSplitDepth uint8
}

// DefaultSplitterConfig returns sensible defaults for the splitter.
func DefaultSplitterConfig() SplitterConfig {
	return SplitterConfig{
		HotPartitionThresholdEPS: 50000,
		CheckInterval:            30 * time.Second,
		CooldownPeriod:           2 * time.Minute,
		MaxSplitDepth:            10,
	}
}

// PartitionLoad describes the ingest load for a single partition,
// aggregated from node heartbeat data.
type PartitionLoad struct {
	Partition     uint32
	IngestRateEPS int64
	// CurrentBit is the current split depth for this partition.
	// 0 means it's an original (unsplit) partition.
	CurrentBit uint8
}

// SplitStateProvider is the interface the splitter uses to read partition
// load data and propose splits.
type SplitStateProvider interface {
	// PartitionLoads returns the current ingest load per partition.
	PartitionLoads() []PartitionLoad
	// ProposeSplit commits a split proposal via Raft consensus.
	ProposeSplit(ctx context.Context, parent, childA, childB uint32, splitBit uint8) error
}

// Splitter monitors partition load and proposes splits for hot partitions.
// It runs as a single goroutine on the meta leader, complementing the Rebalancer.
//
// Split algorithm: Hash-bit subdivision. Partition P splits by examining one
// additional bit of the hash value. child_a gets events where bit N is 0,
// child_b gets bit N is 1. No rehashing of existing data is required.
type Splitter struct {
	cfg      SplitterConfig
	provider SplitStateProvider
	logger   *slog.Logger

	lastSplit time.Time
}

// NewSplitter creates a new Splitter.
func NewSplitter(cfg SplitterConfig, provider SplitStateProvider, logger *slog.Logger) *Splitter {
	return &Splitter{
		cfg:      cfg,
		provider: provider,
		logger:   logger.With("component", "splitter"),
	}
}

// Run starts the splitter loop. It blocks until ctx is cancelled.
func (s *Splitter) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.CheckInterval)
	defer ticker.Stop()

	s.logger.Info("splitter started",
		"threshold_eps", s.cfg.HotPartitionThresholdEPS,
		"max_depth", s.cfg.MaxSplitDepth)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("splitter stopped")

			return ctx.Err()
		case <-ticker.C:
			if err := s.check(ctx); err != nil {
				s.logger.Error("splitter check failed", "error", err)
			}
		}
	}
}

// check evaluates partition loads and proposes a split for the hottest
// partition that exceeds the threshold.
func (s *Splitter) check(ctx context.Context) error {
	// Enforce cooldown.
	if !s.lastSplit.IsZero() && time.Since(s.lastSplit) < s.cfg.CooldownPeriod {
		return nil
	}

	loads := s.provider.PartitionLoads()
	if len(loads) == 0 {
		return nil
	}

	// Find the hottest partition above threshold.
	var hottest *PartitionLoad
	for i := range loads {
		pl := &loads[i]
		if pl.IngestRateEPS < s.cfg.HotPartitionThresholdEPS {
			continue
		}
		if pl.CurrentBit >= s.cfg.MaxSplitDepth {
			continue // Already at max split depth.
		}
		if hottest == nil || pl.IngestRateEPS > hottest.IngestRateEPS {
			hottest = pl
		}
	}

	if hottest == nil {
		return nil
	}

	// Compute child partition IDs using a simple scheme:
	// childA = parent * 2, childB = parent * 2 + 1.
	// This gives unique IDs as long as we don't exceed the bit depth.
	childA := hottest.Partition*2 + 1<<20 // Offset to avoid collision with vpart range.
	childB := childA + 1
	splitBit := hottest.CurrentBit

	s.logger.Info("proposing partition split",
		"partition", hottest.Partition,
		"ingest_eps", hottest.IngestRateEPS,
		"split_bit", splitBit,
		"child_a", childA,
		"child_b", childB)

	if err := s.provider.ProposeSplit(ctx, hottest.Partition, childA, childB, splitBit); err != nil {
		return fmt.Errorf("rebalance.Splitter.check: %w", err)
	}

	s.lastSplit = time.Now()

	return nil
}
