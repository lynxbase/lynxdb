package rebalance

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// RebalancerConfig controls the rebalancer's behavior.
type RebalancerConfig struct {
	// VirtualPartitionCount is the total number of virtual partitions.
	VirtualPartitionCount uint32
	// ReplicationFactor is the number of replicas (including primary).
	ReplicationFactor int
	// DrainTimeout is how long to wait for a drain to complete before force-completing.
	DrainTimeout time.Duration
	// CooldownPeriod is the minimum time between consecutive rebalances.
	CooldownPeriod time.Duration
	// CheckInterval is how often the rebalancer checks for topology changes.
	CheckInterval time.Duration
}

// DefaultRebalancerConfig returns sensible defaults for the rebalancer.
func DefaultRebalancerConfig() RebalancerConfig {
	return RebalancerConfig{
		VirtualPartitionCount: 1024,
		ReplicationFactor:     1,
		DrainTimeout:          5 * time.Minute,
		CooldownPeriod:        30 * time.Second,
		CheckInterval:         2 * time.Second,
	}
}

// MetaStateProvider is the interface the rebalancer uses to read cluster state
// and apply rebalance plans. This decouples the rebalancer from the concrete
// Raft/FSM implementation to allow testing.
type MetaStateProvider interface {
	// StateVersion returns the current MetaState version.
	StateVersion() uint64
	// ComputeRebalancePlan computes an incremental rebalance plan.
	ComputeRebalancePlan(vPartCount uint32, rf int) *RebalancePlan
	// ApplyRebalancePlan commits the plan via Raft consensus.
	ApplyRebalancePlan(ctx context.Context, plan *RebalancePlan) error
	// PromoteISRReplica promotes the first ISR replica for shards where
	// the primary is a dead node. Returns the number of promotions.
	PromoteISRReplica(ctx context.Context, deadNode string) (int, error)
}

// Rebalancer monitors cluster topology changes and triggers incremental
// shard rebalancing. It runs as a single goroutine on the meta leader.
//
// The rebalancer is leader-aware: it only performs rebalancing when the
// local node is the Raft leader. When leadership changes, the caller
// should cancel the context to stop the old rebalancer and start a new one.
type Rebalancer struct {
	cfg      RebalancerConfig
	provider MetaStateProvider
	logger   *slog.Logger

	lastVersion   uint64
	lastRebalance time.Time
}

// NewRebalancer creates a new Rebalancer.
func NewRebalancer(cfg RebalancerConfig, provider MetaStateProvider, logger *slog.Logger) *Rebalancer {
	return &Rebalancer{
		cfg:      cfg,
		provider: provider,
		logger:   logger.With("component", "rebalancer"),
	}
}

// Run starts the rebalancer loop. It blocks until ctx is cancelled.
// The rebalancer detects topology changes by monitoring MetaState.Version
// and applies incremental rebalance plans with a cooldown between operations.
func (r *Rebalancer) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.CheckInterval)
	defer ticker.Stop()

	r.logger.Info("rebalancer started",
		"vpart_count", r.cfg.VirtualPartitionCount,
		"rf", r.cfg.ReplicationFactor,
		"cooldown", r.cfg.CooldownPeriod)

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("rebalancer stopped")

			return ctx.Err()
		case <-ticker.C:
			if err := r.check(ctx); err != nil {
				r.logger.Error("rebalancer check failed", "error", err)
			}
		}
	}
}

// check examines the current state and triggers a rebalance if the topology
// has changed and the cooldown period has elapsed.
func (r *Rebalancer) check(ctx context.Context) error {
	currentVersion := r.provider.StateVersion()
	if currentVersion <= r.lastVersion {
		return nil // No state change.
	}

	// Enforce cooldown.
	if !r.lastRebalance.IsZero() && time.Since(r.lastRebalance) < r.cfg.CooldownPeriod {
		return nil
	}

	plan := r.provider.ComputeRebalancePlan(r.cfg.VirtualPartitionCount, r.cfg.ReplicationFactor)
	if plan.IsEmpty() {
		r.lastVersion = currentVersion

		return nil
	}

	r.logger.Info("applying rebalance plan",
		"moves", len(plan.Moves),
		"epoch", plan.Epoch)

	if err := r.provider.ApplyRebalancePlan(ctx, plan); err != nil {
		return fmt.Errorf("rebalance.Rebalancer.check: apply plan: %w", err)
	}

	r.lastVersion = currentVersion
	r.lastRebalance = time.Now()

	r.logger.Info("rebalance plan applied",
		"moves", len(plan.Moves),
		"epoch", plan.Epoch)

	return nil
}
