package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// drainISRPollInterval is how often we poll the ISR tracker during drain
// to check if replicas have caught up.
const drainISRPollInterval = 500 * time.Millisecond

// drainISRTimeout is the maximum time to wait for ISR replicas to catch up
// during a drain operation before giving up.
const drainISRTimeout = 30 * time.Second

// DrainController orchestrates graceful shard draining. When a node is being
// removed from the cluster (scale-down, maintenance), it:
//  1. Proposes CmdProposeDrain via meta gRPC → shard transitions to ShardDraining
//  2. Flushes the local batcher for affected shards
//  3. Waits for ISR replicas to catch up to the primary's current batch_seq
//  4. Proposes CmdCompleteDrain via meta gRPC → new primary assigned, shard Active
type DrainController struct {
	selfID     sharding.NodeID
	metaClient clusterpb.MetaServiceClient
	sequencer  *BatchSequencer
	isrTracker *ISRTracker
	logger     *slog.Logger
}

// NewDrainController creates a new drain controller.
func NewDrainController(
	selfID sharding.NodeID,
	metaClient clusterpb.MetaServiceClient,
	sequencer *BatchSequencer,
	tracker *ISRTracker,
	logger *slog.Logger,
) *DrainController {
	return &DrainController{
		selfID:     selfID,
		metaClient: metaClient,
		sequencer:  sequencer,
		isrTracker: tracker,
		logger:     logger,
	}
}

// DrainResult holds the outcome of draining a single shard.
type DrainResult struct {
	ShardID      string
	NewPrimaryID sharding.NodeID
	Err          error
}

// StartDrain initiates the drain process for the given shards.
// It processes each shard sequentially: propose drain → wait for ISR catchup
// → complete drain with new primary. The flushFn callback is invoked after
// proposing drain to flush the local batcher for affected shards.
//
// Returns results for each shard (success or error).
func (d *DrainController) StartDrain(
	ctx context.Context,
	shardIDs []string,
	flushFn func(ctx context.Context, shardIDs []string) error,
) []DrainResult {
	results := make([]DrainResult, len(shardIDs))

	// Propose drain for all shards.
	for i, shardID := range shardIDs {
		results[i].ShardID = shardID

		resp, err := d.metaClient.ProposeDrain(ctx, &clusterpb.ProposeDrainRequest{
			ShardId: shardID,
			NodeId:  string(d.selfID),
		})
		if err != nil {
			results[i].Err = fmt.Errorf("propose drain for %s: %w", shardID, err)

			continue
		}
		if !resp.Ok {
			results[i].Err = fmt.Errorf("propose drain for %s: rejected", shardID)

			continue
		}

		d.logger.Info("drain proposed", "shard_id", shardID)
	}

	// Flush batcher for affected shards.
	if flushFn != nil {
		activeShards := make([]string, 0, len(shardIDs))
		for i, shardID := range shardIDs {
			if results[i].Err == nil {
				activeShards = append(activeShards, shardID)
			}
		}

		if len(activeShards) > 0 {
			if err := flushFn(ctx, activeShards); err != nil {
				d.logger.Error("drain flush failed", "error", err)
				// Mark all active shards as failed.
				for i := range results {
					if results[i].Err == nil {
						results[i].Err = fmt.Errorf("flush for %s: %w", results[i].ShardID, err)
					}
				}

				return results
			}
		}
	}

	// Wait for ISR catchup and complete drain.
	for i, shardID := range shardIDs {
		if results[i].Err != nil {
			continue
		}

		newPrimary, err := d.waitAndComplete(ctx, shardID)
		if err != nil {
			results[i].Err = err
		} else {
			results[i].NewPrimaryID = newPrimary
			d.logger.Info("drain completed",
				"shard_id", shardID,
				"new_primary", newPrimary)
		}
	}

	return results
}

// waitAndComplete waits for ISR replicas to catch up, selects a new primary,
// and proposes CmdCompleteDrain.
func (d *DrainController) waitAndComplete(ctx context.Context, shardID string) (sharding.NodeID, error) {
	currentSeq := d.sequencer.Current(shardID)

	// Wait for highest replica to catch up.
	deadline := time.After(drainISRTimeout)
	ticker := time.NewTicker(drainISRPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline:
			// Timeout — proceed with best available replica.
			d.logger.Warn("drain ISR catchup timeout, proceeding with best replica",
				"shard_id", shardID,
				"target_seq", currentSeq)

		case <-ticker.C:
			bestNode, bestSeq := d.isrTracker.HighestReplicatedNode(shardID)
			if bestSeq >= currentSeq && bestNode != "" {
				return d.completeDrain(ctx, shardID, bestNode)
			}

			continue
		}

		// Reached via timeout — find best available.
		bestNode, _ := d.isrTracker.HighestReplicatedNode(shardID)
		if bestNode == "" {
			return "", fmt.Errorf("drain %s: no replica available for failover", shardID)
		}

		return d.completeDrain(ctx, shardID, bestNode)
	}
}

// completeDrain proposes CmdCompleteDrain to the meta leader.
func (d *DrainController) completeDrain(ctx context.Context, shardID string, newPrimary sharding.NodeID) (sharding.NodeID, error) {
	resp, err := d.metaClient.CompleteDrain(ctx, &clusterpb.CompleteDrainRequest{
		ShardId:      shardID,
		NodeId:       string(d.selfID),
		NewPrimaryId: string(newPrimary),
	})
	if err != nil {
		return "", fmt.Errorf("complete drain for %s: %w", shardID, err)
	}
	if !resp.Ok {
		return "", fmt.Errorf("complete drain for %s: rejected", shardID)
	}

	return newPrimary, nil
}
