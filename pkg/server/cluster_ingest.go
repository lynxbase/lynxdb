package server

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/lynxbase/lynxdb/internal/objstore"
	"github.com/lynxbase/lynxdb/pkg/cluster"
	ingestcluster "github.com/lynxbase/lynxdb/pkg/cluster/ingest"
	"github.com/lynxbase/lynxdb/pkg/cluster/rpc"
	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage/part"
)

// InitCluster wires up all cluster ingest components when cluster mode is
// enabled and this node has the ingest role. Must be called after NewEngine
// but before Start.
//
// In single-node mode (clusterCfg.Enabled == false), this is a no-op.
// All cluster fields remain nil, and the engine operates in single-node mode.
func (e *Engine) InitCluster(node *cluster.Node, clientPool *rpc.ClientPool, objStore objstore.ObjectStore) error {
	if !e.clusterCfg.Enabled {
		return nil
	}

	selfID := sharding.NodeID(e.clusterCfg.NodeID)
	logger := e.logger.With("component", "cluster-ingest")

	// Batch sequence tracker.
	e.clusterSequencer = ingestcluster.NewBatchSequencer()

	// ISR tracker.
	isrTracker := ingestcluster.NewISRTracker()

	// Meta-loss detector.
	e.clusterMetaLoss = ingestcluster.NewMetaLossDetector(
		e.clusterCfg.MetaLossTimeout.Duration(), logger)

	// Part catalog (S3-backed).
	if objStore != nil {
		e.clusterCatalog = ingestcluster.NewPartCatalog(objStore, logger)
	}

	// Part notifier (fire-and-forget to query nodes).
	e.clusterNotifier = ingestcluster.NewPartNotifier(clientPool, func() []ingestcluster.NodeAddress {
		// TODO: resolve query node addresses from shard map cache or node registry.
		// For now, return empty — notifications will be skipped until Phase 3
		// adds query node discovery.
		return nil
	}, logger)

	// Replicator.
	ackLevel := ingestcluster.ParseAckLevel(e.clusterCfg.AckLevel)
	replicator := ingestcluster.NewBatcherReplicator(
		selfID, clientPool, ackLevel, isrTracker, logger)

	// Build the shard config from cluster config.
	shardCfg := sharding.ShardConfig{
		TimeBucketSize:        e.clusterCfg.TimeBucketSize.Duration(),
		VirtualPartitionCount: uint32(e.clusterCfg.VirtualPartitionCount),
	}

	// Router.
	e.clusterRouter = ingestcluster.NewRouter(ingestcluster.RouterConfig{
		SelfID:        selfID,
		ShardMapCache: node.ShardMapCache(),
		ClientPool:    clientPool,
		LocalIngest:   e.localClusterIngest,
		ShardCfg:      shardCfg,
		Ring:          nil, // Ring is managed by meta node; router falls back to self if nil.
		Replicator:    replicator,
		Sequencer:     e.clusterSequencer,
		MetaLoss:      e.clusterMetaLoss,
		NodeAddrs: func() map[sharding.NodeID]string {
			// TODO: build from node registry / shard map cache.
			// For now return empty — remote routing will fail gracefully.
			return nil
		},
		Logger: logger,
	})

	// Shadow batcher for replica-side buffering.
	shadowBatcher := ingestcluster.NewShadowBatcher()

	// Register IngestService on the node's gRPC server so remote nodes can
	// forward events and replicate batches to this node.
	ingestSvc := ingestcluster.NewService(selfID, e.localClusterIngest, shadowBatcher, isrTracker, logger)
	node.RegisterIngestService(ingestSvc)

	logger.Info("cluster ingest initialized",
		"node_id", selfID,
		"ack_level", e.clusterCfg.AckLevel,
		"replication_factor", e.clusterCfg.ReplicationFactor)

	return nil
}

// localClusterIngest is the callback used by the cluster router for events
// whose shard primary is this node. It follows the same path as single-node
// ingest (batcher → part → segment) but skips cluster routing (already routed).
func (e *Engine) localClusterIngest(_ context.Context, events []*event.Event) error {
	if e.batcher != nil {
		if err := e.batcher.Add(events); err != nil {
			return err
		}
	} else {
		handles, err := e.flushInMemory(events)
		if err != nil {
			return err
		}

		e.mu.Lock()
		cur := e.currentEpoch.Load().segments
		combined := make([]*segmentHandle, len(cur)+len(handles))
		copy(combined, cur)
		copy(combined[len(cur):], handles)
		e.advanceEpoch(combined, nil)
		e.mu.Unlock()
	}

	e.ensureIndexes(events)
	e.ensureSources(events)
	e.ingestGen.Add(1)
	e.metrics.IngestEvents.Add(int64(len(events)))
	e.metrics.IngestBatches.Add(1)

	return nil
}

// uploadAndCatalog uploads a committed part to S3 and records it in the
// part catalog. Called from the batcher onCommit callback in cluster mode.
// Runs in a background goroutine to avoid blocking the flush path.
func (e *Engine) uploadAndCatalog(ctx context.Context, meta *part.Meta, objStore objstore.ObjectStore, logger *slog.Logger) {
	if e.clusterCatalog == nil || objStore == nil {
		return
	}

	// Read part file.
	data, err := os.ReadFile(meta.Path)
	if err != nil {
		logger.Error("upload part: read file failed",
			"path", meta.Path,
			"error", err)

		return
	}

	// Compute S3 key.
	// Virtual partition defaults to 0 for now — the full shard-aware batcher
	// (Phase 3) will propagate the virtual partition from the ShardID.
	var virtualPartition uint32
	partFilename := filepath.Base(meta.Path)
	s3Key := part.S3PartKey(meta.Index, virtualPartition, meta.MinTime, partFilename)

	// Upload to S3.
	if err := objStore.Put(ctx, s3Key, data); err != nil {
		logger.Error("upload part: S3 put failed",
			"s3_key", s3Key,
			"error", err)

		return
	}

	// Add to catalog.
	entry := ingestcluster.PartEntry{
		PartID:      meta.ID,
		Index:       meta.Index,
		MinTimeNs:   meta.MinTime.UnixNano(),
		MaxTimeNs:   meta.MaxTime.UnixNano(),
		EventCount:  meta.EventCount,
		SizeBytes:   int64(len(data)),
		Level:       meta.Level,
		S3Key:       s3Key,
		BatchSeq:    e.clusterSequencer.Current(fmt.Sprintf("%s/p%d", meta.Index, virtualPartition)),
		CreatedAtNs: meta.CreatedAt.UnixNano(),
	}

	if err := e.clusterCatalog.AddPart(ctx, meta.Index, virtualPartition, entry); err != nil {
		logger.Error("upload part: catalog add failed",
			"part_id", meta.ID,
			"error", err)

		return
	}

	// Notify query nodes.
	if e.clusterNotifier != nil {
		e.clusterNotifier.NotifyPartCommitted(ctx, &clusterpb.PartCommittedNotification{
			ShardId:       fmt.Sprintf("%s/p%d", meta.Index, virtualPartition),
			PartId:        meta.ID,
			MinTimeUnixNs: meta.MinTime.UnixNano(),
			MaxTimeUnixNs: meta.MaxTime.UnixNano(),
			EventCount:    meta.EventCount,
			S3Key:         s3Key,
			BatchSeq:      entry.BatchSeq,
		})
	}

	logger.Debug("part uploaded and cataloged",
		"part_id", meta.ID,
		"s3_key", s3Key,
		"events", meta.EventCount)
}

// ClusterRouter returns the cluster ingest router, or nil in single-node mode.
func (e *Engine) ClusterRouter() *ingestcluster.Router {
	return e.clusterRouter
}

// ClusterSequencer returns the batch sequencer, or nil in single-node mode.
func (e *Engine) ClusterSequencer() *ingestcluster.BatchSequencer {
	return e.clusterSequencer
}
