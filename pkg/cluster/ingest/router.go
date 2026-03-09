package ingest

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/lynxbase/lynxdb/pkg/cluster"
	"github.com/lynxbase/lynxdb/pkg/cluster/rpc"
	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
	"github.com/lynxbase/lynxdb/pkg/cluster/tracing"
	"github.com/lynxbase/lynxdb/pkg/event"
)

// Router routes ingested events to their shard primaries. Events are grouped
// by ShardID (computed from index, timestamp, source, and host), then dispatched:
//   - Local fast path: events whose shard primary is this node are ingested locally
//   - Remote path: events are serialized and forwarded via gRPC IngestBatch RPC
//
// The router uses ShardMapCache for shard-to-node mapping, with fallback to the
// HashRing for lazy assignment when a shard has no explicit assignment yet.
//
// Thread-safe.
type Router struct {
	selfID        sharding.NodeID
	shardMapCache *cluster.ShardMapCache
	clientPool    *rpc.ClientPool
	localIngest   func(context.Context, []*event.Event) error
	shardCfg      sharding.ShardConfig
	ring          *sharding.HashRing
	replicator    *BatcherReplicator
	sequencer     *BatchSequencer
	metaLoss      *MetaLossDetector
	logger        *slog.Logger

	// nodeAddrs maps NodeID to gRPC address. Updated when shard map changes.
	// For simplicity, this is populated externally via SetNodeAddrs.
	nodeAddrs func() map[sharding.NodeID]string
}

// RouterConfig holds the configuration for creating a Router.
type RouterConfig struct {
	SelfID        sharding.NodeID
	ShardMapCache *cluster.ShardMapCache
	ClientPool    *rpc.ClientPool
	LocalIngest   func(context.Context, []*event.Event) error
	ShardCfg      sharding.ShardConfig
	Ring          *sharding.HashRing
	Replicator    *BatcherReplicator
	Sequencer     *BatchSequencer
	MetaLoss      *MetaLossDetector
	NodeAddrs     func() map[sharding.NodeID]string
	Logger        *slog.Logger
}

// NewRouter creates a new event Router.
func NewRouter(cfg RouterConfig) *Router {
	return &Router{
		selfID:        cfg.SelfID,
		shardMapCache: cfg.ShardMapCache,
		clientPool:    cfg.ClientPool,
		localIngest:   cfg.LocalIngest,
		shardCfg:      cfg.ShardCfg,
		ring:          cfg.Ring,
		replicator:    cfg.Replicator,
		sequencer:     cfg.Sequencer,
		metaLoss:      cfg.MetaLoss,
		nodeAddrs:     cfg.NodeAddrs,
		logger:        cfg.Logger,
	}
}

// Route distributes events to their shard primaries.
// Events are grouped by ShardID, then dispatched either locally or via gRPC.
// Returns an error if any dispatch fails (partial success is possible).
func (r *Router) Route(ctx context.Context, events []*event.Event) error {
	if len(events) == 0 {
		return nil
	}

	ctx, span := tracing.Tracer().Start(ctx, "lynxdb.ingest.route",
		trace.WithAttributes(
			attribute.Int(tracing.AttrEventsCount, len(events)),
		))
	defer span.End()

	// Step 1: Group events by ShardID.
	shardGroups := make(map[string][]*event.Event)
	shardIDs := make(map[string]sharding.ShardID)

	for _, ev := range events {
		idx := ev.Index
		if idx == "" {
			idx = "main"
		}

		sid := sharding.AssignShard(ev.Source, ev.Host, idx, ev.Time, r.shardCfg)
		key := sid.String()
		shardGroups[key] = append(shardGroups[key], ev)
		shardIDs[key] = sid
	}

	span.SetAttributes(
		attribute.Int(tracing.AttrShardsCount, len(shardGroups)),
	)

	// Step 2: Resolve shard -> primary node.
	sm := r.shardMapCache.Get()
	nodeGroups := make(map[sharding.NodeID][]*event.Event)
	shardsByNode := make(map[sharding.NodeID][]string) // node -> shard IDs for sequencer

	for shardKey, evts := range shardGroups {
		sid := shardIDs[shardKey]
		primary := r.resolvePrimary(sm, sid)

		nodeGroups[primary] = append(nodeGroups[primary], evts...)
		shardsByNode[primary] = append(shardsByNode[primary], shardKey)
	}

	// Step 3: Dispatch to each primary.
	addrs := r.nodeAddrs()

	// Local fast path.
	if localEvents, ok := nodeGroups[r.selfID]; ok {
		_, localSpan := tracing.Tracer().Start(ctx, "lynxdb.ingest.route.local",
			trace.WithAttributes(
				attribute.Int(tracing.AttrEventsCount, len(localEvents)),
			))

		if err := r.localIngest(ctx, localEvents); err != nil {
			localSpan.RecordError(err)
			localSpan.SetStatus(codes.Error, "local ingest failed")
			localSpan.End()

			return fmt.Errorf("ingest.Router.Route: local ingest: %w", err)
		}
		localSpan.End()

		// Replicate to ISR peers for local shards.
		if r.replicator != nil {
			for _, shardKey := range shardsByNode[r.selfID] {
				shardEvents := shardGroups[shardKey]
				seq := r.sequencer.Next(shardKey)

				// Best-effort replication — don't fail the ingest if replication fails
				// (unless AckAll, which blocks).
				if err := r.replicator.ReplicateBatch(ctx, shardKey, shardEvents, seq, addrs); err != nil {
					r.logger.Warn("replication failed for shard",
						"shard_id", shardKey,
						"error", err)
				}
			}
		}

		delete(nodeGroups, r.selfID)
	}

	if len(nodeGroups) == 0 {
		return nil
	}

	// Remote path: fan-out with errgroup.
	g, gCtx := errgroup.WithContext(ctx)

	for nodeID, evts := range nodeGroups {
		nodeID := nodeID
		evts := evts

		g.Go(func() error {
			_, remoteSpan := tracing.Tracer().Start(gCtx, "lynxdb.ingest.route.remote",
				trace.WithAttributes(
					attribute.String(tracing.AttrNodeID, string(nodeID)),
					attribute.Int(tracing.AttrEventsCount, len(evts)),
				))

			if err := r.sendRemote(gCtx, nodeID, addrs[nodeID], evts); err != nil {
				remoteSpan.RecordError(err)
				remoteSpan.SetStatus(codes.Error, "remote ingest failed")
				remoteSpan.End()

				return err
			}

			remoteSpan.End()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("ingest.Router.Route: %w", err)
	}

	return nil
}

// resolvePrimary determines the primary node for a shard.
// Uses the shard map first, falls back to hash ring for lazy assignment.
func (r *Router) resolvePrimary(sm *sharding.ShardMap, sid sharding.ShardID) sharding.NodeID {
	if sm != nil {
		if a := sm.Assignment(sid); a != nil {
			return a.Primary
		}
	}

	// Fallback: use the hash ring for lazy assignment.
	if r.ring != nil {
		if primary := r.ring.AssignPartition(sid.Partition); primary != "" {
			return primary
		}
	}

	// Last resort: route to self.
	return r.selfID
}

// sendRemote serializes events and sends them to a remote node via gRPC.
func (r *Router) sendRemote(ctx context.Context, nodeID sharding.NodeID, addr string, events []*event.Event) error {
	if addr == "" {
		return fmt.Errorf("no address for node %s", nodeID)
	}

	eventsData, err := msgpack.Marshal(events)
	if err != nil {
		return fmt.Errorf("marshal events for node %s: %w", nodeID, err)
	}

	conn, err := r.clientPool.GetConn(ctx, addr)
	if err != nil {
		return fmt.Errorf("dial node %s at %s: %w", nodeID, addr, err)
	}

	client := clusterpb.NewIngestServiceClient(conn)
	resp, err := client.IngestBatch(ctx, &clusterpb.IngestBatchRequest{
		Events: eventsData,
	})
	if err != nil {
		return fmt.Errorf("IngestBatch to node %s: %w", nodeID, err)
	}

	if !resp.Ok {
		return fmt.Errorf("IngestBatch to node %s: rejected", nodeID)
	}

	return nil
}
