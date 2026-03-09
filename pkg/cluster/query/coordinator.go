package query

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/lynxbase/lynxdb/pkg/cluster/rpc"
	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/cluster/tracing"
	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// DefaultPartialFailureThreshold is the minimum fraction of successful shards
// before the query is considered a total failure. Below this, we return an error.
const DefaultPartialFailureThreshold = 0.5

// DistributedQueryResult holds the output of a scatter-gather query.
type DistributedQueryResult struct {
	Rows    []map[string]event.Value
	Meta    QueryMeta
	ScanMS  float64
	MergeMS float64
}

// QueryMeta holds metadata about shard-level execution for observability.
type QueryMeta struct {
	ShardsTotal    int      `json:"shards_total"`
	ShardsSuccess  int      `json:"shards_success"`
	ShardsFailed   int      `json:"shards_failed"`
	ShardsTimedOut int      `json:"shards_timed_out"`
	Partial        bool     `json:"partial"`
	Warnings       []string `json:"warnings,omitempty"`
}

// CoordinatorConfig holds settings for the distributed query coordinator.
type CoordinatorConfig struct {
	ShardQueryTimeout       time.Duration
	PartialResultsEnabled   bool
	PartialFailureThreshold float64
}

// Coordinator orchestrates scatter-gather query execution across the cluster.
// It plans the distributed query, prunes irrelevant shards, fans out to shard
// nodes via gRPC, and merges results using the appropriate strategy.
type Coordinator struct {
	clientPool *rpc.ClientPool
	pruner     *ShardPruner
	flowCtrl   *FlowController
	cfg        CoordinatorConfig
	logger     *slog.Logger
}

// NewCoordinator creates a new distributed query coordinator.
func NewCoordinator(
	clientPool *rpc.ClientPool,
	pruner *ShardPruner,
	flowCtrl *FlowController,
	cfg CoordinatorConfig,
	logger *slog.Logger,
) *Coordinator {
	if cfg.ShardQueryTimeout == 0 {
		cfg.ShardQueryTimeout = 30 * time.Second
	}
	if cfg.PartialFailureThreshold == 0 {
		cfg.PartialFailureThreshold = DefaultPartialFailureThreshold
	}

	return &Coordinator{
		clientPool: clientPool,
		pruner:     pruner,
		flowCtrl:   flowCtrl,
		cfg:        cfg,
		logger:     logger,
	}
}

// ExecuteQuery plans, fans out, and merges a distributed query.
func (c *Coordinator) ExecuteQuery(
	ctx context.Context,
	prog *spl2.Program,
	hints *spl2.QueryHints,
) (*DistributedQueryResult, error) {
	ctx, span := tracing.Tracer().Start(ctx, "lynxdb.query.distributed")
	defer span.End()

	scanStart := time.Now()

	// 1. Plan the distributed query.
	plan, err := PlanDistributedQuery(prog)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "plan failed")

		return nil, fmt.Errorf("Coordinator.ExecuteQuery: plan: %w", err)
	}

	// 2. Find relevant shards.
	targets, err := c.pruner.FindRelevantShards(ctx, hints)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "prune shards failed")

		return nil, fmt.Errorf("Coordinator.ExecuteQuery: prune shards: %w", err)
	}

	span.SetAttributes(
		attribute.String(tracing.AttrQueryText, plan.ShardQuery),
		attribute.String(tracing.AttrMergeStrategy, plan.Strategy.String()),
		attribute.Int(tracing.AttrShardsTotal, len(targets)),
	)

	meta := QueryMeta{
		ShardsTotal: len(targets),
	}

	if len(targets) == 0 {
		return &DistributedQueryResult{
			Meta: meta,
		}, nil
	}

	// 3. Fan out by strategy.
	var rows []map[string]event.Value

	switch plan.Strategy {
	case MergePartialAgg, MergeTopK:
		rows, err = c.executePartialAgg(ctx, plan, targets, &meta)
	case MergeConcat:
		rows, err = c.executeConcat(ctx, plan, targets, &meta)
	default:
		return nil, fmt.Errorf("Coordinator.ExecuteQuery: unknown strategy %v", plan.Strategy)
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "fan-out failed")

		return nil, err
	}

	scanMS := float64(time.Since(scanStart).Milliseconds())

	// 4. Apply coordinator commands if any.
	var mergeMS float64
	if len(plan.CoordCommands) > 0 {
		_, mergeSpan := tracing.Tracer().Start(ctx, "lynxdb.query.merge")
		mergeStart := time.Now()

		rows, err = applyCoordCommands(ctx, rows, plan.CoordCommands)
		if err != nil {
			mergeSpan.RecordError(err)
			mergeSpan.SetStatus(codes.Error, "coord pipeline failed")
			mergeSpan.End()

			return nil, fmt.Errorf("Coordinator.ExecuteQuery: coord pipeline: %w", err)
		}

		mergeMS = float64(time.Since(mergeStart).Milliseconds())
		mergeSpan.End()
	}

	meta.Partial = meta.ShardsFailed > 0 && meta.ShardsSuccess > 0

	span.SetAttributes(
		attribute.Int(tracing.AttrShardsSuccess, meta.ShardsSuccess),
		attribute.Int(tracing.AttrShardsFailed, meta.ShardsFailed),
		attribute.Bool("lynxdb.query.partial", meta.Partial),
	)

	return &DistributedQueryResult{
		Rows:    rows,
		Meta:    meta,
		ScanMS:  scanMS,
		MergeMS: mergeMS,
	}, nil
}

// executePartialAgg fans out partial aggregation to all shards and merges.
func (c *Coordinator) executePartialAgg(
	ctx context.Context,
	plan *DistributedPlan,
	targets []ShardTarget,
	meta *QueryMeta,
) ([]map[string]event.Value, error) {
	// Encode partial agg spec for RPC.
	specBytes, err := msgpack.Marshal(plan.PartialAggSpec)
	if err != nil {
		return nil, fmt.Errorf("Coordinator.executePartialAgg: encode spec: %w", err)
	}

	var (
		mu       sync.Mutex
		partials = make([][]*pipeline.PartialAggGroup, 0, len(targets))
	)

	g, gctx := errgroup.WithContext(ctx)

	for _, target := range targets {
		target := target
		g.Go(func() error {
			if err := c.flowCtrl.Acquire(gctx); err != nil {
				return err
			}
			defer c.flowCtrl.Release()

			shardCtx, shardSpan := tracing.Tracer().Start(gctx, "lynxdb.query.shard",
				trace.WithAttributes(
					attribute.String(tracing.AttrShardID, target.ShardID.String()),
					attribute.String(tracing.AttrNodeID, target.NodeAddr),
				))
			shardCtx, cancel := context.WithTimeout(shardCtx, c.cfg.ShardQueryTimeout)
			defer cancel()

			groups, err := c.queryShardPartialAgg(shardCtx, target, plan, specBytes)
			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				meta.ShardsFailed++
				if shardCtx.Err() != nil {
					meta.ShardsTimedOut++
				}
				meta.Warnings = append(meta.Warnings,
					fmt.Sprintf("shard %s failed: %v", target.ShardID, err))
				c.logger.Warn("shard query failed",
					"shard", target.ShardID,
					"node", target.NodeAddr,
					"error", err)

				shardSpan.RecordError(err)
				shardSpan.SetStatus(codes.Error, "shard query failed")
				shardSpan.End()

				return nil // don't abort — collect partial results
			}

			meta.ShardsSuccess++
			if groups != nil {
				partials = append(partials, groups)
			}

			shardSpan.End()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("Coordinator.executePartialAgg: %w", err)
	}

	// Check partial failure threshold.
	if err := c.checkPartialFailure(meta); err != nil {
		return nil, err
	}

	// Merge all partial results.
	if plan.Strategy == MergeTopK && plan.TopK > 0 {
		return pipeline.MergePartialAggsTopK(partials, plan.PartialAggSpec, plan.TopK, plan.TopKSortFields), nil
	}

	return pipeline.MergePartialAggs(partials, plan.PartialAggSpec), nil
}

// executeConcat fans out full scans to all shards and concatenates results.
func (c *Coordinator) executeConcat(
	ctx context.Context,
	plan *DistributedPlan,
	targets []ShardTarget,
	meta *QueryMeta,
) ([]map[string]event.Value, error) {
	var (
		mu      sync.Mutex
		allRows []map[string]event.Value
	)

	g, gctx := errgroup.WithContext(ctx)

	for _, target := range targets {
		target := target
		g.Go(func() error {
			if err := c.flowCtrl.Acquire(gctx); err != nil {
				return err
			}
			defer c.flowCtrl.Release()

			shardCtx, shardSpan := tracing.Tracer().Start(gctx, "lynxdb.query.shard",
				trace.WithAttributes(
					attribute.String(tracing.AttrShardID, target.ShardID.String()),
					attribute.String(tracing.AttrNodeID, target.NodeAddr),
				))
			shardCtx, cancel := context.WithTimeout(shardCtx, c.cfg.ShardQueryTimeout)
			defer cancel()

			rows, err := c.queryShardConcat(shardCtx, target, plan)
			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				meta.ShardsFailed++
				if shardCtx.Err() != nil {
					meta.ShardsTimedOut++
				}
				meta.Warnings = append(meta.Warnings,
					fmt.Sprintf("shard %s failed: %v", target.ShardID, err))

				shardSpan.RecordError(err)
				shardSpan.SetStatus(codes.Error, "shard query failed")
				shardSpan.End()

				return nil
			}

			meta.ShardsSuccess++
			allRows = append(allRows, rows...)

			shardSpan.End()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("Coordinator.executeConcat: %w", err)
	}

	if err := c.checkPartialFailure(meta); err != nil {
		return nil, err
	}

	return allRows, nil
}

// queryShardPartialAgg sends a partial aggregation query to a single shard.
func (c *Coordinator) queryShardPartialAgg(
	ctx context.Context,
	target ShardTarget,
	plan *DistributedPlan,
	specBytes []byte,
) ([]*pipeline.PartialAggGroup, error) {
	conn, err := c.clientPool.GetConn(ctx, target.NodeAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", target.NodeAddr, err)
	}

	client := clusterpb.NewQueryServiceClient(conn)
	stream, err := client.ExecuteQuery(ctx, &clusterpb.ExecuteQueryRequest{
		Query:          plan.ShardQuery,
		ShardId:        target.ShardID.String(),
		PartialAggSpec: specBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("ExecuteQuery RPC: %w", err)
	}

	var groups []*pipeline.PartialAggGroup
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			// Check for clean stream end.
			if ctx.Err() != nil {
				return groups, ctx.Err()
			}

			break
		}

		if len(msg.Row) == 0 {
			continue
		}

		var group pipeline.PartialAggGroup
		if err := msgpack.Unmarshal(msg.Row, &group); err != nil {
			return nil, fmt.Errorf("decode partial group: %w", err)
		}
		groups = append(groups, &group)
	}

	return groups, nil
}

// queryShardConcat sends a full scan query to a single shard and collects rows.
func (c *Coordinator) queryShardConcat(
	ctx context.Context,
	target ShardTarget,
	plan *DistributedPlan,
) ([]map[string]event.Value, error) {
	conn, err := c.clientPool.GetConn(ctx, target.NodeAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", target.NodeAddr, err)
	}

	client := clusterpb.NewQueryServiceClient(conn)
	stream, err := client.ExecuteQuery(ctx, &clusterpb.ExecuteQueryRequest{
		Query:   plan.ShardQuery,
		ShardId: target.ShardID.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("ExecuteQuery RPC: %w", err)
	}

	var rows []map[string]event.Value
	for {
		msg, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return rows, ctx.Err()
			}

			break
		}

		if len(msg.Row) == 0 {
			continue
		}

		var row map[string]event.Value
		if err := msgpack.Unmarshal(msg.Row, &row); err != nil {
			return nil, fmt.Errorf("decode row: %w", err)
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// checkPartialFailure returns an error if too many shards failed.
func (c *Coordinator) checkPartialFailure(meta *QueryMeta) error {
	if meta.ShardsTotal == 0 {
		return nil
	}

	successRate := float64(meta.ShardsSuccess) / float64(meta.ShardsTotal)
	if successRate < c.cfg.PartialFailureThreshold {
		return fmt.Errorf("Coordinator: too many shard failures (%d/%d succeeded, threshold %.0f%%)",
			meta.ShardsSuccess, meta.ShardsTotal, c.cfg.PartialFailureThreshold*100)
	}

	return nil
}

// applyCoordCommands runs coordinator-only pipeline commands on merged rows.
func applyCoordCommands(ctx context.Context, rows []map[string]event.Value, commands []spl2.Command) ([]map[string]event.Value, error) {
	if len(commands) == 0 || len(rows) == 0 {
		return rows, nil
	}

	// Build events from rows for pipeline execution.
	events := make([]*event.Event, len(rows))
	for i, row := range rows {
		ev := &event.Event{
			Fields: make(map[string]event.Value, len(row)),
		}
		for k, v := range row {
			switch k {
			case "_time":
				if v.Type() == event.FieldTypeTimestamp {
					ev.Time = v.AsTimestamp()
				}
			case "_raw":
				ev.Raw = v.AsString()
			case "source", "_source":
				ev.Source = v.AsString()
			case "host":
				ev.Host = v.AsString()
			default:
				ev.Fields[k] = v
			}
		}
		events[i] = ev
	}

	// Execute the coordinator pipeline.
	pipeStore := &pipeline.ServerIndexStore{
		Events: map[string][]*event.Event{"_coordinator": events},
	}
	subQuery := &spl2.Query{
		Source:   &spl2.SourceClause{Index: "_coordinator"},
		Commands: commands,
	}

	iter, err := pipeline.BuildPipelineWithStats(ctx, subQuery, pipeStore, 0)
	if err != nil {
		return nil, fmt.Errorf("applyCoordCommands: build pipeline: %w", err)
	}
	defer iter.Close()

	pipeRows, err := pipeline.CollectAll(ctx, iter)
	if err != nil {
		return nil, fmt.Errorf("applyCoordCommands: collect: %w", err)
	}

	return pipeRows, nil
}
