package server

import (
	"context"
	"fmt"
	"time"

	"github.com/lynxbase/lynxdb/pkg/cluster"
	ingestcluster "github.com/lynxbase/lynxdb/pkg/cluster/ingest"
	querycluster "github.com/lynxbase/lynxdb/pkg/cluster/query"
	"github.com/lynxbase/lynxdb/pkg/cluster/rpc"
	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
	"github.com/lynxbase/lynxdb/pkg/engine/physical"
	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/planner"
	"github.com/lynxbase/lynxdb/pkg/storage"
)

// InitClusterQuery wires up cluster query components when cluster mode is
// enabled and this node has the query role. Must be called after NewEngine
// but before Start.
//
// In single-node mode (clusterCfg.Enabled == false), this is a no-op.
func (e *Engine) InitClusterQuery(node *cluster.Node, clientPool *rpc.ClientPool) error {
	if !e.clusterCfg.Enabled {
		return nil
	}

	logger := e.logger.With("component", "cluster-query")
	if e.objStore != nil && e.clusterCatalog == nil {
		e.clusterCatalog = ingestcluster.NewPartCatalog(e.objStore, logger)
	}

	// Node address resolver — reads from the shard map cache which is
	// populated by WatchShardMap with node gRPC addresses.
	nodeAddrs := func() map[sharding.NodeID]string {
		return node.ShardMapCache().GetNodeAddrs()
	}

	pruner := querycluster.NewShardPruner(
		node.ShardMapCache(),
		e.sourceRegistry,
		e.clusterCfg,
		nodeAddrs,
		logger,
	)

	maxShardQueries := e.clusterCfg.MaxConcurrentShardQueries
	if maxShardQueries == 0 {
		maxShardQueries = querycluster.DefaultMaxConcurrentShards
	}
	flowCtrl := querycluster.NewFlowController(maxShardQueries, logger)

	coordCfg := querycluster.CoordinatorConfig{
		ShardQueryTimeout:       e.clusterCfg.ShardQueryTimeout.Duration(),
		PartialResultsEnabled:   true,
		PartialFailureThreshold: e.clusterCfg.PartialFailureThreshold,
	}
	if e.clusterCfg.PartialResultsEnabled != nil {
		coordCfg.PartialResultsEnabled = *e.clusterCfg.PartialResultsEnabled
	}

	e.clusterCoordinator = querycluster.NewCoordinator(
		clientPool,
		pruner,
		flowCtrl,
		coordCfg,
		logger,
	)

	// Register QueryService on the gRPC server.
	cacheInvalidator := querycluster.NewCacheInvalidator(e.cache, logger)
	handler := querycluster.NewShardQueryHandler(
		&engineShardQueryAdapter{engine: e},
		cacheInvalidator,
		logger,
	)
	node.RegisterQueryService(handler)

	if e.clusterCatalog != nil {
		if err := e.reconcileClusterCatalogs(context.Background()); err != nil {
			return fmt.Errorf("InitClusterQuery: reconcile catalogs: %w", err)
		}
	}

	logger.Info("cluster query initialized",
		"max_concurrent_shard_queries", maxShardQueries,
		"shard_query_timeout", coordCfg.ShardQueryTimeout)

	return nil
}

// ClusterCoordinator returns the cluster query coordinator, or nil in single-node mode.
func (e *Engine) ClusterCoordinator() *querycluster.Coordinator {
	return e.clusterCoordinator
}

// engineShardQueryAdapter adapts the Engine to the ShardQueryEngine interface
// required by the shard handler, avoiding import cycles.
type engineShardQueryAdapter struct {
	engine *Engine
}

// SubmitShardQuery runs the shard part of a query over local shard data.
func (a *engineShardQueryAdapter) SubmitShardQuery(ctx context.Context, params querycluster.ShardQueryParams) ([]map[string]event.Value, error) {
	rows, err := a.runShardPlan(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("engineShardQueryAdapter.SubmitShardQuery: %w", err)
	}
	return rows, nil
}

// SubmitShardPartialAgg runs the shard part of a query and aggregates its
// rows to mergeable partial states.
func (a *engineShardQueryAdapter) SubmitShardPartialAgg(ctx context.Context, params querycluster.ShardQueryParams) ([]*pipeline.PartialAggGroup, error) {
	if params.PartialAggSpec == nil {
		return nil, fmt.Errorf("engineShardQueryAdapter.SubmitShardPartialAgg: nil partial agg spec")
	}
	rows, err := a.runShardPlan(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("engineShardQueryAdapter.SubmitShardPartialAgg: %w", err)
	}
	batch := pipeline.NewBatch(len(rows))
	for _, row := range rows {
		batch.AddRow(row)
	}
	return pipeline.ComputePartialAggFromBatches([]*pipeline.Batch{batch}, params.PartialAggSpec), nil
}

// runShardPlan compiles the query, takes the shard prefix of its
// distributed split and runs it over local data.
func (a *engineShardQueryAdapter) runShardPlan(ctx context.Context, params querycluster.ShardQueryParams) ([]map[string]event.Value, error) {
	res, err := planner.New().Plan(planner.PlanRequest{Query: params.Query})
	if err != nil {
		return nil, err
	}
	split, err := querycluster.PlanDistributedQueryIR(res.Plan)
	if err != nil {
		return nil, err
	}
	hints := physical.PlanHints(split.ShardPlan, planner.DefaultSource, time.Now())
	if params.FromNs > 0 || params.ToNs > 0 {
		tb := &model.TimeBounds{}
		if params.FromNs > 0 {
			tb.Earliest = time.Unix(0, params.FromNs)
		}
		if params.ToNs > 0 {
			tb.Latest = time.Unix(0, params.ToNs)
		}
		hints.TimeBounds = tb
	}
	noop := func(*SearchProgress) {}
	result, err := a.engine.runStandardPipeline(ctx, split.ShardPlan, hints, QueryParams{}, noop, nil, time.Now())
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]event.Value, len(result.rows))
	for i, rr := range result.rows {
		rows[i] = make(map[string]event.Value, len(rr.Fields))
		for k, v := range rr.Fields {
			rows[i][k] = interfaceToValue(v)
		}
	}
	return rows, nil
}

// EventBus returns the engine's event bus for live tail subscriptions.
func (a *engineShardQueryAdapter) EventBus() *storage.EventBus {
	return a.engine.eventBus
}

func (a *engineShardQueryAdapter) HandlePartCommitted(ctx context.Context, n *clusterpb.PartCommittedNotification) error {
	return a.engine.handleClusterPartCommitted(ctx, n)
}

// interfaceToValue converts an interface{} (from model.ResultRow.Fields) to event.Value.
func interfaceToValue(v interface{}) event.Value {
	if v == nil {
		return event.NullValue()
	}
	switch val := v.(type) {
	case event.Value:
		return val
	case string:
		return event.StringValue(val)
	case int64:
		return event.IntValue(val)
	case int:
		return event.IntValue(int64(val))
	case float64:
		return event.FloatValue(val)
	case bool:
		return event.BoolValue(val)
	default:
		return event.StringValue(fmt.Sprintf("%v", v))
	}
}
