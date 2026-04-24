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
	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
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

// SubmitShardQuery runs a query scoped to local shard data.
func (a *engineShardQueryAdapter) SubmitShardQuery(ctx context.Context, params querycluster.ShardQueryParams) ([]map[string]event.Value, error) {
	prog, err := spl2.ParseProgram(params.Query)
	if err != nil {
		return nil, fmt.Errorf("engineShardQueryAdapter.SubmitShardQuery: parse: %w", err)
	}

	hints := spl2.ExtractQueryHints(prog)

	// Apply time bounds from the shard query params.
	if params.FromNs > 0 || params.ToNs > 0 {
		tb := &spl2.TimeBounds{}
		if params.FromNs > 0 {
			tb.Earliest = time.Unix(0, params.FromNs)
		}
		if params.ToNs > 0 {
			tb.Latest = time.Unix(0, params.ToNs)
		}
		hints.TimeBounds = tb
	}

	qp := QueryParams{}
	noop := func(*SearchProgress) {}
	result, err := a.engine.runQueryPipeline(
		ctx, prog, hints, qp, nil, queryAnnotations{}, noop, nil, time.Now())
	if err != nil {
		return nil, fmt.Errorf("engineShardQueryAdapter.SubmitShardQuery: %w", err)
	}

	// Convert ResultRows to maps.
	rows := make([]map[string]event.Value, len(result.rows))
	for i, rr := range result.rows {
		rows[i] = make(map[string]event.Value, len(rr.Fields))
		for k, v := range rr.Fields {
			rows[i][k] = interfaceToValue(v)
		}
	}

	return rows, nil
}

// SubmitShardPartialAgg runs partial aggregation against local shard data.
func (a *engineShardQueryAdapter) SubmitShardPartialAgg(ctx context.Context, params querycluster.ShardQueryParams) ([]*pipeline.PartialAggGroup, error) {
	// Parse the shard query to extract hints for event store building.
	prog, err := spl2.ParseProgram(params.Query)
	if err != nil {
		return nil, fmt.Errorf("engineShardQueryAdapter.SubmitShardPartialAgg: parse: %w", err)
	}

	hints := spl2.ExtractQueryHints(prog)

	// Apply time bounds.
	if params.FromNs > 0 || params.ToNs > 0 {
		tb := &spl2.TimeBounds{}
		if params.FromNs > 0 {
			tb.Earliest = time.Unix(0, params.FromNs)
		}
		if params.ToNs > 0 {
			tb.Latest = time.Unix(0, params.ToNs)
		}
		hints.TimeBounds = tb
	}

	store, _, storeErr := a.engine.buildEventStore(ctx, hints, nil)
	if storeErr != nil {
		return nil, fmt.Errorf("engineShardQueryAdapter.SubmitShardPartialAgg: build event store: %w", storeErr)
	}

	// Compute partial aggregation across all events in all indexes.
	var allEvents []*event.Event
	for _, evts := range store {
		allEvents = append(allEvents, evts...)
	}

	if params.PartialAggSpec == nil {
		return nil, fmt.Errorf("engineShardQueryAdapter.SubmitShardPartialAgg: nil partial agg spec")
	}

	groups := pipeline.ComputePartialAgg(allEvents, params.PartialAggSpec)

	return groups, nil
}

// EventBus returns the engine's event bus for live tail subscriptions.
func (a *engineShardQueryAdapter) EventBus() *storage.EventBus {
	return a.engine.eventBus
}

func (a *engineShardQueryAdapter) HandlePartCommitted(ctx context.Context, n *clusterpb.PartCommittedNotification) error {
	return a.engine.handleClusterPartCommitted(ctx, n)
}

// interfaceToValue converts an interface{} (from spl2.ResultRow.Fields) to event.Value.
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
