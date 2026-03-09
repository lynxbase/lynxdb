// Package views provides cluster-aware materialized view coordination.
// It handles distributed backfill orchestration and view lifecycle
// management across cluster nodes via the Raft FSM.
package views

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/lynxbase/lynxdb/pkg/cluster/meta"
	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// MetaClient provides access to the meta leader for view RPCs.
type MetaClient interface {
	RegisterView(ctx context.Context, req *clusterpb.RegisterViewRequest) (*clusterpb.RegisterViewResponse, error)
}

// QueryExecutor executes a distributed query and returns results.
// Abstracted to avoid a direct dependency on the query coordinator.
type QueryExecutor interface {
	// ExecuteBackfill runs the view's query as a distributed scatter-gather
	// operation and returns the result events. The implementation should
	// handle partial aggregation merging internally.
	ExecuteBackfill(ctx context.Context, query string) error
}

// BackfillCoordinator orchestrates distributed backfill for materialized views.
// It registers the view in the cluster FSM, runs the backfill query via the
// distributed query coordinator, and updates the FSM status on completion.
type BackfillCoordinator struct {
	metaClient    MetaClient
	queryExecutor QueryExecutor
	selfID        sharding.NodeID
	logger        *slog.Logger
}

// NewBackfillCoordinator creates a new distributed backfill coordinator.
func NewBackfillCoordinator(
	metaClient MetaClient,
	queryExecutor QueryExecutor,
	selfID sharding.NodeID,
	logger *slog.Logger,
) *BackfillCoordinator {
	return &BackfillCoordinator{
		metaClient:    metaClient,
		queryExecutor: queryExecutor,
		selfID:        selfID,
		logger:        logger.With("component", "cluster-views"),
	}
}

// RegisterView registers or updates a view definition in the cluster FSM.
// All nodes will learn about this view via the shard map watch stream.
func (c *BackfillCoordinator) RegisterView(ctx context.Context, name, query string, status meta.ViewStatus) error {
	_, err := c.metaClient.RegisterView(ctx, &clusterpb.RegisterViewRequest{
		Name:                name,
		Query:               query,
		Status:              string(status),
		CoordinatorNodeId:   string(c.selfID),
	})
	if err != nil {
		return fmt.Errorf("cluster.views.RegisterView: %w", err)
	}

	return nil
}

// RunBackfill orchestrates a distributed backfill for the named view:
//  1. Register view in FSM with status=backfill
//  2. Execute the view's query as a distributed scatter-gather
//  3. Update FSM status to "active" on completion
//
// The actual query execution is delegated to the QueryExecutor, which
// uses the existing distributed query coordinator for scatter-gather.
func (c *BackfillCoordinator) RunBackfill(ctx context.Context, name, query string) error {
	// 1. Register view as backfilling.
	if err := c.RegisterView(ctx, name, query, meta.ViewStatusBackfill); err != nil {
		return fmt.Errorf("cluster.views.RunBackfill: register: %w", err)
	}

	c.logger.Info("starting distributed backfill", "view", name)

	// 2. Execute the backfill query via distributed coordinator.
	if err := c.queryExecutor.ExecuteBackfill(ctx, query); err != nil {
		c.logger.Error("distributed backfill failed", "view", name, "error", err)

		return fmt.Errorf("cluster.views.RunBackfill: execute: %w", err)
	}

	// 3. Mark view as active.
	if err := c.RegisterView(ctx, name, query, meta.ViewStatusActive); err != nil {
		return fmt.Errorf("cluster.views.RunBackfill: mark active: %w", err)
	}

	c.logger.Info("distributed backfill complete", "view", name)

	return nil
}
