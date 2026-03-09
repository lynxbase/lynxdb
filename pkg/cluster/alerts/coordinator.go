// Package alerts provides cluster-aware alert coordination.
// It wraps the single-node alerts.Manager to distribute alert evaluation
// across query nodes using rendezvous hashing via the Raft FSM.
package alerts

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/lynxbase/lynxdb/pkg/cluster/meta"
	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// MetaClient provides access to the meta leader for alert RPCs.
type MetaClient interface {
	AssignAlert(ctx context.Context, req *clusterpb.AssignAlertRequest) (*clusterpb.AssignAlertResponse, error)
	ReportAlertFired(ctx context.Context, req *clusterpb.ReportAlertFiredRequest) (*clusterpb.ReportAlertFiredResponse, error)
}

// AlertAssignmentCache provides read access to cached alert assignments.
type AlertAssignmentCache interface {
	GetAlertAssignment(alertID string) *meta.AlertAssignment
}

// AlertCoordinator wraps the single-node alert system for cluster mode.
// It determines which query node is responsible for evaluating each alert
// using rendezvous hashing, ensuring exactly-once evaluation cluster-wide.
type AlertCoordinator struct {
	metaClient MetaClient
	cache      AlertAssignmentCache
	selfID     sharding.NodeID
	logger     *slog.Logger
}

// NewAlertCoordinator creates a new cluster-aware alert coordinator.
func NewAlertCoordinator(
	metaClient MetaClient,
	cache AlertAssignmentCache,
	selfID sharding.NodeID,
	logger *slog.Logger,
) *AlertCoordinator {
	return &AlertCoordinator{
		metaClient: metaClient,
		cache:      cache,
		selfID:     selfID,
		logger:     logger.With("component", "cluster-alerts"),
	}
}

// OnCreate requests alert assignment via the meta leader.
// The meta leader computes the rendezvous hash and stores the assignment in the FSM.
func (c *AlertCoordinator) OnCreate(ctx context.Context, alertID string, queryNodes []sharding.NodeID) error {
	nodeIDs := make([]string, len(queryNodes))
	for i, id := range queryNodes {
		nodeIDs[i] = string(id)
	}

	resp, err := c.metaClient.AssignAlert(ctx, &clusterpb.AssignAlertRequest{
		AlertId:      alertID,
		QueryNodeIds: nodeIDs,
	})
	if err != nil {
		return fmt.Errorf("cluster.alerts.OnCreate: %w", err)
	}

	c.logger.Info("alert assigned",
		"alert_id", alertID,
		"assigned_to", resp.AssignedNodeId,
		"is_local", resp.AssignedNodeId == string(c.selfID))

	return nil
}

// OnDelete removes an alert assignment by assigning with empty node list.
func (c *AlertCoordinator) OnDelete(ctx context.Context, alertID string) error {
	_, err := c.metaClient.AssignAlert(ctx, &clusterpb.AssignAlertRequest{
		AlertId:      alertID,
		QueryNodeIds: nil, // empty = unassign
	})
	if err != nil {
		return fmt.Errorf("cluster.alerts.OnDelete: %w", err)
	}

	return nil
}

// ShouldEvaluate returns true if this node is responsible for evaluating
// the given alert. Reads from the cached FSM state for zero-latency checks.
// Implements the ClusterGuard interface used by alerts.Scheduler.
func (c *AlertCoordinator) ShouldEvaluate(alertID string) bool {
	aa := c.cache.GetAlertAssignment(alertID)
	if aa == nil {
		// Not assigned yet — allow evaluation (single-node fallback).
		return true
	}

	return aa.AssignedNode == c.selfID
}

// OnFired reports an alert fire event to the meta leader so other nodes
// can see the LastFiredAt timestamp and avoid duplicate evaluation during
// failover windows.
func (c *AlertCoordinator) OnFired(ctx context.Context, alertID string) {
	_, err := c.metaClient.ReportAlertFired(ctx, &clusterpb.ReportAlertFiredRequest{
		AlertId:      alertID,
		FiredAtUnixNs: time.Now().UnixNano(),
	})
	if err != nil {
		c.logger.Warn("failed to report alert fired",
			"alert_id", alertID,
			"error", err)
	}
}
