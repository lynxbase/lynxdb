package query

import (
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// JoinStrategy describes how a distributed join is executed.
type JoinStrategy int

const (
	// JoinBroadcast sends the smaller (right) side to all shards.
	// Used when the right side has a small result set (head N <= 10000).
	JoinBroadcast JoinStrategy = iota
	// JoinColocated executes the join on each shard independently.
	// Used when both sides share the shard key matching the join field.
	JoinColocated
	// JoinCoordinator materializes both sides on the coordinator.
	// Default fallback when no optimization applies.
	JoinCoordinator
)

// String returns a human-readable name for the join strategy.
func (s JoinStrategy) String() string {
	switch s {
	case JoinBroadcast:
		return "broadcast"
	case JoinColocated:
		return "colocated"
	case JoinCoordinator:
		return "coordinator"
	default:
		return "unknown"
	}
}

// broadcastThreshold is the maximum result count for the right side
// before we fall back from broadcast to coordinator join.
const broadcastThreshold = 10_000

// shardKeyFields are the fields used in the two-level sharding hash key.
// A colocated join is valid when the join field matches a shard key component
// and both sides share the same index (same partition space).
var shardKeyFields = map[string]bool{
	"source": true, "_source": true, "host": true,
}

// PlanDistributedJoin selects the optimal strategy for a distributed join.
//
// Strategy selection:
//   - Broadcast: if the right side has `head N` with N <= broadcastThreshold,
//     the right side results are small enough to broadcast to all shards.
//   - Colocated: if both sides share the same index and the join field is a
//     shard key component, each shard can execute the join independently.
//   - Coordinator: default fallback — both sides are materialized on the
//     coordinator node and joined there.
func PlanDistributedJoin(cmd *spl2.JoinCommand, leftSource *spl2.SourceClause, _ *spl2.QueryHints) JoinStrategy {
	if cmd == nil || cmd.Subquery == nil {
		return JoinCoordinator
	}

	// Check if the right side (subquery) has a head limit small enough for broadcast.
	if limit := terminalHeadLimit(cmd.Subquery); limit > 0 && limit <= broadcastThreshold {
		return JoinBroadcast
	}

	// Check for colocated join when both sides share the shard key
	// matching the join field.
	if isColocatedJoin(cmd, leftSource) {
		return JoinColocated
	}

	return JoinCoordinator
}

// isColocatedJoin returns true when both sides of a join reference the same
// single index and the join field is a shard key component. In this case every
// shard contains matching data from both sides and the join can execute locally.
func isColocatedJoin(cmd *spl2.JoinCommand, leftSource *spl2.SourceClause) bool {
	// Left source must be a single, concrete source.
	if leftSource == nil || !leftSource.IsSingleSource() {
		return false
	}

	// Right source (subquery) must also be a single, concrete source.
	rightSource := cmd.Subquery.Source
	if rightSource == nil || !rightSource.IsSingleSource() {
		return false
	}

	// Both must reference the same index (same partition space).
	if leftSource.Index != rightSource.Index {
		return false
	}

	// The join field must be a shard key component so that matching rows
	// are guaranteed to land on the same shard.
	return shardKeyFields[cmd.Field]
}

// terminalHeadLimit returns the head limit of a query's last command,
// or 0 if the query doesn't end with a HeadCommand.
func terminalHeadLimit(q *spl2.Query) int {
	if q == nil || len(q.Commands) == 0 {
		return 0
	}

	last := q.Commands[len(q.Commands)-1]
	if head, ok := last.(*spl2.HeadCommand); ok {
		return head.Count
	}

	return 0
}
