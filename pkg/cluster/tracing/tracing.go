// Package tracing provides OpenTelemetry helpers and attribute constants
// for LynxDB cluster components. It centralizes the tracer name and common
// attribute keys so that spans across ingest, query, and meta layers share
// a consistent naming scheme.
package tracing

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "lynxdb.cluster"

// Tracer returns the OTel tracer for cluster components.
func Tracer() trace.Tracer { return otel.Tracer(tracerName) }

// Common attribute keys for cluster span annotations.
const (
	AttrShardID       = "lynxdb.shard_id"
	AttrNodeID        = "lynxdb.node_id"
	AttrQueryText     = "lynxdb.query.text"
	AttrMergeStrategy = "lynxdb.query.merge_strategy"
	AttrShardsTotal   = "lynxdb.query.shards_total"
	AttrShardsSuccess = "lynxdb.query.shards_success"
	AttrShardsFailed  = "lynxdb.query.shards_failed"
	AttrEventsCount   = "lynxdb.ingest.events_count"
	AttrShardsCount   = "lynxdb.ingest.shards_count"
	AttrIsLeader      = "lynxdb.raft.is_leader"
)

// WithNodeID returns a SpanStartOption that sets the node_id attribute.
func WithNodeID(nodeID string) trace.SpanStartOption {
	return trace.WithAttributes(attribute.String(AttrNodeID, nodeID))
}

// WithIsLeader returns a SpanStartOption that sets the is_leader attribute.
func WithIsLeader(isLeader bool) trace.SpanStartOption {
	return trace.WithAttributes(attribute.Bool(AttrIsLeader, isLeader))
}
