package query

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage"
)

// ShardQueryEngine is the interface that the shard handler needs from the
// local engine. Defined here to avoid import cycles with pkg/server.
type ShardQueryEngine interface {
	// SubmitShardQuery runs a query scoped to a specific shard's local data
	// and returns results as rows.
	SubmitShardQuery(ctx context.Context, params ShardQueryParams) ([]map[string]event.Value, error)

	// SubmitShardPartialAgg runs partial aggregation against local shard data.
	SubmitShardPartialAgg(ctx context.Context, params ShardQueryParams) ([]*pipeline.PartialAggGroup, error)

	// EventBus returns the event bus for live tail subscriptions.
	EventBus() *storage.EventBus

	// HandlePartCommitted reconciles newly committed remote parts into the local query view.
	HandlePartCommitted(ctx context.Context, n *clusterpb.PartCommittedNotification) error
}

// ShardQueryParams describes a query scoped to a specific shard.
type ShardQueryParams struct {
	Query          string
	ShardID        string
	FromNs         int64
	ToNs           int64
	PartialAggSpec *pipeline.PartialAggSpec
	RequiredCols   []string
}

// ShardQueryHandler implements the server side of QueryService RPCs.
// It runs queries against local shard data and streams results back.
type ShardQueryHandler struct {
	clusterpb.UnimplementedQueryServiceServer
	engine      ShardQueryEngine
	invalidator *CacheInvalidator
	logger      *slog.Logger
}

// NewShardQueryHandler creates a new handler.
func NewShardQueryHandler(engine ShardQueryEngine, invalidator *CacheInvalidator, logger *slog.Logger) *ShardQueryHandler {
	return &ShardQueryHandler{
		engine:      engine,
		invalidator: invalidator,
		logger:      logger,
	}
}

// ExecuteQuery runs a query against local shard data and streams results.
func (h *ShardQueryHandler) ExecuteQuery(req *clusterpb.ExecuteQueryRequest, stream clusterpb.QueryService_ExecuteQueryServer) error {
	ctx := stream.Context()

	params := ShardQueryParams{
		Query:        req.Query,
		ShardID:      req.ShardId,
		FromNs:       req.FromUnixNs,
		ToNs:         req.ToUnixNs,
		RequiredCols: req.RequiredCols,
	}

	// Decode partial agg spec if present.
	if len(req.PartialAggSpec) > 0 {
		var spec pipeline.PartialAggSpec
		if err := msgpack.Unmarshal(req.PartialAggSpec, &spec); err != nil {
			return fmt.Errorf("ShardQueryHandler.ExecuteQuery: decode partial_agg_spec: %w", err)
		}
		params.PartialAggSpec = &spec
	}

	if params.PartialAggSpec != nil {
		return h.executePartialAgg(ctx, params, stream)
	}

	return h.executeFullScan(ctx, params, stream)
}

// executePartialAgg runs partial aggregation and sends results as msgpack rows.
func (h *ShardQueryHandler) executePartialAgg(
	ctx context.Context,
	params ShardQueryParams,
	stream clusterpb.QueryService_ExecuteQueryServer,
) error {
	groups, err := h.engine.SubmitShardPartialAgg(ctx, params)
	if err != nil {
		return fmt.Errorf("ShardQueryHandler.executePartialAgg: %w", err)
	}

	// Encode each partial group as a msgpack row and stream back.
	for _, group := range groups {
		data, err := msgpack.Marshal(group)
		if err != nil {
			return fmt.Errorf("ShardQueryHandler.executePartialAgg: encode group: %w", err)
		}

		if err := stream.Send(&clusterpb.QueryResult{
			Row:       data,
			IsPartial: true,
		}); err != nil {
			return fmt.Errorf("ShardQueryHandler.executePartialAgg: send: %w", err)
		}
	}

	return nil
}

// executeFullScan runs the query and streams rows as msgpack.
func (h *ShardQueryHandler) executeFullScan(
	ctx context.Context,
	params ShardQueryParams,
	stream clusterpb.QueryService_ExecuteQueryServer,
) error {
	rows, err := h.engine.SubmitShardQuery(ctx, params)
	if err != nil {
		return fmt.Errorf("ShardQueryHandler.executeFullScan: %w", err)
	}

	for _, row := range rows {
		data, err := msgpack.Marshal(row)
		if err != nil {
			return fmt.Errorf("ShardQueryHandler.executeFullScan: encode row: %w", err)
		}

		if err := stream.Send(&clusterpb.QueryResult{
			Row:       data,
			IsPartial: false,
		}); err != nil {
			return fmt.Errorf("ShardQueryHandler.executeFullScan: send: %w", err)
		}
	}

	return nil
}

// NotifyPartCommitted handles part commit notifications for remote-part reconciliation
// and cache invalidation.
func (h *ShardQueryHandler) NotifyPartCommitted(ctx context.Context, n *clusterpb.PartCommittedNotification) (*clusterpb.PartCommittedResponse, error) {
	if h.engine != nil {
		if err := h.engine.HandlePartCommitted(ctx, n); err != nil {
			return nil, fmt.Errorf("ShardQueryHandler.NotifyPartCommitted: %w", err)
		}
	}

	if h.invalidator != nil {
		h.invalidator.HandlePartCommitted(n)
	}

	return &clusterpb.PartCommittedResponse{Ok: true}, nil
}

// SubscribeTail opens a live tail stream for real-time event delivery.
func (h *ShardQueryHandler) SubscribeTail(req *clusterpb.SubscribeTailRequest, stream clusterpb.QueryService_SubscribeTailServer) error {
	ctx := stream.Context()
	bus := h.engine.EventBus()
	if bus == nil {
		return fmt.Errorf("ShardQueryHandler.SubscribeTail: event bus not available")
	}

	subID, ch, err := bus.Subscribe()
	if err != nil {
		return fmt.Errorf("ShardQueryHandler.SubscribeTail: subscribe: %w", err)
	}
	defer bus.Unsubscribe(subID)

	// Send watermarks periodically.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-ch:
			if !ok {
				return nil // channel closed
			}
			data, err := msgpack.Marshal(eventToMap(ev))
			if err != nil {
				h.logger.Warn("tail: encode event failed", "error", err)

				continue
			}

			if err := stream.Send(&clusterpb.TailEvent{
				Event:       data,
				WatermarkNs: time.Now().UnixNano(),
				IsCatchup:   false,
			}); err != nil {
				return fmt.Errorf("ShardQueryHandler.SubscribeTail: send: %w", err)
			}
		case <-ticker.C:
			// Send watermark heartbeat.
			if err := stream.Send(&clusterpb.TailEvent{
				WatermarkNs: time.Now().UnixNano(),
			}); err != nil {
				return fmt.Errorf("ShardQueryHandler.SubscribeTail: send watermark: %w", err)
			}
		}
	}
}

// eventToMap converts an event to a field map for serialization.
func eventToMap(ev *event.Event) map[string]event.Value {
	fields := make(map[string]event.Value, len(ev.Fields)+5)
	if !ev.Time.IsZero() {
		fields["_time"] = event.TimestampValue(ev.Time)
	}
	if ev.Raw != "" {
		fields["_raw"] = event.StringValue(ev.Raw)
	}
	if ev.Source != "" {
		fields["source"] = event.StringValue(ev.Source)
	}
	if ev.Host != "" {
		fields["host"] = event.StringValue(ev.Host)
	}
	if ev.Index != "" {
		fields["index"] = event.StringValue(ev.Index)
	}
	for k, v := range ev.Fields {
		fields[k] = v
	}

	return fields
}
