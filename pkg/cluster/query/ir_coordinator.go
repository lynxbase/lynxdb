package query

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/lynxbase/lynxdb/pkg/cluster/tracing"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/model"
)

// ExecuteQueryIR splits plan, fans the shard part of query out to the
// shards selected by hints, and merges their results.
func (c *Coordinator) ExecuteQueryIR(
	ctx context.Context,
	query string,
	plan *ir.Plan,
	hints *model.QueryHints,
) (*DistributedQueryResult, error) {
	ctx, span := tracing.Tracer().Start(ctx, "lynxdb.query.distributed.ir")
	defer span.End()

	scanStart := time.Now()

	// 1. Plan the distributed query on the IR.
	irPlan, err := PlanDistributedQueryIR(plan)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "ir plan failed")
		return nil, fmt.Errorf("Coordinator.ExecuteQueryIR: plan: %w", err)
	}

	// 2. Find relevant shards using the pre-resolved hints.
	if hints == nil {
		hints = &model.QueryHints{}
	}
	targets, err := c.pruner.FindRelevantShards(ctx, hints)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "prune shards failed")
		return nil, fmt.Errorf("Coordinator.ExecuteQueryIR: prune shards: %w", err)
	}

	span.SetAttributes(
		attribute.String(tracing.AttrQueryText, query),
		attribute.String(tracing.AttrMergeStrategy, irPlan.Strategy.String()),
		attribute.Int(tracing.AttrShardsTotal, len(targets)),
	)

	meta := QueryMeta{
		ShardsTotal: len(targets),
	}

	if len(targets) == 0 {
		return &DistributedQueryResult{Meta: meta}, nil
	}

	// 3. Build a DistributedPlan compatible with the existing fan-out methods.
	compatPlan := &DistributedPlan{
		ShardQuery:     query,
		Strategy:       irPlan.Strategy,
		PartialAggSpec: irPlan.PartialAggSpec,
		TopK:           irPlan.TopK,
		TopKSortFields: irPlan.TopKSortFields,
		SplitIndex:     irPlan.SplitIndex,
	}

	var rows []map[string]event.Value

	switch irPlan.Strategy {
	case MergePartialAgg, MergeTopK:
		rows, err = c.executePartialAgg(ctx, compatPlan, targets, &meta)
	case MergeConcat:
		rows, err = c.executeConcat(ctx, compatPlan, targets, &meta)
	default:
		return nil, fmt.Errorf("Coordinator.ExecuteQueryIR: unknown strategy %v", irPlan.Strategy)
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "fan-out failed")
		return nil, err
	}

	scanMS := float64(time.Since(scanStart).Milliseconds())

	// 4. Apply coordinator commands via physical.Build on the IR coord nodes.
	var mergeMS float64
	if len(irPlan.CoordNodes) > 0 && len(rows) > 0 {
		_, mergeSpan := tracing.Tracer().Start(ctx, "lynxdb.query.merge")
		mergeStart := time.Now()

		rows, err = applyCoordCommands(ctx, rows, irPlan.CoordNodes)
		if err != nil {
			mergeSpan.RecordError(err)
			mergeSpan.SetStatus(codes.Error, "coord pipeline failed")
			mergeSpan.End()
			return nil, fmt.Errorf("Coordinator.ExecuteQueryIR: coord pipeline: %w", err)
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
