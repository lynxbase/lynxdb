package usecases

import (
	"context"
	"time"

	enginepipeline "github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/server"
)

// QueryEngine is the subset of server.Engine needed by QueryService.
type QueryEngine interface {
	SubmitQuery(ctx context.Context, params server.QueryParams) (*server.SearchJob, error)
	ActiveJobCount() int64
	MaxConcurrent() int32
	BuildStreamingPipeline(ctx context.Context, prog *ir.Plan,
		hints *model.QueryHints,
		externalTB *model.TimeBounds) (enginepipeline.Iterator, server.StreamingStats, error)
	BuildEventStoreFromHints(hints *model.QueryHints) map[string][]*event.Event

	// HistogramFromMetadata computes histogram buckets using segment metadata
	// (zone maps), without loading all events into memory.
	// Writes counts directly into the provided buckets slice and returns total.
	HistogramFromMetadata(ctx context.Context, indexName string,
		from, to time.Time, interval time.Duration, buckets []server.HistogramBucket) (total int, err error)

	// HistogramByFieldFromMetadata computes histogram buckets grouped by a field value.
	HistogramByFieldFromMetadata(ctx context.Context, indexName string,
		from, to time.Time, interval time.Duration, fieldName string, bucketCount int) ([]server.GroupedHistogramBucket, int, error)

	// FieldValuesFromMetadata returns top field values using streaming scan with context cancellation.
	FieldValuesFromMetadata(ctx context.Context, fieldName string, indexName string,
		from, to time.Time, limit int) (*server.FieldValuesResult, error)

	// ListSourcesFromMetadata returns distinct sources using streaming scan with context cancellation.
	ListSourcesFromMetadata(ctx context.Context, indexName string,
		from, to time.Time) (*server.SourcesResult, error)

	// SourceCount returns the number of registered sources (index names) in the registry.
	SourceCount() int

	// ListFieldNames returns all known field names from the field catalog.
	ListFieldNames() []string
}
