package usecases

import (
	"context"
	"time"

	enginepipeline "github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/server"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage"
	"github.com/lynxbase/lynxdb/pkg/storage/views"
)

// QueryEngine is the subset of server.Engine needed by QueryService.
type QueryEngine interface {
	SubmitQuery(ctx context.Context, params server.QueryParams) (*server.SearchJob, error)
	ActiveJobCount() int64
	MaxConcurrent() int32
	BuildStreamingPipeline(ctx context.Context, prog *spl2.Program,
		externalTB *spl2.TimeBounds) (enginepipeline.Iterator, server.StreamingStats, error)
	BuildEventStoreFromHints(hints *spl2.QueryHints) map[string][]*event.Event

	// HistogramFromMetadata computes histogram buckets using segment metadata
	// (zone maps), without loading all events into memory.
	// Writes counts directly into the provided buckets slice and returns total.
	HistogramFromMetadata(ctx context.Context, indexName string,
		from, to time.Time, interval time.Duration, buckets []server.HistogramBucket) (total int, err error)

	// FieldValuesFromMetadata returns top field values using streaming scan with context cancellation.
	FieldValuesFromMetadata(ctx context.Context, fieldName string, indexName string,
		from, to time.Time, limit int) (*server.FieldValuesResult, error)

	// ListSourcesFromMetadata returns distinct sources using streaming scan with context cancellation.
	ListSourcesFromMetadata(ctx context.Context, indexName string,
		from, to time.Time) (*server.SourcesResult, error)

	// SourceCount returns the number of registered sources (index names) in the registry.
	SourceCount() int
}

// ViewEngine is the subset of server.Engine needed by ViewService.
type ViewEngine interface {
	CreateMV(def views.ViewDefinition) error
	ListMV() []views.ViewDefinition
	GetMV(name string) (views.ViewDefinition, error)
	UpdateMV(def views.ViewDefinition) error
	DeleteMV(name string) error
	TriggerBackfill(name string) error
	BackfillProgress(name string) *server.BackfillProgressInfo
}

// EventBusProvider is the subset of server.Engine needed by TailService.
type EventBusProvider interface {
	EventBus() *storage.EventBus
}

// TailEngine is the subset of server.Engine needed by the new TailService.
type TailEngine interface {
	EventBusProvider
	BuildStreamingPipeline(ctx context.Context, prog *spl2.Program,
		externalTB *spl2.TimeBounds) (enginepipeline.Iterator, server.StreamingStats, error)
}
