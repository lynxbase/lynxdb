package usecases

import (
	"fmt"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/views"
)

// ViewService orchestrates materialized view operations.
type ViewService struct {
	engine ViewEngine
}

// NewViewService creates a ViewService.
func NewViewService(engine ViewEngine) *ViewService {
	return &ViewService{engine: engine}
}

// Create creates a new materialized view with default column schema.
// The view starts in "backfill" status; the engine launches an async backfill
// goroutine that executes the view's query and populates the view's storage.
//
// If the query contains a terminal aggregation (stats, timechart, top, rare),
// the view is created as an aggregation view with a PartialAggSpec that drives
// insert-time partial aggregation. Unsupported patterns (eventstats, stdev,
// percentiles, etc.) are rejected with a descriptive error.
func (s *ViewService) Create(req CreateViewRequest) error {
	def := views.ViewDefinition{
		Name:      req.Name,
		Version:   1,
		Type:      views.ViewTypeProjection,
		Query:     req.Query,
		Columns:   defaultColumns(),
		Status:    views.ViewStatusBackfill,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Analyze the query to determine view type and extract pipeline metadata.
	if req.Query != "" {
		analysis, err := views.AnalyzeQuery(req.Query)
		if err != nil {
			return fmt.Errorf("usecases.CreateView: %w", err)
		}

		if analysis.SourceIndex != "" {
			def.SourceIndex = analysis.SourceIndex
		}

		if analysis.IsAggregation {
			def.Type = views.ViewTypeAggregation
			def.AggSpec = analysis.AggSpec
			def.GroupBy = analysis.GroupBy
		}
	}

	return s.engine.CreateMV(def)
}

// List returns all materialized views.
func (s *ViewService) List() []ViewSummary {
	defs := s.engine.ListMV()
	result := make([]ViewSummary, len(defs))
	for i, d := range defs {
		result[i] = ViewSummary{
			Name:      d.Name,
			Status:    d.Status,
			Query:     d.Query,
			Type:      d.Type,
			CreatedAt: d.CreatedAt,
			UpdatedAt: d.UpdatedAt,
		}
	}

	return result
}

// Get returns a single materialized view by name.
func (s *ViewService) Get(name string) (*ViewDetail, error) {
	def, err := s.engine.GetMV(name)
	if err != nil {
		return nil, err
	}

	detail := &ViewDetail{
		ViewSummary: ViewSummary{
			Name:      def.Name,
			Status:    def.Status,
			Query:     def.Query,
			Type:      def.Type,
			CreatedAt: def.CreatedAt,
			UpdatedAt: def.UpdatedAt,
		},
		Filter:    def.Filter,
		Columns:   def.Columns,
		Retention: def.Retention,
	}

	// Attach live backfill progress if a backfill is currently running.
	if def.Status == views.ViewStatusBackfill {
		detail.BackfillProgress = s.engine.BackfillProgress(name)
	}

	return detail, nil
}

// Delete removes a materialized view by name.
func (s *ViewService) Delete(name string) error {
	return s.engine.DeleteMV(name)
}

// TriggerBackfill manually triggers a backfill for an existing view.
// Sets the view status to backfill and launches the async backfill goroutine.
func (s *ViewService) TriggerBackfill(name string) error {
	return s.engine.TriggerBackfill(name)
}

// Patch updates a materialized view in-place without delete-and-recreate.
// This avoids the window where events could be lost between delete and create.
func (s *ViewService) Patch(name string, req PatchViewRequest) (*ViewSummary, error) {
	def, err := s.engine.GetMV(name)
	if err != nil {
		return nil, err
	}

	if req.Retention != nil {
		dur, err := time.ParseDuration(*req.Retention)
		if err != nil {
			return nil, err
		}
		def.Retention = dur
	}

	if req.Paused != nil {
		if *req.Paused {
			def.Status = views.ViewStatusPaused
		} else if def.Status == views.ViewStatusPaused {
			def.Status = views.ViewStatusActive
		}
	}

	def.UpdatedAt = time.Now()

	if err := s.engine.UpdateMV(def); err != nil {
		return nil, err
	}

	return &ViewSummary{
		Name:      def.Name,
		Status:    def.Status,
		Query:     def.Query,
		Type:      def.Type,
		CreatedAt: def.CreatedAt,
		UpdatedAt: def.UpdatedAt,
	}, nil
}

// defaultColumns returns the standard column schema for new projections.
func defaultColumns() []views.ColumnDef {
	return []views.ColumnDef{
		{Name: "_time", Type: event.FieldTypeTimestamp},
		{Name: "_raw", Type: event.FieldTypeString},
		{Name: "_source", Type: event.FieldTypeString},
	}
}
