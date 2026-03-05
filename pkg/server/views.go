package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	enginepipeline "github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/optimizer"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage/views"
)

// backfillTimeout is the maximum duration for a single backfill run.
const backfillTimeout = 30 * time.Minute

// CreateMV creates a materialized view definition and launches async backfill.
func (e *Engine) CreateMV(def views.ViewDefinition) error {
	if e.viewRegistry == nil {
		return fmt.Errorf("materialized views require disk persistence (set dataDir)")
	}
	if err := e.viewRegistry.Create(def); err != nil {
		return err
	}
	if e.layout != nil {
		if err := e.layout.EnsureViewDirs(def.Name); err != nil {
			return fmt.Errorf("ensure view dirs: %w", err)
		}
	}
	if err := e.mvDispatcher.ActivateView(def); err != nil {
		return fmt.Errorf("activate view: %w", err)
	}

	// Launch async backfill if the view has a query to backfill from.
	if def.Query != "" {
		e.launchBackfill(def.Name)
	}

	return nil
}

// ListMV returns all materialized view definitions.
func (e *Engine) ListMV() []views.ViewDefinition {
	if e.viewRegistry == nil {
		return nil
	}

	return e.viewRegistry.List()
}

// GetMV returns a materialized view definition by name.
func (e *Engine) GetMV(name string) (views.ViewDefinition, error) {
	if e.viewRegistry == nil {
		return views.ViewDefinition{}, fmt.Errorf("materialized views not available")
	}

	return e.viewRegistry.Get(name)
}

// UpdateMV updates a materialized view definition in-place without delete-and-recreate.
// Bug #4 fix: paused views are deactivated from dispatch — no ActivateView call.
func (e *Engine) UpdateMV(def views.ViewDefinition) error {
	if e.viewRegistry == nil {
		return fmt.Errorf("materialized views not available")
	}

	e.mvDispatcher.DeactivateView(def.Name)

	if err := e.viewRegistry.Update(def); err != nil {
		return err
	}

	// Only re-activate if the view is not paused.
	if def.Status != views.ViewStatusPaused {
		return e.mvDispatcher.ActivateView(def)
	}

	return nil
}

// DeleteMV deletes a materialized view.
func (e *Engine) DeleteMV(name string) error {
	if e.viewRegistry == nil {
		return fmt.Errorf("materialized views not available")
	}
	e.mvDispatcher.DeactivateView(name)

	return e.viewRegistry.Drop(name)
}

// NewBackfiller creates a Backfiller with the engine's memory budget configuration.
// When a UnifiedPool is active, the backfiller gets a dedicated BudgetMonitor as a
// child of the RootMonitor with backpressure on pool exhaustion.
func (e *Engine) NewBackfiller() *views.Backfiller {
	cfg := views.BackfillConfig{
		MaxMemoryBytes:   int64(e.viewsCfg.MaxBackfillMemoryBytes),
		BackpressureWait: e.viewsCfg.BackfillBackpressureWait.Duration(),
		MaxRetries:       e.viewsCfg.BackfillMaxRetries,
	}

	return views.NewBackfillerWithBudget(e.viewRegistry, e.rootMonitor, cfg, e.logger)
}

// ResolveView implements pipeline.ViewResolver for the engine.
func (e *Engine) ResolveView(name string) ([]*event.Event, error) {
	if e.viewRegistry == nil {
		return nil, fmt.Errorf("materialized views not available")
	}
	if _, err := e.viewRegistry.Get(name); err != nil {
		return nil, err
	}

	return e.mvDispatcher.ViewAllEvents(name)
}

// CreateView implements pipeline.ViewManager for the engine.
func (e *Engine) CreateView(name, query, retention string) error {
	if e.viewRegistry == nil {
		return fmt.Errorf("materialized views not available")
	}
	def := views.ViewDefinition{
		Name:    name,
		Version: 1,
		Type:    views.ViewTypeProjection,
		Query:   query,
		Columns: []views.ColumnDef{
			{Name: "_time", Type: event.FieldTypeTimestamp},
			{Name: "_raw", Type: event.FieldTypeString},
			{Name: "_source", Type: event.FieldTypeString},
		},
		Status:    views.ViewStatusBackfill,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := e.viewRegistry.Create(def); err != nil {
		return err
	}
	if e.layout != nil {
		if err := e.layout.EnsureViewDirs(name); err != nil {
			return fmt.Errorf("ensure view dirs: %w", err)
		}
	}
	if err := e.mvDispatcher.ActivateView(def); err != nil {
		return fmt.Errorf("activate view: %w", err)
	}

	// Launch async backfill if the view has a query.
	if query != "" {
		e.launchBackfill(name)
	}

	return nil
}

// launchBackfill starts an asynchronous backfill goroutine for the named view.
// The goroutine executes the view's SPL2 query through the full query engine,
// converts result rows to events, and injects them into the view's storage.
func (e *Engine) launchBackfill(viewName string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), backfillTimeout)
		defer cancel()
		if err := e.RunQueryBackfill(ctx, viewName); err != nil {
			e.logger.Error("MV backfill failed", "view", viewName, "error", err)
		}
	}()
}

// RunQueryBackfill executes the view's SPL2 query through the normal query engine
// and stores the results in the view's segment storage. This reuses the entire
// pipeline (optimizer, VM, partial aggregation) with zero new scan infrastructure.
//
// For aggregation views (stats/timechart/top/rare), the finalized query results
// are converted back to partial aggregation state before storage. This ensures
// that backfill data and live data use the same format, enabling correct merge
// at query time (e.g., weighted avg instead of mean-of-means).
func (e *Engine) RunQueryBackfill(ctx context.Context, viewName string) error {
	if e.viewRegistry == nil {
		return fmt.Errorf("materialized views not available")
	}

	def, err := e.viewRegistry.Get(viewName)
	if err != nil {
		return fmt.Errorf("backfill: load view %s: %w", viewName, err)
	}

	if def.Query == "" {
		// No query to backfill from — mark active immediately.
		def.Status = views.ViewStatusActive
		def.UpdatedAt = time.Now()

		return e.viewRegistry.Update(def)
	}

	// Parse and optimize the query so SubmitQuery receives a fully populated
	// QueryParams (with Program and Hints). Without this, executeQuery will
	// dereference nil Program.Main and panic.
	query := spl2.NormalizeQuery(def.Query)

	prog, err := spl2.ParseProgram(query)
	if err != nil {
		return fmt.Errorf("backfill: parse query for %s: %w", viewName, err)
	}

	opt := optimizer.New()
	prog.Main = opt.Optimize(prog.Main)
	for i := range prog.Datasets {
		prog.Datasets[i].Query = opt.Optimize(prog.Datasets[i].Query)
	}

	hints := spl2.ExtractQueryHints(prog)
	resultType := DetectResultType(prog)

	// Submit the view's query through the normal engine pipeline.
	job, err := e.SubmitQuery(ctx, QueryParams{
		Query:      query,
		Program:    prog,
		Hints:      hints,
		ResultType: resultType,
	})
	if err != nil {
		return fmt.Errorf("backfill: submit query for %s: %w", viewName, err)
	}

	// Track the active backfill job so BackfillProgress() can report on it.
	e.backfillJobs.Store(viewName, job)
	defer e.backfillJobs.Delete(viewName)

	// Wait for completion.
	select {
	case <-ctx.Done():
		job.Cancel()

		return fmt.Errorf("backfill: context canceled for %s: %w", viewName, ctx.Err())
	case <-job.Done():
	}

	snap := job.Snapshot()
	if snap.Status == JobStatusError {
		return fmt.Errorf("backfill: query failed for %s: %s", viewName, snap.Error)
	}

	// For aggregation views, convert finalized results to partial state format.
	// This ensures backfill data merges correctly with live data at query time.
	var backfillEvents []*event.Event
	if def.Type == views.ViewTypeAggregation && def.AggSpec != nil {
		groups := finalizedResultsToPartialGroups(snap.Results, def.AggSpec)
		backfillEvents = views.PartialGroupsToEvents(groups, def.AggSpec, viewName)
	} else {
		backfillEvents = resultRowsToEvents(snap.Results, viewName)
	}

	if len(backfillEvents) > 0 {
		if err := e.mvDispatcher.InjectBackfillEvents(viewName, backfillEvents); err != nil {
			return fmt.Errorf("backfill: inject events for %s: %w", viewName, err)
		}
	}

	// Mark backfill complete. If the view was deleted during backfill,
	// the Get will fail — propagate the error since the caller should know.
	def, err = e.viewRegistry.Get(viewName)
	if err != nil {
		return fmt.Errorf("backfill: reload view %s after completion: %w", viewName, err)
	}
	def.Status = views.ViewStatusActive
	def.UpdatedAt = time.Now()
	if err := e.viewRegistry.Update(def); err != nil {
		return fmt.Errorf("backfill: update status for %s: %w", viewName, err)
	}

	e.logger.Info("MV backfill complete", "view", viewName, "events", len(backfillEvents))

	return nil
}

// finalizedResultsToPartialGroups converts finalized query result rows into
// PartialAggGroup slices with intermediate state. This allows backfill data
// to be stored in the same partial-state format as live dispatch data.
//
// For most functions the conversion is straightforward (count→{Count:N},
// min→{Min:V}, max→{Max:V}). For avg, both sum and count are needed to store
// correct intermediate state. If the spec also includes a count function,
// it is used to reconstruct: sum = avg_value * count. Otherwise
// {Sum: avg_value, Count: 1} is stored — a documented v1 limitation that means
// backfill avg and live avg merge as if backfill represents 1 observation
// per group. Users who need exact avg merge should include count in the query.
func finalizedResultsToPartialGroups(
	results []spl2.ResultRow,
	spec *enginepipeline.PartialAggSpec,
) []*enginepipeline.PartialAggGroup {
	// Pre-scan: find a count function in the spec so we can reconstruct avg state.
	countAlias := ""
	for _, fn := range spec.Funcs {
		if fn.Name == "count" {
			countAlias = fn.Alias

			break
		}
	}

	groups := make([]*enginepipeline.PartialAggGroup, 0, len(results))

	for _, row := range results {
		g := &enginepipeline.PartialAggGroup{
			Key:    make(map[string]event.Value, len(spec.GroupBy)),
			States: make([]enginepipeline.PartialAggState, len(spec.Funcs)),
		}

		// Extract group-by keys.
		for _, field := range spec.GroupBy {
			if v, ok := row.Fields[field]; ok {
				g.Key[field] = event.ValueFromInterface(v)
			} else {
				g.Key[field] = event.NullValue()
			}
		}

		// Get the count value for avg reconstruction.
		var rowCount int64 = 1 // fallback when no count function in spec
		if countAlias != "" {
			if v, ok := row.Fields[countAlias]; ok {
				rowCount = toInt64FromInterface(v)
				if rowCount <= 0 {
					rowCount = 1
				}
			}
		}

		// Convert each finalized result to partial state.
		for j, fn := range spec.Funcs {
			g.States[j] = finalizedValueToState(fn, row, rowCount)
		}

		groups = append(groups, g)
	}

	return groups
}

// finalizedValueToState converts a single finalized aggregation value from a
// result row back into partial aggregation state. The rowCount parameter is
// used to reconstruct avg state: sum = avg * rowCount.
func finalizedValueToState(
	fn enginepipeline.PartialAggFunc,
	row spl2.ResultRow,
	rowCount int64,
) enginepipeline.PartialAggState {
	s := enginepipeline.PartialAggState{
		Min: event.NullValue(),
		Max: event.NullValue(),
	}

	rawVal, ok := row.Fields[fn.Alias]
	if !ok {
		return s
	}
	val := event.ValueFromInterface(rawVal)

	switch fn.Name {
	case "count":
		s.Count = toInt64FromInterface(rawVal)
	case "sum":
		s.Sum = toFloat64FromInterface(rawVal)
	case "avg":
		// Reconstruct intermediate state: sum = avg * count.
		avgVal := toFloat64FromInterface(rawVal)
		s.Sum = avgVal * float64(rowCount)
		s.Count = rowCount
	case "min":
		s.Min = val
	case "max":
		s.Max = val
	case "dc":
		// Distinct count from finalized result — the exact set is lost.
		// Store count only; merge will sum (upper bound). Documented limitation.
		s.Count = toInt64FromInterface(rawVal)
	}

	return s
}

// toInt64FromInterface extracts an int64 from an interface{} value.
func toInt64FromInterface(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

// toFloat64FromInterface extracts a float64 from an interface{} value.
func toFloat64FromInterface(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case int:
		return float64(n)
	default:
		return 0
	}
}

// TriggerBackfill manually triggers a backfill for an existing view.
// Sets the view status to backfill and launches the async backfill goroutine.
func (e *Engine) TriggerBackfill(name string) error {
	if e.viewRegistry == nil {
		return fmt.Errorf("materialized views not available")
	}

	def, err := e.viewRegistry.Get(name)
	if err != nil {
		return err
	}

	def.Status = views.ViewStatusBackfill
	def.UpdatedAt = time.Now()
	if err := e.viewRegistry.Update(def); err != nil {
		return fmt.Errorf("trigger backfill: update status: %w", err)
	}

	e.launchBackfill(name)

	return nil
}

// BackfillProgressInfo holds real-time progress metrics for an active backfill.
// Returned by BackfillProgress() and surfaced through the REST API and CLI.
type BackfillProgressInfo struct {
	Phase           string  `json:"phase"`
	SegmentsTotal   int     `json:"segments_total"`
	SegmentsScanned int     `json:"segments_scanned"`
	SegmentsSkipped int     `json:"segments_skipped"`
	RowsScanned     int64   `json:"rows_scanned"`
	ElapsedMS       float64 `json:"elapsed_ms"`
}

// BackfillProgress returns the real-time progress of an active backfill for the
// named view. Returns nil if no backfill is currently running for this view.
func (e *Engine) BackfillProgress(name string) *BackfillProgressInfo {
	v, ok := e.backfillJobs.Load(name)
	if !ok {
		return nil
	}

	job, ok := v.(*SearchJob)
	if !ok {
		return nil
	}

	p := job.Progress.Load()
	if p == nil {
		return &BackfillProgressInfo{Phase: "starting"}
	}

	skipped := p.SegmentsSkippedIdx + p.SegmentsSkippedTime +
		p.SegmentsSkippedStat + p.SegmentsSkippedBF + p.SegmentsSkippedRange

	return &BackfillProgressInfo{
		Phase:           string(p.Phase),
		SegmentsTotal:   p.SegmentsTotal,
		SegmentsScanned: p.SegmentsScanned,
		SegmentsSkipped: skipped,
		RowsScanned:     p.RowsReadSoFar,
		ElapsedMS:       p.ElapsedMS,
	}
}

// resultRowsToEvents converts query result rows into events suitable for view storage.
// For each ResultRow, it maps known fields (_time, _raw, _source) to the Event struct
// and stores all other fields in the Fields map. If no _raw field exists, a JSON
// representation of all fields is used so full-text search works on view data.
func resultRowsToEvents(rows []spl2.ResultRow, viewName string) []*event.Event {
	events := make([]*event.Event, 0, len(rows))

	for _, row := range rows {
		ev := &event.Event{
			Index:  viewName,
			Fields: make(map[string]event.Value, len(row.Fields)),
		}

		// Extract well-known fields.
		if v, ok := row.Fields["_time"]; ok {
			switch t := v.(type) {
			case time.Time:
				ev.Time = t
			case string:
				if parsed, parseErr := time.Parse(time.RFC3339Nano, t); parseErr == nil {
					ev.Time = parsed
				} else {
					ev.Time = time.Now()
				}
			default:
				ev.Time = time.Now()
			}
		} else {
			ev.Time = time.Now()
		}

		if v, ok := row.Fields["_raw"]; ok {
			if s, ok := v.(string); ok {
				ev.Raw = s
			}
		}

		if v, ok := row.Fields["_source"]; ok {
			if s, ok := v.(string); ok {
				ev.Source = s
			}
		}

		if v, ok := row.Fields["source"]; ok {
			if s, ok := v.(string); ok && ev.Source == "" {
				ev.Source = s
			}
		}

		// Map all fields (including _time, _raw, _source) into ev.Fields
		// so they're available for queries against the view.
		for k, v := range row.Fields {
			ev.Fields[k] = event.ValueFromInterface(v)
		}

		// If no _raw field, build JSON from all fields for full-text search.
		if ev.Raw == "" {
			if b, err := json.Marshal(row.Fields); err == nil {
				ev.Raw = string(b)
			}
		}

		events = append(events, ev)
	}

	return events
}

// ListViews implements pipeline.ViewManager for the engine.
func (e *Engine) ListViews() []enginepipeline.ViewInfo {
	if e.viewRegistry == nil {
		return nil
	}
	defs := e.viewRegistry.List()
	result := make([]enginepipeline.ViewInfo, len(defs))
	for i, d := range defs {
		vtype := "projection"
		if d.Type == views.ViewTypeAggregation {
			vtype = "aggregation"
		}
		result[i] = enginepipeline.ViewInfo{
			Name:      d.Name,
			Status:    string(d.Status),
			Query:     d.Query,
			Type:      vtype,
			CreatedAt: d.CreatedAt.Format(time.RFC3339),
		}
	}

	return result
}

// DropView implements pipeline.ViewManager for the engine.
func (e *Engine) DropView(name string) error {
	if e.viewRegistry == nil {
		return fmt.Errorf("materialized views not available")
	}
	e.mvDispatcher.DeactivateView(name)

	return e.viewRegistry.Drop(name)
}
