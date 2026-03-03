package views

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/engine/pipeline"
	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/stats"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment"
)

const (
	batchMaxEvents = 1000                   // flush batch after this many events
	batchMaxDelay  = 100 * time.Millisecond // flush batch after this delay

	// ProcessInsertTimeout bounds insert-time pipeline execution. If a rex
	// regex backtracks or an eval hangs, we abort rather than blocking the
	// entire ingest hot path indefinitely.
	processInsertTimeout = 5 * time.Second

	// Aggregation function name constants shared across serialization and merge.
	aggFnCount = "count"
	aggFnSum   = "sum"
	aggFnAvg   = "avg"
	aggFnMin   = "min"
	aggFnMax   = "max"
	aggFnDC    = "dc"
)

// activeView holds the runtime state for a single materialized view.
type activeView struct {
	def       ViewDefinition
	filter    *Filter
	extractor *Extractor
	mu        sync.RWMutex
	closed    atomic.Bool // set by DeactivateView, checked by Dispatch

	// analysis holds the pipeline splitting result from AnalyzeQuery.
	// Non-nil when def.Query is non-empty. For aggregation views, contains
	// the AggSpec and streaming commands used at insert time.
	analysis *QueryAnalysis

	// events holds unflushed events (replaces the old memtable dependency).
	// Protected by mu. Sorted by timestamp on read (deferred sort).
	events []*event.Event

	// Batching state (protected by mu).
	pending    []*event.Event
	firstAdded time.Time // time of first event in current batch
}

// addPendingBatch adds multiple events under a single lock acquisition.
func (av *activeView) addPendingBatch(events []*event.Event) {
	if av.closed.Load() || len(events) == 0 {
		return
	}
	av.mu.Lock()
	if len(av.pending) == 0 {
		av.firstAdded = time.Now()
	}
	av.pending = append(av.pending, events...)
	if len(av.pending) >= batchMaxEvents {
		av.flushPendingLocked()
	}
	av.mu.Unlock()
}

// flushPendingLocked moves buffered events into the events slice. Caller must hold av.mu.
func (av *activeView) flushPendingLocked() {
	if len(av.pending) == 0 {
		return
	}
	av.events = append(av.events, av.pending...)
	av.pending = av.pending[:0]
}

// sortedEvents returns all events sorted by timestamp.
func (av *activeView) sortedEvents() []*event.Event {
	sort.Slice(av.events, func(i, j int) bool {
		return av.events[i].Time.Before(av.events[j].Time)
	})

	return av.events
}

// flushIfExpired flushes the batch if it has exceeded the max delay.
func (av *activeView) flushIfExpired() {
	av.mu.Lock()
	if len(av.pending) > 0 && time.Since(av.firstAdded) >= batchMaxDelay {
		av.flushPendingLocked()
	}
	av.mu.Unlock()
}

// Dispatcher routes incoming events to matching materialized views.
type Dispatcher struct {
	mu       sync.RWMutex
	views    map[string]*activeView
	registry *ViewRegistry
	layout   *storage.Layout
	logger   *slog.Logger
	done     chan struct{}
	wg       sync.WaitGroup
}

// NewDispatcher creates a new MV dispatcher.
func NewDispatcher(registry *ViewRegistry, layout *storage.Layout, logger *slog.Logger) *Dispatcher {
	return &Dispatcher{
		views:    make(map[string]*activeView),
		registry: registry,
		layout:   layout,
		logger:   logger,
		done:     make(chan struct{}),
	}
}

// Start loads all active views from the registry and starts background goroutines.
func (d *Dispatcher) Start(ctx context.Context) error {
	defs := d.registry.List()
	for _, def := range defs {
		if def.Status == ViewStatusDropping {
			continue
		}
		if err := d.activateView(def); err != nil {
			d.logger.Warn("views: failed to activate view", "name", def.Name, "err", err)
		}
	}

	d.wg.Add(2)
	go func() {
		defer d.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				d.logger.Error("panic in views batchFlushLoop",
					"panic", fmt.Sprintf("%v", r))
			}
		}()
		d.batchFlushLoop()
	}()
	go func() {
		defer d.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				d.logger.Error("panic in views mergeLoop",
					"panic", fmt.Sprintf("%v", r))
			}
		}()
		d.mergeLoop()
	}()

	return nil
}

// batchFlushLoop periodically flushes expired view batches.
func (d *Dispatcher) batchFlushLoop() {
	ticker := time.NewTicker(batchMaxDelay / 2) // check at half the max delay
	defer ticker.Stop()

	for {
		select {
		case <-d.done:
			return
		case <-ticker.C:
			d.mu.RLock()
			for _, av := range d.views {
				av.flushIfExpired()
			}
			d.mu.RUnlock()
		}
	}
}

// Stop flushes all pending batches and view memtables, and stops the background goroutines.
// Blocks until all goroutines have exited.
func (d *Dispatcher) Stop() error {
	select {
	case <-d.done:
		// Already closed.
	default:
		close(d.done)
	}

	// Wait for background goroutines to exit.
	d.wg.Wait()

	// Snapshot views under lock, then flush outside lock to avoid holding
	// d.mu.RLock while performing slow I/O operations.
	d.mu.RLock()
	views := make([]*activeView, 0, len(d.views))
	for _, av := range d.views {
		views = append(views, av)
	}
	d.mu.RUnlock()

	// Flush remaining pending batches without holding d.mu.
	for _, av := range views {
		av.mu.Lock()
		av.flushPendingLocked()
		av.mu.Unlock()
	}

	return d.FlushAll()
}

// ActivateView makes a view active for dispatching. Called when creating a new view.
func (d *Dispatcher) ActivateView(def ViewDefinition) error {
	return d.activateView(def)
}

// DeactivateView removes a view from active dispatching and marks it closed
// so in-flight Dispatch calls skip it.
func (d *Dispatcher) DeactivateView(name string) {
	d.mu.Lock()
	if av, ok := d.views[name]; ok {
		av.closed.Store(true)
	}
	delete(d.views, name)
	d.mu.Unlock()
}

func (d *Dispatcher) activateView(def ViewDefinition) error {
	filter, err := Compile(def.Filter)
	if err != nil {
		return fmt.Errorf("views: compile filter for %s: %w", def.Name, err)
	}

	av := &activeView{
		def:       def,
		filter:    filter,
		extractor: NewExtractor(def.Columns),
	}

	// Rebuild pipeline analysis from query string. This is idempotent — we
	// derive StreamingCmds, AggSpec, etc. from the persisted Query. On
	// server restart the analysis is re-derived; only AggSpec is persisted
	// (for state column deserialization).
	if def.Query != "" {
		analysis, err := AnalyzeQuery(def.Query)
		if err != nil {
			d.logger.Warn("views: query analysis failed for view",
				"name", def.Name, "err", err, "query", def.Query)
		} else {
			av.analysis = analysis
			// If the definition doesn't have AggSpec populated (e.g., older
			// views created before this code), populate it from analysis.
			if def.AggSpec == nil && analysis.AggSpec != nil {
				av.def.AggSpec = analysis.AggSpec
			}
		}
	}

	d.mu.Lock()
	d.views[def.Name] = av
	d.mu.Unlock()

	return nil
}

// viewBatch pairs an activeView with a slice of events matching that view's filter.
type viewBatch struct {
	av     *activeView
	events []*event.Event
}

// Dispatch evaluates each event against all active views and buffers matching
// events. Collects matching events under RLock, then processes pipelines
// (streaming commands, partial aggregation) outside the lock to avoid
// holding d.mu during expensive operations.
func (d *Dispatcher) Dispatch(events []*event.Event) error {
	if len(events) == 0 {
		return nil
	}

	// Collect matching events per view (under RLock).
	d.mu.RLock()
	var batches []viewBatch
	for _, av := range d.views {
		// Bug #4: skip paused views.
		if av.def.Status == ViewStatusPaused {
			continue
		}
		if av.closed.Load() {
			continue
		}

		var matching []*event.Event
		for _, e := range events {
			// Source index filter — skip events from wrong index.
			if av.def.SourceIndex != "" && e.Index != av.def.SourceIndex {
				continue
			}

			if av.filter.Match(e) {
				matching = append(matching, e)
			}
		}
		if len(matching) > 0 {
			batches = append(batches, viewBatch{av: av, events: matching})
		}
	}
	d.mu.RUnlock()

	// Process pipelines outside lock (expensive: rex, eval, partial agg).
	for _, vb := range batches {
		// Re-check closed — view may have been deactivated between collect and process.
		if vb.av.closed.Load() {
			continue
		}

		if vb.av.analysis != nil && vb.av.analysis.IsAggregation && vb.av.analysis.AggSpec != nil {
			// Aggregation view: run streaming pipeline + compute partial agg + serialize.
			results, err := d.processInsertBatch(vb.av, vb.events)
			if err != nil {
				d.logger.Warn("views: insert pipeline failed",
					"view", vb.av.def.Name, "err", err)

				continue
			}
			vb.av.addPendingBatch(results)
		} else {
			// Projection view: simple filter + extract.
			out := make([]*event.Event, 0, len(vb.events))
			for _, e := range vb.events {
				extracted := vb.av.extractor.Extract(e)
				extracted.Index = vb.av.def.Name
				out = append(out, extracted)
			}
			vb.av.addPendingBatch(out)
		}
	}

	return nil
}

// processInsertBatch runs the insert-time pipeline for an aggregation view.
// It applies streaming commands, computes partial aggregates, and serializes state.
// Uses a bounded context (processInsertTimeout) to avoid blocking ingestion
// indefinitely if a rex regex backtracks or an eval expression hangs.
func (d *Dispatcher) processInsertBatch(av *activeView, events []*event.Event) ([]*event.Event, error) {
	var transformed []*event.Event

	if len(av.analysis.StreamingCmds) > 0 {
		// Run streaming commands through the pipeline engine with a bounded context.
		ctx, cancel := context.WithTimeout(context.Background(), processInsertTimeout)
		defer cancel()
		source := pipeline.NewScanIteratorWithBudget(events, pipeline.DefaultBatchSize, stats.NopAccount())
		iter, err := pipeline.BuildFromSource(ctx, source, av.analysis.StreamingCmds, pipeline.DefaultBatchSize)
		if err != nil {
			return nil, fmt.Errorf("views.processInsertBatch: build pipeline: %w", err)
		}
		rows, err := pipeline.CollectAll(ctx, iter)
		if err != nil {
			return nil, fmt.Errorf("views.processInsertBatch: collect: %w", err)
		}
		transformed = batchRowsToEvents(rows, av.def.Name)
	} else {
		transformed = events
	}

	if len(transformed) == 0 {
		return nil, nil
	}

	// Compute partial aggregates with correct intermediate state.
	partialGroups := pipeline.ComputePartialAgg(transformed, av.analysis.AggSpec)

	// Serialize partial state to events for storage.
	return PartialGroupsToEvents(partialGroups, av.analysis.AggSpec, av.def.Name), nil
}

// FlushView flushes a single view's memtable to a segment on disk.
// Bug #6 fix: atomic swap of memtable under lock, then write outside lock.
func (d *Dispatcher) FlushView(name string) error {
	d.mu.RLock()
	av, ok := d.views[name]
	d.mu.RUnlock()
	if !ok {
		return ErrViewNotFound
	}

	// Atomic swap: flush pending into events, snapshot and clear — all under
	// a single lock acquisition.
	av.mu.Lock()
	av.flushPendingLocked()
	events := av.sortedEvents()
	if len(events) == 0 {
		av.mu.Unlock()

		return nil
	}
	av.events = nil
	av.mu.Unlock()

	// Write segment outside lock — no concurrent access to oldMemtable.
	if d.layout != nil {
		if err := d.layout.EnsureViewDirs(name); err != nil {
			return fmt.Errorf("views: ensure dirs: %w", err)
		}
	}

	// Sort by sort key if defined.
	sortEventsBySortKey(events, av.def.SortKey)

	segPath := d.viewSegmentPath(name)
	f, err := os.Create(segPath)
	if err != nil {
		// Events lost from view on disk error — recoverable via re-backfill.
		d.logger.Error("views: segment create failed, events lost from view",
			"view", name, "events", len(events), "err", err)

		return fmt.Errorf("views: create segment: %w", err)
	}

	sw := segment.NewWriter(f)
	if len(av.def.SortKey) > 0 {
		sw.SetSortKey(av.def.SortKey)
	}
	if _, err := sw.Write(events); err != nil {
		f.Close()
		os.Remove(segPath)
		d.logger.Error("views: segment write failed, events lost from view",
			"view", name, "events", len(events), "err", err)

		return fmt.Errorf("views: write segment: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(segPath)

		return fmt.Errorf("views: sync segment: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("views: close segment: %w", err)
	}

	d.logger.Info("views: flushed view", "name", name, "events", len(events), "path", segPath)

	return nil
}

// FlushAll flushes all active views.
func (d *Dispatcher) FlushAll() error {
	d.mu.RLock()
	names := make([]string, 0, len(d.views))
	for name := range d.views {
		names = append(names, name)
	}
	d.mu.RUnlock()

	for _, name := range names {
		if err := d.FlushView(name); err != nil {
			return err
		}
	}

	return nil
}

// ViewBufferedEvents returns the current buffered (unflushed) events for a view,
// including any pending batched events.
func (d *Dispatcher) ViewBufferedEvents(name string) []*event.Event {
	d.mu.RLock()
	av, ok := d.views[name]
	d.mu.RUnlock()
	if !ok {
		return nil
	}

	// Flush pending before reading.
	av.mu.Lock()
	av.flushPendingLocked()
	result := av.sortedEvents()
	av.mu.Unlock()

	return result
}

// InjectBackfillEvents writes pre-processed events directly into a view's memtable,
// bypassing Filter.Match() and Extractor.Extract() (the query engine already produced
// the correct output). After insertion, the view is flushed to a persistent segment.
//
// This is used by query-based backfill: the MV's SPL2 query is executed through the
// normal query engine, and the result rows (already filtered, aggregated, etc.) are
// injected directly into the view's storage.
func (d *Dispatcher) InjectBackfillEvents(name string, events []*event.Event) error {
	if len(events) == 0 {
		return nil
	}

	d.mu.RLock()
	av, ok := d.views[name]
	d.mu.RUnlock()
	if !ok {
		return ErrViewNotFound
	}

	// Set index on each event so segment writer stores them under the view name.
	for _, e := range events {
		e.Index = name
	}

	// Insert directly into the events buffer, bypassing filter/extract.
	av.mu.Lock()
	av.events = append(av.events, events...)
	av.mu.Unlock()

	// Flush to a persistent segment immediately so backfill data survives restarts.
	if err := d.FlushView(name); err != nil {
		return fmt.Errorf("views: flush backfill events: %w", err)
	}

	d.logger.Info("views: injected backfill events", "name", name, "events", len(events))

	return nil
}

// ViewAllEvents returns all events for a view: flushed segments + memtable.
// For aggregation views, deserializes partial state, merges, and finalizes
// into result events with clean column names. For projection views, returns
// raw events in time order.
func (d *Dispatcher) ViewAllEvents(name string) ([]*event.Event, error) {
	d.mu.RLock()
	av, ok := d.views[name]
	d.mu.RUnlock()
	if !ok {
		return nil, ErrViewNotFound
	}

	// Flush any pending batched events to the memtable first.
	av.mu.Lock()
	av.flushPendingLocked()
	av.mu.Unlock()

	var all []*event.Event

	// Read flushed segment files.
	if d.layout != nil {
		segDir := d.layout.ViewSegmentDir(name)
		segEvents, err := d.readViewSegments(segDir)
		if err != nil {
			d.logger.Warn("views: read segments", "name", name, "err", err)
			// Non-fatal: continue with memtable events.
		} else {
			all = append(all, segEvents...)
		}
	}

	// Append current unflushed events.
	all = append(all, av.sortedEvents()...)

	// For aggregation views: deserialize → merge → finalize → events.
	spec := av.def.AggSpec
	if av.analysis != nil && av.analysis.IsAggregation && spec != nil {
		groups := EventsToPartialGroups(all, spec)
		finalRows := pipeline.MergePartialAggs([][]*pipeline.PartialAggGroup{groups}, spec)

		return rowsToEvents(finalRows, name), nil
	}

	return all, nil
}

// readViewSegments reads all .lsg segment files from a directory.
func (d *Dispatcher) readViewSegments(dir string) ([]*event.Event, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("views: read segment dir: %w", err)
	}

	var all []*event.Event
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".lsg" {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		ms, err := segment.OpenSegmentFile(path)
		if err != nil {
			d.logger.Warn("views: open segment", "path", path, "err", err)

			continue
		}
		reader := ms.Reader()
		events, err := reader.ReadEvents()
		ms.Close()
		if err != nil {
			d.logger.Warn("views: read events", "path", path, "err", err)

			continue
		}
		all = append(all, events...)
	}

	return all, nil
}

// mergeLoop periodically triggers background flush, merge, and retention
// enforcement for all active views.
func (d *Dispatcher) mergeLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-d.done:
			return
		case <-ticker.C:
			// Bug #3 fix: flush in-memory events to disk segments first.
			if err := d.FlushAll(); err != nil {
				d.logger.Warn("views: periodic flush failed", "err", err)
			}

			// Collect view definitions under RLock, then release before expensive ops.
			d.mu.RLock()
			defs := make([]ViewDefinition, 0, len(d.views))
			for _, av := range d.views {
				defs = append(defs, av.def)
			}
			layout := d.layout
			d.mu.RUnlock()

			if layout != nil {
				for _, def := range defs {
					if err := MergeView(def, layout, d.logger); err != nil {
						d.logger.Warn("views: merge failed", "view", def.Name, "err", err)
					}
					if err := EnforceRetention(def, layout, d.logger); err != nil {
						d.logger.Warn("views: retention failed", "view", def.Name, "err", err)
					}
				}
			}
		}
	}
}

// UpdateView applies a mutation to a view definition. If the mutation requires a
// rebuild (filter/GROUP BY/column change), it initiates versioned rebuild. Otherwise
// it applies the safe mutation in place.
func (d *Dispatcher) UpdateView(newDef ViewDefinition) error {
	old, err := d.registry.Get(newDef.Name)
	if err != nil {
		return err
	}

	if RebuildRequired(old, newDef) {
		rebuilt, err := StartRebuild(d.registry, newDef)
		if err != nil {
			return err
		}
		// Re-activate the view with new definition (dispatcher will serve dual-write).
		d.DeactivateView(old.Name)

		return d.activateView(rebuilt)
	}

	// Safe mutation: update retention/aggs in-place.
	updated := SafeUpdate(old, newDef.Retention, nil)
	if err := d.registry.Update(updated); err != nil {
		return err
	}

	// Refresh the active view.
	d.mu.Lock()
	if av, ok := d.views[updated.Name]; ok {
		av.def = updated
	}
	d.mu.Unlock()

	return nil
}

// Partial Aggregation Serialization

// PartialGroupsToEvents converts partial aggregation groups into events for storage.
// Each group becomes one event with group-by key fields and intermediate state
// stored in _pa_ prefixed columns (e.g., _pa_count_count, _pa_avg_sum).
func PartialGroupsToEvents(groups []*pipeline.PartialAggGroup, spec *pipeline.PartialAggSpec, viewName string) []*event.Event {
	events := make([]*event.Event, 0, len(groups))
	for _, g := range groups {
		ev := &event.Event{
			Index:  viewName,
			Time:   time.Now(),
			Fields: make(map[string]event.Value, len(spec.GroupBy)+len(spec.Funcs)*2),
		}
		// Store group-by keys — populate both Fields map and struct fields
		// so that segment roundtrip (which maps "host" → ev.Host) is correct.
		for k, v := range g.Key {
			ev.Fields[k] = v
			mapBuiltinField(ev, k, v)
		}
		// Store partial state per function.
		for j, fn := range spec.Funcs {
			serializePartialState(ev, fn, &g.States[j])
		}
		events = append(events, ev)
	}

	return events
}

// serializePartialState writes one function's intermediate state into event fields.
func serializePartialState(ev *event.Event, fn pipeline.PartialAggFunc, s *pipeline.PartialAggState) {
	prefix := "_pa_" + fn.Alias + "_"
	switch strings.ToLower(fn.Name) {
	case aggFnCount:
		ev.Fields[prefix+"count"] = event.IntValue(s.Count)
	case aggFnSum:
		ev.Fields[prefix+"sum"] = event.FloatValue(s.Sum)
		ev.Fields[prefix+"count"] = event.IntValue(s.Count)
	case aggFnAvg:
		ev.Fields[prefix+"sum"] = event.FloatValue(s.Sum)
		ev.Fields[prefix+"count"] = event.IntValue(s.Count)
	case aggFnMin:
		ev.Fields[prefix+"min"] = s.Min
	case aggFnMax:
		ev.Fields[prefix+"max"] = s.Max
	case aggFnDC:
		// JSON array — safe for values containing commas, quotes, etc.
		vals := make([]string, 0, len(s.DistinctSet))
		for k := range s.DistinctSet {
			vals = append(vals, k)
		}
		b, _ := json.Marshal(vals)
		ev.Fields[prefix+"dc_set"] = event.StringValue(string(b))
		// Fallback count for the backfill path where DistinctSet may be
		// lost (finalizedResultsToPartialGroups sets Count but not the set).
		// On deserialization we prefer len(DistinctSet) when non-empty,
		// falling back to dc_count otherwise.
		ev.Fields[prefix+"dc_count"] = event.IntValue(s.Count)
	}
}

// EventsToPartialGroups deserializes events with _pa_ prefixed columns back
// into PartialAggGroup slices for merge/finalize.
func EventsToPartialGroups(events []*event.Event, spec *pipeline.PartialAggSpec) []*pipeline.PartialAggGroup {
	var groups []*pipeline.PartialAggGroup
	for _, ev := range events {
		g := &pipeline.PartialAggGroup{
			Key:    make(map[string]event.Value, len(spec.GroupBy)),
			States: make([]pipeline.PartialAggState, len(spec.Funcs)),
		}
		// Extract group-by keys using GetField, which checks both struct
		// fields (Host, Source, etc.) and the Fields map. After a segment
		// roundtrip, "host" lives in ev.Host, not ev.Fields["host"].
		for _, field := range spec.GroupBy {
			v := ev.GetField(field)
			g.Key[field] = v
		}
		// Deserialize partial state per function.
		for j, fn := range spec.Funcs {
			g.States[j] = deserializePartialState(ev, fn)
		}
		groups = append(groups, g)
	}

	return groups
}

// deserializePartialState reads one function's intermediate state from event fields.
func deserializePartialState(ev *event.Event, fn pipeline.PartialAggFunc) pipeline.PartialAggState {
	prefix := "_pa_" + fn.Alias + "_"
	s := pipeline.PartialAggState{
		Min: event.NullValue(),
		Max: event.NullValue(),
	}

	switch strings.ToLower(fn.Name) {
	case aggFnCount:
		if v, ok := ev.Fields[prefix+"count"]; ok {
			s.Count = toInt64(v)
		}
	case aggFnSum:
		if v, ok := ev.Fields[prefix+"sum"]; ok {
			s.Sum = toFloat64(v)
		}
		if v, ok := ev.Fields[prefix+"count"]; ok {
			s.Count = toInt64(v)
		}
	case aggFnAvg:
		if v, ok := ev.Fields[prefix+"sum"]; ok {
			s.Sum = toFloat64(v)
		}
		if v, ok := ev.Fields[prefix+"count"]; ok {
			s.Count = toInt64(v)
		}
	case aggFnMin:
		if v, ok := ev.Fields[prefix+"min"]; ok {
			s.Min = v
		}
	case aggFnMax:
		if v, ok := ev.Fields[prefix+"max"]; ok {
			s.Max = v
		}
	case aggFnDC:
		if v, ok := ev.Fields[prefix+"dc_set"]; ok && !v.IsNull() {
			var vals []string
			if err := json.Unmarshal([]byte(v.AsString()), &vals); err == nil {
				s.DistinctSet = make(map[string]bool, len(vals))
				for _, val := range vals {
					s.DistinctSet[val] = true
				}
			}
		}
		// Fallback: if DistinctSet is empty but dc_count was serialized
		// (backfill path where exact set is lost), use that as the count.
		if v, ok := ev.Fields[prefix+"dc_count"]; ok {
			s.Count = toInt64(v)
		}
	}

	return s
}

// toInt64 extracts an int64 from an event.Value, handling type coercion.
func toInt64(v event.Value) int64 {
	switch v.Type() {
	case event.FieldTypeInt:
		return v.AsInt()
	case event.FieldTypeFloat:
		return int64(v.AsFloat())
	default:
		return 0
	}
}

// toFloat64 extracts a float64 from an event.Value, handling type coercion.
func toFloat64(v event.Value) float64 {
	switch v.Type() {
	case event.FieldTypeFloat:
		return v.AsFloat()
	case event.FieldTypeInt:
		return float64(v.AsInt())
	default:
		return 0
	}
}

// batchRowsToEvents converts pipeline result rows into events.
// Used after running streaming commands through the pipeline engine.
// Maps well-known field names to Event struct fields so GetField works correctly.
func batchRowsToEvents(rows []map[string]event.Value, viewName string) []*event.Event {
	events := make([]*event.Event, 0, len(rows))
	for _, row := range rows {
		ev := &event.Event{
			Index:  viewName,
			Time:   time.Now(),
			Fields: make(map[string]event.Value, len(row)),
		}
		for k, v := range row {
			ev.Fields[k] = v
			mapBuiltinField(ev, k, v)
		}
		events = append(events, ev)
	}

	return events
}

// rowsToEvents converts finalized aggregation rows into events for pipeline consumption.
// Used by ViewAllEvents after merging and finalizing partial aggregation state.
// Maps well-known field names to Event struct fields so GetField works correctly.
func rowsToEvents(rows []map[string]event.Value, viewName string) []*event.Event {
	events := make([]*event.Event, 0, len(rows))
	for _, row := range rows {
		ev := &event.Event{
			Index:  viewName,
			Time:   time.Now(),
			Fields: make(map[string]event.Value, len(row)),
		}
		for k, v := range row {
			ev.Fields[k] = v
			mapBuiltinField(ev, k, v)
		}
		// Build JSON _raw so full-text search works on finalized view data.
		if ev.Raw == "" {
			rawParts := make(map[string]string, len(row))
			for k, v := range row {
				rawParts[k] = v.String()
			}
			if b, err := json.Marshal(rawParts); err == nil {
				ev.Raw = string(b)
			}
		}
		events = append(events, ev)
	}

	return events
}

// mapBuiltinField maps a field name/value pair to the corresponding Event struct
// field. GetField() checks struct fields first (not Fields map) for built-in names
// like "host", "_source", "_time", so we must populate both.
func mapBuiltinField(ev *event.Event, k string, v event.Value) {
	switch k {
	case "_time":
		if v.Type() == event.FieldTypeTimestamp {
			ev.Time = v.AsTimestamp()
		}
	case "_raw":
		ev.Raw = v.String()
	case "host":
		ev.Host = v.String()
	case "_source", "source":
		if ev.Source == "" {
			ev.Source = v.String()
		}
	case "_sourcetype", "sourcetype":
		if ev.SourceType == "" {
			ev.SourceType = v.String()
		}
	}
}

// Helpers

// sortEventsBySortKey sorts events by the view's sort key fields.
// If no sort key is defined, events remain in insertion order.
func sortEventsBySortKey(events []*event.Event, sortKey []string) {
	if len(sortKey) == 0 {
		return
	}
	sort.SliceStable(events, func(i, j int) bool {
		for _, field := range sortKey {
			vi := events[i].GetField(field).String()
			vj := events[j].GetField(field).String()
			if vi != vj {
				return vi < vj
			}
		}

		return false
	})
}

func (d *Dispatcher) viewSegmentPath(name string) string {
	ts := time.Now()
	segName := fmt.Sprintf("seg-%s-L0-%d.lsg", name, ts.UnixNano())
	if d.layout != nil {
		return fmt.Sprintf("%s/%s", d.layout.ViewSegmentDir(name), segName)
	}

	return segName
}
