package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/buffer"
	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
	"github.com/OrlovEvgeny/Lynxdb/pkg/stats"
	"github.com/OrlovEvgeny/Lynxdb/pkg/vm"
)

// IndexStore provides events for a given index.
type IndexStore interface {
	GetEvents(index string) []*event.Event
}

// ServerIndexStore adapts a pre-built map of events to the IndexStore interface.
// Used by the server to feed pipeline execution with already-filtered events.
type ServerIndexStore struct {
	Events map[string][]*event.Event
}

// GetEvents returns events for the given index.
func (s *ServerIndexStore) GetEvents(index string) []*event.Event {
	return s.Events[index]
}

// StreamingIndexStore provides a streaming iterator for a given index.
// When the pipeline's store implements this interface, the pipeline uses
// the streaming scan path instead of materializing all events via GetEvents().
// The streaming path reads segment row groups on-demand, bounding memory to
// one row group (~65K events) plus pipeline working set.
type StreamingIndexStore interface {
	GetEventIterator(index string) Iterator
}

// BatchStore provides pre-built columnar batches for a given index.
// When the pipeline's IndexStore also implements BatchStore, the columnar
// read path is used (ColumnarScanIterator) instead of the row-oriented path
// (ScanIterator + BatchFromEvents), eliminating the row-to-columnar conversion.
type BatchStore interface {
	GetBatches(index string) []*Batch
}

// ColumnarBatchStore implements both IndexStore and BatchStore.
// Segment data is stored as pre-built columnar batches (from ReadColumnar ->
// SplitColumnarBatches). Memtable data is converted to batches via BatchFromEvents.
// Used by the server's direct columnar read path.
type ColumnarBatchStore struct {
	Batches map[string][]*Batch
}

// GetEvents returns nil — the columnar path does not use event-based access.
// Satisfies the IndexStore interface for backward compatibility.
func (s *ColumnarBatchStore) GetEvents(_ string) []*event.Event {
	return nil
}

// GetBatches returns pre-built columnar batches for the given index.
func (s *ColumnarBatchStore) GetBatches(index string) []*Batch {
	return s.Batches[index]
}

// queryContext holds shared state for building a pipeline from a Program.
// It threads CTE datasets through subquery handling (Join, Append, Multisearch).
type queryContext struct {
	ctx                 context.Context
	store               IndexStore
	datasets            map[string][]map[string]event.Value
	batchSize           int
	viewResolver        ViewResolver
	viewManager         ViewManager
	systemTableResolver SystemTableResolver
	progCache           *vm.ProgramCache
	joinStrategy        string
	appendOrdering      string // "interleaved" when optimizer determines downstream is order-insensitive
	instrument          bool
	profileLevel        string
	monitor             *stats.BudgetMonitor
	spillMgr            *SpillManager      // lifecycle manager for spill files (nil = no spill support)
	dedupExact          bool               // use exact string keys in dedup instead of xxhash64
	bufferPool          *buffer.Pool       // unified buffer pool (nil = use BoundAccount from monitor)
	queryID             string             // unique query ID for buffer pool pin tracking
	parallelCfg         ParallelConfig     // branch-level parallelism configuration
	coordinator         *MemoryCoordinator // nil when no coordination needed
}

// newAccount creates a MemoryAccount for an operator. When the unified buffer
// pool is enabled (bufferPool != nil), accounts are backed by pool pages which
// coordinate memory across cache, queries, and memtable. Otherwise, accounts
// are backed by the per-query BudgetMonitor.
func (qc *queryContext) newAccount(label string) stats.MemoryAccount {
	if qc.bufferPool != nil {
		acct := buffer.NewPoolAccount(qc.bufferPool, qc.queryID, qc.monitor)
		if acct != nil {
			return acct
		}
	}
	// Fallback: BoundAccount from monitor. A nil monitor returns nil
	// *BoundAccount, which becomes a typed-nil MemoryAccount that
	// dispatches to nil-safe no-op methods.
	return qc.monitor.NewAccount(label)
}

// newCoordinatedAccount creates a MemoryAccount for a spillable operator.
// When a coordinator is active, the returned account has a coordinator-managed
// sub-limit that enables budget redistribution after spill. When no coordinator
// is active (single spillable operator, no budget, no spill support), the
// returned account is a plain account — zero overhead.
func (qc *queryContext) newCoordinatedAccount(label string, reservation int64) stats.MemoryAccount {
	inner := qc.newAccount(label)
	if qc.coordinator != nil {
		return qc.coordinator.RegisterOperator(label, inner, reservation)
	}

	return inner
}

// BuildProgram converts a full SPL2 Program (with optional CTEs) into a pipeline.
// CTEs are materialized in order and available to the main query and subqueries.
func BuildProgram(ctx context.Context, prog *spl2.Program, store IndexStore, batchSize int) (Iterator, error) {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	qc := &queryContext{
		ctx:       ctx,
		store:     store,
		datasets:  make(map[string][]map[string]event.Value),
		batchSize: batchSize,
		progCache: vm.NewProgramCache(),
	}

	// Materialize CTEs using DAG-based execution plan. Independent CTEs
	// at the same dependency level are materialized in parallel when enabled.
	if err := qc.materializeCTEs(ctx, prog.Datasets); err != nil {
		return nil, err
	}

	// Build main query.
	return qc.buildQuery(ctx, prog.Main)
}

// BuildResult wraps the pipeline iterator and optional MemoryCoordinator returned
// by the budget-aware build functions. The coordinator is exposed so callers can
// extract per-operator budget statistics via CollectOperatorBudgets for --analyze.
type BuildResult struct {
	Iterator    Iterator
	Coordinator *MemoryCoordinator
}

// Option configures optional components of the pipeline build.
type Option func(*queryContext)

// WithSystemTables attaches a SystemTableResolver for FROM system.* queries.
func WithSystemTables(resolver SystemTableResolver) Option {
	return func(qc *queryContext) {
		qc.systemTableResolver = resolver
	}
}

// BuildProgramWithBudget is like BuildProgramWithViewsStatsAndProfile but also
// attaches a BudgetMonitor for per-query memory tracking and enforcement.
// Operators that buffer data (aggregate, sort, join, etc.) will call
// monitor.NewAccount to track their allocations against the shared budget.
// The spillMgr parameter enables disk spill for sort and aggregate operators;
// pass nil to disable spill support (operators will fail on budget exceeded).
// When dedupExact is true, the dedup operator uses exact string keys instead
// of xxhash64 hashing, eliminating collision risk at the cost of higher memory.
// The parallelCfg parameter controls branch-level parallelism for APPEND,
// MULTISEARCH, and multi-source FROM; pass nil to use sequential execution.
func BuildProgramWithBudget(ctx context.Context, prog *spl2.Program, store IndexStore, resolver ViewResolver, mgr ViewManager, batchSize int, profileLevel string, monitor *stats.BudgetMonitor, spillMgr *SpillManager, dedupExact bool, parallelCfg *ParallelConfig, opts ...Option) (*BuildResult, error) {
	return buildProgramWithViewsAndBudget(ctx, prog, store, resolver, mgr, batchSize, true, profileLevel, monitor, spillMgr, dedupExact, nil, "", parallelCfg, opts...)
}

// BuildProgramWithBufferPool is like BuildProgramWithBudget but uses the unified
// buffer pool for operator memory accounting instead of the per-query BudgetMonitor.
// When pool is non-nil, operators allocate memory credits as buffer pool pages,
// enabling automatic cross-consumer rebalancing (cache ↔ queries ↔ memtable).
// The monitor is still used for high-water mark tracking and PeakMemoryBytes reporting.
func BuildProgramWithBufferPool(ctx context.Context, prog *spl2.Program, store IndexStore, resolver ViewResolver, mgr ViewManager, batchSize int, profileLevel string, monitor *stats.BudgetMonitor, spillMgr *SpillManager, dedupExact bool, pool *buffer.Pool, queryID string, parallelCfg *ParallelConfig, opts ...Option) (*BuildResult, error) {
	return buildProgramWithViewsAndBudget(ctx, prog, store, resolver, mgr, batchSize, true, profileLevel, monitor, spillMgr, dedupExact, pool, queryID, parallelCfg, opts...)
}

func buildProgramWithViewsAndBudget(ctx context.Context, prog *spl2.Program, store IndexStore, resolver ViewResolver, mgr ViewManager, batchSize int, instrument bool, profileLevel string, monitor *stats.BudgetMonitor, spillMgr *SpillManager, dedupExact bool, pool *buffer.Pool, queryID string, parallelCfg *ParallelConfig, opts ...Option) (*BuildResult, error) {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	qc := &queryContext{
		ctx:          ctx,
		store:        store,
		datasets:     make(map[string][]map[string]event.Value),
		batchSize:    batchSize,
		viewResolver: resolver,
		viewManager:  mgr,
		progCache:    vm.NewProgramCache(),
		instrument:   instrument,
		profileLevel: profileLevel,
		monitor:      monitor,
		spillMgr:     spillMgr,
		dedupExact:   dedupExact,
		bufferPool:   pool,
		queryID:      queryID,
	}
	if parallelCfg != nil {
		qc.parallelCfg = *parallelCfg
	}

	for _, opt := range opts {
		opt(qc)
	}

	// Create coordinator when 2+ spillable operators share a budget.
	// The coordinator partitions the per-query budget among spillable operators
	// with dynamic redistribution after spill.
	budgetLimit := int64(0)
	if monitor != nil {
		budgetLimit = monitor.Limit()
	}
	if countSpillableOps(prog) >= 2 && budgetLimit > 0 && spillMgr != nil {
		qc.coordinator = NewMemoryCoordinator(budgetLimit, 0.10)
	}

	// Extract optimizer annotations.
	if prog.Main != nil && prog.Main.Annotations != nil {
		if ann, ok := prog.Main.Annotations["joinStrategy"]; ok {
			if ja, ok := ann.(interface{ GetStrategy() string }); ok {
				qc.joinStrategy = ja.GetStrategy()
			}
		}
		if ann, ok := prog.Main.Annotations["appendOrdering"]; ok {
			if s, ok := ann.(string); ok {
				qc.appendOrdering = s
			}
		}
	}

	// Materialize CTEs using DAG-based execution plan. Independent CTEs
	// at the same dependency level are materialized in parallel when enabled.
	if err := qc.materializeCTEs(ctx, prog.Datasets); err != nil {
		return nil, err
	}

	iter, err := qc.buildQuery(ctx, prog.Main)
	if err != nil {
		return nil, err
	}

	// Finalize coordinator sub-limits after all operators have registered.
	if qc.coordinator != nil {
		qc.coordinator.Finalize()
	}

	return &BuildResult{Iterator: iter, Coordinator: qc.coordinator}, nil
}

// BuildProgramWithStats is like BuildProgram but wraps each pipeline operator
// with InstrumentedIterator to collect per-stage row counts and timing.
// After CollectAll completes, call CollectStageStats on the returned iterator
// to extract the pipeline breakdown.
func BuildProgramWithStats(ctx context.Context, prog *spl2.Program, store IndexStore, batchSize int) (Iterator, error) {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	qc := &queryContext{
		ctx:        ctx,
		store:      store,
		datasets:   make(map[string][]map[string]event.Value),
		batchSize:  batchSize,
		progCache:  vm.NewProgramCache(),
		instrument: true,
	}

	// Materialize CTEs using DAG-based execution plan.
	if err := qc.materializeCTEs(ctx, prog.Datasets); err != nil {
		return nil, err
	}

	return qc.buildQuery(ctx, prog.Main)
}

// buildQuery builds a pipeline for a single Query, resolving variable sources from CTEs.
func (qc *queryContext) buildQuery(ctx context.Context, query *spl2.Query) (Iterator, error) {
	var iter Iterator

	if query.Source != nil {
		if query.Source.IsVariable {
			rows, ok := qc.datasets[query.Source.Index]
			if !ok {
				return nil, fmt.Errorf("unknown dataset $%s", query.Source.Index)
			}
			iter = NewRowScanIterator(rows, qc.batchSize)
		} else {
			iter = qc.buildSourceIterator(query.Source)
		}
	} else {
		iter = NewScanIteratorWithBudget(nil, qc.batchSize, qc.newAccount("scan"))
	}

	if qc.instrument {
		iter = WrapInstrumented(iter, "Scan")
	}

	for _, cmd := range query.Commands {
		var err error
		iter, err = qc.buildCommand(iter, cmd)
		if err != nil {
			return nil, fmt.Errorf("build pipeline: %w", err)
		}

		if qc.instrument {
			iter = WrapInstrumented(iter, commandStageName(cmd))
		}
	}

	return iter, nil
}

// buildSourceIterator creates a scan iterator for a SourceClause.
// It handles single sources, multi-source lists (FROM a, b, c), and globs
// (FROM logs*). For streaming/batch stores (server mode), the server layer
// resolves source scope before calling the pipeline — the pipeline just sees
// the primary index name. For IndexStore (CLI mode), multi-source lists are
// resolved by merging events from multiple GetEvents calls.
func (qc *queryContext) buildSourceIterator(source *spl2.SourceClause) Iterator {
	// Multi-source list: FROM a, b, c — merge events from all sources.
	// Only applies to the basic IndexStore path (CLI mode). Streaming/batch
	// stores handle multi-source at the server layer via source scope.
	if len(source.Indices) > 0 {
		if st, isStreaming := qc.store.(StreamingIndexStore); isStreaming {
			// Server mode: create one iterator per source and union them.
			// For single-source, return directly (no union overhead).
			if len(source.Indices) == 1 {
				return st.GetEventIterator(source.Indices[0])
			}
			iters := make([]Iterator, 0, len(source.Indices))
			for _, idx := range source.Indices {
				iters = append(iters, st.GetEventIterator(idx))
			}
			if qc.parallelCfg.Enabled && len(iters) > 1 {
				// Parallel multi-source FROM: each index reads non-overlapping
				// segments, so interleaved fan-in gives maximum I/O throughput.
				return NewConcurrentUnionIterator(
					iters, OrderInterleaved, &qc.parallelCfg,
				)
			}

			return NewUnionIterator(iters)
		}
		if bs, isBatch := qc.store.(BatchStore); isBatch {
			var allBatches []*Batch
			for _, idx := range source.Indices {
				allBatches = append(allBatches, bs.GetBatches(idx)...)
			}

			return NewColumnarScanIteratorWithBudget(allBatches, qc.newAccount("scan"))
		}
		// IndexStore fallback: merge events from all source names.
		var allEvents []*event.Event
		if qc.store != nil {
			for _, idx := range source.Indices {
				allEvents = append(allEvents, qc.store.GetEvents(idx)...)
			}
		}

		return NewScanIteratorWithBudget(allEvents, qc.batchSize, qc.newAccount("scan"))
	}

	// Glob pattern (FROM logs*, FROM *): server resolves to concrete source list
	// via SourceRegistry before pipeline construction. At pipeline level, we just
	// use the primary index name which the server has already set up.
	// For IndexStore (CLI), FROM * scans the default index ("main").

	// Route through the best available store interface, in priority order:
	// StreamingIndexStore (lazy row-group streaming) > BatchStore (columnar
	// batches) > IndexStore.GetEvents (full materialization, fallback).
	switch st := qc.store.(type) {
	case StreamingIndexStore:
		return st.GetEventIterator(source.Index)
	case BatchStore:
		batches := st.GetBatches(source.Index)

		return NewColumnarScanIteratorWithBudget(batches, qc.newAccount("scan"))
	default:
		var events []*event.Event
		if qc.store != nil {
			events = qc.store.GetEvents(source.Index)
		}

		return NewScanIteratorWithBudget(events, qc.batchSize, qc.newAccount("scan"))
	}
}

// commandStageName returns a human-readable stage name for a command type.
func commandStageName(cmd spl2.Command) string {
	switch cmd.(type) {
	case *spl2.SearchCommand:
		return "Search"
	case *spl2.WhereCommand:
		return "Filter"
	case *spl2.StatsCommand:
		return "Aggregate"
	case *spl2.EvalCommand:
		return "Eval"
	case *spl2.HeadCommand:
		return "Head"
	case *spl2.TailCommand:
		return "Tail"
	case *spl2.FieldsCommand:
		return "Fields"
	case *spl2.TableCommand:
		return "Table"
	case *spl2.RenameCommand:
		return "Rename"
	case *spl2.SortCommand:
		return "Sort"
	case *spl2.TopNCommand:
		return "TopN"
	case *spl2.DedupCommand:
		return "Dedup"
	case *spl2.RexCommand:
		return "Rex"
	case *spl2.BinCommand:
		return "Bin"
	case *spl2.StreamstatsCommand:
		return "StreamStats"
	case *spl2.EventstatsCommand:
		return "EventStats"
	case *spl2.JoinCommand:
		return "Join"
	case *spl2.AppendCommand:
		return "Append"
	case *spl2.MultisearchCommand:
		return "Multisearch"
	case *spl2.XYSeriesCommand:
		return "XYSeries"
	case *spl2.TransactionCommand:
		return "Transaction"
	case *spl2.TopCommand:
		return "Top"
	case *spl2.RareCommand:
		return "Rare"
	case *spl2.FillnullCommand:
		return "Fillnull"
	case *spl2.TimechartCommand:
		return "Timechart"
	case *spl2.FromCommand:
		return "From"
	case *spl2.MaterializeCommand:
		return "Materialize"
	case *spl2.ViewsCommand:
		return "Views"
	case *spl2.DropviewCommand:
		return "Dropview"
	default:
		return "Unknown"
	}
}

// BuildProgramWithViews is like BuildProgram but includes ViewResolver and ViewManager.
func BuildProgramWithViews(ctx context.Context, prog *spl2.Program, store IndexStore, resolver ViewResolver, mgr ViewManager, batchSize int) (Iterator, error) {
	return buildProgramWithViews(ctx, prog, store, resolver, mgr, batchSize, false)
}

// BuildProgramWithViewsAndStats is like BuildProgramWithViews but wraps each
// pipeline operator with InstrumentedIterator to collect per-stage row counts
// and timing. After CollectAll completes, call CollectStageStats on the returned
// iterator to extract the pipeline breakdown and MatchedRows.
func BuildProgramWithViewsAndStats(ctx context.Context, prog *spl2.Program, store IndexStore, resolver ViewResolver, mgr ViewManager, batchSize int) (Iterator, error) {
	return buildProgramWithViews(ctx, prog, store, resolver, mgr, batchSize, true)
}

// BuildProgramWithViewsStatsAndProfile is like BuildProgramWithViewsAndStats but
// also sets the profile level on the query context, enabling trace-level VM
// profiling on FilterIterator and EvalIterator when profileLevel == "trace".
func BuildProgramWithViewsStatsAndProfile(ctx context.Context, prog *spl2.Program, store IndexStore, resolver ViewResolver, mgr ViewManager, batchSize int, profileLevel string) (Iterator, error) {
	return buildProgramWithViewsOpts(ctx, prog, store, resolver, mgr, batchSize, true, profileLevel)
}

func buildProgramWithViews(ctx context.Context, prog *spl2.Program, store IndexStore, resolver ViewResolver, mgr ViewManager, batchSize int, instrument bool) (Iterator, error) {
	return buildProgramWithViewsOpts(ctx, prog, store, resolver, mgr, batchSize, instrument, "")
}

func buildProgramWithViewsOpts(ctx context.Context, prog *spl2.Program, store IndexStore, resolver ViewResolver, mgr ViewManager, batchSize int, instrument bool, profileLevel string) (Iterator, error) {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	qc := &queryContext{
		ctx:          ctx,
		store:        store,
		datasets:     make(map[string][]map[string]event.Value),
		batchSize:    batchSize,
		viewResolver: resolver,
		viewManager:  mgr,
		progCache:    vm.NewProgramCache(),
		instrument:   instrument,
		profileLevel: profileLevel,
	}

	// Extract optimizer annotations.
	if prog.Main != nil && prog.Main.Annotations != nil {
		if ann, ok := prog.Main.Annotations["joinStrategy"]; ok {
			// The annotation is *optimizer.JoinAnnotation which has a Strategy field.
			// Use reflection-free approach via fmt.
			if ja, ok := ann.(interface{ GetStrategy() string }); ok {
				qc.joinStrategy = ja.GetStrategy()
			}
		}
		if ann, ok := prog.Main.Annotations["appendOrdering"]; ok {
			if s, ok := ann.(string); ok {
				qc.appendOrdering = s
			}
		}
	}

	// Materialize CTEs using DAG-based execution plan.
	if err := qc.materializeCTEs(ctx, prog.Datasets); err != nil {
		return nil, err
	}

	return qc.buildQuery(ctx, prog.Main)
}

// BuildFromSource builds a pipeline from the given source iterator and query
// commands. Used by tail to plug LiveScanIterator as the source instead of
// an event store.
func BuildFromSource(ctx context.Context, source Iterator, commands []spl2.Command, batchSize int) (Iterator, error) {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	qc := &queryContext{
		ctx:       ctx,
		datasets:  make(map[string][]map[string]event.Value),
		batchSize: batchSize,
		progCache: vm.NewProgramCache(),
	}
	iter := source
	for _, cmd := range commands {
		var err error
		iter, err = qc.buildCommand(iter, cmd)
		if err != nil {
			return nil, fmt.Errorf("build pipeline: %w", err)
		}
	}

	return iter, nil
}

// BuildPipeline converts an SPL2 Query into a chain of streaming iterators.
// For queries without CTEs. Use BuildProgram for full Program support.
func BuildPipeline(ctx context.Context, query *spl2.Query, store IndexStore, batchSize int) (Iterator, error) {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	qc := &queryContext{
		ctx:       ctx,
		store:     store,
		datasets:  make(map[string][]map[string]event.Value),
		batchSize: batchSize,
		progCache: vm.NewProgramCache(),
	}

	return qc.buildQuery(ctx, query)
}

// BuildPipelineWithStats is like BuildPipeline but wraps each operator with
// InstrumentedIterator to collect per-stage row counts and timing. Use this
// for sub-pipelines (e.g., partial agg post-processing) where pipeline stage
// stats are needed.
func BuildPipelineWithStats(ctx context.Context, query *spl2.Query, store IndexStore, batchSize int) (Iterator, error) {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	qc := &queryContext{
		ctx:        ctx,
		store:      store,
		datasets:   make(map[string][]map[string]event.Value),
		batchSize:  batchSize,
		progCache:  vm.NewProgramCache(),
		instrument: true,
	}

	return qc.buildQuery(ctx, query)
}

// BuildPipelineWithViews is like BuildPipeline but also supports | from via a ViewResolver.
func BuildPipelineWithViews(ctx context.Context, query *spl2.Query, store IndexStore, resolver ViewResolver, batchSize int) (Iterator, error) {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	qc := &queryContext{
		ctx:          ctx,
		store:        store,
		datasets:     make(map[string][]map[string]event.Value),
		batchSize:    batchSize,
		viewResolver: resolver,
		progCache:    vm.NewProgramCache(),
	}

	return qc.buildQuery(ctx, query)
}

//nolint:maintidx // large switch dispatch over all SPL2 command types; splitting would hurt readability
func (qc *queryContext) buildCommand(child Iterator, cmd spl2.Command) (Iterator, error) {
	switch c := cmd.(type) {
	case *spl2.SearchCommand:
		// Handle SEARCH index=<idx> syntax: load events from the store.
		if c.Index != "" && qc.store != nil {
			// Prefer columnar batch path when the store provides pre-built batches.
			var iter Iterator
			if bs, ok := qc.store.(BatchStore); ok {
				batches := bs.GetBatches(c.Index)
				iter = NewColumnarScanIteratorWithBudget(batches, qc.newAccount("scan"))
			} else {
				events := qc.store.GetEvents(c.Index)
				iter = NewScanIteratorWithBudget(events, qc.batchSize, qc.newAccount("scan"))
			}
			// Apply search expression or predicates on top.
			result := iter
			if c.Expression != nil {
				eval := spl2.NewSearchEvaluator(c.Expression)
				result = NewSearchExprIteratorWithExpr(result, eval, c.Expression)
			}

			return result, nil
		}
		if c.Expression != nil {
			eval := spl2.NewSearchEvaluator(c.Expression)

			return NewSearchExprIteratorWithExpr(child, eval, c.Expression), nil
		}
		if c.Term != "" {
			return NewSearchFilterIterator(child, c.Term), nil
		}

		return child, nil

	case *spl2.WhereCommand:
		key := c.Expr.String()
		prog := qc.progCache.Get(key)
		if prog == nil {
			var err error
			prog, err = vm.CompilePredicate(c.Expr)
			if err != nil {
				return nil, fmt.Errorf("compile WHERE: %w", err)
			}
			qc.progCache.Put(key, prog)
		}
		fi := NewFilterIteratorWithExpr(child, prog, c.Expr)
		if qc.profileLevel == "trace" {
			fi.SetProfileVM(true)
		}

		return fi, nil

	case *spl2.StatsCommand:
		aggs := qc.convertAggs(c.Aggregations)
		iter := NewAggregateIteratorWithSpill(child, aggs, c.GroupBy, qc.newCoordinatedAccount("aggregate", reservationAggregate), qc.spillMgr)
		if qc.monitor != nil {
			iter.budgetLimit = qc.monitor.Limit()
		}

		return iter, nil

	case *spl2.EvalCommand:
		var assigns []EvalAssignment
		if len(c.Assignments) > 0 {
			for _, a := range c.Assignments {
				key := a.Expr.String()
				prog := qc.progCache.Get(key)
				if prog == nil {
					var err error
					prog, err = vm.CompileExpr(a.Expr)
					if err != nil {
						return nil, fmt.Errorf("compile EVAL %s: %w", a.Field, err)
					}
					qc.progCache.Put(key, prog)
				}
				assigns = append(assigns, EvalAssignment{Field: a.Field, Program: prog})
			}
		} else if c.Field != "" && c.Expr != nil {
			key := c.Expr.String()
			prog := qc.progCache.Get(key)
			if prog == nil {
				var err error
				prog, err = vm.CompileExpr(c.Expr)
				if err != nil {
					return nil, fmt.Errorf("compile EVAL %s: %w", c.Field, err)
				}
				qc.progCache.Put(key, prog)
			}
			assigns = append(assigns, EvalAssignment{Field: c.Field, Program: prog})
		}

		ei := NewEvalIterator(child, assigns)
		if qc.profileLevel == "trace" {
			ei.SetProfileVM(true)
		}

		return ei, nil

	case *spl2.HeadCommand:
		return NewLimitIterator(child, c.Count), nil

	case *spl2.TailCommand:
		// Tail requires materialization — use sort-like approach
		return NewTailIteratorWithBudget(child, c.Count, qc.batchSize, qc.newAccount("tail")), nil

	case *spl2.FieldsCommand:
		return NewProjectIterator(child, c.Fields, c.Remove), nil

	case *spl2.TableCommand:
		return NewProjectIterator(child, c.Fields, false), nil

	case *spl2.RenameCommand:
		renames := make(map[string]string, len(c.Renames))
		for _, r := range c.Renames {
			renames[r.Old] = r.New
		}

		return NewRenameIterator(child, renames), nil

	case *spl2.SortCommand:
		fields := make([]SortField, len(c.Fields))
		for i, f := range c.Fields {
			fields[i] = SortField{Name: f.Name, Desc: f.Desc}
		}
		acct := qc.newCoordinatedAccount("sort", reservationSort)
		if qc.bufferPool != nil && qc.spillMgr != nil {
			return NewBufferedSortIterator(
				child, fields, qc.batchSize, acct,
				qc.bufferPool, qc.queryID, qc.spillMgr,
			), nil
		}

		return NewSortIteratorWithSpill(child, fields, qc.batchSize, acct, qc.spillMgr), nil

	case *spl2.TopNCommand:
		fields := make([]SortField, len(c.Fields))
		for i, f := range c.Fields {
			fields[i] = SortField{Name: f.Name, Desc: f.Desc}
		}

		return NewTopNIteratorWithBudget(child, fields, c.Limit, qc.batchSize, qc.newAccount("topn")), nil

	case *spl2.DedupCommand:
		acct := qc.newCoordinatedAccount("dedup", reservationDedup)
		if qc.dedupExact {
			return newDedupIteratorExactWithSpill(child, c.Fields, c.Limit, acct, qc.spillMgr), nil
		}

		return newDedupIteratorWithSpill(child, c.Fields, c.Limit, acct, qc.spillMgr), nil

	case *spl2.RexCommand:
		field := c.Field
		if field == "" {
			field = "_raw"
		}

		return NewRexIterator(child, field, c.Pattern)

	case *spl2.BinCommand:
		dur := parseDuration(c.Span)

		return NewBinIterator(child, c.Field, c.Alias, dur), nil

	case *spl2.StreamstatsCommand:
		aggs := qc.convertAggs(c.Aggregations)

		return NewStreamStatsIterator(child, aggs, c.GroupBy, c.Window, c.Current), nil

	case *spl2.EventstatsCommand:
		aggs := qc.convertAggs(c.Aggregations)

		return newEventStatsIteratorWithSpill(child, aggs, c.GroupBy, qc.batchSize, qc.newCoordinatedAccount("eventstats", reservationEventStats), qc.spillMgr), nil

	case *spl2.JoinCommand:
		if c.Subquery == nil {
			return child, nil
		}
		rightIter, err := qc.buildQuery(qc.ctx, c.Subquery)
		if err != nil {
			return nil, fmt.Errorf("build JOIN subquery: %w", err)
		}
		strategy := qc.joinStrategy
		var ji *JoinIterator
		joinAcct := qc.newCoordinatedAccount("join", reservationJoin)
		if strategy != "" {
			ji = NewJoinIteratorWithStrategy(child, rightIter, c.Field, c.JoinType, strategy, joinAcct, qc.spillMgr)
		} else {
			ji = NewJoinIteratorWithSpill(child, rightIter, c.Field, c.JoinType, joinAcct, qc.spillMgr)
		}
		// Overlap left-side I/O with right-side hash table build when parallelism is enabled.
		ji.SetPrefetch(qc.parallelCfg.Enabled)

		return ji, nil

	case *spl2.AppendCommand:
		if c.Subquery == nil {
			return child, nil
		}
		appendIter, err := qc.buildQuery(qc.ctx, c.Subquery)
		if err != nil {
			return nil, fmt.Errorf("build APPEND subquery: %w", err)
		}
		if qc.parallelCfg.Enabled {
			// Parallel APPEND: run main and sub-query branches concurrently.
			// Default OrderPreserved ensures main query results appear before
			// sub-query, matching sequential APPEND semantics.
			// RelaxAppendOrderingRule sets appendOrdering=interleaved when
			// downstream is commutative (stats, sort), enabling better throughput.
			mode := OrderPreserved
			if qc.appendOrdering == "interleaved" {
				mode = OrderInterleaved
			}

			return NewConcurrentUnionIterator(
				[]Iterator{child, appendIter}, mode, &qc.parallelCfg,
			), nil
		}

		return NewUnionIterator([]Iterator{child, appendIter}), nil

	case *spl2.MultisearchCommand:
		children := make([]Iterator, 0, len(c.Searches))
		for _, search := range c.Searches {
			subIter, err := qc.buildQuery(qc.ctx, search)
			if err != nil {
				return nil, fmt.Errorf("build MULTISEARCH branch: %w", err)
			}
			children = append(children, subIter)
		}
		if qc.parallelCfg.Enabled && len(children) > 1 {
			// Parallel MULTISEARCH: all branches run concurrently with
			// interleaved output (no ordering guarantee across branches).
			return NewConcurrentUnionIterator(
				children, OrderInterleaved, &qc.parallelCfg,
			), nil
		}

		return NewUnionIterator(children), nil

	case *spl2.XYSeriesCommand:
		return NewXYSeriesIteratorWithBudget(child, c.XField, c.YField, c.ValueField, qc.batchSize, qc.newAccount("xyseries")), nil

	case *spl2.TransactionCommand:
		dur := parseDuration(c.MaxSpan)

		return NewTransactionIteratorWithBudget(child, c.Field, dur, c.StartsWith, c.EndsWith, qc.batchSize, qc.newAccount("transaction")), nil

	case *spl2.TopCommand:
		return NewTopIteratorWithBudget(child, c.Field, c.ByField, c.N, false, qc.batchSize, qc.newAccount("top")), nil

	case *spl2.RareCommand:
		return NewTopIteratorWithBudget(child, c.Field, c.ByField, c.N, true, qc.batchSize, qc.newAccount("rare")), nil

	case *spl2.FillnullCommand:
		return NewFillnullIterator(child, c.Value, c.Fields), nil

	case *spl2.TimechartCommand:
		// Timechart = BIN + STATS.
		dur := parseDuration(c.Span)
		binIter := NewBinIterator(child, "_time", "_time", dur)
		aggs := qc.convertAggs(c.Aggregations)
		groupBy := append([]string{"_time"}, c.GroupBy...)
		tcIter := NewAggregateIteratorWithSpill(binIter, aggs, groupBy, qc.newCoordinatedAccount("timechart", reservationAggregate), qc.spillMgr)
		if qc.monitor != nil {
			tcIter.budgetLimit = qc.monitor.Limit()
		}

		return tcIter, nil

	case *spl2.FromCommand:
		// System tables: FROM system.parts, system.merges, system.columns.
		if strings.HasPrefix(c.ViewName, "system.") {
			tableName := strings.TrimPrefix(c.ViewName, "system.")

			return NewSystemTableIterator(tableName, qc.systemTableResolver, qc.batchSize), nil
		}

		return NewFromIterator(c.ViewName, qc.viewResolver, qc.batchSize), nil

	case *spl2.MaterializeCommand:
		if qc.viewManager == nil {
			return child, nil // pass-through when no manager
		}

		return NewMaterializeIterator(c.Name, "", c.Retention, qc.viewManager), nil

	case *spl2.ViewsCommand:
		if qc.viewManager == nil {
			return child, nil
		}

		return NewViewsIterator(qc.viewManager, qc.batchSize), nil

	case *spl2.DropviewCommand:
		if qc.viewManager == nil {
			return child, nil
		}

		return NewDropviewIterator(c.Name, qc.viewManager), nil

	default:
		return nil, fmt.Errorf("unsupported command type: %T", cmd)
	}
}

// TailIterator keeps only the last N rows from the input using a fixed-size
// circular buffer. Memory usage is O(N) regardless of input size.
//
// Ring buffer emission order — worked example:
//
//	5 rows inserted into ring of size 3:
//	  Insert row0 → ring[0]=row0, totalSeen=1
//	  Insert row1 → ring[1]=row1, totalSeen=2
//	  Insert row2 → ring[2]=row2, totalSeen=3
//	  Insert row3 → ring[0]=row3, totalSeen=4  (overwrites row0)
//	  Insert row4 → ring[1]=row4, totalSeen=5  (overwrites row1)
//
//	Emission: kept=3, start = (5-3) % 3 = 2
//	  Read: ring[2]=row2, ring[0]=row3, ring[1]=row4 ✓ (correct chronological order)
type TailIterator struct {
	child     Iterator
	count     int
	ring      []map[string]event.Value // fixed-size circular buffer of size `count`
	totalSeen int                      // total rows written (for ring position and fill tracking)
	emitted   bool
	result    []map[string]event.Value // linearized output after accumulation
	offset    int
	batchSize int
	acct      stats.MemoryAccount // per-operator memory tracking
}

// NewTailIterator creates a tail operator that keeps the last count rows.
func NewTailIterator(child Iterator, count, batchSize int) *TailIterator {
	if count < 0 {
		count = 0
	}

	return &TailIterator{
		child:     child,
		count:     count,
		ring:      make([]map[string]event.Value, count),
		batchSize: batchSize,
		acct:      stats.NopAccount(),
	}
}

// NewTailIteratorWithBudget creates a tail operator with memory budget tracking.
// The ring buffer is bounded to O(count) rows; the account provides accounting
// visibility rather than enforcement.
func NewTailIteratorWithBudget(child Iterator, count, batchSize int, acct stats.MemoryAccount) *TailIterator {
	t := NewTailIterator(child, count, batchSize)
	t.acct = stats.EnsureAccount(acct)

	return t
}

func (t *TailIterator) Init(ctx context.Context) error { return t.child.Init(ctx) }

func (t *TailIterator) Next(ctx context.Context) (*Batch, error) {
	if !t.emitted {
		if t.count == 0 {
			// tail 0: drain child but keep nothing.
			for {
				batch, err := t.child.Next(ctx)
				if err != nil {
					return nil, err
				}
				if batch == nil {
					break
				}
			}
		} else {
			// Accumulation phase: drain child, writing into the ring buffer.
			// Each row overwrites the oldest entry — never allocates beyond `count` slots.
			for {
				batch, err := t.child.Next(ctx)
				if err != nil {
					return nil, err
				}
				if batch == nil {
					break
				}
				for i := 0; i < batch.Len; i++ {
					// Track memory: only Grow for new slots (first fill of ring).
					// Replacements are memory-neutral (one row in, one row out).
					if t.totalSeen < t.count {
						_ = t.acct.Grow(estimatedRowBytes)
					}
					t.ring[t.totalSeen%t.count] = batch.Row(i)
					t.totalSeen++
				}
			}

			// Linearize ring buffer in chronological order.
			kept := t.totalSeen
			if kept > t.count {
				kept = t.count
			}
			t.result = make([]map[string]event.Value, kept)
			start := 0
			if t.totalSeen > t.count {
				start = (t.totalSeen - kept) % t.count
			}
			for i := 0; i < kept; i++ {
				t.result[i] = t.ring[(start+i)%t.count]
			}
		}
		// Release ring buffer — result holds the linearized output now.
		t.ring = nil
		t.emitted = true
	}

	if t.offset >= len(t.result) {
		return nil, nil
	}
	end := t.offset + t.batchSize
	if end > len(t.result) {
		end = len(t.result)
	}
	batch := BatchFromRows(t.result[t.offset:end])
	t.offset = end

	return batch, nil
}

func (t *TailIterator) Close() error {
	t.acct.Close()

	return t.child.Close()
}
func (t *TailIterator) Schema() []FieldInfo { return t.child.Schema() }

func (qc *queryContext) convertAggs(aggs []spl2.AggExpr) []AggFunc {
	result := make([]AggFunc, len(aggs))
	for i, a := range aggs {
		alias := a.Alias
		if alias == "" {
			if len(a.Args) > 0 {
				alias = fmt.Sprintf("%s(%s)", a.Func, a.Args[0])
			} else {
				alias = a.Func // "count" not "count()" — matches Splunk convention
			}
		}
		field := ""
		var prog *vm.Program
		if len(a.Args) > 0 {
			// Check if arg is a nested eval (FuncCallExpr)
			if _, ok := a.Args[0].(*spl2.FuncCallExpr); ok {
				key := a.Args[0].String()
				prog = qc.progCache.Get(key)
				if prog == nil {
					compiled, err := vm.CompileExpr(a.Args[0])
					if err == nil {
						prog = compiled
						qc.progCache.Put(key, prog)
					}
				}
			} else {
				field = a.Args[0].String()
			}
		}
		result[i] = AggFunc{
			Name:    a.Func,
			Field:   field,
			Alias:   alias,
			Program: prog,
		}
	}

	return result
}

func parseDuration(s string) time.Duration {
	if s == "" {
		return 0
	}
	// Try Go duration format first
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	// SPL2 style: "1d", "7d", "30d"
	if len(s) > 1 && s[len(s)-1] == 'd' {
		n := 0
		for _, c := range s[:len(s)-1] {
			if c >= '0' && c <= '9' {
				n = n*10 + int(c-'0')
			}
		}

		return time.Duration(n) * 24 * time.Hour
	}

	return 0
}

// CheckVectorizedFilter walks the iterator chain and returns true if any
// FilterIterator used the vectorized fast path during execution.
// Call after CollectAll to inspect the pipeline post-execution.
func CheckVectorizedFilter(iter Iterator) bool {
	if iter == nil {
		return false
	}
	switch it := iter.(type) {
	case *FilterIterator:
		if it.WasVectorized() {
			return true
		}

		return CheckVectorizedFilter(it.child)
	case *InstrumentedIterator:
		return CheckVectorizedFilter(it.inner)
	case *InstrumentedFilterIterator:
		return CheckVectorizedFilter(it.inner)
	default:
		child := iteratorChild(iter)

		return CheckVectorizedFilter(child)
	}
}

// CollectAll runs a pipeline to completion and returns all result rows.
func CollectAll(ctx context.Context, iter Iterator) ([]map[string]event.Value, error) {
	if err := iter.Init(ctx); err != nil {
		return nil, err
	}
	defer iter.Close()

	var rows []map[string]event.Value
	for {
		batch, err := iter.Next(ctx)
		if err != nil {
			return nil, err
		}
		if batch == nil {
			break
		}
		for i := 0; i < batch.Len; i++ {
			rows = append(rows, batch.Row(i))
		}
	}

	return rows, nil
}
