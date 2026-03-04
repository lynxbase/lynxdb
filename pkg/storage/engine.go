package storage

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	ingestpipeline "github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
	"github.com/lynxbase/lynxdb/pkg/optimizer"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/stats"
)

// applyBudgetAndCPUStats populates resource stats from a BudgetMonitor and CPU snapshots.
// The monitor provides accurate per-query memory tracking (high-water mark from
// operator-level BoundAccounts). CPU stats come from getrusage which is acceptable
// for single-query CLI mode.
func applyBudgetAndCPUStats(st *stats.QueryStats, monitor *stats.BudgetMonitor, cpuBefore, cpuAfter stats.CPUSnapshot) {
	st.PeakMemoryBytes = monitor.MaxAllocated()
	st.MemAllocBytes = monitor.MaxAllocated()
	stats.ApplyCPUStats(st, cpuBefore, cpuAfter)
}

const defaultIndex = "main"

// EngineConfig configures a StorageEngine.
type EngineConfig struct {
	DataDir string
	Storage config.StorageConfig
}

// Engine is the unified storage engine for LynxDB.
// It supports three profiles — Ephemeral, Persistent, and Tiered —
// with feature flags controlling which subsystems are active.
//
// In Ephemeral mode (DataDir=""), events are held purely in memory
// and cleaned up on Close. This replaces the old pkg/local engine.
//
// Engine is NOT goroutine-safe. In Ephemeral mode it is used for
// single-threaded CLI file/stdin queries only. The events map has
// no synchronization — do not use concurrently.
type Engine struct {
	profile  Profile
	features ProfileFeatures
	events   map[string][]*event.Event
	pipe     *ingestpipeline.Pipeline
	closed   bool
}

// NewEngine creates a StorageEngine configured by the given config.
// The storage profile is auto-detected from the config.
func NewEngine(cfg EngineConfig) (*Engine, error) {
	profile := ResolveProfile(cfg.DataDir, cfg.Storage.S3Bucket)
	features := Features(profile)

	return &Engine{
		profile:  profile,
		features: features,
		events:   make(map[string][]*event.Event),
		pipe:     ingestpipeline.DefaultPipeline(),
	}, nil
}

// NewEphemeralEngine creates an Ephemeral StorageEngine (in-memory only).
// Primary constructor for CLI file/stdin queries.
func NewEphemeralEngine() *Engine {
	return &Engine{
		profile:  Ephemeral,
		features: Features(Ephemeral),
		events:   make(map[string][]*event.Event),
		pipe:     ingestpipeline.DefaultPipeline(),
	}
}

// Profile returns the resolved storage profile.
func (e *Engine) Profile() Profile {
	return e.profile
}

// IngestOpts controls how raw data is parsed during ingestion.
type IngestOpts struct {
	Source     string
	SourceType string
	Index      string
}

// QueryOpts controls query execution.
type QueryOpts struct {
	// MaxMemory limits the memory budget for this query (bytes).
	// 0 means auto-detect using EphemeralMemoryLimit (50% of system RAM).
	MaxMemory int64

	// SpillThreshold is the estimated memory at which QueryReader escalates
	// from in-memory accumulation to on-disk segment spill. 0 uses the default (256MB).
	SpillThreshold int64
}

// QueryResult holds the results of a query execution.
type QueryResult struct {
	Rows []map[string]interface{}
}

// IngestFile reads a file and ingests all lines as events.
func (e *Engine) IngestFile(path string, opts IngestOpts) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	if opts.Source == "" {
		opts.Source = path
	}

	return e.IngestReader(f, opts)
}

// IngestReader reads from r and ingests all lines as events.
func (e *Engine) IngestReader(r io.Reader, opts IngestOpts) (int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	idx := defaultIndex
	if opts.Index != "" {
		idx = opts.Index
	}

	const batchSize = 1024
	batch := make([]*event.Event, 0, batchSize)
	total := 0

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || isBlankStr(line) {
			continue
		}

		ev := event.NewEvent(time.Time{}, line)
		ev.Source = opts.Source
		ev.SourceType = opts.SourceType
		batch = append(batch, ev)

		if len(batch) >= batchSize {
			n, err := e.processBatch(batch, idx)
			if err != nil {
				return total, err
			}
			total += n
			batch = batch[:0]
		}
	}
	if err := scanner.Err(); err != nil {
		return total, fmt.Errorf("read: %w", err)
	}

	if len(batch) > 0 {
		n, err := e.processBatch(batch, idx)
		if err != nil {
			return total, err
		}
		total += n
	}

	return total, nil
}

// isBlankStr returns true if the string contains only whitespace.
func isBlankStr(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != ' ' && s[i] != '\t' && s[i] != '\r' && s[i] != '\n' {
			return false
		}
	}

	return true
}

// processBatch runs a batch of events through the ingest pipeline and stores them.
func (e *Engine) processBatch(events []*event.Event, defaultIndex string) (int, error) {
	return e.processBatchWith(e.pipe, events, defaultIndex)
}

// processBatchWith runs events through a specific pipeline and stores them.
func (e *Engine) processBatchWith(pipe *ingestpipeline.Pipeline, events []*event.Event, defaultIndex string) (int, error) {
	processed, err := pipe.Process(events)
	if err != nil {
		return 0, fmt.Errorf("pipeline: %w", err)
	}

	for _, ev := range processed {
		if ev.Index == "" {
			ev.Index = defaultIndex
		}
		e.events[ev.Index] = append(e.events[ev.Index], ev)
	}

	return len(processed), nil
}

// IngestLines ingests pre-split lines as events.
func (e *Engine) IngestLines(lines []string, opts IngestOpts) (int, error) {
	idx := defaultIndex
	if opts.Index != "" {
		idx = opts.Index
	}

	events := make([]*event.Event, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		ev := event.NewEvent(time.Time{}, line)
		ev.Source = opts.Source
		ev.SourceType = opts.SourceType
		events = append(events, ev)
	}

	if len(events) == 0 {
		return 0, nil
	}

	return e.processBatch(events, idx)
}

// EventCount returns the total number of ingested events.
func (e *Engine) EventCount() int {
	total := 0
	for _, evts := range e.events {
		total += len(evts)
	}

	return total
}

// Query parses and executes an SPL2 query against ingested events.
// Returns the result, query statistics, and any error.
func (e *Engine) Query(ctx context.Context, spl2Query string, opts QueryOpts) (*QueryResult, *stats.QueryStats, error) {
	st := &stats.QueryStats{
		Ephemeral: true,
		ScanType:  "ephemeral",
	}

	parseStart := time.Now()

	prog, err := spl2.ParseProgram(spl2Query)
	if err != nil {
		return nil, nil, fmt.Errorf("parse: %w", err)
	}

	st.ParseDuration = time.Since(parseStart)

	optStart := time.Now()

	opt := optimizer.New()
	prog.Main = opt.Optimize(prog.Main)
	for i := range prog.Datasets {
		prog.Datasets[i].Query = opt.Optimize(prog.Datasets[i].Query)
	}

	st.OptimizeDuration = time.Since(optStart)

	// Capture optimizer rule details for --analyze profile output.
	for _, rd := range opt.GetRuleDetails() {
		st.OptimizerRules = append(st.OptimizerRules, stats.OptimizerRuleDetail{
			Name:        rd.Name,
			Description: rd.Description,
			Count:       rd.Count,
		})
	}
	st.OptimizerTotalRules = opt.TotalRules()

	store := &pipeline.ServerIndexStore{Events: e.events}

	// Resolve ephemeral memory limit: explicit opts > auto-detect (50% system RAM).
	limit := opts.MaxMemory
	if limit == 0 {
		limit = stats.EphemeralMemoryLimit()
	}
	monitor := stats.NewBudgetMonitor("ephemeral", limit)
	cpuBefore := stats.TakeCPUSnapshot()
	execStart := time.Now()

	// Create an ephemeral SpillManager so sort/dedup/join can spill to disk
	// when the memory budget is exceeded, instead of returning a hard OOM error.
	spillMgr, spillErr := pipeline.NewSpillManager("", nil)
	if spillErr != nil {
		spillMgr = nil // Non-fatal: proceed without spill support
	}
	defer func() {
		if spillMgr != nil {
			spillMgr.CleanupAll()
		}
	}()

	buildResult, err := pipeline.BuildProgramWithBudget(ctx, prog, store, nil, nil, 0, "", monitor, spillMgr, false, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build pipeline: %w", err)
	}

	iter := buildResult.Iterator
	pipeRows, err := pipeline.CollectAll(ctx, iter)
	if err != nil {
		return nil, nil, fmt.Errorf("execute: %w", err)
	}

	st.ExecDuration = time.Since(execStart)
	applyBudgetAndCPUStats(st, monitor, cpuBefore, stats.TakeCPUSnapshot())

	// Extract per-stage stats and warnings from instrumented pipeline.
	st.Stages = pipeline.CollectStageStats(iter)
	st.Warnings = pipeline.CollectWarnings(iter)
	extractMatchedRows(st)

	// Extract per-operator memory budget statistics from the coordinator.
	if buildResult.Coordinator != nil {
		st.OperatorBudgets = pipeline.CollectOperatorBudgets(buildResult.Coordinator)
	}

	// Count total events across all indexes for ScannedRows.
	var totalEvents int64
	for _, evts := range e.events {
		totalEvents += int64(len(evts))
	}

	st.ScannedRows = totalEvents
	// Estimate processed bytes for ephemeral queries (no segment metadata available).
	// Use approximate average event size based on typical log lines (~200 bytes/event).
	const ephemeralAvgEventBytes int64 = 200
	st.ProcessedBytes = totalEvents * ephemeralAvgEventBytes
	st.ResultRows = int64(len(pipeRows))

	rows := make([]map[string]interface{}, len(pipeRows))
	for i, row := range pipeRows {
		fields := make(map[string]interface{}, len(row))
		for k, v := range row {
			fields[k] = v.Interface()
		}
		rows[i] = fields
	}

	return &QueryResult{Rows: rows}, st, nil
}

// extractMatchedRows derives MatchedRows from pipeline stage stats.
// It looks for the first Filter/Search stage output as the matched count.
func extractMatchedRows(st *stats.QueryStats) {
	for _, stage := range st.Stages {
		if stage.Name == "Filter" || stage.Name == "Search" {
			st.MatchedRows = stage.OutputRows

			return
		}
	}
}

// IngestReaderFiltered reads from r with query-aware predicate pushdown.
// Lines that cannot match the query's search keywords are skipped before
// Event allocation.
func (e *Engine) IngestReaderFiltered(ctx context.Context, r io.Reader, spl2Query string, opts IngestOpts) (int, error) {
	prog, err := spl2.ParseProgram(spl2Query)
	if err != nil {
		return 0, fmt.Errorf("parse: %w", err)
	}

	hints := spl2.ExtractQueryHints(prog)

	var preFilter [][]byte
	if hints.CanPushdownToReader() {
		preFilter = hints.CollectPreFilterBytes()
	}

	pipe := ingestpipeline.SelectivePipeline(hints.RequiredFieldsMap())

	idx := defaultIndex
	if opts.Index != "" {
		idx = opts.Index
	}

	fr := NewFilterReader(r, preFilter)

	const batchSize = 1024
	batch := make([]*event.Event, 0, batchSize)
	total := 0

	for {
		line, ok := fr.Next()
		if !ok {
			break
		}

		ev := event.NewEvent(time.Time{}, line)
		ev.Source = opts.Source
		ev.SourceType = opts.SourceType
		batch = append(batch, ev)

		if len(batch) >= batchSize {
			n, err := e.processBatchWith(pipe, batch, idx)
			if err != nil {
				return total, err
			}
			total += n
			batch = batch[:0]
		}
	}
	if err := fr.Err(); err != nil {
		return total, fmt.Errorf("read: %w", err)
	}

	if len(batch) > 0 {
		n, err := e.processBatchWith(pipe, batch, idx)
		if err != nil {
			return total, err
		}
		total += n
	}

	return total, nil
}

// cleanupSpiller logs a warning if spiller.Cleanup() fails. Temp directories
// can leak if errors are silently suppressed.
func cleanupSpiller(s *SegmentSpiller) {
	if err := s.Cleanup(); err != nil {
		slog.Warn("spiller cleanup failed", "err", err)
	}
}

// DefaultSpillThreshold is the estimated memory usage at which QueryReader
// escalates from in-memory accumulation to on-disk segment spill.
const DefaultSpillThreshold = 256 * 1024 * 1024

// QueryReader parses the SPL2 query first, then reads from r with predicate
// pushdown. For large inputs (>DefaultSpillThreshold), events are spilled to
// temporary .lsg segment files on disk.
// Returns the result, query statistics, and any error.
func (e *Engine) QueryReader(ctx context.Context, r io.Reader, spl2Query string, opts IngestOpts, qopts QueryOpts) (*QueryResult, *stats.QueryStats, error) {
	st := &stats.QueryStats{
		Ephemeral: true,
		ScanType:  "ephemeral",
	}

	parseStart := time.Now()

	prog, err := spl2.ParseProgram(spl2Query)
	if err != nil {
		return nil, nil, fmt.Errorf("parse: %w", err)
	}

	st.ParseDuration = time.Since(parseStart)

	optStart := time.Now()

	opt := optimizer.New()
	prog.Main = opt.Optimize(prog.Main)
	for i := range prog.Datasets {
		prog.Datasets[i].Query = opt.Optimize(prog.Datasets[i].Query)
	}

	st.OptimizeDuration = time.Since(optStart)

	// Capture optimizer rule details for --analyze profile output.
	for _, rd := range opt.GetRuleDetails() {
		st.OptimizerRules = append(st.OptimizerRules, stats.OptimizerRuleDetail{
			Name:        rd.Name,
			Description: rd.Description,
			Count:       rd.Count,
		})
	}
	st.OptimizerTotalRules = opt.TotalRules()

	hints := spl2.ExtractQueryHints(prog)

	var preFilter [][]byte
	if hints.CanPushdownToReader() {
		preFilter = hints.CollectPreFilterBytes()
	}

	pipe := ingestpipeline.SelectivePipeline(hints.RequiredFieldsMap())

	earlyLimit := 0
	if hints.IsStreamable() && hints.Limit > 0 {
		earlyLimit = hints.Limit * 2
		if earlyLimit < 100 {
			earlyLimit = 100
		}
	}

	idx := defaultIndex
	if opts.Index != "" {
		idx = opts.Index
	}

	fr := NewFilterReader(r, preFilter)

	const batchSize = 1024
	batch := make([]*event.Event, 0, batchSize)
	total := 0

	spillThreshold := qopts.SpillThreshold
	if spillThreshold <= 0 {
		spillThreshold = DefaultSpillThreshold
	}

	var spiller *SegmentSpiller
	var memEstimate int64

	for earlyLimit <= 0 || total+len(batch) < earlyLimit {
		line, ok := fr.Next()
		if !ok {
			break
		}

		ev := event.NewEvent(time.Time{}, line)
		ev.Source = opts.Source
		ev.SourceType = opts.SourceType
		batch = append(batch, ev)
		memEstimate += int64(len(line)) + 128

		if len(batch) >= batchSize {
			if spiller == nil && memEstimate >= spillThreshold {
				sp, serr := NewSegmentSpiller(spillThreshold / 2)
				if serr != nil {
					return nil, nil, fmt.Errorf("create spiller: %w", serr)
				}
				spiller = sp

				for _, evts := range e.events {
					if err := spiller.AddBatch(evts); err != nil {
						cleanupSpiller(spiller)

						return nil, nil, fmt.Errorf("spill existing: %w", err)
					}
				}
				e.events = make(map[string][]*event.Event)
			}

			if spiller != nil {
				processed, perr := pipe.Process(batch)
				if perr != nil {
					cleanupSpiller(spiller)

					return nil, nil, fmt.Errorf("pipeline: %w", perr)
				}
				for _, pev := range processed {
					if pev.Index == "" {
						pev.Index = idx
					}
				}
				if err := spiller.AddBatch(processed); err != nil {
					cleanupSpiller(spiller)

					return nil, nil, fmt.Errorf("spill: %w", err)
				}
				total += len(processed)
			} else {
				n, perr := e.processBatchWith(pipe, batch, idx)
				if perr != nil {
					return nil, nil, perr
				}
				total += n
			}
			batch = batch[:0]
		}
	}
	if err := fr.Err(); err != nil {
		if spiller != nil {
			cleanupSpiller(spiller)
		}

		return nil, nil, fmt.Errorf("read: %w", err)
	}

	if len(batch) > 0 {
		if spiller != nil {
			processed, perr := pipe.Process(batch)
			if perr != nil {
				cleanupSpiller(spiller)

				return nil, nil, fmt.Errorf("pipeline: %w", perr)
			}
			for _, pev := range processed {
				if pev.Index == "" {
					pev.Index = idx
				}
			}
			if err := spiller.AddBatch(processed); err != nil {
				cleanupSpiller(spiller)

				return nil, nil, fmt.Errorf("spill: %w", err)
			}
			total += len(processed)
		} else {
			n, perr := e.processBatchWith(pipe, batch, idx)
			if perr != nil {
				return nil, nil, perr
			}
			total += n
		}
	}

	if spiller != nil {
		defer func() { cleanupSpiller(spiller) }()

		iter := spiller.Iterator(hints.SearchTerms)
		for {
			ev, iterErr := iter.Next()
			if iterErr != nil {
				return nil, nil, fmt.Errorf("read spilled: %w", iterErr)
			}
			if ev == nil {
				break
			}
			idx := ev.Index
			if idx == "" {
				idx = defaultIndex
			}
			e.events[idx] = append(e.events[idx], ev)
		}
	}

	store := &pipeline.ServerIndexStore{Events: e.events}

	// Resolve ephemeral memory limit: explicit opts > auto-detect (50% system RAM).
	ephLimit := qopts.MaxMemory
	if ephLimit == 0 {
		ephLimit = stats.EphemeralMemoryLimit()
	}
	monitor := stats.NewBudgetMonitor("ephemeral", ephLimit)
	cpuBefore := stats.TakeCPUSnapshot()
	execStart := time.Now()

	// Create an ephemeral SpillManager so sort/dedup/join can spill to disk
	// when the memory budget is exceeded, instead of returning a hard OOM error.
	qrSpillMgr, qrSpillErr := pipeline.NewSpillManager("", nil)
	if qrSpillErr != nil {
		qrSpillMgr = nil // Non-fatal: proceed without spill support
	}
	defer func() {
		if qrSpillMgr != nil {
			qrSpillMgr.CleanupAll()
		}
	}()

	buildResult, err := pipeline.BuildProgramWithBudget(ctx, prog, store, nil, nil, 0, "", monitor, qrSpillMgr, false, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build pipeline: %w", err)
	}

	qrIter := buildResult.Iterator
	pipeRows, err := pipeline.CollectAll(ctx, qrIter)
	if err != nil {
		return nil, nil, fmt.Errorf("execute: %w", err)
	}

	st.ExecDuration = time.Since(execStart)
	applyBudgetAndCPUStats(st, monitor, cpuBefore, stats.TakeCPUSnapshot())

	// Extract per-stage stats and warnings from instrumented pipeline.
	st.Stages = pipeline.CollectStageStats(qrIter)
	st.Warnings = pipeline.CollectWarnings(qrIter)
	extractMatchedRows(st)

	// Extract per-operator memory budget statistics from the coordinator.
	if buildResult.Coordinator != nil {
		st.OperatorBudgets = pipeline.CollectOperatorBudgets(buildResult.Coordinator)
	}

	st.ScannedRows = int64(total)
	// For QueryReader, use actual bytes tracked during ingestion for processed bytes estimate.
	// memEstimate counts len(line)+128 per line — use it directly as a better approximation
	// than the fixed 200 bytes/event constant, since it reflects actual line lengths.
	st.ProcessedBytes = memEstimate
	st.ResultRows = int64(len(pipeRows))

	rows := make([]map[string]interface{}, len(pipeRows))
	for i, row := range pipeRows {
		fields := make(map[string]interface{}, len(row))
		for k, v := range row {
			fields[k] = v.Interface()
		}
		rows[i] = fields
	}

	return &QueryResult{Rows: rows}, st, nil
}

// GetEvents implements pipeline.IndexStore so Engine can be used directly.
func (e *Engine) GetEvents(index string) []*event.Event {
	return e.events[index]
}

// Close releases all resources held by the engine.
// In Ephemeral mode, all in-memory events are discarded.
func (e *Engine) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	e.events = nil

	return nil
}
