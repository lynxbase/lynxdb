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
	"github.com/lynxbase/lynxdb/pkg/engine/physical"
	"github.com/lynxbase/lynxdb/pkg/engine/unpack"
	"github.com/lynxbase/lynxdb/pkg/event"
	ingestpipeline "github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/query"
	"github.com/lynxbase/lynxdb/pkg/stats"
)

// applyBudgetAndCPUStats populates resource stats from a governor BudgetAdapter
// and CPU snapshots. The adapter provides accurate per-query memory tracking
// (high-water mark). CPU stats come from getrusage which is acceptable for
// single-query CLI mode.
func applyBudgetAndCPUStats(st *stats.QueryStats, budget *memgov.BudgetAdapter, cpuBefore, cpuAfter stats.CPUSnapshot) {
	if budget != nil {
		st.PeakMemoryBytes = budget.MaxAllocated()
		st.MemAllocBytes = budget.MaxAllocated()
	}
	stats.ApplyCPUStats(st, cpuBefore, cpuAfter)
}

const defaultIndex = "main"

// EphemeralMaxEvents caps total in-memory events for the ephemeral engine
// to prevent OOM when piping large files. Default: 10M events (~2-5GB depending on line length).
const EphemeralMaxEvents = 10_000_000

// EngineConfig configures a StorageEngine.
type EngineConfig struct {
	DataDir   string
	Storage   config.StorageConfig
	MaxEvents int // 0 = use EphemeralMaxEvents
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
	profile       Profile
	features      ProfileFeatures
	events        map[string][]*event.Event
	pipe          *ingestpipeline.Pipeline
	closed        bool
	totalRawBytes int64 // cumulative raw bytes ingested (sum of line lengths)
	maxEvents     int   // max total events (0 = unlimited for persistent engines)
}

// NewEngine creates a StorageEngine configured by the given config.
// The storage profile is auto-detected from the config.
func NewEngine(cfg EngineConfig) (*Engine, error) {
	profile := ResolveProfile(cfg.DataDir, cfg.Storage.S3Bucket)
	features := Features(profile)

	maxEv := cfg.MaxEvents
	if maxEv == 0 && profile == Ephemeral {
		maxEv = EphemeralMaxEvents
	}

	return &Engine{
		profile:   profile,
		features:  features,
		events:    make(map[string][]*event.Event),
		pipe:      ingestpipeline.DefaultPipeline(),
		maxEvents: maxEv,
	}, nil
}

// NewEphemeralEngine creates an Ephemeral StorageEngine (in-memory only).
// Primary constructor for CLI file/stdin queries.
func NewEphemeralEngine() *Engine {
	return &Engine{
		profile:   Ephemeral,
		features:  Features(Ephemeral),
		events:    make(map[string][]*event.Event),
		pipe:      ingestpipeline.DefaultPipeline(),
		maxEvents: EphemeralMaxEvents,
	}
}

// Profile returns the resolved storage profile.
func (e *Engine) Profile() Profile {
	return e.profile
}

// IngestOpts controls how raw data is parsed during ingestion.
type IngestOpts struct {
	Source       string
	SourceType   string
	Index        string
	MaxFileSize  int64 // Maximum allowed file size in bytes. 0 = unlimited.
	MaxLineBytes int   // Maximum line length for scanner buffer. 0 = 1MB default.
	// Format names an unpack field extractor applied to each line, such as
	// combined or syslog; empty applies none.
	Format string
}

// withFormat appends the field extractor of format to pipe.
func withFormat(pipe *ingestpipeline.Pipeline, format string) (*ingestpipeline.Pipeline, error) {
	if format == "" {
		return pipe, nil
	}
	p, err := unpack.NewParser(format)
	if err != nil {
		return nil, err
	}
	return ingestpipeline.New(pipe, formatExtractor{p}), nil
}

// formatExtractor sets the fields a format parser finds in each event's raw
// text, keeping fields the event already has.
type formatExtractor struct {
	parser unpack.FormatParser
}

func (f formatExtractor) Process(events []*event.Event) ([]*event.Event, error) {
	for _, ev := range events {
		if ev.Fields == nil {
			ev.Fields = make(map[string]event.Value)
		}
		_ = f.parser.Parse(ev.Raw, func(key string, val event.Value) bool {
			if _, ok := ev.Fields[key]; !ok {
				ev.Fields[key] = val
			}
			return true
		})
	}
	return events, nil
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
func (e *Engine) IngestFile(ctx context.Context, path string, opts IngestOpts) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	if opts.MaxFileSize > 0 {
		fi, err := f.Stat()
		if err != nil {
			return 0, fmt.Errorf("stat %s: %w", path, err)
		}
		if fi.Size() > opts.MaxFileSize {
			return 0, fmt.Errorf("file %s exceeds max size: %d > %d bytes", path, fi.Size(), opts.MaxFileSize)
		}
	}

	if opts.Source == "" {
		opts.Source = path
	}

	return e.IngestReader(ctx, f, opts)
}

// IngestReader reads from r and ingests all lines as events.
func (e *Engine) IngestReader(ctx context.Context, r io.Reader, opts IngestOpts) (int, error) {
	pipe, err := withFormat(e.pipe, opts.Format)
	if err != nil {
		return 0, err
	}
	maxLine := opts.MaxLineBytes
	if maxLine <= 0 {
		maxLine = 1024 * 1024 // 1MB default
	}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), maxLine)

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

		e.totalRawBytes += int64(len(line))
		ev := event.NewEvent(time.Time{}, line)
		if opts.Index != "" {
			ev.Index = idx
		}
		ev.Source = opts.Source
		ev.SourceType = opts.SourceType
		batch = append(batch, ev)

		if len(batch) >= batchSize {
			if err := ctx.Err(); err != nil {
				return total, err
			}
			n, err := e.processBatchWith(pipe, batch, idx)
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
		n, err := e.processBatchWith(pipe, batch, idx)
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

	if e.maxEvents > 0 {
		total := 0
		for _, evts := range e.events {
			total += len(evts)
		}
		if total+len(processed) > e.maxEvents {
			return 0, fmt.Errorf("ephemeral engine event limit reached (%d events); use --file with a persistent server for large datasets", e.maxEvents)
		}
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
func (e *Engine) IngestLines(ctx context.Context, lines []string, opts IngestOpts) (int, error) {
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
		if opts.Index != "" {
			ev.Index = idx
		}
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

// Query compiles a PPL query and runs it over the ingested events.
func (e *Engine) Query(ctx context.Context, q string, _ QueryOpts) (*QueryResult, *stats.QueryStats, error) {
	st := &stats.QueryStats{
		Ephemeral: true,
		ScanType:  "ephemeral",
	}
	c, err := compileQuery(q, st)
	if err != nil {
		return nil, nil, err
	}
	pipeRows, err := e.run(ctx, c, defaultIndex, st)
	if err != nil {
		return nil, nil, err
	}

	var totalEvents int64
	for _, evts := range e.events {
		totalEvents += int64(len(evts))
	}
	st.ScannedRows = totalEvents
	st.ProcessedBytes = e.totalRawBytes
	st.ResultRows = int64(len(pipeRows))

	return &QueryResult{Rows: pipelineRowsToInterfaceRows(pipeRows, true)}, st, nil
}

func compileQuery(q string, st *stats.QueryStats) (*query.Compiled, error) {
	start := time.Now()
	c, err := query.Compile(q)
	st.ParseDuration = time.Since(start)
	return c, err
}

// run executes a compiled plan over the engine's events; a query written
// with the explain command returns its rendered plans instead.
func (e *Engine) run(ctx context.Context, c *query.Compiled, defaultIdx string, st *stats.QueryStats) ([]map[string]event.Value, error) {
	if c.Explain {
		return []map[string]event.Value{{"plan": event.StringValue(query.Explain(c))}}, nil
	}
	collect := map[ir.Node]*physical.NodeStats{}
	cpuBefore := stats.TakeCPUSnapshot()
	start := time.Now()
	rows, err := query.Run(ctx, c, physical.MapStore(e.events), query.Options{DefaultSource: defaultIdx, Collect: collect})
	if err != nil {
		return nil, err
	}
	st.ExecDuration = time.Since(start)
	applyBudgetAndCPUStats(st, nil, cpuBefore, stats.TakeCPUSnapshot())
	st.Stages = stageStats(c.Plan, collect)
	extractMatchedRows(st)
	return rows, nil
}

// stageStats lists per-operator statistics in pipeline order, from the
// scans up to the root.
func stageStats(plan *ir.Plan, collect map[ir.Node]*physical.NodeStats) []stats.StageStats {
	var out []stats.StageStats
	ir.WalkPlan(plan.Root, func(n ir.Node) bool {
		ns, ok := collect[n]
		if !ok {
			return true
		}
		name, _, _ := strings.Cut(n.Label(), "(")
		s := stats.StageStats{Name: name, OutputRows: ns.Rows, Duration: ns.Wall}
		if in := n.Inputs(); len(in) > 0 {
			if is, ok := collect[in[0]]; ok {
				s.InputRows = is.Rows
			}
		}
		out = append(out, s)
		return true
	})
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
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

// IngestReaderFiltered reads from r after checking that the query compiles,
// so a bad query fails before the input is read.
func (e *Engine) IngestReaderFiltered(ctx context.Context, r io.Reader, q string, opts IngestOpts) (int, error) {
	if _, err := query.Compile(q); err != nil {
		return 0, err
	}

	var preFilter [][]byte
	pipe, err := withFormat(ingestpipeline.SelectivePipeline(nil), opts.Format)
	if err != nil {
		return 0, err
	}

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
		if opts.Index != "" {
			ev.Index = idx
		}
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

// QueryReader compiles the query, reads events from r and runs the query
// over them. Inputs larger than the spill threshold are spilled to
// temporary segment files, read back filtered by the query's search terms.
func (e *Engine) QueryReader(ctx context.Context, r io.Reader, q string, opts IngestOpts, qopts QueryOpts) (*QueryResult, *stats.QueryStats, error) {
	st := &stats.QueryStats{
		Ephemeral: true,
		ScanType:  "ephemeral",
	}
	c, err := compileQuery(q, st)
	if err != nil {
		return nil, nil, err
	}

	var preFilter [][]byte
	pipe, err := withFormat(ingestpipeline.SelectivePipeline(nil), opts.Format)
	if err != nil {
		return nil, nil, err
	}
	earlyLimit := 0

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
		if opts.Index != "" {
			ev.Index = idx
		}
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

		hints := physical.PlanHints(c.Plan, idx, time.Now())
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

	pipeRows, err := e.run(ctx, c, idx, st)
	if err != nil {
		return nil, nil, err
	}

	st.ScannedRows = int64(total)
	st.ProcessedBytes = memEstimate
	st.ResultRows = int64(len(pipeRows))

	return &QueryResult{Rows: pipelineRowsToInterfaceRows(pipeRows, true)}, st, nil
}

func pipelineRowsToInterfaceRows(pipeRows []map[string]event.Value, addDefaultEventMetadata bool) []map[string]interface{} {
	rows := make([]map[string]interface{}, len(pipeRows))
	for i, row := range pipeRows {
		fields := make(map[string]interface{}, len(row)+2)
		for k, v := range row {
			fields[k] = v.Interface()
		}
		canonicalizeDefaultEventMetadata(fields, addDefaultEventMetadata)
		rows[i] = fields
	}

	return rows
}

func canonicalizeDefaultEventMetadata(fields map[string]interface{}, addDefaults bool) {
	if addDefaults && interfaceRowHasEventBuiltins(fields) {
		if _, ok := fields["_source"]; !ok {
			if source, ok := fields["source"]; ok {
				fields["_source"] = source
			} else {
				fields["_source"] = ""
			}
		}
		if _, ok := fields["_sourcetype"]; !ok {
			if sourceType, ok := fields["sourcetype"]; ok {
				fields["_sourcetype"] = sourceType
			} else {
				fields["_sourcetype"] = ""
			}
		}
	}
	delete(fields, "source")
	delete(fields, "sourcetype")
}

// interfaceRowHasEventBuiltins reports whether a row is a whole event rather
// than a computed row such as a time bucket of timechart.
func interfaceRowHasEventBuiltins(fields map[string]interface{}) bool {
	_, ok := fields["_raw"]
	return ok
}

// Events returns the full events map of an ephemeral engine. The returned
// map is not a copy; do not modify it concurrently.
func (e *Engine) Events() map[string][]*event.Event {
	return e.events
}

// MaterializeEvents implements pipeline.IndexStore so Engine can be used directly.
func (e *Engine) MaterializeEvents(_ context.Context, index string) ([]*event.Event, error) {
	return e.events[index], nil
}

// SetEvents replaces the events for a given index. Used for ephemeral
// re-execution (e.g., underscore queries against last results).
func (e *Engine) SetEvents(index string, events []*event.Event) {
	e.events[index] = events
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
