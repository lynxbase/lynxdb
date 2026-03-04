package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/output"
	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/stats"
	"github.com/lynxbase/lynxdb/pkg/storage"
	"github.com/lynxbase/lynxdb/pkg/timerange"
)

// noResultsError is a sentinel error for --fail-on-empty.
type noResultsError struct{}

func (noResultsError) Error() string { return "no results found" }

func init() {
	rootCmd.AddCommand(newQueryCmd())
}

func newQueryCmd() *cobra.Command {
	var (
		since      string
		from       string
		to         string
		file       string
		sourcetype string
		source     string
		outputFile string
		timeout    string
		analyze    string
		maxMemory  string
		failEmpty  bool
		copyFlag   bool
		explain    bool
	)

	cmd := &cobra.Command{
		Use:     "query [SPL2 query]",
		Aliases: []string{"q"},
		Short:   "Execute an SPL2 query",
		Long:    `Execute an SPL2 query against a running LynxDB server or local files.`,
		Example: `  lynxdb query 'level=error'                           Search errors
  lynxdb query 'level=error | stats count by source'   Aggregate by source
  lynxdb query 'status>=500 | top 10 uri' --since 1h   Top failing URIs
  lynxdb query --file app.log '| stats count by level'  Query local file
  cat app.log | lynxdb query '| where dur > 1000'       Pipe from stdin`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			query := strings.Join(args, " ")
			stdinPiped := isStdinPiped()

			if explain {
				return runExplain(query)
			}

			if file != "" && stdinPiped {
				return fmt.Errorf("cannot use --file and stdin simultaneously")
			}
			if (file != "" || stdinPiped) && copyFlag {
				return fmt.Errorf("--copy is only supported in server mode")
			}
			if file != "" {
				return runQueryFile(query, file, source, sourcetype, outputFile, failEmpty, analyze, maxMemory)
			}
			if stdinPiped {
				return runQueryStdin(query, source, sourcetype, outputFile, failEmpty, analyze, maxMemory)
			}

			SaveLastQuery(query, since, from, to)

			err := runQueryServer(query, since, from, to, timeout, failEmpty, analyze)
			if err != nil {
				return err
			}

			if copyFlag {
				return copyLastResultToClipboard()
			}

			return nil
		},
	}

	f := cmd.Flags()
	f.StringVarP(&since, "since", "s", "", "Relative time range (e.g., 15m, 1h, 7d)")
	f.StringVar(&from, "from", "", "Absolute start time (ISO 8601)")
	f.StringVar(&to, "to", "", "Absolute end time (ISO 8601)")
	f.StringVarP(&file, "file", "f", "", "Query a local file instead of server")
	f.StringVar(&sourcetype, "sourcetype", "", "Override source type detection")
	f.StringVar(&source, "source", "", "Set source metadata")
	f.StringVarP(&outputFile, "output", "o", "", "Write results to file")
	f.StringVar(&timeout, "timeout", "", "Query timeout (e.g., 10s, 5m)")
	f.StringVar(&analyze, "analyze", "", "Profile query execution (basic, full, trace)")
	f.StringVar(&maxMemory, "max-memory", "", "Max memory for ephemeral query (e.g., 512mb, 1gb)")
	f.BoolVar(&failEmpty, "fail-on-empty", false, "Exit with code 6 if no results")
	f.BoolVar(&copyFlag, "copy", false, "Copy results to clipboard as TSV")
	f.BoolVar(&explain, "explain", false, "Show query plan without executing")

	// Allow bare --analyze (no value) to default to "basic".
	cmd.Flags().Lookup("analyze").NoOptDefVal = "basic"

	// Hidden alias for discoverability: --profile-query works the same as --analyze.
	f.StringVar(&analyze, "profile-query", "", "Alias for --analyze (profile query execution)")
	cmd.Flags().Lookup("profile-query").NoOptDefVal = "basic"
	_ = cmd.Flags().MarkHidden("profile-query")

	return cmd
}

func runQueryFile(query, file, source, sourcetype, outputFile string, failEmpty bool, analyze, maxMemory string) error {
	matches, err := filepath.Glob(file)
	if err != nil {
		return fmt.Errorf("invalid file pattern: %w", err)
	}
	if len(matches) == 0 {
		return fmt.Errorf("no files matching: %s", file)
	}

	var memLimit int64
	if maxMemory != "" {
		b, err := config.ParseByteSize(maxMemory)
		if err != nil {
			return fmt.Errorf("invalid --max-memory: %w", err)
		}
		memLimit = int64(b)
	}

	eng := storage.NewEphemeralEngine()
	defer eng.Close()
	start := time.Now()
	totalEvents := 0
	normalizedQuery := ensureFromClause(query)

	if hints := spl2.DetectCompatHints(normalizedQuery); len(hints) > 0 {
		fmt.Fprint(os.Stderr, spl2.FormatCompatHints(hints))
	}

	ctx := context.Background()
	for _, path := range matches {
		src := source
		if src == "" {
			src = path
		}
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open %s: %w", path, err)
		}
		n, err := eng.IngestReaderFiltered(ctx, f, normalizedQuery, storage.IngestOpts{
			Source:     src,
			SourceType: sourcetype,
		})
		if closeErr := f.Close(); closeErr != nil {
			return fmt.Errorf("close %s: %w", path, closeErr)
		}
		if err != nil {
			return fmt.Errorf("ingest %s: %w", path, err)
		}
		totalEvents += n
	}

	result, qstats, err := eng.Query(ctx, normalizedQuery, storage.QueryOpts{MaxMemory: memLimit})
	if err != nil {
		return fmt.Errorf("%s", spl2.FormatParseError(err, normalizedQuery))
	}

	qstats.TotalDuration = time.Since(start)
	qstats.ScannedRows = int64(totalEvents)

	if len(result.Rows) == 0 {
		if failEmpty {
			printMeta("No results found.")

			return noResultsError{}
		}

		printHint("No results. Try: lynxdb query --file <file> '| stats count' to verify data loads.")
	}

	return printLocalResults(result.Rows, qstats, outputFile, analyze)
}

func runQueryStdin(query, source, sourcetype, outputFile string, failEmpty bool, analyze, maxMemory string) error {
	var memLimit int64
	if maxMemory != "" {
		b, err := config.ParseByteSize(maxMemory)
		if err != nil {
			return fmt.Errorf("invalid --max-memory: %w", err)
		}
		memLimit = int64(b)
	}

	eng := storage.NewEphemeralEngine()
	defer eng.Close()
	start := time.Now()

	src := source
	if src == "" {
		src = "stdin"
	}
	normalizedQuery := ensureFromClause(query)

	if hints := spl2.DetectCompatHints(normalizedQuery); len(hints) > 0 {
		fmt.Fprint(os.Stderr, spl2.FormatCompatHints(hints))
	}

	ctx := context.Background()
	result, qstats, err := eng.QueryReader(ctx, os.Stdin, normalizedQuery, storage.IngestOpts{
		Source:     src,
		SourceType: sourcetype,
	}, storage.QueryOpts{MaxMemory: memLimit})
	if err != nil {
		return fmt.Errorf("%s", spl2.FormatParseError(err, normalizedQuery))
	}

	qstats.TotalDuration = time.Since(start)

	if len(result.Rows) == 0 {
		if failEmpty {
			printMeta("No results found.")

			return noResultsError{}
		}

		printHint("No results. Check your filter or try a broader query.")
	}

	return printLocalResults(result.Rows, qstats, outputFile, analyze)
}

func printLocalResults(rows []map[string]interface{}, st *stats.QueryStats, outputFile, analyze string) error {
	var w io.Writer = os.Stdout
	if outputFile != "" {
		f, err := os.Create(outputFile)
		if err != nil {
			return fmt.Errorf("create output file: %w", err)
		}
		defer f.Close()
		w = f
	}

	f := output.DetectFormat(output.Format(globalFormat), rows)

	serStart := time.Now()
	if err := f.Format(w, rows); err != nil {
		return fmt.Errorf("format output: %w", err)
	}

	st.SerializeDuration = time.Since(serStart)
	st.ResultRows = int64(len(rows))

	if analyze != "" {
		st.Recommendations = stats.GenerateRecommendations(st)

		if isJSONFormat() {
			return stats.FormatProfileJSON(os.Stderr, st)
		}

		stats.FormatProfile(os.Stderr, st)

		return nil
	}

	if !globalNoStats {
		fmt.Fprintln(os.Stderr)

		if isTTY() {
			fmt.Fprintln(os.Stderr, ui.HRuleSep())
			var statsBuf bytes.Buffer
			stats.FormatTTY(&statsBuf, st, globalVerbose, globalQuiet)
			fmt.Fprintln(os.Stderr, ui.Stderr.Dim.Render(strings.TrimRight(statsBuf.String(), "\n")))
		} else if !globalQuiet {
			_ = stats.FormatJSON(os.Stderr, st)
		}
	}

	return nil
}

func runQueryServer(query, since, from, to, timeout string, failEmpty bool, analyze string) error {
	var earliest, latest string
	if since != "" {
		tr, err := timerange.FromSince(since, time.Now())
		if err != nil {
			return fmt.Errorf("invalid --since: %w", err)
		}
		earliest = tr.Earliest.Format(time.RFC3339Nano)
		latest = tr.Latest.Format(time.RFC3339Nano)
	} else if from != "" && to != "" {
		tr, err := timerange.FromAbsoluteRange(from, to)
		if err != nil {
			return fmt.Errorf("invalid time range: %w", err)
		}
		earliest = tr.Earliest.Format(time.RFC3339Nano)
		latest = tr.Latest.Format(time.RFC3339Nano)
	} else if from != "" || to != "" {
		return fmt.Errorf("--from and --to must both be specified")
	}

	ctx := context.Background()
	if timeout != "" {
		dur, err := time.ParseDuration(timeout)
		if err != nil {
			return fmt.Errorf("invalid --timeout: %w", err)
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, dur)
		defer cancel()
	}

	if isTTY() {
		return doQueryTUI(ctx, query, since, earliest, latest, failEmpty, analyze)
	}

	return doQueryPlain(ctx, query, since, earliest, latest, failEmpty, analyze)
}

func doQueryPlain(ctx context.Context, query, since, earliest, latest string, failEmpty bool, analyze string) error {
	start := time.Now()

	result, err := apiClient().Query(ctx, client.QueryRequest{
		Q:       query,
		From:    earliest,
		To:      latest,
		Profile: analyze,
	})
	if err != nil {
		return &queryError{inner: err, query: query}
	}

	// Handle async fallback (HTTP 202 → job).
	if result.Type == client.ResultTypeJob && result.Job != nil {
		return handleAsyncFallback(ctx, result.Job.JobID, failEmpty, analyze)
	}

	rows := queryResultToRows(result)
	if len(rows) == 0 {
		printEmptyResultGuidance(query, since)

		if failEmpty {
			return noResultsError{}
		}

		return nil
	}

	setClipboardRows(rows)

	if err := printFormattedRows(rows); err != nil {
		return err
	}

	st := buildQueryStatsFromMeta(result.Meta, int64(len(rows)), time.Since(start))

	if analyze != "" {
		st.Recommendations = stats.GenerateRecommendations(st)

		if isJSONFormat() {
			return stats.FormatProfileJSON(os.Stderr, st)
		}

		stats.FormatProfile(os.Stderr, st)

		return nil
	}

	if !globalNoStats {
		fmt.Fprintln(os.Stderr)

		if isTTY() {
			fmt.Fprintln(os.Stderr, ui.HRuleSep())
			var statsBuf bytes.Buffer
			stats.FormatTTY(&statsBuf, st, globalVerbose, globalQuiet)
			fmt.Fprintln(os.Stderr, ui.Stderr.Dim.Render(strings.TrimRight(statsBuf.String(), "\n")))
		} else if !globalQuiet {
			_ = stats.FormatJSON(os.Stderr, st)
		}
	}

	return nil
}

// printFormattedRows formats rows using the global --format flag and writes to stdout.
// Single point of output formatting for all commands that produce row data.
func printFormattedRows(rows []map[string]interface{}) error {
	f := output.DetectFormat(output.Format(globalFormat), rows)

	return f.Format(os.Stdout, rows)
}

// queryResultToRows converts a typed *client.QueryResult to []map[string]interface{}.
// Used by count.go, diff.go, sample.go, and the plain query path.
func queryResultToRows(result *client.QueryResult) []map[string]interface{} {
	if result == nil {
		return nil
	}

	switch result.Type {
	case client.ResultTypeEvents:
		if result.Events != nil {
			return result.Events.Events
		}
	case client.ResultTypeAggregate, client.ResultTypeTimechart:
		if result.Aggregate != nil {
			rows := make([]map[string]interface{}, 0, len(result.Aggregate.Rows))
			for _, row := range result.Aggregate.Rows {
				m := make(map[string]interface{}, len(result.Aggregate.Columns))
				for j, col := range result.Aggregate.Columns {
					if j < len(row) {
						m[col] = row[j]
					}
				}

				rows = append(rows, m)
			}

			return rows
		}
	}

	return nil
}

// buildQueryStatsFromMeta converts client.Meta (from a REST API response) into
// a *stats.QueryStats suitable for FormatTTY / FormatJSON display.
func buildQueryStatsFromMeta(meta client.Meta, rowsReturned int64, totalDuration time.Duration) *stats.QueryStats {
	ss := meta.Stats
	if ss == nil {
		// Minimal stats from basic meta fields.
		st := stats.FromMeta(meta.TookMS, meta.Scanned, rowsReturned)
		st.TotalDuration = totalDuration

		return st
	}

	st := stats.FromSearchStats(
		ss.RowsScanned,
		rowsReturned,
		meta.TookMS,
		ss.SegmentsTotal,
		ss.SegmentsScanned,
		ss.SegmentsSkippedIdx,
		ss.SegmentsSkippedTime,
		ss.SegmentsSkippedStat,
		ss.SegmentsSkippedBF,
		ss.SegmentsSkippedRange,
		meta.SegmentsErrored,
		ss.BufferedEvents,
		ss.InvertedIndexHits,
		ss.CountStarOptimized,
		ss.PartialAggUsed,
		ss.TopKUsed,
		ss.PrefetchUsed,
		ss.VectorizedFilterUsed,
		ss.DictFilterUsed,
		ss.JoinStrategy,
		ss.ScanMS,
		ss.PipelineMS,
		ss.IndexesUsed,
		ss.AcceleratedBy,
	)
	st.TotalDuration = totalDuration
	st.MatchedRows = ss.MatchedRows
	st.CacheHit = ss.CacheHit
	st.MVStatus = ss.MVStatus
	st.MVSpeedup = ss.MVSpeedup
	st.MVOriginalScan = ss.MVOriginalScan

	// Parse/optimize timing from planner.
	if ss.ParseMS > 0 {
		st.ParseDuration = time.Duration(ss.ParseMS * float64(time.Millisecond))
	}
	if ss.OptimizeMS > 0 {
		st.OptimizeDuration = time.Duration(ss.OptimizeMS * float64(time.Millisecond))
	}

	// Optimizer rule details.
	for _, r := range ss.OptimizerRules {
		st.OptimizerRules = append(st.OptimizerRules, stats.OptimizerRuleDetail{
			Name:        r.Name,
			Description: r.Description,
			Count:       r.Count,
		})
	}
	st.OptimizerTotalRules = ss.TotalRules

	// Total processed bytes for throughput display.
	st.ProcessedBytes = ss.ProcessedBytes

	// I/O bytes breakdown.
	st.DiskBytesRead = ss.DiskBytesRead
	st.S3BytesRead = ss.S3BytesRead
	st.CacheBytesRead = ss.CacheBytesRead

	// Scan/pipeline timing breakdown (server-side).
	if ss.ScanMS > 0 {
		st.ScanDuration = time.Duration(ss.ScanMS * float64(time.Millisecond))
	}
	if ss.PipelineMS > 0 {
		st.PipelineDuration = time.Duration(ss.PipelineMS * float64(time.Millisecond))
	}

	// Server-side resource usage.
	st.PeakMemoryBytes = ss.PeakMemoryBytes
	st.MemAllocBytes = ss.MemAllocBytes
	st.SpilledToDisk = ss.SpilledToDisk
	st.SpillBytes = ss.SpillBytes
	st.SpillFiles = ss.SpillFiles
	st.PoolUtilization = ss.PoolUtilization
	st.Warnings = ss.Warnings
	if ss.CPUUserMS > 0 {
		st.CPUTimeUser = time.Duration(ss.CPUUserMS * float64(time.Millisecond))
	}
	if ss.CPUSysMS > 0 {
		st.CPUTimeSys = time.Duration(ss.CPUSysMS * float64(time.Millisecond))
	}
	for _, ps := range ss.PipelineStages {
		st.Stages = append(st.Stages, stats.StageStats{
			Name:        ps.Name,
			InputRows:   ps.InputRows,
			OutputRows:  ps.OutputRows,
			Duration:    time.Duration(ps.DurationMS * float64(time.Millisecond)),
			MemoryBytes: ps.MemoryBytes,
			SpilledRows: ps.SpilledRows,
			SpillBytes:  ps.SpillBytes,
		})
	}

	// Per-operator memory budget statistics from coordinator.
	for _, b := range ss.OperatorBudgets {
		st.OperatorBudgets = append(st.OperatorBudgets, stats.OperatorBudgetStats{
			Name:      b.Label,
			SoftLimit: b.SoftLimit,
			PeakBytes: b.PeakBytes,
			Phase:     b.Phase,
			Spilled:   b.Spilled,
		})
	}

	// Trace-level profiling fields.
	st.VMCalls = ss.VMCalls
	st.VMTotalTime = time.Duration(ss.VMTotalNS)
	for _, sd := range ss.SegmentDetails {
		st.SegmentDetails = append(st.SegmentDetails, stats.SegmentDetail{
			SegmentID:       sd.SegmentID,
			Source:          sd.Source,
			Rows:            sd.Rows,
			RowsAfterFilter: sd.RowsAfterFilter,
			BloomHit:        sd.BloomHit,
			InvertedUsed:    sd.InvertedUsed,
			ReadDuration:    time.Duration(sd.ReadDurationNS),
			BytesRead:       sd.BytesRead,
		})
	}

	return st
}

func handleAsyncFallback(ctx context.Context, jobID string, failEmpty bool, analyze string) error {
	start := time.Now()

	result, err := apiClient().PollJob(ctx, jobID, nil)
	if err != nil {
		// Check for specific error patterns from the job.
		var apiErr *client.APIError
		if errors.As(err, &apiErr) {
			return apiErr
		}

		return err
	}

	rows := queryResultToRows(result)
	if failEmpty && len(rows) == 0 {
		printMeta("No results found.")

		return noResultsError{}
	}

	setClipboardRows(rows)

	if err := printFormattedRows(rows); err != nil {
		return err
	}

	st := buildQueryStatsFromMeta(result.Meta, int64(len(rows)), time.Since(start))

	if analyze != "" {
		st.Recommendations = stats.GenerateRecommendations(st)

		if isJSONFormat() {
			return stats.FormatProfileJSON(os.Stderr, st)
		}

		stats.FormatProfile(os.Stderr, st)

		return nil
	}

	if !globalNoStats {
		fmt.Fprintln(os.Stderr)

		if isTTY() {
			fmt.Fprintln(os.Stderr, ui.HRuleSep())
			var statsBuf bytes.Buffer
			stats.FormatTTY(&statsBuf, st, globalVerbose, globalQuiet)
			fmt.Fprintln(os.Stderr, ui.Stderr.Dim.Render(strings.TrimRight(statsBuf.String(), "\n")))
		} else if !globalQuiet {
			_ = stats.FormatJSON(os.Stderr, st)
		}
	}

	return nil
}
