package main

import (
	"bufio"
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
	"github.com/lynxbase/lynxdb/pkg/database"
	"github.com/lynxbase/lynxdb/pkg/ingest/ndjson"
	ingestpipeline "github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/ppl"
	"github.com/lynxbase/lynxdb/pkg/query"
	"github.com/lynxbase/lynxdb/pkg/stats"
	"github.com/lynxbase/lynxdb/pkg/timerange"
)

// defaultQueryTimeout bounds a server-backed CLI query when the user passes no
// --timeout. It matches the client's poll ceiling so an async-promoted query
// cannot poll indefinitely.
const defaultQueryTimeout = 6 * time.Minute

// noResultsError is a sentinel error for --fail-on-empty.
type noResultsError struct{}

func (noResultsError) Error() string { return "no results found" }

func init() {
	rootCmd.AddCommand(newQueryCmd())
}

func newQueryCmd() *cobra.Command {
	var (
		file       string
		sourcetype string
		source     string
		outputFile string
		timeout    string
		analyze    bool
		failEmpty  bool
		copyFlag   bool
		explain    bool
		rawMode    bool
	)

	cmd := &cobra.Command{
		Use:     "query [PPL query]",
		Aliases: []string{"q"},
		Short:   "Run a PPL query",
		Long: `Run a PPL query against a LynxDB server, or with --file against local
files. --file - reads standard input. Local input is loaded into the index
main, and a local query that names no source reads main.`,
		Example: `  lynxdb query 'source=main | where level = "error"'                     Search errors
  lynxdb query 'source=main | where level = "error" | stats count() by host'
  lynxdb query 'source=main | where status >= 500 | top 10 uri' --since 1h
  lynxdb query 'source=main level=ERROR' --from -1d@d --to @d             Yesterday, in UTC
  lynxdb query --file app.log 'stats count() by level'                    Query a local file
  cat app.log | lynxdb query --file - 'where duration_ms > 1000'          Query standard input`,
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("requires a PPL query")
			}
			query := strings.Join(args, " ")
			if projectRC != nil && projectRC.DefaultSource != "" && !cmd.Flags().Changed("source") {
				source = projectRC.DefaultSource
			}
			var defaultSince string
			if file == "" && projectRC != nil {
				// default_since of .lynxdbrc bounds server queries only:
				// local files hold data of any age.
				defaultSince = projectRC.DefaultSince
			}
			tr, err := timeRangeOf(cmd.Flags(), defaultSince, time.Now())
			if err != nil {
				return err
			}

			if file == "" {
				if explain {
					return runLocalExplain(query, serverQueryOptions)
				}
				err := runQueryServer(query, tr, timeout, outputFile, failEmpty, analyze)
				if err != nil {
					return err
				}
				if copyFlag {
					return copyLastResultToClipboard()
				}
				return nil
			}

			if copyFlag {
				return fmt.Errorf("--copy is only supported in server mode")
			}
			if explain {
				return runLocalExplain(query, localQueryOptions)
			}
			lq := localQuery{
				query:      query,
				earliest:   tr.from,
				latest:     tr.to,
				source:     source,
				sourcetype: sourcetype,
				outputFile: outputFile,
				failEmpty:  failEmpty,
				analyze:    analyze,
				raw:        rawMode,
			}
			if file == "-" {
				return runQueryStdin(lq)
			}
			return runQueryFile(lq, file)
		},
	}

	f := cmd.Flags()
	f.StringP("since", "s", "", "Read events from this time until now: 15m, -1h, -1d@d, @d, 2026-11-24")
	f.String("from", "", "Start of the time range: -1h, -1d@d, @d, now, 2026-11-24T09:00:00Z")
	f.String("to", "", "End of the time range, exclusive: -1h, @d, now, 2026-11-24T09:00:00Z")
	f.StringVarP(&file, "file", "f", "", "Query local files (path or glob) instead of the server; - reads standard input")
	f.StringVar(&sourcetype, "sourcetype", "", "Override source type detection")
	f.StringVar(&source, "source", "", "Set source metadata")
	f.StringVarP(&outputFile, "output", "o", "", "Write results to file")
	f.StringVar(&timeout, "timeout", "", "Query timeout (e.g., 10s, 5m)")
	f.BoolVar(&analyze, "analyze", false, "Profile query execution")
	f.BoolVar(&failEmpty, "fail-on-empty", false, "Exit with code 6 if no results")
	f.BoolVar(&copyFlag, "copy", false, "Copy results to clipboard as TSV")
	f.BoolVar(&explain, "explain", false, "Show query plan without executing")
	f.BoolVar(&rawMode, "raw", false, "Disable auto-format detection (treat input as plain text)")

	return cmd
}

// serverQueryOptions compiles a query as the server does.
var serverQueryOptions = query.Options{}

// localQueryOptions compiles a query over local input: a query that names no
// source reads the index the input is loaded into.
var localQueryOptions = query.Options{DefaultSource: database.DefaultSource}

// localQuery is a query over local files or standard input.
type localQuery struct {
	query string
	// earliest and latest bound the time of the events the query reads.
	earliest, latest string
	// source and sourcetype are the metadata of the loaded events.
	source     string
	sourcetype string
	outputFile string
	failEmpty  bool
	analyze    bool
	// raw loads lines as text without extracting fields.
	raw bool
}

func runQueryFile(lq localQuery, file string) error {
	matches, err := filepath.Glob(file)
	if err != nil {
		return fmt.Errorf("invalid file pattern: %w", err)
	}
	if len(matches) == 0 {
		return noFilesMatchingError{pattern: file}
	}
	if done, err := checkLocalQuery(lq.query); done || err != nil {
		return err
	}

	defer mem.BoundGarbage(garbageAllowance)()
	db, err := database.Open(database.Config{})
	if err != nil {
		return err
	}
	defer db.Close()
	start := time.Now()

	format := ""
	if !lq.raw {
		format = detectFileFormat(matches[0])
	}

	for _, path := range matches {
		src := lq.source
		if src == "" {
			src = path
		}
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open %s: %w", path, err)
		}
		err = loadLocal(db, f, src, lq.sourcetype, format)
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		if err != nil {
			return fmt.Errorf("load %s: %w", path, err)
		}
	}

	return runLocalQuery(db, lq, start)
}

// garbageAllowance bounds the heap awaiting collection in a process that
// opens a database.
const garbageAllowance = 512 << 20

// loadLocal appends the lines of r to the index main of a local database.
func loadLocal(db *database.DB, r io.Reader, source, sourcetype, format string) error {
	conv, err := ndjson.NewText(format)
	if err != nil {
		return err
	}
	conv.Source, conv.SourceType = source, sourcetype

	return conv.Read(r, func(b []byte) error {
		_, err := db.AppendLines(database.DefaultSource, b)
		return err
	})
}

func runQueryStdin(lq localQuery) error {
	if done, err := checkLocalQuery(lq.query); done || err != nil {
		return err
	}

	defer mem.BoundGarbage(garbageAllowance)()
	db, err := database.Open(database.Config{})
	if err != nil {
		return err
	}
	defer db.Close()
	start := time.Now()

	src := lq.source
	if src == "" {
		src = "stdin"
	}

	var reader io.Reader = os.Stdin
	format := ""
	if !lq.raw {
		format, reader = detectReaderFormat(os.Stdin)
	}

	if err := loadLocal(db, reader, src, lq.sourcetype, format); err != nil {
		return fmt.Errorf("load stdin: %w", err)
	}

	return runLocalQuery(db, lq, start)
}

// checkLocalQuery compiles a local query before any input is read, so a bad
// query fails fast with its position and hint. A query that asks for its
// plan with explain gets it here, and done reports that nothing is left to
// run.
func checkLocalQuery(q string) (done bool, err error) {
	c, err := query.CompileWith(q, localQueryOptions)
	if err != nil {
		return true, compileError(q, err)
	}
	if c.Explain {
		fmt.Print(query.Explain(c))
		return true, nil
	}
	return false, nil
}

// options returns the options that bound lq to its time range. Relative
// bounds count from now, which is after the input loaded: a line without a
// timestamp has its load time.
func (lq localQuery) options() (database.QueryOptions, error) {
	now := time.Now()
	tr, err := timerange.ParseOptionalRange(lq.earliest, lq.latest, now)
	if err != nil || tr == nil {
		return database.QueryOptions{Now: now}, err
	}
	return database.QueryOptions{From: tr.Earliest, To: tr.Latest, Now: now}, nil
}

// runLocalQuery runs a query over the events loaded into a local database
// and prints the result.
func runLocalQuery(db *database.DB, lq localQuery, start time.Time) error {
	if err := db.Sync(); err != nil {
		return err
	}
	st := &stats.QueryStats{}
	for _, ix := range db.Stats() {
		st.ScannedRows += int64(ix.Rows)
	}
	compileStart := time.Now()
	c, err := query.CompileWith(lq.query, localQueryOptions)
	if err != nil {
		return compileError(lq.query, err)
	}
	st.ParseDuration = time.Since(compileStart)
	execStart := time.Now()
	opts, err := lq.options()
	if err != nil {
		return err
	}
	res, err := db.Query(context.Background(), c, opts)
	if err != nil {
		return compileError(lq.query, err)
	}
	st.ExecDuration = time.Since(execStart)
	st.TotalDuration = time.Since(start)
	maps := res.Maps()
	rows := make([]map[string]interface{}, len(maps))
	for i, m := range maps {
		row := make(map[string]interface{}, len(m))
		for k, v := range m {
			row[k] = v.Interface()
		}
		rows[i] = row
	}
	if len(rows) == 0 {
		if lq.failEmpty {
			printQueryWarnings(res.Warnings)
			printMeta("No results found.")
			return noResultsError{}
		}
		printHint("No results. Try a broader query.")
	}

	return printLocalResults(rows, res.Warnings, st, lq.outputFile, lq.analyze)
}

// printQueryWarnings prints, on stderr, what a query could not know to be
// an error.
func printQueryWarnings(warnings []string) {
	for _, w := range warnings {
		printWarning("%s", w)
	}
}

// compileError renders a PPL compile error with a caret under its
// position; other errors pass through.
func compileError(q string, err error) error {
	pe, ok := ppl.AsError(err)
	if !ok {
		return err
	}
	if !globalQuiet && pe.Pos.Line > 0 {
		lines := strings.Split(q, "\n")
		if pe.Pos.Line <= len(lines) {
			fmt.Fprintf(os.Stderr, "  %s\n  %s^\n", lines[pe.Pos.Line-1], strings.Repeat(" ", max(pe.Pos.Col-1, 0)))
		}
	}
	return pe
}

// runLocalExplain prints the logical and optimized plans of a query without
// running it.
func runLocalExplain(q string, opts query.Options) error {
	c, err := query.CompileWith(q, opts)
	if err != nil {
		return compileError(q, err)
	}
	fmt.Print(query.Explain(c))
	return nil
}

const formatPeekLines = 20

// detectFileFormat reports the log format whose fields the CLI extracts
// while loading a file, or "" for none.
func detectFileFormat(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024)
	var lines []string
	for scanner.Scan() && len(lines) < formatPeekLines {
		lines = append(lines, scanner.Text())
	}
	return extractedFormat(lines)
}

// detectReaderFormat peeks at the first lines of r and returns the format to
// extract and a reader that still yields every byte of r.
func detectReaderFormat(r io.Reader) (string, io.Reader) {
	br := bufio.NewReaderSize(r, 64*1024)
	var peek bytes.Buffer
	var lines []string
	for len(lines) < formatPeekLines {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			peek.Write(line)
			lines = append(lines, strings.TrimRight(string(line), "\r\n"))
		}
		if err != nil {
			break
		}
	}
	return extractedFormat(lines), io.MultiReader(bytes.NewReader(peek.Bytes()), br)
}

func extractedFormat(lines []string) string {
	if len(lines) == 0 {
		return ""
	}
	format, ratio := ingestpipeline.DetectFormatWithRatio(lines)
	name := ingestpipeline.ExtractFormatName(format)
	if name != "" {
		fmt.Fprintf(os.Stderr, "  ℹ Detected %s (%d%% of lines); fields are extracted while loading. Use --raw to disable.\n",
			ingestpipeline.FormatDisplayName(format), int(ratio*100+0.5))
	}
	return name
}

// printLocalResults prints rows, then the warnings of the query, then its
// statistics.
func printLocalResults(rows []map[string]interface{}, warnings []string, st *stats.QueryStats, outputFile string, analyze bool) error {
	var w io.Writer = os.Stdout
	if outputFile != "" {
		f, err := os.Create(outputFile)
		if err != nil {
			return fmt.Errorf("create output file: %w", err)
		}
		defer f.Close()
		w = f
	}

	tableTheme := ui.Stdout
	if outputFile != "" {
		tableTheme = nil
	}
	f := output.DetectFormatWithOptions(output.Format(globalFormat), rows, output.HumanTableOptions{
		Theme:     tableTheme,
		Compact:   globalCompact,
		MaxRows:   globalMaxRows,
		NullValue: globalNullValue,
		MaxWidth:  globalMaxWidth,
	})

	serStart := time.Now()
	if err := f.Format(w, rows); err != nil {
		return fmt.Errorf("format output: %w", err)
	}

	st.SerializeDuration = time.Since(serStart)
	st.ResultRows = int64(len(rows))
	printQueryWarnings(warnings)

	if analyze {
		if isJSONFormat() {
			return stats.FormatProfileJSON(os.Stderr, st)
		}

		stats.FormatProfile(os.Stderr, st)

		return nil
	}

	if !globalNoStats {
		fmt.Fprintln(os.Stderr)

		if isTTY() {
			printQueryInsightFooter(st)
		} else if !globalQuiet {
			_ = stats.FormatJSON(os.Stderr, st)
		}
	}

	return nil
}

func runQueryServer(query string, tr timeRange, timeout, outputFile string, failEmpty, analyze bool) error {
	ctx := context.Background()
	if timeout != "" {
		dur, err := time.ParseDuration(timeout)
		if err != nil {
			return fmt.Errorf("invalid --timeout: %w", err)
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, dur)
		defer cancel()
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
	}

	return doQueryPlain(ctx, query, tr.since, tr.from, tr.to, outputFile, failEmpty, analyze)
}

func doQueryPlain(ctx context.Context, query, since, earliest, latest, outputFile string, failEmpty, analyze bool) error {
	start := time.Now()

	result, err := apiClient().Query(ctx, client.QueryRequest{
		Q:    query,
		From: earliest,
		To:   latest,
	})
	if err != nil {
		return &queryError{inner: err, query: query}
	}

	// Handle async fallback (HTTP 202 → job).
	if result.Type == client.ResultTypeJob && result.Job != nil {
		return handleAsyncFallback(ctx, result.Job.JobID, outputFile, failEmpty, analyze)
	}

	rows := queryResultToRows(result)
	if len(rows) == 0 {
		printQueryWarnings(result.Meta.Warnings)
		printEmptyResultGuidance(query, since)

		if failEmpty {
			return noResultsError{}
		}

		return nil
	}

	setClipboardRows(rows)

	if err := printFormattedRows(rows, outputFile); err != nil {
		return err
	}
	printQueryWarnings(result.Meta.Warnings)

	st := buildQueryStatsFromMeta(result.Meta, int64(len(rows)), time.Since(start))

	if analyze {
		if isJSONFormat() {
			return stats.FormatProfileJSON(os.Stderr, st)
		}

		stats.FormatProfile(os.Stderr, st)

		return nil
	}

	if !globalNoStats {
		fmt.Fprintln(os.Stderr)

		if isTTY() {
			printQueryInsightFooter(st)
		} else if !globalQuiet {
			_ = stats.FormatJSON(os.Stderr, st)
		}
	}

	return nil
}

// printFormattedRows formats the rows of a server query with --format and
// writes them to stdout, or to outputFile when it is set.
func printFormattedRows(rows []map[string]interface{}, outputFile string) error {
	if outputFile == "" {
		f := output.DetectFormatWithOptions(output.Format(globalFormat), rows, humanOutputOptions(ui.Stdout))

		return f.Format(os.Stdout, rows)
	}
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	f := output.DetectFormatWithOptions(output.Format(globalFormat), rows, humanOutputOptions(nil))
	if err := f.Format(file, rows); err != nil {
		file.Close()
		return fmt.Errorf("format output: %w", err)
	}

	return file.Close()
}

func printQueryInsightFooter(st *stats.QueryStats) {
	if globalQuiet || st == nil {
		return
	}

	fmt.Fprintln(os.Stderr, ui.HRuleSep())

	metrics := []ui.Metric{
		{Label: "Rows", Value: formatCount(st.ResultRows)},
		{Label: "Took", Value: formatElapsed(st.TotalDuration)},
	}
	if st.ScannedRows > 0 {
		metrics = append(metrics, ui.Metric{Label: "Scanned", Value: formatCount(st.ScannedRows)})
	}

	fmt.Fprintln(os.Stderr, ui.Stderr.MetricGrid(metrics, globalCompact))

	if globalVerbose {
		var statsBuf bytes.Buffer
		stats.FormatTTY(&statsBuf, st, true, false)
		detail := strings.TrimRight(statsBuf.String(), "\n")
		if detail != "" {
			fmt.Fprintln(os.Stderr)
			fmt.Fprintln(os.Stderr, ui.Stderr.Dim.Render(detail))
		}
	}
}

// queryResultToRows converts a typed *client.QueryResult to []map[string]interface{}.
func queryResultToRows(result *client.QueryResult) []map[string]interface{} {
	if result == nil {
		return nil
	}

	switch result.Type {
	case client.ResultTypeEvents:
		if result.Events != nil {
			return result.Events.Events
		}
	case client.ResultTypeAggregate:
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

// buildQueryStatsFromMeta turns the meta of a REST API response into a
// *stats.QueryStats for FormatTTY / FormatJSON display.
func buildQueryStatsFromMeta(meta client.Meta, rowsReturned int64, totalDuration time.Duration) *stats.QueryStats {
	st := stats.FromMeta(meta.TookMS, 0, rowsReturned)
	st.TotalDuration = totalDuration

	return st
}

func handleAsyncFallback(ctx context.Context, jobID, outputFile string, failEmpty, analyze bool) error {
	start := time.Now()

	result, err := apiClient().PollJob(ctx, jobID, nil)
	if err != nil {
		var apiErr *client.APIError
		if errors.As(err, &apiErr) {
			return apiErr
		}

		return err
	}

	rows := queryResultToRows(result)
	if failEmpty && len(rows) == 0 {
		printQueryWarnings(result.Meta.Warnings)
		printMeta("No results found.")

		return noResultsError{}
	}

	setClipboardRows(rows)

	if err := printFormattedRows(rows, outputFile); err != nil {
		return err
	}
	printQueryWarnings(result.Meta.Warnings)

	st := buildQueryStatsFromMeta(result.Meta, int64(len(rows)), time.Since(start))

	if analyze {
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
