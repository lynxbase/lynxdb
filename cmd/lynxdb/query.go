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
		since      string
		from       string
		to         string
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
  lynxdb query --file app.log 'stats count() by level'                    Query a local file
  cat app.log | lynxdb query --file - 'where duration_ms > 1000'          Query standard input`,
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("requires a PPL query")
			}
			query := strings.Join(args, " ")
			if stripped, ok := stripVerticalQuerySuffix(query); ok {
				query = stripped
				if output.Format(globalFormat) == output.FormatAuto {
					previousFormat := globalFormat
					globalFormat = string(output.FormatVertical)
					defer func() {
						globalFormat = previousFormat
					}()
				}
			}
			if projectRC != nil {
				if projectRC.DefaultSince != "" && !cmd.Flags().Changed("since") {
					since = projectRC.DefaultSince
				}
				if projectRC.DefaultSource != "" && !cmd.Flags().Changed("source") {
					source = projectRC.DefaultSource
				}
			}

			if file == "" {
				if explain {
					return runLocalExplain(query, serverQueryOptions)
				}
				err := runQueryServer(query, since, from, to, timeout, failEmpty, analyze)
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
	f.StringVarP(&since, "since", "s", "", "Relative time range (e.g., 15m, 1h, 7d)")
	f.StringVar(&from, "from", "", "Absolute start time (ISO 8601)")
	f.StringVar(&to, "to", "", "Absolute end time (ISO 8601)")
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
	if err := checkLocalQuery(lq.query); err != nil {
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
	if err := checkLocalQuery(lq.query); err != nil {
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
// query fails fast with its position and hint.
func checkLocalQuery(q string) error {
	if _, err := query.CompileWith(q, localQueryOptions); err != nil {
		return compileError(q, err)
	}
	return nil
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
	res, err := db.Query(context.Background(), c, database.QueryOptions{})
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
			printMeta("No results found.")
			return noResultsError{}
		}
		printHint("No results. Try a broader query.")
	}

	return printLocalResults(rows, st, lq.outputFile, lq.analyze)
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

func printLocalResults(rows []map[string]interface{}, st *stats.QueryStats, outputFile string, analyze bool) error {
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

func runQueryServer(query, since, from, to, timeout string, failEmpty, analyze bool) error {
	earliest, latest, err := queryTimeBoundsFromFlags(since, from, to)
	if err != nil {
		return err
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
	} else {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultQueryTimeout)
		defer cancel()
	}

	return doQueryPlain(ctx, query, since, earliest, latest, failEmpty, analyze)
}

func queryTimeBoundsFromFlags(since, from, to string) (string, string, error) {
	if since != "" {
		duration := strings.TrimPrefix(since, "-")
		if _, err := timerange.ParseRelative(duration); err != nil {
			return "", "", fmt.Errorf("invalid --since: %w", err)
		}

		return "-" + duration, "now", nil
	}
	if from != "" && to != "" {
		tr, err := timerange.FromAbsoluteRange(from, to)
		if err != nil {
			return "", "", fmt.Errorf("invalid time range: %w", err)
		}

		return tr.Earliest.Format(time.RFC3339Nano), tr.Latest.Format(time.RFC3339Nano), nil
	}
	if from != "" || to != "" {
		return "", "", fmt.Errorf("--from and --to must both be specified")
	}

	return "", "", nil
}

func stripVerticalQuerySuffix(query string) (string, bool) {
	trimmed := strings.TrimRight(query, " \t\r\n;")
	trimmed = strings.TrimRight(trimmed, " \t\r\n")
	if !strings.HasSuffix(trimmed, `\G`) {
		return query, false
	}

	stripped := strings.TrimRight(strings.TrimSuffix(trimmed, `\G`), " \t\r\n;")
	return stripped, true
}

func doQueryPlain(ctx context.Context, query, since, earliest, latest string, failEmpty, analyze bool) error {
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

// printFormattedRows formats rows using the global --format flag and writes to stdout.
// Single point of output formatting for all commands that produce row data.
func printFormattedRows(rows []map[string]interface{}) error {
	f := output.DetectFormatWithOptions(output.Format(globalFormat), rows, humanOutputOptions(ui.Stdout))

	return f.Format(os.Stdout, rows)
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

func handleAsyncFallback(ctx context.Context, jobID string, failEmpty, analyze bool) error {
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
		printMeta("No results found.")

		return noResultsError{}
	}

	setClipboardRows(rows)

	if err := printFormattedRows(rows); err != nil {
		return err
	}

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
