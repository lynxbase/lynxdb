package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"charm.land/bubbles/v2/progress"
	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
)

func init() {
	rootCmd.AddCommand(newImportCmd())
}

// Supported import formats.
const (
	importFormatAuto   = "auto"
	importFormatNDJSON = "ndjson"
	importFormatCSV    = "csv"
	importFormatESBulk = "esbulk"
)

func newImportCmd() *cobra.Command {
	var (
		format    string
		source    string
		index     string
		batchSize int
		dryRun    bool
		transform string
		delimiter string
	)

	cmd := &cobra.Command{
		Use:   "import <file>",
		Short: "Import data from files (NDJSON, CSV, Elasticsearch bulk)",
		Long: `Bulk import data from structured files into a running LynxDB server.
Supports NDJSON, CSV, and Elasticsearch _bulk export formats.
Format is auto-detected from file extension and content, or set explicitly with --format.

Unlike 'ingest' which handles raw log lines, 'import' understands structured
formats and preserves field types, timestamps, and metadata from the source system.`,
		Example: `  # Import NDJSON (auto-detected)
  lynxdb import events.json
  lynxdb import events.ndjson

  # Import CSV with headers
  lynxdb import splunk_export.csv
  lynxdb import data.csv --source web-01 --index nginx

  # Import Elasticsearch _bulk export
  lynxdb import es_dump.json --format esbulk

  # Validate without importing (dry run)
  lynxdb import events.json --dry-run

  # Apply SPL2 transform during import
  lynxdb import events.json --transform '| where level!="DEBUG"'

  # Import from stdin
  cat events.ndjson | lynxdb import - --format ndjson

  # Import with custom CSV delimiter
  lynxdb import data.tsv --format csv --delimiter '\t'`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runImport(args[0], importOptions{
				format:    format,
				source:    source,
				index:     index,
				batchSize: batchSize,
				dryRun:    dryRun,
				transform: transform,
				delimiter: delimiter,
			})
		},
	}

	f := cmd.Flags()
	f.StringVar(&format, "format", importFormatAuto, "Input format: auto, ndjson, csv, esbulk")
	f.StringVar(&source, "source", "", "Source metadata for all events")
	f.StringVar(&index, "index", "", "Target index name")
	f.IntVar(&batchSize, "batch-size", 5000, "Number of events per batch")
	f.BoolVar(&dryRun, "dry-run", false, "Validate and count events without importing")
	f.StringVar(&transform, "transform", "", "SPL2 pipeline to apply during import (e.g., '| where level!=\"DEBUG\"')")
	f.StringVar(&delimiter, "delimiter", ",", "Field delimiter for CSV format")

	return cmd
}

type importOptions struct {
	format    string
	source    string
	index     string
	batchSize int
	dryRun    bool
	transform string
	delimiter string
}

type importStats struct {
	totalEvents int
	totalBytes  int64
	failedLines int
	batches     int
}

func runImport(file string, opts importOptions) error {
	if opts.batchSize <= 0 {
		return fmt.Errorf("--batch-size must be a positive integer (got %d)", opts.batchSize)
	}

	var input *os.File
	var fileSize int64
	var fileName string

	if file == "-" {
		input = os.Stdin
		fileName = "stdin"
		if opts.format == importFormatAuto {
			return fmt.Errorf("cannot auto-detect format from stdin; use --format (ndjson, csv, esbulk)")
		}
	} else {
		f, err := os.Open(file)
		if err != nil {
			return fmt.Errorf("open file: %w", err)
		}
		defer f.Close()
		input = f
		fileName = filepath.Base(file)

		if fi, err := f.Stat(); err == nil {
			fileSize = fi.Size()
		}

		if opts.format == importFormatAuto {
			opts.format = detectImportFormat(file, input)
			// Seek back after peeking for format detection.
			if _, err := input.Seek(0, io.SeekStart); err != nil {
				return fmt.Errorf("seek: %w", err)
			}
			printHint("Auto-detected format: %s. Use --format to override.", opts.format)
		}
	}

	if opts.dryRun {
		printMeta("Dry run — validating %s (format: %s)", fileName, opts.format)
	} else {
		printMeta("Importing %s (format: %s)", fileName, opts.format)
	}

	start := time.Now()
	var stats importStats

	var err error
	switch opts.format {
	case importFormatNDJSON:
		stats, err = importNDJSON(input, fileSize, opts)
	case importFormatCSV:
		stats, err = importCSV(input, fileSize, opts)
	case importFormatESBulk:
		stats, err = importESBulk(input, fileSize, opts)
	default:
		return fmt.Errorf("unknown format %q; supported: ndjson, csv, esbulk", opts.format)
	}

	if err != nil {
		return err
	}

	// Clear progress line.
	if !globalQuiet && isTTY() {
		fmt.Fprintf(os.Stderr, "\r%s\r", strings.Repeat(" ", 80))
	}

	elapsed := time.Since(start)
	printImportSummary(stats, elapsed, opts.dryRun)

	return nil
}

// detectImportFormat guesses the format from file extension and content.
func detectImportFormat(path string, f *os.File) string {
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".csv", ".tsv":
		return importFormatCSV
	case ".ndjson", ".jsonl":
		return importFormatNDJSON
	}

	// For .json files, peek at the first line to decide.
	if ext == ".json" || ext == ".log" || ext == "" {
		return peekFormatFromContent(f)
	}

	// Default to NDJSON for unknown extensions.
	return importFormatNDJSON
}

// peekFormatFromContent reads the first non-empty lines to guess the format.
func peekFormatFromContent(f *os.File) string {
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	linesRead := 0
	for scanner.Scan() && linesRead < 3 {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		linesRead++

		// ES bulk format: action lines contain {"index":, {"create":, etc.
		if linesRead == 1 && isESBulkActionLine(line) {
			return importFormatESBulk
		}

		// NDJSON: each line is a JSON object.
		if strings.HasPrefix(line, "{") {
			return importFormatNDJSON
		}

		// If it contains commas and doesn't start with {, likely CSV.
		if strings.Contains(line, ",") && !strings.HasPrefix(line, "{") {
			return importFormatCSV
		}

		break
	}

	return importFormatNDJSON
}

func isESBulkActionLine(line string) bool {
	// ES bulk action lines look like: {"index":{"_index":"..."}} or {"create":{...}}
	return (strings.Contains(line, `"index"`) || strings.Contains(line, `"create"`)) &&
		strings.Contains(line, `"_index"`)
}

// NDJSON Import

func importNDJSON(input *os.File, fileSize int64, opts importOptions) (importStats, error) {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var batch []string
	var stats importStats
	start := time.Now()

	for scanner.Scan() {
		line := scanner.Text()
		stats.totalBytes += int64(len(line)) + 1

		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		// Validate JSON.
		if !json.Valid([]byte(trimmed)) {
			stats.failedLines++

			continue
		}

		// Apply source/index metadata if specified.
		enriched := enrichNDJSONLine(trimmed, opts.source, opts.index)
		batch = append(batch, enriched)
		stats.totalEvents++

		if len(batch) >= opts.batchSize {
			if !opts.dryRun {
				if err := sendImportBatch(batch, opts); err != nil {
					return stats, fmt.Errorf("send batch %d: %w", stats.batches+1, err)
				}
			}

			stats.batches++
			printImportProgress(stats.totalEvents, stats.totalBytes, fileSize, stats.failedLines, start)
			batch = batch[:0]
		}
	}

	if err := scanner.Err(); err != nil {
		return stats, fmt.Errorf("read: %w", err)
	}

	// Flush remaining batch.
	if len(batch) > 0 {
		if !opts.dryRun {
			if err := sendImportBatch(batch, opts); err != nil {
				return stats, fmt.Errorf("send final batch: %w", err)
			}
		}

		stats.batches++
	}

	return stats, nil
}

func enrichNDJSONLine(line, source, index string) string {
	if source == "" && index == "" {
		return line
	}

	var obj map[string]interface{}
	if json.Unmarshal([]byte(line), &obj) != nil {
		return line
	}

	if source != "" {
		if _, exists := obj["source"]; !exists {
			obj["source"] = source
		}
	}

	if index != "" {
		if _, exists := obj["index"]; !exists {
			obj["index"] = index
		}
	}

	enriched, err := json.Marshal(obj)
	if err != nil {
		return line
	}

	return string(enriched)
}

// CSV Import

func importCSV(input *os.File, fileSize int64, opts importOptions) (importStats, error) {
	// Wrap input in a counting reader for progress.
	cr := &countingReader{r: input}
	reader := csv.NewReader(cr)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	if opts.delimiter != "" && len(opts.delimiter) == 1 {
		reader.Comma = rune(opts.delimiter[0])
	} else if opts.delimiter == `\t` {
		reader.Comma = '\t'
	}

	// Read header row.
	headers, err := reader.Read()
	if err != nil {
		return importStats{}, fmt.Errorf("read CSV headers: %w", err)
	}

	// Trim BOM from first header if present.
	if len(headers) > 0 {
		headers[0] = strings.TrimPrefix(headers[0], "\xef\xbb\xbf")
	}

	var batch []string
	var stats importStats
	start := time.Now()

	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			stats.failedLines++

			continue
		}

		obj := csvRecordToJSON(headers, record, opts.source, opts.index)

		jsonLine, err := json.Marshal(obj)
		if err != nil {
			stats.failedLines++

			continue
		}

		batch = append(batch, string(jsonLine))
		stats.totalEvents++

		if len(batch) >= opts.batchSize {
			if !opts.dryRun {
				if err := sendImportBatch(batch, opts); err != nil {
					return stats, fmt.Errorf("send batch %d: %w", stats.batches+1, err)
				}
			}

			stats.batches++
			stats.totalBytes = int64(cr.n)
			printImportProgress(stats.totalEvents, stats.totalBytes, fileSize, stats.failedLines, start)
			batch = batch[:0]
		}
	}

	stats.totalBytes = int64(cr.n)

	// Flush remaining batch.
	if len(batch) > 0 {
		if !opts.dryRun {
			if err := sendImportBatch(batch, opts); err != nil {
				return stats, fmt.Errorf("send final batch: %w", err)
			}
		}

		stats.batches++
	}

	return stats, nil
}

func csvRecordToJSON(headers, record []string, source, index string) map[string]interface{} {
	obj := make(map[string]interface{}, len(headers)+2)

	for i, header := range headers {
		if i >= len(record) {
			break
		}

		val := record[i]
		if val == "" {
			continue
		}

		// Map well-known Splunk export fields.
		switch header {
		case "_time":
			obj["_time"] = val
			// Also try to parse and set as timestamp.
			if t, err := time.Parse(time.RFC3339Nano, val); err == nil {
				obj["timestamp"] = t.Format(time.RFC3339Nano)
			} else if t, err := time.Parse("2006-01-02 15:04:05", val); err == nil {
				obj["timestamp"] = t.Format(time.RFC3339Nano)
			}
		case "_raw":
			obj["_raw"] = val
		default:
			obj[header] = tryParseNumber(val)
		}
	}

	if source != "" {
		if _, exists := obj["source"]; !exists {
			obj["source"] = source
		}
	}

	if index != "" {
		if _, exists := obj["index"]; !exists {
			obj["index"] = index
		}
	}

	return obj
}

// tryParseNumber attempts to parse a string as int64 or float64.
// Returns the original string if it doesn't look numeric.
func tryParseNumber(s string) interface{} {
	// Quick check: must start with digit, minus, or dot.
	if s == "" {
		return s
	}

	ch := s[0]
	if ch != '-' && ch != '.' && (ch < '0' || ch > '9') {
		return s
	}

	// Try integer first.
	var i int64
	if _, err := fmt.Sscanf(s, "%d", &i); err == nil && fmt.Sprintf("%d", i) == s {
		return i
	}

	// Try float.
	var f float64
	if _, err := fmt.Sscanf(s, "%g", &f); err == nil {
		return f
	}

	return s
}

// ES Bulk Import

// esBulkImportAction represents an action line in Elasticsearch _bulk format.
type esBulkImportAction struct {
	Index  *esBulkImportMeta `json:"index,omitempty"`
	Create *esBulkImportMeta `json:"create,omitempty"`
}

// esBulkImportMeta holds the metadata from an ES bulk action line.
type esBulkImportMeta struct {
	Index string `json:"_index"`
}

func importESBulk(input *os.File, fileSize int64, opts importOptions) (importStats, error) {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var batch []string
	var stats importStats
	start := time.Now()

	for scanner.Scan() {
		actionLine := strings.TrimSpace(scanner.Text())
		stats.totalBytes += int64(len(actionLine)) + 1

		if actionLine == "" {
			continue
		}

		// Parse action line.
		var action esBulkImportAction
		if json.Unmarshal([]byte(actionLine), &action) != nil {
			stats.failedLines++
			// Try to consume data line.
			if scanner.Scan() {
				stats.totalBytes += int64(len(scanner.Text())) + 1
			}

			continue
		}

		// Determine index from action.
		var esIndex string

		switch {
		case action.Index != nil:
			esIndex = action.Index.Index
		case action.Create != nil:
			esIndex = action.Create.Index
		default:
			// Unknown action type (delete, update) — skip.
			stats.failedLines++
			if scanner.Scan() {
				stats.totalBytes += int64(len(scanner.Text())) + 1
			}

			continue
		}

		// Read data line.
		if !scanner.Scan() {
			stats.failedLines++

			break
		}

		dataLine := strings.TrimSpace(scanner.Text())
		stats.totalBytes += int64(len(dataLine)) + 1

		if !json.Valid([]byte(dataLine)) {
			stats.failedLines++

			continue
		}

		// Enrich with source from _index.
		enriched := enrichESBulkLine(dataLine, esIndex, opts.source, opts.index)
		batch = append(batch, enriched)
		stats.totalEvents++

		if len(batch) >= opts.batchSize {
			if !opts.dryRun {
				if err := sendImportBatch(batch, opts); err != nil {
					return stats, fmt.Errorf("send batch %d: %w", stats.batches+1, err)
				}
			}

			stats.batches++
			printImportProgress(stats.totalEvents, stats.totalBytes, fileSize, stats.failedLines, start)
			batch = batch[:0]
		}
	}

	if err := scanner.Err(); err != nil {
		return stats, fmt.Errorf("read: %w", err)
	}

	// Flush remaining batch.
	if len(batch) > 0 {
		if !opts.dryRun {
			if err := sendImportBatch(batch, opts); err != nil {
				return stats, fmt.Errorf("send final batch: %w", err)
			}
		}

		stats.batches++
	}

	return stats, nil
}

func enrichESBulkLine(line, esIndex, source, index string) string {
	var obj map[string]interface{}
	if json.Unmarshal([]byte(line), &obj) != nil {
		return line
	}

	// Map ES @timestamp to LynxDB timestamp.
	if ts, ok := obj["@timestamp"]; ok {
		obj["timestamp"] = ts
		delete(obj, "@timestamp")
	}

	// Set source from ES _index if not explicitly overridden.
	if source != "" {
		obj["source"] = source
	} else if esIndex != "" {
		if _, exists := obj["source"]; !exists {
			obj["source"] = esIndex
		}
	}

	if index != "" {
		obj["index"] = index
	}

	enriched, err := json.Marshal(obj)
	if err != nil {
		return line
	}

	return string(enriched)
}

// Batch Sending

func sendImportBatch(lines []string, opts importOptions) error {
	body := strings.Join(lines, "\n")
	ctx := context.Background()

	_, err := apiClient().IngestNDJSON(ctx, strings.NewReader(body), client.IngestOpts{
		Source:    opts.source,
		Index:     opts.index,
		Transform: opts.transform,
	})
	if err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	return nil
}

// Progress & Summary

func printImportProgress(totalEvents int, bytesRead, fileSize int64, failed int, start time.Time) {
	if globalQuiet || !isTTY() {
		return
	}

	t := ui.Stderr

	elapsed := time.Since(start)

	eps := int64(0)
	if elapsed.Seconds() > 0 {
		eps = int64(float64(totalEvents) / elapsed.Seconds())
	}

	failStr := ""
	if failed > 0 {
		failStr = t.Warning.Render(fmt.Sprintf("  %d failed", failed))
	}

	if fileSize > 0 {
		pct := float64(bytesRead) / float64(fileSize)
		if pct > 1 {
			pct = 1
		}

		bar := progress.New(
			progress.WithDefaultBlend(),
			progress.WithWidth(30),
			progress.WithoutPercentage(),
		)
		line := fmt.Sprintf("  Importing %s %3.0f%%  %s events (%s/sec)%s",
			bar.ViewAs(pct), pct*100,
			formatCount(int64(totalEvents)), formatCount(eps), failStr)
		fmt.Fprintf(os.Stderr, "\r%s", line)
	} else {
		fmt.Fprintf(os.Stderr, "\r  %s %s events (%s/sec)%s",
			t.Dim.Render("Importing..."),
			formatCount(int64(totalEvents)), formatCount(eps), failStr)
	}
}

func printImportSummary(stats importStats, elapsed time.Duration, dryRun bool) {
	if dryRun {
		printSuccess("Dry run complete: %s events validated, %d skipped, %s processed",
			formatCount(int64(stats.totalEvents)),
			stats.failedLines,
			formatBytes(stats.totalBytes))

		if stats.failedLines > 0 {
			printWarning("%d lines skipped (invalid format)", stats.failedLines)
		}

		printNextSteps(
			"lynxdb import <file>   Run import without --dry-run to load data",
		)

		return
	}

	eps := int64(0)
	if elapsed.Seconds() > 0 {
		eps = int64(float64(stats.totalEvents) / elapsed.Seconds())
	}

	printSuccess("Imported %s events in %s (%s events/sec)",
		formatCount(int64(stats.totalEvents)),
		formatElapsed(elapsed),
		formatCount(eps))

	if stats.failedLines > 0 {
		printWarning("%d lines skipped (invalid format)", stats.failedLines)
	}

	printNextSteps(
		"lynxdb query '| stats count by source'   Query imported data",
		"lynxdb fields                             Explore field names",
		"lynxdb tail                               Live tail events",
	)
}

// Helpers

// countingReader wraps an io.Reader and counts bytes read.
type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n

	return n, err
}
