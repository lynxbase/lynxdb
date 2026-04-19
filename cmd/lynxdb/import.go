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
	"strconv"
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

var importTimestampAliases = []string{
	"time",
	"_time",
	"timestamp",
	"_timestamp",
	"@timestamp",
	"ts",
	"datetime",
}

var structuredImportKeys = map[string]bool{
	"event":      true,
	"time":       true,
	"source":     true,
	"sourcetype": true,
	"host":       true,
	"index":      true,
	"fields":     true,
}

var importMetadataKeys = map[string]bool{
	"source":     true,
	"sourcetype": true,
	"host":       true,
	"index":      true,
}

func newImportCmd() *cobra.Command {
	var (
		format    string
		source    string
		index     string
		batchSize int
		dryRun    bool
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

  # Import from stdin
  cat events.ndjson | lynxdb import - --format ndjson

  # Import with custom CSV delimiter
  lynxdb import data.tsv --format csv --delimiter '\t'`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if projectRC != nil && projectRC.DefaultSource != "" && !cmd.Flags().Changed("source") {
				source = projectRC.DefaultSource
			}
			return runImport(args[0], importOptions{
				format:    format,
				source:    source,
				index:     index,
				batchSize: batchSize,
				dryRun:    dryRun,
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
	f.StringVar(&delimiter, "delimiter", ",", "Field delimiter for CSV format")

	return cmd
}

type importOptions struct {
	format    string
	source    string
	index     string
	batchSize int
	dryRun    bool
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

	var batch []client.IngestEvent
	var stats importStats
	start := time.Now()

	for scanner.Scan() {
		line := scanner.Text()
		stats.totalBytes += int64(len(line)) + 1

		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		ev, err := normalizeImportJSONLine(trimmed, opts)
		if err != nil {
			stats.failedLines++

			continue
		}

		batch = append(batch, ev)
		stats.totalEvents++

		if len(batch) >= opts.batchSize {
			if !opts.dryRun {
				if err := sendStructuredImportBatch(batch); err != nil {
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
			if err := sendStructuredImportBatch(batch); err != nil {
				return stats, fmt.Errorf("send final batch: %w", err)
			}
		}

		stats.batches++
	}

	return stats, nil
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

	var batch []client.IngestEvent
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

		ev, err := normalizeImportJSONObject(obj, "", opts)
		if err != nil {
			stats.failedLines++

			continue
		}

		batch = append(batch, ev)
		stats.totalEvents++

		if len(batch) >= opts.batchSize {
			if !opts.dryRun {
				if err := sendStructuredImportBatch(batch); err != nil {
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
			if err := sendStructuredImportBatch(batch); err != nil {
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
	if opts.index != "" {
		return importStats{}, fmt.Errorf("--index is not supported with --format esbulk; use ndjson/csv import for custom index routing")
	}

	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var batch []string
	batchEvents := 0
	var stats importStats
	start := time.Now()

	for scanner.Scan() {
		actionLine := strings.TrimSpace(scanner.Text())
		stats.totalBytes += int64(len(actionLine)) + 1

		if actionLine == "" {
			continue
		}

		actionLine, err := normalizeESBulkActionLine(actionLine, opts.source)
		if err != nil {
			stats.failedLines++
			// Try to consume data line.
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

		batch = append(batch, actionLine, dataLine)
		batchEvents++
		stats.totalEvents++

		if batchEvents >= opts.batchSize {
			if !opts.dryRun {
				if err := sendESBulkImportBatch(batch); err != nil {
					return stats, fmt.Errorf("send batch %d: %w", stats.batches+1, err)
				}
			}

			stats.batches++
			printImportProgress(stats.totalEvents, stats.totalBytes, fileSize, stats.failedLines, start)
			batch = batch[:0]
			batchEvents = 0
		}
	}

	if err := scanner.Err(); err != nil {
		return stats, fmt.Errorf("read: %w", err)
	}

	// Flush remaining batch.
	if len(batch) > 0 {
		if !opts.dryRun {
			if err := sendESBulkImportBatch(batch); err != nil {
				return stats, fmt.Errorf("send final batch: %w", err)
			}
		}

		stats.batches++
	}

	return stats, nil
}

// Batch Sending

func sendStructuredImportBatch(events []client.IngestEvent) error {
	ctx := context.Background()

	_, err := apiClient().IngestEvents(ctx, events)
	if err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	return nil
}

func sendESBulkImportBatch(lines []string) error {
	body := strings.Join(lines, "\n")
	if body != "" {
		body += "\n"
	}

	ctx := context.Background()
	result, err := apiClient().ESBulk(ctx, strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	if result.Errors {
		return fmt.Errorf("Elasticsearch bulk response reported item errors")
	}

	return nil
}

func normalizeImportJSONLine(line string, opts importOptions) (client.IngestEvent, error) {
	dec := json.NewDecoder(strings.NewReader(line))
	dec.UseNumber()

	var obj map[string]interface{}
	if err := dec.Decode(&obj); err != nil {
		return client.IngestEvent{}, err
	}

	return normalizeImportJSONObject(obj, line, opts)
}

func normalizeImportJSONObject(obj map[string]interface{}, rawLine string, opts importOptions) (client.IngestEvent, error) {
	if isStructuredImportEnvelope(obj) {
		ev, err := toStructuredImportEvent(obj)
		if err != nil {
			return client.IngestEvent{}, err
		}
		applyImportDefaults(&ev, opts)

		return ev, nil
	}

	rawText := extractImportRawValue(obj)
	if rawText == "" {
		rawText = rawLine
	}
	if rawText == "" {
		encoded, err := json.Marshal(obj)
		if err != nil {
			return client.IngestEvent{}, err
		}
		rawText = string(encoded)
	}

	ev := client.IngestEvent{
		Event: rawText,
	}
	if ts, ok := extractImportEventTime(obj); ok {
		ev.Time = &ts
	}
	if v, ok := obj["source"].(string); ok {
		ev.Source = v
	}
	if v, ok := obj["sourcetype"].(string); ok {
		ev.Sourcetype = v
	}
	if v, ok := obj["host"].(string); ok {
		ev.Host = v
	}
	if v, ok := obj["index"].(string); ok {
		ev.Index = v
	}

	fields := make(map[string]interface{})
	for key, value := range obj {
		if key == "_raw" || key == "event" || importMetadataKeys[key] || isImportTimestampAlias(key) {
			continue
		}
		if scalar, ok := normalizeImportScalar(value); ok {
			fields[key] = scalar
		}
	}
	if len(fields) > 0 {
		ev.Fields = fields
	}

	applyImportDefaults(&ev, opts)

	return ev, nil
}

func isStructuredImportEnvelope(obj map[string]interface{}) bool {
	if _, ok := obj["event"].(string); !ok {
		return false
	}
	for key := range obj {
		if !structuredImportKeys[key] {
			return false
		}
	}

	return true
}

func toStructuredImportEvent(obj map[string]interface{}) (client.IngestEvent, error) {
	msg, _ := obj["event"].(string)
	if msg == "" {
		return client.IngestEvent{}, fmt.Errorf("structured event is missing required %q", "event")
	}

	ev := client.IngestEvent{Event: msg}
	if v, ok := obj["time"]; ok {
		ts, ok := parseImportTimestampValue(v)
		if !ok {
			return client.IngestEvent{}, fmt.Errorf("structured event field %q must be a recognized timestamp", "time")
		}
		ev.Time = &ts
	}
	if v, ok := obj["source"].(string); ok {
		ev.Source = v
	}
	if v, ok := obj["sourcetype"].(string); ok {
		ev.Sourcetype = v
	}
	if v, ok := obj["host"].(string); ok {
		ev.Host = v
	}
	if v, ok := obj["index"].(string); ok {
		ev.Index = v
	}
	if v, ok := obj["fields"]; ok {
		fields, ok := v.(map[string]interface{})
		if !ok {
			return client.IngestEvent{}, fmt.Errorf("structured event field %q must be an object", "fields")
		}
		ev.Fields = make(map[string]interface{}, len(fields))
		for key, value := range fields {
			if scalar, ok := normalizeImportScalar(value); ok {
				ev.Fields[key] = scalar
			}
		}
		if len(ev.Fields) == 0 {
			ev.Fields = nil
		}
	}

	return ev, nil
}

func applyImportDefaults(ev *client.IngestEvent, opts importOptions) {
	if ev.Source == "" && opts.source != "" {
		ev.Source = opts.source
	}
	if ev.Index == "" && opts.index != "" {
		ev.Index = opts.index
	}
}

func extractImportRawValue(obj map[string]interface{}) string {
	if raw, ok := obj["_raw"].(string); ok && raw != "" {
		return raw
	}

	return ""
}

func extractImportEventTime(obj map[string]interface{}) (float64, bool) {
	for _, key := range importTimestampAliases {
		value, ok := obj[key]
		if !ok {
			continue
		}
		if ts, ok := parseImportTimestampValue(value); ok {
			return ts, true
		}
	}

	return 0, false
}

func isImportTimestampAlias(key string) bool {
	for _, alias := range importTimestampAliases {
		if key == alias {
			return true
		}
	}

	return false
}

func parseImportTimestampValue(v interface{}) (float64, bool) {
	switch tv := v.(type) {
	case float64:
		return tv, true
	case int:
		return float64(tv), true
	case int64:
		return float64(tv), true
	case json.Number:
		if i, err := tv.Int64(); err == nil {
			return float64(i), true
		}
		if f, err := tv.Float64(); err == nil {
			return f, true
		}
	case string:
		if tv == "" {
			return 0, false
		}
		if f, err := strconv.ParseFloat(tv, 64); err == nil {
			return f, true
		}
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
			if parsed, err := time.Parse(layout, tv); err == nil {
				return float64(parsed.UnixNano()) / float64(time.Second), true
			}
		}
	}

	return 0, false
}

func normalizeImportScalar(v interface{}) (interface{}, bool) {
	switch tv := v.(type) {
	case string, bool, float64, int, int64:
		return tv, true
	case json.Number:
		if i, err := tv.Int64(); err == nil {
			return i, true
		}
		if f, err := tv.Float64(); err == nil {
			return f, true
		}
	}

	return nil, false
}

func normalizeESBulkActionLine(line, source string) (string, error) {
	var action esBulkImportAction
	if err := json.Unmarshal([]byte(line), &action); err != nil {
		return "", err
	}

	var meta *esBulkImportMeta
	switch {
	case action.Index != nil:
		meta = action.Index
	case action.Create != nil:
		meta = action.Create
	default:
		return "", fmt.Errorf("unsupported bulk action")
	}

	if source == "" {
		return line, nil
	}

	meta.Index = source
	encoded, err := json.Marshal(action)
	if err != nil {
		return "", err
	}

	return string(encoded), nil
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
