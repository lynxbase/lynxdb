package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"charm.land/bubbles/v2/progress"
	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
)

func init() {
	rootCmd.AddCommand(newIngestCmd())
}

// Input formats of lynxdb ingest.
const (
	ingestFormatAuto   = "auto"
	ingestFormatNDJSON = "ndjson"
	ingestFormatCSV    = "csv"
	ingestFormatESBulk = "esbulk"
	ingestFormatText   = "text"
)

// maxIngestLine bounds a line of input.
const maxIngestLine = 1 << 20

// ingestOptions holds the flags of lynxdb ingest.
type ingestOptions struct {
	format     string
	source     string
	sourcetype string
	index      string
	delimiter  string
	batchSize  int
	// delimiterSet reports that --delimiter was given.
	delimiterSet bool
}

func newIngestCmd() *cobra.Command {
	var opts ingestOptions

	cmd := &cobra.Command{
		Use:     "ingest [file]",
		Aliases: []string{"i"},
		Short:   "Ingest logs from a file or stdin",
		Long: `Ingest data into a running LynxDB server, from a file, or from standard
input when the file is - or absent.

--format names the input format:
  text     log lines; the server extracts their time and fields (POST /api/v1/ingest/raw)
  ndjson   one JSON object per line (POST /api/v1/ingest)
  csv      a header row, then one event per row with the header names as keys
           and numbers as numbers (POST /api/v1/ingest)
  esbulk   an Elasticsearch bulk request body (POST /_bulk)
  auto     by the file extension (.ndjson, .jsonl, .csv, .tsv), else by the first
           non-empty line: a bulk action, a JSON object, or text (the default)

The server takes each event's time from its configured time fields.`,
		Example: `  lynxdb ingest access.log
  lynxdb ingest access.log --source web-01 --sourcetype nginx
  lynxdb ingest events.ndjson --index web
  lynxdb ingest export.csv --index orders
  lynxdb ingest export.tsv --format csv --delimiter '\t'
  lynxdb ingest dump.json --format esbulk
  cat events.json | lynxdb ingest --format ndjson`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if projectRC != nil && projectRC.DefaultSource != "" && !cmd.Flags().Changed("source") {
				opts.source = projectRC.DefaultSource
			}
			opts.delimiterSet = cmd.Flags().Changed("delimiter")
			path := ""
			if len(args) > 0 && args[0] != "-" {
				path = args[0]
			}
			return runIngest(path, opts)
		},
	}

	f := cmd.Flags()
	f.StringVar(&opts.format, "format", ingestFormatAuto, "Input format: auto, ndjson, csv, esbulk, text")
	f.StringVar(&opts.source, "source", "", "Source of the events; for ndjson and csv, set where an event has none")
	f.StringVar(&opts.sourcetype, "sourcetype", "", "Sourcetype of the events; for ndjson and csv, set where an event has none")
	f.StringVar(&opts.index, "index", "", "Target index name")
	f.StringVar(&opts.delimiter, "delimiter", ",", `Field delimiter of CSV input, one character or \t`)
	f.IntVar(&opts.batchSize, "batch-size", 5000, "Number of events per request")

	return cmd
}

// runIngest sends the input at path, or standard input when path is empty,
// to the server.
func runIngest(path string, opts ingestOptions) error {
	if opts.batchSize <= 0 {
		return fmt.Errorf("--batch-size must be a positive integer (got %d)", opts.batchSize)
	}
	if opts.batchSize > 1_000_000 {
		return fmt.Errorf("--batch-size exceeds maximum (1,000,000)")
	}

	var input io.Reader = os.Stdin
	var fileSize int64
	if path != "" {
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open file: %w", err)
		}
		defer f.Close()
		input = f
		if fi, err := f.Stat(); err == nil {
			fileSize = fi.Size()
		}
	}
	counted := &countingReader{r: input}
	in := &ingester{opts: opts, read: counted, fileSize: fileSize, start: time.Now()}

	r := io.Reader(counted)
	format := opts.format
	if format == ingestFormatAuto {
		var err error
		if format, r, err = detectIngestFormat(path, counted); err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if format == ingestFormatCSV && strings.EqualFold(filepath.Ext(path), ".tsv") && !opts.delimiterSet {
			in.opts.delimiter = `\t`
		}
		printHint("Detected format %s. Use --format to override.", format)
	}
	if err := checkIngestFlags(format, in.opts); err != nil {
		return err
	}

	var err error
	switch format {
	case ingestFormatText:
		err = in.lines(r, in.sendText)
	case ingestFormatNDJSON:
		err = in.lines(r, in.sendNDJSON)
	case ingestFormatCSV:
		err = in.csv(r)
	case ingestFormatESBulk:
		err = in.bulk(r)
	default:
		return fmt.Errorf("unknown format %q; use auto, ndjson, csv, esbulk or text", format)
	}
	if err != nil {
		return err
	}

	in.summary()

	return nil
}

// checkIngestFlags rejects flags that do not apply to format.
func checkIngestFlags(format string, opts ingestOptions) error {
	if opts.delimiterSet && format != ingestFormatCSV {
		return fmt.Errorf("--delimiter applies to CSV input; the input is %s", format)
	}
	if format == ingestFormatESBulk && (opts.source != "" || opts.sourcetype != "") {
		return fmt.Errorf("--source and --sourcetype do not apply to Elasticsearch bulk input")
	}
	if format == ingestFormatCSV {
		if _, err := csvDelimiter(opts.delimiter); err != nil {
			return err
		}
	}

	return nil
}

// detectIngestFormat names the format of the input at path by its
// extension, else by its first non-empty line, and returns a reader that
// still yields every byte of r.
func detectIngestFormat(path string, r io.Reader) (string, io.Reader, error) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".ndjson", ".jsonl":
		return ingestFormatNDJSON, r, nil
	case ".csv", ".tsv":
		return ingestFormatCSV, r, nil
	}

	br := bufio.NewReaderSize(r, 64*1024)
	var peeked bytes.Buffer
	format := ingestFormatText
	for {
		line, err := br.ReadBytes('\n')
		peeked.Write(line)
		if t := bytes.TrimSpace(line); len(t) > 0 {
			switch {
			case isBulkAction(t):
				format = ingestFormatESBulk
			case t[0] == '{' && json.Valid(t):
				format = ingestFormatNDJSON
			}
			break
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", nil, err
		}
	}

	return format, io.MultiReader(&peeked, br), nil
}

// bulkActions maps each Elasticsearch bulk action to whether a document
// line follows it.
var bulkActions = map[string]bool{"index": true, "create": true, "update": true, "delete": false}

// bulkAction returns the action that line, an object with one member, names
// and the value of that member.
func bulkAction(line []byte) (action string, meta json.RawMessage, ok bool) {
	var m map[string]json.RawMessage
	if json.Unmarshal(line, &m) != nil || len(m) != 1 {
		return "", nil, false
	}
	for action, meta = range m {
	}
	_, ok = bulkActions[action]

	return action, bytes.TrimSpace(meta), ok
}

// isBulkAction reports whether line is the action line of an Elasticsearch
// bulk request: an object whose one member, an action, holds an object.
func isBulkAction(line []byte) bool {
	_, meta, ok := bulkAction(line)

	return ok && len(meta) > 0 && meta[0] == '{'
}

// ingester sends the events of one input in batches and adds up the
// server's answers.
type ingester struct {
	opts     ingestOptions
	read     *countingReader
	fileSize int64
	start    time.Time

	sent, accepted, failed, expired, dropped int
}

// lines sends the non-empty lines of r in batches of --batch-size.
func (in *ingester) lines(r io.Reader, send func([][]byte) (*client.IngestResult, error)) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), maxIngestLine)

	var batch [][]byte
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		batch = append(batch, bytes.Clone(line))
		if len(batch) >= in.opts.batchSize {
			if err := in.flush(len(batch), func() (*client.IngestResult, error) { return send(batch) }); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if len(batch) == 0 {
		return nil
	}

	return in.flush(len(batch), func() (*client.IngestResult, error) { return send(batch) })
}

// flush sends one batch of n events and adds up its result.
func (in *ingester) flush(n int, send func() (*client.IngestResult, error)) error {
	result, err := send()
	if err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	in.sent += n
	in.accepted += result.Accepted
	in.expired += result.Expired
	// The server stops reading a body it cuts short, so what it neither
	// accepted nor expired was dropped.
	if result.Truncated {
		in.dropped += n - result.Accepted - result.Expired
	} else {
		in.failed += result.Failed
	}
	in.progress()

	return nil
}

func (in *ingester) sendText(lines [][]byte) (*client.IngestResult, error) {
	return apiClient().IngestRaw(context.Background(), bytes.NewReader(bytes.Join(lines, []byte{'\n'})), client.IngestOpts{
		Source:     in.opts.source,
		Sourcetype: in.opts.sourcetype,
		Index:      in.opts.index,
	})
}

func (in *ingester) sendNDJSON(lines [][]byte) (*client.IngestResult, error) {
	var body bytes.Buffer
	for _, line := range lines {
		body.Write(withOrigin(line, in.opts.source, in.opts.sourcetype))
		body.WriteByte('\n')
	}

	return apiClient().Ingest(context.Background(), in.opts.index, &body)
}

// withOrigin sets the members source and sourcetype of the object line to
// the values given where the object has none. A line that is not a JSON
// object is returned as it is, for the server to count as failed.
func withOrigin(line []byte, source, sourcetype string) []byte {
	if source == "" && sourcetype == "" {
		return line
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(line, &obj) != nil || obj == nil {
		return line
	}
	for key, value := range map[string]string{"source": source, "sourcetype": sourcetype} {
		if _, ok := obj[key]; value != "" && !ok {
			obj[key], _ = json.Marshal(value)
		}
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return line
	}

	return out
}

// csv sends each row of CSV input as a JSON object keyed by the header row.
func (in *ingester) csv(r io.Reader) error {
	reader := csv.NewReader(r)
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1
	reader.Comma, _ = csvDelimiter(in.opts.delimiter)

	header, err := reader.Read()
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read CSV header: %w", err)
	}
	if len(header) > 0 {
		header[0] = strings.TrimPrefix(header[0], "\xef\xbb\xbf")
	}

	var batch [][]byte
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read CSV: %w", err)
		}
		line, err := json.Marshal(csvObject(header, record, in.opts.source, in.opts.sourcetype))
		if err != nil {
			return err
		}
		batch = append(batch, line)
		if len(batch) >= in.opts.batchSize {
			if err := in.flush(len(batch), func() (*client.IngestResult, error) { return in.sendNDJSON(batch) }); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) == 0 {
		return nil
	}

	return in.flush(len(batch), func() (*client.IngestResult, error) { return in.sendNDJSON(batch) })
}

// csvDelimiter returns the rune of --delimiter.
func csvDelimiter(s string) (rune, error) {
	if s == `\t` {
		return '\t', nil
	}
	if r := []rune(s); len(r) == 1 && r[0] != '"' && r[0] != '\r' && r[0] != '\n' {
		return r[0], nil
	}

	return 0, fmt.Errorf("--delimiter must be one character or \\t (got %q)", s)
}

// jsonNumber matches the text of a JSON number.
var jsonNumber = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

// csvObject returns a row as an object keyed by the header: a value that is
// a JSON number stays a number, an empty value is left out, and source and
// sourcetype are set where the row has no such column.
func csvObject(header, record []string, source, sourcetype string) map[string]any {
	obj := make(map[string]any, len(header)+2)
	for i, value := range record {
		if i >= len(header) || value == "" {
			continue
		}
		if jsonNumber.MatchString(value) {
			obj[header[i]] = json.Number(value)
		} else {
			obj[header[i]] = value
		}
	}
	for key, value := range map[string]string{"source": source, "sourcetype": sourcetype} {
		if _, ok := obj[key]; value != "" && !ok {
			obj[key] = value
		}
	}

	return obj
}

// bulk sends an Elasticsearch bulk body in batches of --batch-size actions,
// keeping each action with its document line.
func (in *ingester) bulk(r io.Reader) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), maxIngestLine)

	var body bytes.Buffer
	actions := 0
	send := func() (*client.IngestResult, error) {
		res, err := apiClient().ESBulk(context.Background(), in.opts.index, bytes.NewReader(body.Bytes()))
		if err != nil {
			return nil, err
		}
		result := &client.IngestResult{}
		for _, item := range res.Items {
			if s := item.Status(); s >= 200 && s < 300 {
				result.Accepted++
			} else {
				result.Failed++
			}
		}

		return result, nil
	}
	for scanner.Scan() {
		action := bytes.TrimSpace(scanner.Bytes())
		if len(action) == 0 {
			continue
		}
		body.Write(action)
		body.WriteByte('\n')
		if bulkActionHasDocument(action) && scanner.Scan() {
			body.Write(bytes.TrimSpace(scanner.Bytes()))
			body.WriteByte('\n')
		}
		actions++
		if actions >= in.opts.batchSize {
			if err := in.flush(actions, send); err != nil {
				return err
			}
			body.Reset()
			actions = 0
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if actions == 0 {
		return nil
	}

	return in.flush(actions, send)
}

// bulkActionHasDocument reports whether a document line follows the bulk
// action line: every action but delete has one, and so has a line that
// names no action, as the server reads it.
func bulkActionHasDocument(line []byte) bool {
	action, _, ok := bulkAction(line)

	return !ok || bulkActions[action]
}

// progress renders a progress bar or counter on stderr.
func (in *ingester) progress() {
	if globalQuiet || !isTTY() {
		return
	}

	elapsed := time.Since(in.start)
	eps := int64(0)
	if elapsed.Seconds() > 0 {
		eps = int64(float64(in.accepted) / elapsed.Seconds())
	}

	if in.fileSize > 0 {
		pct := float64(in.read.n) / float64(in.fileSize)
		if pct > 1 {
			pct = 1
		}
		bar := progress.New(
			progress.WithDefaultBlend(),
			progress.WithWidth(30),
			progress.WithoutPercentage(),
		)
		fmt.Fprintf(os.Stderr, "\r  Ingesting %s %3.0f%%  %s events  %s/sec",
			bar.ViewAs(pct), pct*100, formatCount(int64(in.accepted)), formatCount(eps))

		return
	}

	fmt.Fprintf(os.Stderr, "\r  %s %s events (%s/sec)",
		ui.Stderr.Dim.Render("Ingesting..."), formatCount(int64(in.accepted)), formatCount(eps))
}

// summary prints the totals of the ingest.
func (in *ingester) summary() {
	if !globalQuiet && isTTY() {
		fmt.Fprintf(os.Stderr, "\r%s\r", strings.Repeat(" ", 80))
	}

	elapsed := time.Since(in.start)
	if in.accepted > 0 && elapsed > 0 {
		eps := float64(in.accepted) / elapsed.Seconds()
		printSuccess("Ingested %s events in %s (%s events/sec)",
			formatCount(int64(in.accepted)), formatElapsed(elapsed), formatCount(int64(eps)))
	} else {
		printSuccess("Ingested %s events", formatCount(int64(in.accepted)))
	}

	if in.failed > 0 || in.expired > 0 {
		printWarning("%s of %s events were not stored: %s failed, %s expired (older than the server's retention).",
			formatCount(int64(in.failed+in.expired)), formatCount(int64(in.sent)),
			formatCount(int64(in.failed)), formatCount(int64(in.expired)))
	}

	if in.dropped > 0 {
		printWarning("%s of %s events were dropped: the server cut a request body short. "+
			"Reduce --batch-size or raise the server's ingest.max_body_size.",
			formatCount(int64(in.dropped)), formatCount(int64(in.sent)))
	}

	printNextSteps(
		"lynxdb query 'source=main | stats count() by level'   Query your data",
		"lynxdb fields                            Explore field names",
	)
}

// countingReader counts the bytes read through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)

	return n, err
}
