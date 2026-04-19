package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
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

func newIngestCmd() *cobra.Command {
	var (
		source     string
		sourcetype string
		index      string
		batchSize  int
	)

	cmd := &cobra.Command{
		Use:     "ingest [file]",
		Aliases: []string{"i"},
		Short:   "Ingest logs from file or stdin",
		Long:    `Ingest log data into a running LynxDB server.`,
		Example: `  lynxdb ingest access.log
  lynxdb ingest access.log --source web-01 --sourcetype nginx
  cat events.json | lynxdb ingest
  lynxdb ingest data.log --batch-size 10000`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if projectRC != nil && projectRC.DefaultSource != "" && !cmd.Flags().Changed("source") {
				source = projectRC.DefaultSource
			}
			return runIngest(args, source, sourcetype, index, batchSize)
		},
	}

	f := cmd.Flags()
	f.StringVar(&source, "source", "", "Source metadata for events")
	f.StringVar(&sourcetype, "sourcetype", "", "Sourcetype metadata for events")
	f.StringVar(&index, "index", "", "Target index name")
	f.IntVar(&batchSize, "batch-size", 5000, "Number of lines per batch")

	return cmd
}

func runIngest(args []string, source, sourcetype, index string, batchSize int) error {
	if batchSize <= 0 {
		return fmt.Errorf("--batch-size must be a positive integer (got %d)", batchSize)
	}
	if batchSize > 1_000_000 {
		return fmt.Errorf("--batch-size exceeds maximum (1,000,000)")
	}

	var input *os.File
	var fileSize int64

	if len(args) > 0 {
		f, err := os.Open(args[0])
		if err != nil {
			return fmt.Errorf("open file: %w", err)
		}
		defer f.Close()
		input = f

		if fi, err := f.Stat(); err == nil {
			fileSize = fi.Size()
		}
	} else {
		input = os.Stdin
	}

	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var batch []string
	totalSent := 0
	totalAccepted := 0
	totalDropped := 0
	totalFailed := 0
	totalBatches := 0
	bytesRead := int64(0)
	start := time.Now()

	for scanner.Scan() {
		line := scanner.Text()
		bytesRead += int64(len(line)) + 1 // +1 for newline.
		if strings.TrimSpace(line) == "" {
			continue
		}
		batch = append(batch, line)

		if len(batch) >= batchSize {
			result, err := sendIngestBatch(batch, source, sourcetype, index)
			if err != nil {
				return err
			}
			sent := len(batch)
			totalSent += sent
			totalAccepted += result.Accepted
			reportBatchResult(result, sent, totalBatches+1)
			if result.Truncated {
				totalDropped += sent - result.Accepted
			} else if result.Failed > 0 {
				totalFailed += result.Failed
			}
			totalBatches++
			printIngestProgress(totalAccepted, bytesRead, fileSize, start)
			batch = batch[:0]
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read: %w", err)
	}

	if len(batch) > 0 {
		result, err := sendIngestBatch(batch, source, sourcetype, index)
		if err != nil {
			return err
		}
		sent := len(batch)
		totalSent += sent
		totalAccepted += result.Accepted
		reportBatchResult(result, sent, totalBatches+1)
		if result.Truncated {
			totalDropped += sent - result.Accepted
		} else if result.Failed > 0 {
			totalFailed += result.Failed
		}
	}

	if !globalQuiet && isTTY() {
		fmt.Fprintf(os.Stderr, "\r%s\r", strings.Repeat(" ", 80))
	}

	elapsed := time.Since(start)
	if totalAccepted > 0 && elapsed > 0 {
		eps := float64(totalAccepted) / elapsed.Seconds()
		printSuccess("Ingested %s events in %s (%s events/sec)",
			formatCount(int64(totalAccepted)), formatElapsed(elapsed), formatCount(int64(eps)))
	} else {
		printSuccess("Ingested %s events", formatCount(int64(totalAccepted)))
	}

	if totalDropped > 0 {
		printWarning("%s of %s events were dropped (body truncated). "+
			"Reduce --batch-size or increase server's ingest.max_body_size.",
			formatCount(int64(totalDropped)), formatCount(int64(totalSent)))
	}

	if totalFailed > 0 {
		printWarning("%s of %s events failed to ingest (backpressure). "+
			"Reduce --batch-size or check server logs.",
			formatCount(int64(totalFailed)), formatCount(int64(totalSent)))
	}

	printNextSteps(
		"lynxdb query '| stats count by level'   Query your data",
		"lynxdb fields                            Explore field names",
		"lynxdb tail                              Live tail events",
	)

	return nil
}

// printIngestProgress renders a progress bar or counter on stderr.
func printIngestProgress(totalLines int, bytesRead, fileSize int64, start time.Time) {
	if globalQuiet || !isTTY() {
		return
	}

	t := ui.Stderr

	if fileSize > 0 {
		// File mode: show styled progress bar.
		pct := float64(bytesRead) / float64(fileSize)
		if pct > 1 {
			pct = 1
		}

		bar := progress.New(
			progress.WithDefaultBlend(),
			progress.WithWidth(30),
			progress.WithoutPercentage(),
		)
		elapsed := time.Since(start)
		eps := int64(0)
		if elapsed.Seconds() > 0 {
			eps = int64(float64(totalLines) / elapsed.Seconds())
		}
		line := fmt.Sprintf("  Ingesting %s %3.0f%%  %s events  %s/sec",
			bar.ViewAs(pct), pct*100, formatCount(int64(totalLines)), formatCount(eps))
		fmt.Fprintf(os.Stderr, "\r%s", line)
	} else {
		// Stdin mode: show counter.
		elapsed := time.Since(start)
		eps := int64(0)
		if elapsed.Seconds() > 0 {
			eps = int64(float64(totalLines) / elapsed.Seconds())
		}

		fmt.Fprintf(os.Stderr, "\r  %s %s events (%s/sec)",
			t.Dim.Render("Ingesting..."),
			formatCount(int64(totalLines)), formatCount(eps))
	}
}

// reportBatchResult prints a per-batch warning when events were truncated or failed.
// Truncation means the server's MaxBytesReader cut the request body short.
// Failures mean events were rejected (e.g., WAL backpressure) but the body was intact.
func reportBatchResult(result *client.IngestResult, sent, batchNum int) {
	if result.Truncated {
		printWarning("Server truncated request body in batch %d: accepted %s of %s events. "+
			"Reduce --batch-size or increase server's ingest.max_body_size.",
			batchNum, formatCount(int64(result.Accepted)), formatCount(int64(sent)))
	} else if result.Failed > 0 {
		printWarning("Server failed to ingest %s of %s events in batch %d (backpressure). "+
			"Reduce --batch-size or check server logs.",
			formatCount(int64(result.Failed)), formatCount(int64(sent)), batchNum)
	}
}

func sendIngestBatch(lines []string, source, sourcetype, index string) (*client.IngestResult, error) {
	body := strings.Join(lines, "\n")
	ctx := context.Background()

	result, err := apiClient().IngestRaw(ctx, strings.NewReader(body), client.IngestOpts{
		Source:     source,
		Sourcetype: sourcetype,
		Index:      index,
	})
	if err != nil {
		return nil, fmt.Errorf("send batch: %w", err)
	}

	return result, nil
}
