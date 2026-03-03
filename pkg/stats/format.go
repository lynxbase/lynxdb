package stats

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// arrow is the Unicode right arrow used in the row funnel.
const arrow = " → "

// FormatTTY writes a styled stats block to w for terminal display.
// The caller is responsible for printing a separator line before calling this.
// When quiet is true, only the headline (result count + time) is shown.
// When verbose is true, the full pipeline breakdown is included.
func FormatTTY(w io.Writer, s *QueryStats, verbose, quiet bool) {
	if quiet {
		fmt.Fprintf(w, "%s  %s\n",
			formatResultCount(s.ResultRows),
			formatDur(s.TotalDuration))

		return
	}

	// Headline: results  time  scanned  read
	headline := fmt.Sprintf("%s  %s",
		formatResultCount(s.ResultRows),
		formatDur(s.TotalDuration))

	if s.AcceleratedBy != "" && s.MVSpeedup != "" {
		// MV-accelerated: show MV row count and speedup in headline.
		if s.ScannedRows > 0 {
			headline += fmt.Sprintf("  %s rows from MV", formatHumanInt64(s.ScannedRows))
		}
		headline += fmt.Sprintf("  ⚡ %s speedup", s.MVSpeedup)
	} else if s.ScannedRows > 0 {
		// Volume: "37K scanned  7.4 MB read"
		headline += fmt.Sprintf("  %s scanned", formatHumanInt64(s.ScannedRows))
		if s.ProcessedBytes > 0 {
			headline += fmt.Sprintf("  %s read", formatBytesHuman(s.ProcessedBytes))
		}
	} else if !s.Ephemeral && s.TotalSegments == 0 && s.BufferedRowsScanned == 0 {
		// Likely a cache hit or count-star optimization.
		if s.CacheHit {
			headline += "  cache hit"
		} else if s.CountStarOptimized {
			headline += "  metadata-only"
		}
	}
	fmt.Fprintln(w, headline)

	// MV acceleration (if applicable).
	if s.AcceleratedBy != "" {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "  %-15s %s", "accelerated", s.AcceleratedBy)
		if s.MVStatus != "" {
			fmt.Fprintf(w, " (%s)", s.MVStatus)
		}
		if s.MVSpeedup != "" && s.MVStatus == "" {
			fmt.Fprintf(w, " (%s speedup)", s.MVSpeedup)
		}
		fmt.Fprintln(w)
		if s.MVOriginalScan > 0 {
			fmt.Fprintf(w, "    original scan: %s events  MV: %s rows",
				formatHumanInt64(s.MVOriginalScan), formatHumanInt64(s.ScannedRows))
			fmt.Fprintln(w)
		}
		if s.MVCoveragePercent > 0 && s.MVCoveragePercent < 100 {
			fmt.Fprintf(w, "    coverage: %.0f%%\n", s.MVCoveragePercent)
		}
	}

	// Segments (only when there are segments).
	if s.TotalSegments > 0 {
		fmt.Fprintf(w, "  %-15s %d of %d scanned",
			"segments", s.ScannedSegments, s.TotalSegments)
		if skip := s.SkipPercent(); skip > 0 {
			fmt.Fprintf(w, " (%.1f%% skipped)", skip)
		}
		fmt.Fprintln(w)

		// Skip breakdown.
		writeSkipBreakdown(w, s, verbose)
	}

	// Memtable events.
	if s.BufferedRowsScanned > 0 {
		fmt.Fprintf(w, "  %-15s %s events\n", "buffer", formatHumanInt64(s.BufferedRowsScanned))
	}

	// Row funnel (only when meaningful).
	if s.ScannedRows > 0 && (s.MatchedRows > 0 || s.ResultRows > 0) {
		fmt.Fprintf(w, "  %-15s %s scanned",
			"funnel", formatCommaInt64(s.ScannedRows))
		if s.MatchedRows > 0 && s.MatchedRows != s.ScannedRows {
			fmt.Fprintf(w, "%s%s matched", arrow, formatCommaInt64(s.MatchedRows))
		}
		if s.ResultRows != s.ScannedRows {
			fmt.Fprintf(w, "%s%s results", arrow, formatCommaInt64(s.ResultRows))
		}
		fmt.Fprintln(w)

		if s.ScanType != "" {
			fmt.Fprintf(w, "    scan:    %s", s.ScanType)
			if sel := s.Selectivity(); sel > 0 && sel < 1.0 {
				fmt.Fprintf(w, "  selectivity: %.1f%%", sel*100)
			}
			fmt.Fprintln(w)
		}
	}

	// I/O (only when there is byte-level data).
	writeIOLine(w, s)

	// Optimizations used.
	if opts := formatOptimizations(s); opts != "" {
		fmt.Fprintf(w, "  %-15s %s\n", "optimizations", opts)
	}

	// Timing breakdown (verbose and when >10ms).
	if verbose && s.TotalDuration > 10*time.Millisecond {
		writeTimingBreakdown(w, s)
	}

	// Memory + CPU combined line (default mode, when non-zero).
	// In verbose mode, show separate detailed lines instead.
	if verbose {
		// Verbose: separate Memory and CPU lines with full detail.
		if s.PeakMemoryBytes > 0 {
			fmt.Fprintf(w, "  %-15s %s peak", "memory", formatBytesHuman(s.PeakMemoryBytes))
			if s.MemAllocBytes > 0 {
				fmt.Fprintf(w, "  %s alloc", formatBytesHuman(s.MemAllocBytes))
			}
			fmt.Fprintln(w)

			// Spill summary (verbose only, when spilling occurred).
			if s.SpilledToDisk {
				fmt.Fprintf(w, "    spill:   %s on disk", formatBytesHuman(s.SpillBytes))
				if s.SpillFiles > 0 {
					fmt.Fprintf(w, " (%d files)", s.SpillFiles)
				}
				fmt.Fprintln(w)
			}

			// Per-operator memory breakdown (verbose only).
			for _, stage := range s.Stages {
				if stage.MemoryBytes > 0 || stage.SpilledRows > 0 {
					fmt.Fprintf(w, "    %-12s %s", stage.Name, formatBytesHuman(stage.MemoryBytes))
					if stage.SpilledRows > 0 {
						fmt.Fprintf(w, "  (spilled %s rows", formatCommaInt64(stage.SpilledRows))
						if stage.SpillBytes > 0 {
							fmt.Fprintf(w, ", %s on disk", formatBytesHuman(stage.SpillBytes))
						}
						fmt.Fprint(w, ")")
					}
					fmt.Fprintln(w)
				}
			}

			// Pool utilization (verbose only).
			if s.PoolUtilization > 0 {
				fmt.Fprintf(w, "    pool:    %.1f%% global pool\n", s.PoolUtilization*100)
			}
		}
		if s.CPUTimeUser > 0 || s.CPUTimeSys > 0 {
			fmt.Fprintf(w, "  %-15s %s user", "cpu", formatDur(s.CPUTimeUser))
			if s.CPUTimeSys > 0 {
				fmt.Fprintf(w, "  %s sys", formatDur(s.CPUTimeSys))
			}
			fmt.Fprintln(w)
		}

		// Throughput (verbose only — removed from headline).
		if rate := s.ScanRateRowsPerSec(); rate > 0 {
			tp := fmt.Sprintf("%s rows/s", formatHumanInt64(int64(rate)))
			if bps := s.BytesPerSec(); bps > 0 {
				tp += fmt.Sprintf("  %s/s", formatBytesHuman(int64(bps)))
			}
			fmt.Fprintf(w, "  %-15s %s\n", "throughput", tp)
		}
	} else if s.PeakMemoryBytes > 0 || s.CPUTimeUser > 0 {
		// Default mode: combined Memory + CPU on one line.
		// When only CPU is present, use "cpu" as the label instead of "memory".
		var parts []string
		label := "memory"
		if s.PeakMemoryBytes > 0 {
			parts = append(parts, fmt.Sprintf("%s peak", formatBytesHuman(s.PeakMemoryBytes)))
		}
		if s.SpilledToDisk {
			parts = append(parts, "spilled to disk")
		}
		if s.CPUTimeUser > 0 {
			if s.PeakMemoryBytes > 0 {
				// Combined line: prefix CPU so it's visually distinct from memory.
				parts = append(parts, fmt.Sprintf("CPU: %s user", formatDur(s.CPUTimeUser)))
			} else {
				// CPU-only: use "cpu" label, no redundant prefix.
				label = "cpu"
				parts = append(parts, fmt.Sprintf("%s user", formatDur(s.CPUTimeUser)))
			}
		}
		fmt.Fprintf(w, "  %-15s %s\n", label, strings.Join(parts, "  "))
	}

	// Warnings (shown in both default and verbose modes).
	for _, warn := range s.Warnings {
		fmt.Fprintf(w, "  %-15s %s\n", "warning", warn)
	}

	// Segment errors.
	if s.SegmentsErrored > 0 {
		fmt.Fprintf(w, "  %-15s %d segment(s) failed to read\n", "errors", s.SegmentsErrored)
	}

	// Pipeline breakdown (verbose).
	if verbose && len(s.Stages) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "  Pipeline:")
		for _, stage := range s.Stages {
			fmt.Fprintf(w, "    %-12s %10s%s%10s  %6s\n",
				stage.Name,
				formatCommaInt64(stage.InputRows),
				arrow,
				formatCommaInt64(stage.OutputRows),
				formatDur(stage.Duration))
		}
	}
}

// FormatJSON writes a machine-readable JSON stats line to w.
// Intended for stderr when stdout is piped to preserve data stream purity.
func FormatJSON(w io.Writer, s *QueryStats) error {
	js := jsonStats{
		TotalMS:         s.TotalDuration.Milliseconds(),
		ScannedRows:     s.ScannedRows,
		MatchedRows:     s.MatchedRows,
		ResultRows:      s.ResultRows,
		ScannedSegments: s.ScannedSegments,
		TotalSegments:   s.TotalSegments,
		ScanType:        s.ScanType,
	}
	if s.ParseDuration > 0 {
		v := float64(s.ParseDuration.Microseconds()) / 1000
		js.ParseMS = &v
	}
	if s.OptimizeDuration > 0 {
		v := float64(s.OptimizeDuration.Microseconds()) / 1000
		js.OptimizeMS = &v
	}
	if s.ExecDuration > 0 {
		v := float64(s.ExecDuration.Microseconds()) / 1000
		js.ExecMS = &v
	}
	if s.BloomSkippedSegments > 0 {
		js.BloomSkipped = &s.BloomSkippedSegments
	}
	if s.TimeSkippedSegments > 0 {
		js.TimeSkipped = &s.TimeSkippedSegments
	}
	if sel := s.Selectivity(); sel > 0 {
		js.Selectivity = &sel
	}
	if skip := s.SkipRatio(); skip > 0 {
		js.SkipRatio = &skip
	}
	if s.PeakMemoryBytes > 0 {
		js.PeakMemoryBytes = &s.PeakMemoryBytes
	}
	if s.BufferedRowsScanned > 0 {
		js.BufferedEvents = &s.BufferedRowsScanned
	}
	if s.AcceleratedBy != "" {
		js.AcceleratedBy = &s.AcceleratedBy
	}
	if s.MVSpeedup != "" {
		js.MVSpeedup = &s.MVSpeedup
	}
	if s.ProcessedBytes > 0 {
		js.ProcessedBytes = &s.ProcessedBytes
	}
	if bps := s.BytesPerSec(); bps > 0 {
		js.BytesPerSec = &bps
	}
	if rate := int64(s.ScanRateRowsPerSec()); rate > 0 {
		js.ScanRatePerSec = &rate
	}
	if s.SegmentsErrored > 0 {
		js.SegmentsErrored = &s.SegmentsErrored
	}
	if s.CacheHit {
		js.CacheHit = &s.CacheHit
	}
	if s.DiskBytesRead > 0 {
		js.DiskBytesRead = &s.DiskBytesRead
	}
	if s.S3BytesRead > 0 {
		js.S3BytesRead = &s.S3BytesRead
	}
	if s.CacheBytesRead > 0 {
		js.CacheBytesRead = &s.CacheBytesRead
	}
	if s.CPUTimeUser > 0 {
		v := float64(s.CPUTimeUser.Microseconds()) / 1000
		js.CPUUserMS = &v
	}
	if s.CPUTimeSys > 0 {
		v := float64(s.CPUTimeSys.Microseconds()) / 1000
		js.CPUSysMS = &v
	}
	if s.SpilledToDisk {
		js.SpilledToDisk = &s.SpilledToDisk
	}
	if s.SpillBytes > 0 {
		js.SpillBytesJSON = &s.SpillBytes
	}
	if s.SpillFiles > 0 {
		js.SpillFilesJSON = &s.SpillFiles
	}
	if len(s.Warnings) > 0 {
		js.WarningsJSON = s.Warnings
	}
	if s.ScanDuration > 0 {
		v := float64(s.ScanDuration.Microseconds()) / 1000
		js.ScanMS = &v
	}
	if s.PipelineDuration > 0 {
		v := float64(s.PipelineDuration.Microseconds()) / 1000
		js.PipelineMS = &v
	}
	if s.MVStatus != "" {
		js.MVStatus = &s.MVStatus
	}
	if s.MVOriginalScan > 0 {
		js.MVOriginalScan = &s.MVOriginalScan
	}
	if s.MVCoveragePercent > 0 {
		js.MVCoveragePercent = &s.MVCoveragePercent
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	return enc.Encode(map[string]jsonStats{"stats": js})
}

// jsonStats is the machine-readable stats format written to stderr when piped.
type jsonStats struct {
	TotalMS           int64    `json:"total_ms"`
	ParseMS           *float64 `json:"parse_ms,omitempty"`
	OptimizeMS        *float64 `json:"optimize_ms,omitempty"`
	ExecMS            *float64 `json:"exec_ms,omitempty"`
	ScannedRows       int64    `json:"scanned_rows"`
	MatchedRows       int64    `json:"matched_rows"`
	ResultRows        int64    `json:"result_rows"`
	ScannedSegments   int      `json:"scanned_segments"`
	TotalSegments     int      `json:"total_segments"`
	BloomSkipped      *int     `json:"bloom_skipped,omitempty"`
	TimeSkipped       *int     `json:"time_skipped,omitempty"`
	ScanType          string   `json:"scan_type,omitempty"`
	Selectivity       *float64 `json:"selectivity,omitempty"`
	SkipRatio         *float64 `json:"skip_ratio,omitempty"`
	PeakMemoryBytes   *int64   `json:"peak_memory_bytes,omitempty"`
	SpilledToDisk     *bool    `json:"spilled_to_disk,omitempty"`
	SpillBytesJSON    *int64   `json:"spill_bytes,omitempty"`
	SpillFilesJSON    *int     `json:"spill_files,omitempty"`
	WarningsJSON      []string `json:"warnings,omitempty"`
	BufferedEvents    *int64   `json:"buffered_events,omitempty"`
	AcceleratedBy     *string  `json:"accelerated_by,omitempty"`
	MVSpeedup         *string  `json:"mv_speedup,omitempty"`
	MVStatus          *string  `json:"mv_status,omitempty"`
	MVOriginalScan    *int64   `json:"mv_original_scan,omitempty"`
	MVCoveragePercent *float64 `json:"mv_coverage_percent,omitempty"`
	ProcessedBytes    *int64   `json:"processed_bytes,omitempty"`
	BytesPerSec       *float64 `json:"bytes_per_sec,omitempty"`
	ScanRatePerSec    *int64   `json:"scan_rate_per_sec,omitempty"`
	SegmentsErrored   *int     `json:"segments_errored,omitempty"`
	CacheHit          *bool    `json:"cache_hit,omitempty"`
	DiskBytesRead     *int64   `json:"disk_bytes_read,omitempty"`
	S3BytesRead       *int64   `json:"s3_bytes_read,omitempty"`
	CacheBytesRead    *int64   `json:"cache_bytes_read,omitempty"`
	CPUUserMS         *float64 `json:"cpu_user_ms,omitempty"`
	CPUSysMS          *float64 `json:"cpu_sys_ms,omitempty"`
	ScanMS            *float64 `json:"scan_ms,omitempty"`
	PipelineMS        *float64 `json:"pipeline_ms,omitempty"`
}

// Internal formatting helpers

func writeIOLine(w io.Writer, s *QueryStats) {
	totalBytes := s.DiskBytesRead + s.CacheBytesRead + s.S3BytesRead
	if totalBytes == 0 {
		return
	}

	fmt.Fprintf(w, "  %-15s %s from disk", "io", formatBytesHuman(s.DiskBytesRead+s.CacheBytesRead))
	if s.CacheBytesRead > 0 && s.DiskBytesRead > 0 {
		ratio := float64(s.CacheBytesRead) / float64(s.CacheBytesRead+s.DiskBytesRead) * 100
		fmt.Fprintf(w, "  cache: %.0f%% hit", ratio)
	}
	fmt.Fprintln(w)
	if s.S3BytesRead > 0 {
		fmt.Fprintf(w, "    s3:      %s fetched\n", formatBytesHuman(s.S3BytesRead))
	}
}

func writeSkipBreakdown(w io.Writer, s *QueryStats, verbose bool) {
	skipped := s.SkippedSegments()
	if skipped == 0 {
		return
	}

	var parts []string
	if s.BloomSkippedSegments > 0 {
		parts = append(parts, fmt.Sprintf("bloom: %d skipped", s.BloomSkippedSegments))
	}
	if s.TimeSkippedSegments > 0 {
		parts = append(parts, fmt.Sprintf("time: %d skipped", s.TimeSkippedSegments))
	}
	if s.IndexSkippedSegments > 0 {
		parts = append(parts, fmt.Sprintf("index: %d skipped", s.IndexSkippedSegments))
	}
	if s.StatSkippedSegments > 0 {
		parts = append(parts, fmt.Sprintf("stats: %d skipped", s.StatSkippedSegments))
	}
	if s.RangeSkippedSegments > 0 {
		parts = append(parts, fmt.Sprintf("range: %d skipped", s.RangeSkippedSegments))
	}

	if len(parts) > 0 {
		if verbose || len(parts) <= 2 {
			fmt.Fprintf(w, "    %s\n", strings.Join(parts, "  "))
		}
	}
}

func writeTimingBreakdown(w io.Writer, s *QueryStats) {
	parts := make([]string, 0, 6)
	if s.ParseDuration > 0 {
		parts = append(parts, fmt.Sprintf("parse: %s", formatDur(s.ParseDuration)))
	}
	if s.OptimizeDuration > 0 {
		parts = append(parts, fmt.Sprintf("optimize: %s", formatDur(s.OptimizeDuration)))
	}
	// For server queries, show scan/pipeline breakdown instead of generic exec.
	if s.ScanDuration > 0 || s.PipelineDuration > 0 {
		if s.ScanDuration > 0 {
			parts = append(parts, fmt.Sprintf("scan: %s", formatDur(s.ScanDuration)))
		}
		if s.PipelineDuration > 0 {
			parts = append(parts, fmt.Sprintf("pipeline: %s", formatDur(s.PipelineDuration)))
		}
	} else if s.ExecDuration > 0 {
		parts = append(parts, fmt.Sprintf("exec: %s", formatDur(s.ExecDuration)))
	}
	if len(parts) > 0 {
		fmt.Fprintf(w, "  %-15s %s\n", "timing", strings.Join(parts, "  "))
	}
}

func formatOptimizations(s *QueryStats) string {
	var opts []string
	if s.CountStarOptimized {
		opts = append(opts, "count(*) metadata")
	}
	if s.PartialAggUsed {
		opts = append(opts, "partial aggregation")
	}
	if s.TopKUsed {
		opts = append(opts, "topK heap")
	}
	if s.PrefetchUsed {
		opts = append(opts, "prefetch")
	}
	if s.VectorizedFilterUsed {
		opts = append(opts, "vectorized filter")
	}
	if s.DictFilterUsed {
		opts = append(opts, "dict filter")
	}
	if s.JoinStrategy != "" {
		opts = append(opts, "join: "+s.JoinStrategy)
	}
	if len(opts) == 0 {
		return ""
	}

	return strings.Join(opts, ", ")
}

func formatResultCount(n int64) string {
	if n == 1 {
		return "1 result"
	}

	return formatHumanInt64(n) + " results"
}

// formatCommaInt64 formats an integer with comma separators (e.g., 142,847).
// Used in detail lines (Rows funnel, Pipeline breakdown) where precision matters.
func formatCommaInt64(n int64) string {
	if n < 0 {
		return "-" + formatCommaInt64(-n)
	}

	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}

	var b strings.Builder
	b.Grow(len(s) + len(s)/3)

	offset := len(s) % 3
	if offset > 0 {
		b.WriteString(s[:offset])
	}

	for i := offset; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}

	return b.String()
}

// formatHumanInt64 formats large numbers with K/M/G suffixes.
func formatHumanInt64(n int64) string {
	abs := n
	if abs < 0 {
		abs = -abs
	}

	switch {
	case abs >= 1_000_000_000:
		return fmt.Sprintf("%.1fG", float64(n)/1e9)
	case abs >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case abs >= 10_000:
		return fmt.Sprintf("%.0fK", float64(n)/1e3)
	case abs >= 1_000:
		return fmt.Sprintf("%.1fK", float64(n)/1e3)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// formatDur formats durations for human readability.
func formatDur(d time.Duration) string {
	switch {
	case d >= time.Minute:
		return fmt.Sprintf("%.1fm", d.Minutes())
	case d >= time.Second:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d >= time.Microsecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	default:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
}

// formatBytesHuman formats byte counts for human readability.
func formatBytesHuman(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
