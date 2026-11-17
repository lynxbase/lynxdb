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
// When verbose is true, the timing breakdown and throughput are included.
func FormatTTY(w io.Writer, s *QueryStats, verbose, quiet bool) {
	headline := fmt.Sprintf("%s  %s",
		formatResultCount(s.ResultRows),
		formatDur(s.TotalDuration))
	if quiet {
		fmt.Fprintln(w, headline)

		return
	}

	if s.ScannedRows > 0 {
		headline += fmt.Sprintf("  %s scanned", formatHumanInt64(s.ScannedRows))
	}
	fmt.Fprintln(w, headline)

	if s.ScannedRows > 0 && s.ResultRows > 0 {
		fmt.Fprintf(w, "  %-15s %s scanned",
			"funnel", formatCommaInt64(s.ScannedRows))
		if s.ResultRows != s.ScannedRows {
			fmt.Fprintf(w, "%s%s results", arrow, formatCommaInt64(s.ResultRows))
		}
		fmt.Fprintln(w)
	}

	if !verbose {
		return
	}

	if s.TotalDuration > 10*time.Millisecond {
		writeTimingBreakdown(w, s)
	}

	if rate := s.ScanRateRowsPerSec(); rate > 0 {
		fmt.Fprintf(w, "  %-15s %s rows/s\n", "throughput", formatHumanInt64(int64(rate)))
	}
}

// FormatJSON writes a machine-readable JSON stats line to w.
// Intended for stderr when stdout is piped to preserve data stream purity.
func FormatJSON(w io.Writer, s *QueryStats) error {
	js := jsonStats{
		TotalMS:     s.TotalDuration.Milliseconds(),
		ScannedRows: s.ScannedRows,
		ResultRows:  s.ResultRows,
	}
	if s.ParseDuration > 0 {
		v := float64(s.ParseDuration.Microseconds()) / 1000
		js.ParseMS = &v
	}
	if s.ExecDuration > 0 {
		v := float64(s.ExecDuration.Microseconds()) / 1000
		js.ExecMS = &v
	}
	if rate := int64(s.ScanRateRowsPerSec()); rate > 0 {
		js.ScanRatePerSec = &rate
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	return enc.Encode(map[string]jsonStats{"stats": js})
}

// jsonStats is the machine-readable stats format written to stderr when piped.
type jsonStats struct {
	TotalMS        int64    `json:"total_ms"`
	ParseMS        *float64 `json:"parse_ms,omitempty"`
	ExecMS         *float64 `json:"exec_ms,omitempty"`
	ScannedRows    int64    `json:"scanned_rows"`
	ResultRows     int64    `json:"result_rows"`
	ScanRatePerSec *int64   `json:"scan_rate_per_sec,omitempty"`
}

func writeTimingBreakdown(w io.Writer, s *QueryStats) {
	parts := make([]string, 0, 2)
	if s.ParseDuration > 0 {
		parts = append(parts, fmt.Sprintf("parse: %s", formatDur(s.ParseDuration)))
	}
	if s.ExecDuration > 0 {
		parts = append(parts, fmt.Sprintf("exec: %s", formatDur(s.ExecDuration)))
	}
	if len(parts) > 0 {
		fmt.Fprintf(w, "  %-15s %s\n", "timing", strings.Join(parts, "  "))
	}
}

func formatResultCount(n int64) string {
	if n == 1 {
		return "1 result"
	}

	return formatHumanInt64(n) + " results"
}

// formatCommaInt64 formats an integer with comma separators (e.g., 142,847).
// Used in detail lines (Rows funnel) where precision matters.
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
