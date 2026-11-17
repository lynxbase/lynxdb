package stats

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// FormatProfile writes an execution profile to w for terminal display.
// Shown when --analyze is used.
func FormatProfile(w io.Writer, s *QueryStats) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "  ══════ Query Profile ══════")
	fmt.Fprintln(w)

	writePhaseBreakdown(w, s)

	if s.ScannedRows > 0 {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "  Rows:  %s scanned", formatCommaInt64(s.ScannedRows))
		if s.ResultRows > 0 {
			fmt.Fprintf(w, " %s %s results", arrow, formatCommaInt64(s.ResultRows))
		}
		fmt.Fprintln(w)
	}

	fmt.Fprintln(w)
}

// FormatProfileJSON writes a machine-readable JSON profile to w.
func FormatProfileJSON(w io.Writer, s *QueryStats) error {
	profile := profileJSON{
		TotalMS:  s.TotalDuration.Milliseconds(),
		ParseMS:  durMS(s.ParseDuration),
		ExecMS:   durMS(s.ExecDuration),
		SerialMS: durMS(s.SerializeDuration),
		Scanned:  s.ScannedRows,
		Results:  s.ResultRows,
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")

	return enc.Encode(map[string]profileJSON{"profile": profile})
}

func writePhaseBreakdown(w io.Writer, s *QueryStats) {
	total := s.TotalDuration
	if total <= 0 {
		return
	}

	fmt.Fprintln(w, "  Phase Breakdown:")

	phases := []struct {
		name string
		dur  time.Duration
	}{
		{"parse", s.ParseDuration},
		{"exec", s.ExecDuration},
		{"serialize", s.SerializeDuration},
	}

	// Find max duration for bar scaling.
	var maxDur time.Duration
	for _, p := range phases {
		if p.dur > maxDur {
			maxDur = p.dur
		}
	}

	const barMaxWidth = 20
	for _, p := range phases {
		if p.dur <= 0 {
			continue
		}
		pct := float64(p.dur) / float64(total) * 100

		barLen := 0
		if maxDur > 0 {
			barLen = int(float64(p.dur) / float64(maxDur) * barMaxWidth)
		}
		if barLen < 1 && p.dur > 0 {
			barLen = 1
		}
		bar := strings.Repeat("█", barLen)

		fmt.Fprintf(w, "    %-12s %8s  %4.0f%%   %s\n",
			p.name, formatDur(p.dur), pct, bar)
	}

	fmt.Fprintf(w, "    %12s ──────────\n", "")
	fmt.Fprintf(w, "    %-12s %8s\n", "total", formatDur(total))
}

// durMS converts a time.Duration to float64 milliseconds.
func durMS(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

type profileJSON struct {
	TotalMS  int64   `json:"total_ms"`
	ParseMS  float64 `json:"parse_ms,omitempty"`
	ExecMS   float64 `json:"exec_ms,omitempty"`
	SerialMS float64 `json:"serialize_ms,omitempty"`
	Scanned  int64   `json:"scanned_rows"`
	Results  int64   `json:"result_rows"`
}
