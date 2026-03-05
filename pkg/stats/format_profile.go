package stats

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// FormatProfile writes a comprehensive execution profile to w for terminal display.
// Shown when --analyze is used.
func FormatProfile(w io.Writer, s *QueryStats) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "  ══════ Query Profile ══════")
	fmt.Fprintln(w)

	// Phase breakdown.
	writePhaseBreakdown(w, s)

	// Operator flow (compact one-liner).
	if len(s.Stages) > 0 {
		fmt.Fprintln(w)
		writeOperatorFlow(w, s)
	}

	// Segments (only for server queries with segments).
	if s.TotalSegments > 0 {
		fmt.Fprintln(w)
		skipPct := s.SkipPercent()
		fmt.Fprintf(w, "  Segments:  %d/%d scanned", s.ScannedSegments, s.TotalSegments)
		if skipPct > 0 {
			fmt.Fprintf(w, " (%.1f%% skipped)", skipPct)
		}
		fmt.Fprintln(w)

		writeSkipBreakdownCompact(w, s)
	}

	// Row funnel.
	if s.ScannedRows > 0 {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "  Rows:  %s scanned", formatCommaInt64(s.ScannedRows))
		if s.MatchedRows > 0 && s.MatchedRows != s.ScannedRows {
			fmt.Fprintf(w, " %s %s matched", arrow, formatCommaInt64(s.MatchedRows))
		}
		if s.ResultRows > 0 {
			fmt.Fprintf(w, " %s %s results", arrow, formatCommaInt64(s.ResultRows))
		}
		fmt.Fprintln(w)
		if s.ScanType != "" {
			fmt.Fprintf(w, "    scan: %s", s.ScanType)
			if sel := s.Selectivity(); sel > 0 && sel < 1.0 {
				fmt.Fprintf(w, "  selectivity: %.1f%%", sel*100)
			}
			fmt.Fprintln(w)
		}
	}

	// Pipeline breakdown.
	if len(s.Stages) > 0 {
		fmt.Fprintln(w)
		writePipelineBreakdown(w, s)
	}

	// Optimizations used.
	if opts := formatOptimizations(s); opts != "" {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "  Optimizations:  %s\n", opts)
	}

	// Optimizer rule details.
	if len(s.OptimizerRules) > 0 {
		fmt.Fprintln(w)
		writeOptimizerRules(w, s)
	}

	// Resources.
	if s.PeakMemoryBytes > 0 || s.CPUTimeUser > 0 {
		fmt.Fprintln(w)
		writeResources(w, s)
	}

	// Per-operator memory budgets (coordinator-managed queries only).
	if len(s.OperatorBudgets) > 0 {
		fmt.Fprintln(w)
		writeOperatorBudgets(w, s)
	}

	// Per-segment I/O details (trace level only).
	if len(s.SegmentDetails) > 0 {
		fmt.Fprintln(w)
		writeSegmentDetails(w, s)
	}

	// VM execution metrics (trace level only).
	if s.VMCalls > 0 {
		fmt.Fprintln(w)
		writeVMMetrics(w, s)
	}

	// Recommendations.
	if len(s.Recommendations) > 0 {
		fmt.Fprintln(w)
		writeRecommendations(w, s)
	}

	fmt.Fprintln(w)
}

// FormatProfileJSON writes a machine-readable JSON profile to w.
func FormatProfileJSON(w io.Writer, s *QueryStats) error {
	profile := buildProfileJSON(s)

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")

	return enc.Encode(map[string]profileJSON{"profile": profile})
}

// buildProfileJSON converts QueryStats into the JSON-serializable profile structure.
func buildProfileJSON(s *QueryStats) profileJSON {
	p := profileJSON{
		TotalMS:   s.TotalDuration.Milliseconds(),
		ParseMS:   durMS(s.ParseDuration),
		OptMS:     durMS(s.OptimizeDuration),
		ExecMS:    durMS(s.ExecDuration),
		SerialMS:  durMS(s.SerializeDuration),
		ScanMS:    durMS(s.ScanDuration),
		PipMS:     durMS(s.PipelineDuration),
		Scanned:   s.ScannedRows,
		Matched:   s.MatchedRows,
		Results:   s.ResultRows,
		ScanType:  s.ScanType,
		Ephemeral: s.Ephemeral,
	}

	if s.TotalSegments > 0 {
		p.Segments = &segmentsJSON{
			Total:   s.TotalSegments,
			Scanned: s.ScannedSegments,
			Bloom:   s.BloomSkippedSegments,
			Time:    s.TimeSkippedSegments,
			Stats:   s.StatSkippedSegments,
			Errored: s.SegmentsErrored,
		}
	}

	for _, stage := range s.Stages {
		p.Pipeline = append(p.Pipeline, stageJSON{
			Name:   stage.Name,
			Input:  stage.InputRows,
			Output: stage.OutputRows,
			MS:     durMS(stage.Duration),
		})
	}

	for _, r := range s.OptimizerRules {
		p.Optimizer = append(p.Optimizer, ruleJSON{
			Name:  r.Name,
			Desc:  r.Description,
			Count: r.Count,
		})
	}

	for _, b := range s.OperatorBudgets {
		p.Budgets = append(p.Budgets, budgetJSON(b))
	}

	for _, r := range s.Recommendations {
		p.Recs = append(p.Recs, recJSON{
			Cat:     r.Category,
			Pri:     r.Priority,
			Message: r.Message,
			Speedup: r.EstimatedSpeedup,
			Action:  r.SuggestedAction,
		})
	}

	for _, sd := range s.SegmentDetails {
		p.SegDetails = append(p.SegDetails, segDetailJSON{
			ID:              sd.SegmentID,
			Source:          sd.Source,
			Rows:            sd.Rows,
			RowsAfterFilter: sd.RowsAfterFilter,
			BloomHit:        sd.BloomHit,
			InvertedUsed:    sd.InvertedUsed,
			ReadMS:          durMS(sd.ReadDuration),
			BytesRead:       sd.BytesRead,
		})
	}

	if s.VMCalls > 0 {
		p.VM = &vmJSON{
			Calls:   s.VMCalls,
			TotalMS: durMS(s.VMTotalTime),
		}
	}

	if s.PeakMemoryBytes > 0 || s.CPUTimeUser > 0 {
		p.Resources = &resourcesJSON{
			PeakMem:    s.PeakMemoryBytes,
			AllocBytes: s.MemAllocBytes,
			CPUUserMS:  durMS(s.CPUTimeUser),
			CPUSysMS:   durMS(s.CPUTimeSys),
		}
	}

	return p
}

func writePhaseBreakdown(w io.Writer, s *QueryStats) {
	total := s.TotalDuration
	if total <= 0 {
		return
	}

	fmt.Fprintln(w, "  Phase Breakdown:")

	type phase struct {
		name string
		dur  time.Duration
	}

	// For server queries, use scan/pipeline breakdown; for ephemeral, use exec.
	var phases []phase
	if s.ScanDuration > 0 || s.PipelineDuration > 0 {
		phases = []phase{
			{"parse", s.ParseDuration},
			{"optimize", s.OptimizeDuration},
			{"scan", s.ScanDuration},
			{"pipeline", s.PipelineDuration},
			{"serialize", s.SerializeDuration},
		}
	} else {
		phases = []phase{
			{"parse", s.ParseDuration},
			{"optimize", s.OptimizeDuration},
			{"exec", s.ExecDuration},
			{"serialize", s.SerializeDuration},
		}
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

func writeSkipBreakdownCompact(w io.Writer, s *QueryStats) {
	var parts []string
	if s.BloomSkippedSegments > 0 {
		parts = append(parts, fmt.Sprintf("bloom: %d", s.BloomSkippedSegments))
	}
	if s.TimeSkippedSegments > 0 {
		parts = append(parts, fmt.Sprintf("time: %d", s.TimeSkippedSegments))
	}
	if s.StatSkippedSegments > 0 {
		parts = append(parts, fmt.Sprintf("stats: %d", s.StatSkippedSegments))
	}
	if s.IndexSkippedSegments > 0 {
		parts = append(parts, fmt.Sprintf("index: %d", s.IndexSkippedSegments))
	}
	if s.RangeSkippedSegments > 0 {
		parts = append(parts, fmt.Sprintf("range: %d", s.RangeSkippedSegments))
	}
	if len(parts) > 0 {
		fmt.Fprintf(w, "    %s\n", strings.Join(parts, "  "))
	}
}

func writePipelineBreakdown(w io.Writer, s *QueryStats) {
	// Compute exclusive timing before rendering.
	ComputeExclusiveTiming(s.Stages)

	fmt.Fprintln(w, "  Pipeline:")
	fmt.Fprintf(w, "    %-14s %10s %s %10s  %8s  %8s  %5s\n",
		"STAGE", "INPUT", " ", "OUTPUT", "TIME", "SELF", "%")
	fmt.Fprintf(w, "    %s\n",
		strings.Repeat("─", 68))

	totalPipeline := s.ExecDuration
	if s.PipelineDuration > 0 {
		totalPipeline = s.PipelineDuration
	}

	for _, stage := range s.Stages {
		pct := float64(0)
		if totalPipeline > 0 {
			pct = float64(stage.Duration) / float64(totalPipeline) * 100
		}
		exclusiveDur := time.Duration(stage.ExclusiveNS)
		fmt.Fprintf(w, "    %-14s %10s %s %10s  %8s  %8s  %4.0f%%\n",
			stage.Name,
			formatCommaInt64(stage.InputRows),
			arrow,
			formatCommaInt64(stage.OutputRows),
			formatDur(stage.Duration),
			formatDur(exclusiveDur),
			pct)
	}
}

func writeOptimizerRules(w io.Writer, s *QueryStats) {
	applied := len(s.OptimizerRules)
	total := s.OptimizerTotalRules
	if total == 0 {
		total = 28 // reasonable default
	}

	fmt.Fprintf(w, "  Optimizer (%d/%d rules applied):\n", applied, total)

	// Find max name length for alignment.
	maxNameLen := 0
	for _, r := range s.OptimizerRules {
		if len(r.Name) > maxNameLen {
			maxNameLen = len(r.Name)
		}
	}

	for _, r := range s.OptimizerRules {
		fmt.Fprintf(w, "    %-*s  %s\n", maxNameLen, r.Name, r.Description)
	}
}

func writeResources(w io.Writer, s *QueryStats) {
	fmt.Fprint(w, "  Resources:  ")

	var parts []string
	if s.PeakMemoryBytes > 0 {
		parts = append(parts, fmt.Sprintf("memory: %s peak", formatBytesHuman(s.PeakMemoryBytes)))
	}
	if s.CPUTimeUser > 0 {
		parts = append(parts, fmt.Sprintf("CPU: %s user", formatDur(s.CPUTimeUser)))
	}
	fmt.Fprintln(w, strings.Join(parts, "  "))
}

// writeOperatorFlow writes a compact one-liner showing the pipeline data flow.
// Example: Operator Flow:  Scan (12.4M) → Search (1,247) → Tail (1).
func writeOperatorFlow(w io.Writer, s *QueryStats) {
	parts := make([]string, 0, len(s.Stages))
	for _, stage := range s.Stages {
		parts = append(parts, fmt.Sprintf("%s (%s)", stage.Name, formatHumanInt64(stage.OutputRows)))
	}
	fmt.Fprintf(w, "  Operator Flow:  %s\n", strings.Join(parts, " → "))
}

// writeSegmentDetails writes a per-segment I/O table (trace level only).
func writeSegmentDetails(w io.Writer, s *QueryStats) {
	fmt.Fprintln(w, "  Per-Segment I/O:")
	fmt.Fprintf(w, "    %-16s %-7s %10s %10s  %-5s  %-7s  %8s\n",
		"SEGMENT", "SOURCE", "ROWS", "FILTERED", "BLOOM", "INV.IDX", "READ")
	fmt.Fprintf(w, "    %s\n", strings.Repeat("─", 74))

	for _, sd := range s.SegmentDetails {
		bloomStr := "no"
		if sd.BloomHit {
			bloomStr = "yes"
		}
		invStr := "no"
		if sd.InvertedUsed {
			invStr = "yes"
		}
		segID := sd.SegmentID
		if len(segID) > 16 {
			segID = segID[:16]
		}
		fmt.Fprintf(w, "    %-16s %-7s %10s %10s  %-5s  %-7s  %8s\n",
			segID,
			sd.Source,
			formatHumanInt64(sd.Rows),
			formatHumanInt64(sd.RowsAfterFilter),
			bloomStr,
			invStr,
			formatDur(sd.ReadDuration))
	}
}

// writeVMMetrics writes a VM execution summary line (trace level only).
func writeVMMetrics(w io.Writer, s *QueryStats) {
	nsPerCall := float64(0)
	if s.VMCalls > 0 {
		nsPerCall = float64(s.VMTotalTime.Nanoseconds()) / float64(s.VMCalls)
	}
	fmt.Fprintf(w, "  VM:  %s calls", formatHumanInt64(s.VMCalls))
	if nsPerCall > 0 {
		fmt.Fprintf(w, "  %.1fns/call", nsPerCall)
	}
	fmt.Fprintf(w, "  %s total\n", formatDur(s.VMTotalTime))
}

// writeOperatorBudgets renders a per-operator memory budget table showing how
// the coordinator distributed memory across spillable operators.
func writeOperatorBudgets(w io.Writer, s *QueryStats) {
	fmt.Fprintln(w, "  Memory Budget:")
	fmt.Fprintf(w, "    %-14s %10s %10s  %-10s  %s\n",
		"OPERATOR", "BUDGET", "PEAK", "PHASE", "SPILLED")
	fmt.Fprintf(w, "    %s\n", strings.Repeat("─", 56))

	for _, b := range s.OperatorBudgets {
		spilled := "no"
		if b.Spilled {
			spilled = "yes"
		}
		fmt.Fprintf(w, "    %-14s %10s %10s  %-10s  %s\n",
			b.Name,
			formatBytesHuman(b.SoftLimit),
			formatBytesHuman(b.PeakBytes),
			b.Phase,
			spilled)
	}
}

func writeRecommendations(w io.Writer, s *QueryStats) {
	fmt.Fprintln(w, "  Recommendations:")
	for _, r := range s.Recommendations {
		if r.EstimatedSpeedup != "" {
			fmt.Fprintf(w, "    [%s] %s (%s speedup)\n", r.Priority, r.Message, r.EstimatedSpeedup)
		} else {
			fmt.Fprintf(w, "    [%s] %s\n", r.Priority, r.Message)
		}
		if r.SuggestedAction != "" {
			fmt.Fprintf(w, "      → %s\n", r.SuggestedAction)
		}
	}
}

// durMS converts a time.Duration to float64 milliseconds.
func durMS(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

type profileJSON struct {
	TotalMS    int64           `json:"total_ms"`
	ParseMS    float64         `json:"parse_ms,omitempty"`
	OptMS      float64         `json:"optimize_ms,omitempty"`
	ExecMS     float64         `json:"exec_ms,omitempty"`
	SerialMS   float64         `json:"serialize_ms,omitempty"`
	ScanMS     float64         `json:"scan_ms,omitempty"`
	PipMS      float64         `json:"pipeline_ms,omitempty"`
	Scanned    int64           `json:"scanned_rows"`
	Matched    int64           `json:"matched_rows"`
	Results    int64           `json:"result_rows"`
	ScanType   string          `json:"scan_type,omitempty"`
	Ephemeral  bool            `json:"ephemeral,omitempty"`
	Segments   *segmentsJSON   `json:"segments,omitempty"`
	Pipeline   []stageJSON     `json:"pipeline,omitempty"`
	Optimizer  []ruleJSON      `json:"optimizer,omitempty"`
	Budgets    []budgetJSON    `json:"budgets,omitempty"`
	Recs       []recJSON       `json:"recommendations,omitempty"`
	Resources  *resourcesJSON  `json:"resources,omitempty"`
	SegDetails []segDetailJSON `json:"segment_details,omitempty"`
	VM         *vmJSON         `json:"vm,omitempty"`
}

type budgetJSON struct {
	Name      string `json:"name"`
	SoftLimit int64  `json:"soft_limit"`
	PeakBytes int64  `json:"peak_bytes"`
	Phase     string `json:"phase"`
	Spilled   bool   `json:"spilled"`
}

type segmentsJSON struct {
	Total   int `json:"total"`
	Scanned int `json:"scanned"`
	Bloom   int `json:"bloom_skipped,omitempty"`
	Time    int `json:"time_skipped,omitempty"`
	Stats   int `json:"stat_skipped,omitempty"`
	Errored int `json:"errored,omitempty"`
}

type stageJSON struct {
	Name   string  `json:"name"`
	Input  int64   `json:"input_rows"`
	Output int64   `json:"output_rows"`
	MS     float64 `json:"duration_ms"`
}

type ruleJSON struct {
	Name  string `json:"name"`
	Desc  string `json:"description"`
	Count int    `json:"count"`
}

type recJSON struct {
	Cat     string `json:"category"`
	Pri     string `json:"priority"`
	Message string `json:"message"`
	Speedup string `json:"estimated_speedup,omitempty"`
	Action  string `json:"suggested_action,omitempty"`
}

type segDetailJSON struct {
	ID              string  `json:"segment_id"`
	Source          string  `json:"source"`
	Rows            int64   `json:"rows"`
	RowsAfterFilter int64   `json:"rows_after_filter"`
	BloomHit        bool    `json:"bloom_hit"`
	InvertedUsed    bool    `json:"inverted_used"`
	ReadMS          float64 `json:"read_ms"`
	BytesRead       int64   `json:"bytes_read"`
}

type vmJSON struct {
	Calls   int64   `json:"calls"`
	TotalMS float64 `json:"total_ms"`
}

type resourcesJSON struct {
	PeakMem    int64   `json:"peak_memory_bytes,omitempty"`
	AllocBytes int64   `json:"alloc_bytes,omitempty"`
	CPUUserMS  float64 `json:"cpu_user_ms,omitempty"`
	CPUSysMS   float64 `json:"cpu_sys_ms,omitempty"`
}
