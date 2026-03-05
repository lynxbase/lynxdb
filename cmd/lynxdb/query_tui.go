package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/stats"
)

type jobCreatedMsg string
type pollTickMsg struct{}

type progressMsg struct {
	phase               string
	segmentsTotal       int
	segmentsScanned     int
	segmentsDispatched  int
	segmentsSkippedIdx  int
	segmentsSkippedTime int
	segmentsSkippedStat int
	segmentsSkippedBF   int
	bufferedEvents      int
	rowsReadSoFar       int64
	elapsedMS           float64
}

type searchDoneMsg struct {
	rows []map[string]interface{}
	meta client.Meta
}

type searchErrMsg struct{ err error }

type searchModel struct {
	spinner   spinner.Model
	client    *client.Client
	query     string
	earliest  string
	latest    string
	jobID     string
	startTime time.Time
	width     int

	rows         []map[string]interface{}
	rowsReturned int64
	serverMeta   client.Meta

	pPhase               string
	pSegmentsTotal       int
	pSegmentsScanned     int
	pSegmentsDispatched  int
	pSegmentsSkippedIdx  int
	pSegmentsSkippedTime int
	pSegmentsSkippedStat int
	pSegmentsSkippedBF   int
	pBufferedEvents      int
	pRowsReadSoFar       int64
	pElapsedMS           float64

	pollCount int

	done     bool
	canceled bool
	errStr   string
	err      error // original error for type-aware rendering in Execute()
}

func newSearchModel(c *client.Client, query, earliest, latest string) searchModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = ui.Stdout.Accent

	return searchModel{
		spinner:   s,
		client:    c,
		query:     query,
		earliest:  earliest,
		latest:    latest,
		startTime: time.Now(),
		width:     120,
	}
}

func (m searchModel) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, submitJobCmd(m.client, m.query, m.earliest, m.latest))
}

func submitJobCmd(c *client.Client, query, earliest, latest string) tea.Cmd {
	return func() tea.Msg {
		job, err := c.QueryAsync(context.Background(), query, earliest, latest)
		if err != nil {
			return searchErrMsg{err}
		}

		if job.JobID == "" {
			return searchErrMsg{fmt.Errorf("missing job_id in response")}
		}

		return jobCreatedMsg(job.JobID)
	}
}

func pollJobCmd(c *client.Client, jobID string, pollCount int) tea.Cmd {
	return func() tea.Msg {
		// Progressive backoff: 0ms first poll (catch fast queries),
		// then 30ms, 60ms, capped at 80ms.
		if pollCount > 0 {
			delay := time.Duration(min(pollCount*30, 80)) * time.Millisecond
			time.Sleep(delay)
		}

		job, err := c.GetJob(context.Background(), jobID)
		if err != nil {
			return searchErrMsg{err}
		}

		switch job.Status {
		case "done", "complete":
			return extractDoneFromJob(job)
		case "error", "failed":
			if job.Error != nil {
				return searchErrMsg{fmt.Errorf("%s", job.Error.Message)}
			}

			return searchErrMsg{fmt.Errorf("query failed")}
		case "canceled":
			return searchErrMsg{fmt.Errorf("query was canceled")}
		case "running":
			if job.Progress != nil {
				return extractProgressFromJob(job.Progress)
			}

			return pollTickMsg{}
		}

		// If the job has results, treat it as done regardless of status string.
		if job.Results != nil {
			return extractDoneFromJob(job)
		}

		return pollTickMsg{}
	}
}

func extractDoneFromJob(job *client.JobResult) searchDoneMsg {
	var rows []map[string]interface{}

	if job.Results != nil {
		var typed struct {
			Type    string                   `json:"type"`
			Events  []map[string]interface{} `json:"events"`
			Columns []string                 `json:"columns"`
			Rows    [][]interface{}          `json:"rows"`
		}
		if json.Unmarshal(*job.Results, &typed) == nil {
			switch typed.Type {
			case "events":
				rows = typed.Events
			case "aggregate", "timechart":
				for _, row := range typed.Rows {
					m := make(map[string]interface{}, len(typed.Columns))
					for j, col := range typed.Columns {
						if j < len(row) {
							m[col] = row[j]
						}
					}
					rows = append(rows, m)
				}
			default:
				// Fallback: try events.
				rows = typed.Events
			}
		}
	}

	return searchDoneMsg{rows: rows, meta: job.Meta}
}

func extractProgressFromJob(prog *client.JobProgress) progressMsg {
	return progressMsg{
		phase:               prog.Phase,
		segmentsTotal:       prog.SegmentsTotal,
		segmentsScanned:     prog.SegmentsScanned,
		segmentsDispatched:  prog.SegmentsDispatched,
		segmentsSkippedIdx:  prog.SegmentsSkippedIndex,
		segmentsSkippedTime: prog.SegmentsSkippedTime,
		segmentsSkippedStat: prog.SegmentsSkippedStats,
		segmentsSkippedBF:   prog.SegmentsSkippedBloom,
		bufferedEvents:      prog.BufferedEvents,
		rowsReadSoFar:       prog.RowsReadSoFar,
		elapsedMS:           float64(prog.ElapsedMS),
	}
}

func (m searchModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		if msg.String() == "ctrl+c" {
			m.canceled = true

			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)

		return m, cmd
	case jobCreatedMsg:
		m.jobID = string(msg)
		m.pollCount = 0

		return m, pollJobCmd(m.client, m.jobID, m.pollCount)
	case progressMsg:
		m.pPhase = msg.phase
		m.pSegmentsTotal = msg.segmentsTotal
		m.pSegmentsScanned = msg.segmentsScanned
		m.pSegmentsDispatched = msg.segmentsDispatched
		m.pSegmentsSkippedIdx = msg.segmentsSkippedIdx
		m.pSegmentsSkippedTime = msg.segmentsSkippedTime
		m.pSegmentsSkippedStat = msg.segmentsSkippedStat
		m.pSegmentsSkippedBF = msg.segmentsSkippedBF
		m.pBufferedEvents = msg.bufferedEvents
		m.pRowsReadSoFar = msg.rowsReadSoFar
		m.pElapsedMS = msg.elapsedMS
		m.pollCount++

		return m, pollJobCmd(m.client, m.jobID, m.pollCount)
	case pollTickMsg:
		m.pollCount++

		return m, pollJobCmd(m.client, m.jobID, m.pollCount)
	case searchDoneMsg:
		m.done = true
		m.parseResults(msg.rows, msg.meta)

		return m, tea.Quit
	case searchErrMsg:
		m.done = true
		m.errStr = msg.err.Error()
		m.err = msg.err

		return m, tea.Quit
	}

	return m, nil
}

func (m *searchModel) parseResults(rows []map[string]interface{}, meta client.Meta) {
	m.rows = rows
	m.rowsReturned = int64(len(rows))
	m.serverMeta = meta
}

// TUI View
//
// The TUI only renders progress on stderr (via tea.WithOutput(os.Stderr)).
// Results and stats are printed after the TUI exits in doQueryTUI, keeping
// stdout clean for any --format value.

func (m searchModel) View() tea.View {
	if m.err != nil || m.done {
		// Don't render in TUI — results/errors are handled after p.Run().
		return tea.NewView("")
	}

	return tea.NewView(m.renderProgress())
}

func phaseDisplayName(phase string) string {
	switch phase {
	case "parsing":
		return "Parsing query..."
	case "scanning_buffer":
		return "Scanning buffer..."
	case "filtering_segments":
		return "Filtering segments..."
	case "scanning_segments":
		return "Scanning segments..."
	case "executing_pipeline":
		return "Executing pipeline..."
	default:
		return "Searching..."
	}
}

func (m searchModel) renderProgress() string {
	el := time.Since(m.startTime).Round(10 * time.Millisecond)
	var b strings.Builder
	t := ui.Stdout
	fmt.Fprintf(&b, "\n  %s %s  %s\n",
		m.spinner.View(),
		t.Value.Render(phaseDisplayName(m.pPhase)),
		t.Label.Render(el.String()))

	hasDetail := m.pBufferedEvents > 0 || m.pSegmentsTotal > 0 || m.pRowsReadSoFar > 0
	if hasDetail {
		b.WriteByte('\n')
	}

	if m.pBufferedEvents > 0 {
		b.WriteString("    " + t.Label.Render("Buffer:  ") +
			t.Value.Render(formatCount(int64(m.pBufferedEvents))+" events") + "\n")
	}

	if m.pSegmentsTotal > 0 {
		// Use SegmentsDispatched if available (batch path sets it).
		// For the streaming path, compute remaining from available data.
		totalSkippedAll := m.pSegmentsSkippedBF + m.pSegmentsSkippedTime +
			m.pSegmentsSkippedIdx + m.pSegmentsSkippedStat
		toScan := m.pSegmentsDispatched
		if toScan == 0 {
			remaining := m.pSegmentsTotal - m.pSegmentsScanned - totalSkippedAll
			if remaining < 0 {
				remaining = 0
			}
			toScan = remaining
		}
		b.WriteString("    " + t.Label.Render("Segments:  ") +
			t.Value.Render(fmt.Sprintf("%s scanned / %s to scan / %s total",
				formatCount(int64(m.pSegmentsScanned)),
				formatCount(int64(toScan)),
				formatCount(int64(m.pSegmentsTotal)))) + "\n")

		totalSkipped := totalSkippedAll
		if totalSkipped > 0 {
			var parts []string
			if m.pSegmentsSkippedBF > 0 {
				parts = append(parts, fmt.Sprintf("bloom:%s", formatCount(int64(m.pSegmentsSkippedBF))))
			}
			if m.pSegmentsSkippedTime > 0 {
				parts = append(parts, fmt.Sprintf("time:%s", formatCount(int64(m.pSegmentsSkippedTime))))
			}
			if m.pSegmentsSkippedIdx > 0 {
				parts = append(parts, fmt.Sprintf("index:%s", formatCount(int64(m.pSegmentsSkippedIdx))))
			}
			if m.pSegmentsSkippedStat > 0 {
				parts = append(parts, fmt.Sprintf("stats:%s", formatCount(int64(m.pSegmentsSkippedStat))))
			}
			b.WriteString("    " + t.Label.Render("Skipped:   ") +
				t.Dim.Render(fmt.Sprintf("%s (%s)", formatCount(int64(totalSkipped)), strings.Join(parts, ", "))) + "\n")
		}
	}

	if m.pRowsReadSoFar > 0 {
		b.WriteString("    " + t.Label.Render("Rows read: ") +
			t.Value.Render(formatCount(m.pRowsReadSoFar)) + "\n")
	}

	b.WriteByte('\n')

	return b.String()
}

// doQueryTUI runs a TUI-mode query with a progress spinner on stderr and
// formatted results on stdout. This allows all --format values (json, csv,
// table, etc.) to work correctly while still showing interactive progress.
func doQueryTUI(_ context.Context, query, since, earliest, latest string, failEmpty bool, analyze string) error {
	c := apiClient()
	m := newSearchModel(c, query, earliest, latest)

	p := tea.NewProgram(m, tea.WithOutput(os.Stderr))

	final, err := p.Run()
	if err != nil {
		return err
	}

	fm, ok := final.(searchModel)
	if !ok {
		return fmt.Errorf("query_tui: unexpected model type")
	}

	if fm.err != nil {
		// Return the original error to preserve *client.APIError type
		// so Execute() can dispatch to the correct renderer.
		return &queryError{inner: fm.err, query: query}
	}

	// User pressed Ctrl+C — exit silently without "no results" guidance.
	if fm.canceled {
		return nil
	}

	rows := fm.rows
	elapsed := time.Since(fm.startTime).Round(time.Millisecond)

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

	st := buildQueryStatsFromMeta(fm.serverMeta, int64(len(rows)), elapsed)

	// Supplement with progress-phase skip counts if the final meta didn't
	// include them (progress is collected during polling, meta is final).
	if st.BloomSkippedSegments == 0 && fm.pSegmentsSkippedBF > 0 {
		st.BloomSkippedSegments = fm.pSegmentsSkippedBF
	}
	if st.TimeSkippedSegments == 0 && fm.pSegmentsSkippedTime > 0 {
		st.TimeSkippedSegments = fm.pSegmentsSkippedTime
	}
	if st.IndexSkippedSegments == 0 && fm.pSegmentsSkippedIdx > 0 {
		st.IndexSkippedSegments = fm.pSegmentsSkippedIdx
	}
	if st.StatSkippedSegments == 0 && fm.pSegmentsSkippedStat > 0 {
		st.StatSkippedSegments = fm.pSegmentsSkippedStat
	}

	if analyze != "" {
		st.Recommendations = stats.GenerateRecommendations(st)

		if isJSONFormat() {
			return stats.FormatProfileJSON(os.Stderr, st)
		}

		stats.FormatProfile(os.Stderr, st)

		return nil
	}

	if !globalNoStats {
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, ui.HRuleSep())
		var statsBuf bytes.Buffer
		stats.FormatTTY(&statsBuf, st, globalVerbose, globalQuiet)
		fmt.Fprintln(os.Stderr, ui.Stderr.Dim.Render(strings.TrimRight(statsBuf.String(), "\n")))

		// Performance hint: suggest MV when query is slow and MV-compatible.
		if hint := suggestMVHint(query, elapsed, st.ScannedRows); hint != "" {
			fmt.Fprintln(os.Stderr)
			fmt.Fprintf(os.Stderr, "  %s %s\n", ui.Stdout.Warning.Render("\u26A1"), hint)
		}
	}

	return nil
}
