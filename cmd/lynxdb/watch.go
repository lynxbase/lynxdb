package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/timerange"
)

func init() {
	rootCmd.AddCommand(newWatchCmd())
}

func newWatchCmd() *cobra.Command {
	var (
		interval string
		since    string
		diff     bool
	)

	cmd := &cobra.Command{
		Use:   "watch [SPL2 query]",
		Short: "Re-run a query at regular intervals",
		Long:  `Periodically executes a query and refreshes the output. Optionally shows deltas from the previous run.`,
		Example: `  lynxdb watch 'level=error | stats count by source'
  lynxdb watch 'level=error | stats count' --interval 10s
  lynxdb watch '| stats count by level' --since 1h --diff`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			query := strings.Join(args, " ")

			return runWatch(query, interval, since, diff)
		},
	}

	f := cmd.Flags()
	f.StringVar(&interval, "interval", "5s", "Refresh interval (e.g., 5s, 30s, 1m)")
	f.StringVarP(&since, "since", "s", "-15m", "Time range for each execution")
	f.BoolVar(&diff, "diff", false, "Show delta from previous run")

	return cmd
}

// Bubbletea messages

type watchResultMsg struct {
	rows     []map[string]interface{}
	queryDur time.Duration
	err      error
}

type watchTickMsg struct{}

// Bubbletea model

type watchModel struct {
	spinner    spinner.Model
	theme      *ui.Theme
	query      string
	interval   time.Duration
	since      string
	showDiff   bool
	rows       []map[string]interface{}
	prevRows   []map[string]interface{}
	queryDur   time.Duration
	iterations int
	loading    bool
	err        error
	width      int
}

func newWatchModel(query string, interval time.Duration, since string, showDiff bool) watchModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = ui.Stdout.Accent

	return watchModel{
		spinner:  s,
		theme:    ui.Stdout,
		query:    query,
		interval: interval,
		since:    since,
		showDiff: showDiff,
		loading:  true,
		width:    120,
	}
}

func (m watchModel) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		executeWatchQueryCmd(m.query, m.since),
	)
}

func (m watchModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
	case watchResultMsg:
		m.loading = false
		m.err = msg.err
		if msg.err == nil {
			m.prevRows = m.rows
			m.rows = msg.rows
			m.queryDur = msg.queryDur
			m.iterations++
		}

		return m, tea.Tick(m.interval, func(time.Time) tea.Msg {
			return watchTickMsg{}
		})
	case watchTickMsg:
		m.loading = true

		return m, executeWatchQueryCmd(m.query, m.since)
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)

		return m, cmd
	}

	return m, nil
}

func (m watchModel) View() tea.View {
	v := tea.NewView(m.renderView())
	v.AltScreen = true

	return v
}

func (m watchModel) renderView() string {
	t := m.theme
	var b strings.Builder

	now := time.Now().Format("15:04:05")
	fmt.Fprintf(&b, "\n  %s %s  %s\n\n",
		t.Dim.Render(fmt.Sprintf("Every %s:", m.interval)),
		m.query,
		t.Dim.Render(now))

	if m.loading && m.iterations == 0 {
		fmt.Fprintf(&b, "  %s %s\n", m.spinner.View(), t.Dim.Render("Loading..."))

		return b.String()
	}

	if m.err != nil {
		fmt.Fprintf(&b, "  %s %s\n", t.IconError(), m.err.Error())

		return b.String()
	}

	if len(m.rows) == 0 {
		fmt.Fprintf(&b, "  %s\n", t.Dim.Render("No results."))
	} else {
		b.WriteString(m.renderWatchTable())
	}

	if m.loading {
		fmt.Fprintf(&b, "\n  %s %s", m.spinner.View(), t.Dim.Render("Refreshing..."))
	} else {
		fmt.Fprintf(&b, "\n  %s",
			t.Dim.Render(fmt.Sprintf("Refreshed (%s, %d rows, iteration #%d) — q to quit",
				formatElapsed(m.queryDur), len(m.rows), m.iterations)))
	}

	b.WriteByte('\n')

	return b.String()
}

func (m watchModel) renderWatchTable() string {
	if len(m.rows) == 0 {
		return ""
	}

	// Collect columns.
	var cols []string
	seen := make(map[string]bool)

	for _, row := range m.rows {
		for k := range row {
			if !seen[k] {
				seen[k] = true
				cols = append(cols, k)
			}
		}
	}

	// Add delta column if showing diff.
	headers := cols
	if m.showDiff && m.prevRows != nil {
		headers = append(headers, "\u0394")
	}

	tbl := ui.NewTable(m.theme).
		SetColumns(headers...)

	prevLookup := buildPrevLookup(m.prevRows, cols)

	for _, row := range m.rows {
		vals := make([]string, 0, len(headers))
		for _, col := range cols {
			vals = append(vals, fmt.Sprint(row[col]))
		}

		if m.showDiff && prevLookup != nil {
			vals = append(vals, computeDelta(row, prevLookup, cols))
		}

		tbl.AddRow(vals...)
	}

	return tbl.String()
}

// Commands

func executeWatchQueryCmd(query, since string) tea.Cmd {
	return func() tea.Msg {
		rows, dur, err := doWatchQuery(query, since)

		return watchResultMsg{rows: rows, queryDur: dur, err: err}
	}
}

func doWatchQuery(query, since string) ([]map[string]interface{}, time.Duration, error) {
	fullQuery := ensureFromClause(query)

	var from, to string
	if since != "" {
		tr, err := timerange.FromSince(strings.TrimPrefix(since, "-"), time.Now())
		if err != nil {
			return nil, 0, fmt.Errorf("invalid --since: %w", err)
		}

		from = tr.Earliest.Format(time.RFC3339Nano)
		to = tr.Latest.Format(time.RFC3339Nano)
	}

	start := time.Now()

	result, err := apiClient().QuerySync(context.Background(), fullQuery, from, to)
	if err != nil {
		return nil, 0, err
	}

	return queryResultToRows(result), time.Since(start), nil
}

// Helpers (reused from previous implementation)

func buildPrevLookup(prevRows []map[string]interface{}, cols []string) map[string]map[string]interface{} {
	if len(prevRows) == 0 || len(cols) == 0 {
		return nil
	}

	lookup := make(map[string]map[string]interface{}, len(prevRows))

	for _, row := range prevRows {
		key := buildRowKey(row, cols)
		lookup[key] = row
	}

	return lookup
}

func buildRowKey(row map[string]interface{}, cols []string) string {
	// Use non-numeric columns as the key.
	var parts []string

	for _, col := range cols {
		v := row[col]
		if _, ok := v.(float64); ok {
			continue
		}

		parts = append(parts, fmt.Sprintf("%s=%v", col, v))
	}

	return strings.Join(parts, "|")
}

func computeDelta(row map[string]interface{}, prevLookup map[string]map[string]interface{}, cols []string) string {
	key := buildRowKey(row, cols)
	prevRow, ok := prevLookup[key]

	if !ok {
		return "new"
	}

	// Find first numeric column and compute delta.
	for _, col := range cols {
		cur, curOK := toFloat(row[col])
		prev, prevOK := toFloat(prevRow[col])

		if !curOK || !prevOK {
			continue
		}

		diff := cur - prev

		if diff == 0 {
			return "+0"
		}

		sign := "+"
		if diff < 0 {
			sign = ""
		}

		return fmt.Sprintf("%s%.0f", sign, diff)
	}

	return ""
}

func toFloat(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	default:
		return 0, false
	}
}

func runWatch(query, intervalStr, since string, showDiff bool) error {
	dur, err := time.ParseDuration(intervalStr)
	if err != nil {
		return fmt.Errorf("invalid --interval: %w", err)
	}

	m := newWatchModel(query, dur, since, showDiff)
	p := tea.NewProgram(m)

	if _, err := p.Run(); err != nil {
		return err
	}

	return nil
}
