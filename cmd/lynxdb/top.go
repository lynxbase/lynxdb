package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
)

func init() {
	rootCmd.AddCommand(newTopCmd())
}

func newTopCmd() *cobra.Command {
	var interval string

	cmd := &cobra.Command{
		Use:   "top",
		Short: "Live TUI dashboard of server metrics",
		Long:  `Full-screen live dashboard showing ingest rate, queries, storage, and sources. Press q to quit.`,
		Example: `  lynxdb top
  lynxdb top --interval 5s`,
		RunE: func(_ *cobra.Command, _ []string) error {
			dur, err := time.ParseDuration(interval)
			if err != nil {
				return fmt.Errorf("invalid --interval: %w", err)
			}

			return runTop(dur)
		},
	}

	cmd.Flags().StringVar(&interval, "interval", "2s", "Refresh interval (e.g., 2s, 5s)")

	return cmd
}

// Data types

type topStats struct {
	Version       string
	UptimeSeconds float64
	Health        string
	StorageBytes  int64
	TotalEvents   int64
	EventsToday   int64
	SegmentCount  int
	BufferedEvts  int
	IndexCount    int
	OldestEvent   string
	ActiveQueries int
	CacheHitRate  float64
	MVTotal       int
	MVActive      int
	TailSessions  int
	Sources       []topSource
}

type topSource struct {
	Name  string
	Count int64
}

// Bubble Tea messages

type topFetchedMsg struct {
	stats topStats
	err   error
}

type topTickMsg struct{}

// Model

type topModel struct {
	spinner  spinner.Model
	theme    *ui.Theme
	server   string
	client   *client.Client
	interval time.Duration
	stats    topStats
	prevEvts int64
	prevTime time.Time
	rate     float64
	loaded   bool
	err      error
	width    int
	height   int
}

func newTopModel(server string, interval time.Duration) topModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = ui.Stdout.Accent

	return topModel{
		spinner:  s,
		theme:    ui.Stdout,
		server:   server,
		client:   apiClient(),
		interval: interval,
		width:    80,
		height:   24,
	}
}

func (m topModel) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		fetchTopStatsCmd(m.client),
	)
}

func (m topModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case topFetchedMsg:
		if msg.err != nil {
			m.err = msg.err
		} else {
			// Compute ingest rate from delta.
			now := time.Now()
			if m.loaded && m.prevTime.Before(now) {
				elapsed := now.Sub(m.prevTime).Seconds()
				if elapsed > 0 {
					delta := msg.stats.TotalEvents - m.prevEvts
					if delta >= 0 {
						m.rate = float64(delta) / elapsed
					}
				}
			}
			m.prevEvts = msg.stats.TotalEvents
			m.prevTime = now
			m.stats = msg.stats
			m.loaded = true
			m.err = nil
		}

		return m, tea.Tick(m.interval, func(time.Time) tea.Msg {
			return topTickMsg{}
		})
	case topTickMsg:
		return m, fetchTopStatsCmd(m.client)
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)

		return m, cmd
	}

	return m, nil
}

func (m topModel) View() tea.View {
	v := tea.NewView(m.renderView())
	v.AltScreen = true

	return v
}

func (m topModel) renderView() string {
	if !m.loaded {
		if m.err != nil {
			return fmt.Sprintf("\n  %s %s\n\n  %s %s\n",
				m.spinner.View(),
				m.theme.Dim.Render("Connecting to "+m.server+"..."),
				m.theme.IconError(),
				m.err.Error())
		}

		return fmt.Sprintf("\n  %s %s\n",
			m.spinner.View(),
			m.theme.Dim.Render("Connecting to "+m.server+"..."))
	}

	t := m.theme
	var b strings.Builder

	// Header line.
	now := time.Now().Format("15:04:05")
	uptimeStr := formatDuration(int64(m.stats.UptimeSeconds))
	healthStr := colorizeHealthStatus(t, m.stats.Health)

	header := fmt.Sprintf("  LynxDB %s \u2014 %s \u2014 uptime %s \u2014 %s",
		m.stats.Version, m.server, uptimeStr, healthStr)
	headerRight := now

	pad := m.width - lipgloss.Width(header) - len(headerRight) - 2
	if pad < 1 {
		pad = 1
	}

	b.WriteByte('\n')
	b.WriteString(t.Bold.Render(header))
	b.WriteString(strings.Repeat(" ", pad))
	b.WriteString(t.Dim.Render(headerRight))
	b.WriteByte('\n')

	if m.err != nil {
		fmt.Fprintf(&b, "\n  %s %s\n", t.IconError(), m.err.Error())
	}

	// Calculate panel widths.
	totalW := m.width - 4 // 2 margin each side
	if totalW < 40 {
		totalW = 40
	}
	leftW := totalW / 2
	rightW := totalW - leftW - 2 // 2 for gap

	// Ingest Panel
	ingestLines := m.renderIngestPanel(leftW)

	// Queries Panel
	queryLines := m.renderQueriesPanel(rightW)

	// Storage Panel
	storageLines := m.renderStoragePanel(leftW)

	// Sources Panel
	sourceLines := m.renderSourcesPanel(rightW)

	// Render side-by-side panels.
	b.WriteByte('\n')
	renderSideBySide(&b, t, ingestLines, queryLines, leftW, rightW)
	b.WriteByte('\n')
	renderSideBySide(&b, t, storageLines, sourceLines, leftW, rightW)

	// Footer.
	b.WriteByte('\n')
	fmt.Fprintf(&b, "  %s\n",
		t.Dim.Render(fmt.Sprintf("Refreshing every %s \u2014 press q to quit", m.interval)))

	return b.String()
}

// Panel Renderers

func (m topModel) renderIngestPanel(width int) []string {
	t := m.theme
	lines := make([]string, 0, 5)
	lines = append(lines,
		renderPanelHeader(t, "Ingest", width),
		renderPanelKV(t, "Rate", fmt.Sprintf("%s evt/s", formatCount(int64(math.Round(m.rate)))), width),
		renderPanelKV(t, "Today", fmt.Sprintf("%s events", formatCountHuman(m.stats.EventsToday)), width),
		renderPanelKV(t, "Total", fmt.Sprintf("%s events", formatCountHuman(m.stats.TotalEvents)), width),
		renderPanelFooter(t, width),
	)

	return lines
}

func (m topModel) renderQueriesPanel(width int) []string {
	t := m.theme
	lines := make([]string, 0, 5)
	cacheStr := "n/a"
	if m.stats.CacheHitRate >= 0 {
		cacheStr = fmt.Sprintf("%.0f%%", m.stats.CacheHitRate*100)
	}
	mvStr := fmt.Sprintf("%d active / %d total", m.stats.MVActive, m.stats.MVTotal)

	lines = append(lines,
		renderPanelHeader(t, "Queries", width),
		renderPanelKV(t, "Active", fmt.Sprintf("%d", m.stats.ActiveQueries), width),
		renderPanelKV(t, "Cache hit", cacheStr, width),
		renderPanelKV(t, "Views", mvStr, width),
	)

	if m.stats.TailSessions > 0 {
		lines = append(lines,
			renderPanelKV(t, "Tail", fmt.Sprintf("%d sessions", m.stats.TailSessions), width))
	}

	lines = append(lines, renderPanelFooter(t, width))

	return lines
}

func (m topModel) renderStoragePanel(width int) []string {
	t := m.theme
	lines := make([]string, 0, 5)
	lines = append(lines,
		renderPanelHeader(t, "Storage", width),
		renderPanelKV(t, "Used", formatBytes(m.stats.StorageBytes), width),
		renderPanelKV(t, "Segments", formatCount(int64(m.stats.SegmentCount)), width),
		renderPanelKV(t, "Buffer", fmt.Sprintf("%s events", formatCount(int64(m.stats.BufferedEvts))), width),
		renderPanelKV(t, "Indexes", formatCount(int64(m.stats.IndexCount)), width),
	)

	if m.stats.OldestEvent != "" {
		lines = append(lines, renderPanelKV(t, "Oldest", formatRelativeTime(m.stats.OldestEvent), width))
	}

	lines = append(lines, renderPanelFooter(t, width))

	return lines
}

func (m topModel) renderSourcesPanel(width int) []string {
	t := m.theme
	lines := make([]string, 0, 10)
	lines = append(lines, renderPanelHeader(t, "Sources (events)", width))

	if len(m.stats.Sources) == 0 {
		lines = append(lines, renderPanelLine(t, t.Dim.Render("  No sources yet"), width))
	} else {
		// Find max count for bar scaling.
		maxCount := int64(1)
		for _, s := range m.stats.Sources {
			if s.Count > maxCount {
				maxCount = s.Count
			}
		}

		// Show up to 8 sources.
		shown := m.stats.Sources
		if len(shown) > 8 {
			shown = shown[:8]
		}

		barMaxWidth := width - 30 // space for name + count + padding
		if barMaxWidth < 5 {
			barMaxWidth = 5
		}

		for _, s := range shown {
			barLen := int(float64(s.Count) / float64(maxCount) * float64(barMaxWidth))
			if barLen < 1 && s.Count > 0 {
				barLen = 1
			}
			bar := strings.Repeat("\u2593", barLen)
			nameStr := fmt.Sprintf("  %-14s", truncateStr(s.Name, 14))
			countStr := formatCountHuman(s.Count)
			content := fmt.Sprintf("%s %s %s",
				nameStr,
				t.Accent.Render(bar),
				t.Value.Render(countStr))
			lines = append(lines, renderPanelLine(t, content, width))
		}

		if len(m.stats.Sources) > 8 {
			more := fmt.Sprintf("  +%d more", len(m.stats.Sources)-8)
			lines = append(lines, renderPanelLine(t, t.Dim.Render(more), width))
		}
	}

	lines = append(lines, renderPanelFooter(t, width))

	return lines
}

// Panel rendering helpers

func renderPanelHeader(t *ui.Theme, title string, width int) string {
	prefix := "\u250c\u2500 "
	suffix := " "
	lineLen := width - lipgloss.Width(prefix) - len(title) - len(suffix) - 1
	if lineLen < 0 {
		lineLen = 0
	}

	return t.Rule.Render(prefix) +
		t.Bold.Render(title) +
		t.Rule.Render(suffix+strings.Repeat("\u2500", lineLen)+"\u2510")
}

func renderPanelFooter(t *ui.Theme, width int) string {
	lineLen := width - 2
	if lineLen < 0 {
		lineLen = 0
	}

	return t.Rule.Render("\u2514" + strings.Repeat("\u2500", lineLen) + "\u2518")
}

func renderPanelKV(t *ui.Theme, key, value string, width int) string {
	kv := fmt.Sprintf("  %-10s %s", key+":", value)
	padLen := width - lipgloss.Width(kv) - 4 // 2 for borders + 2 margin
	if padLen < 0 {
		padLen = 0
	}

	return t.Rule.Render("\u2502") + " " +
		t.Label.Render(fmt.Sprintf("%-10s", key+":")) + " " +
		t.Value.Render(value) +
		strings.Repeat(" ", padLen) +
		" " + t.Rule.Render("\u2502")
}

func renderPanelLine(t *ui.Theme, content string, width int) string {
	padLen := width - lipgloss.Width(content) - 4
	if padLen < 0 {
		padLen = 0
	}

	return t.Rule.Render("\u2502") + " " +
		content +
		strings.Repeat(" ", padLen) +
		" " + t.Rule.Render("\u2502")
}

func renderSideBySide(b *strings.Builder, _ *ui.Theme, left, right []string, leftW, rightW int) {
	maxLines := len(left)
	if len(right) > maxLines {
		maxLines = len(right)
	}

	for i := 0; i < maxLines; i++ {
		b.WriteString("  ")

		if i < len(left) {
			line := left[i]
			lineW := lipgloss.Width(line)
			b.WriteString(line)
			if lineW < leftW {
				b.WriteString(strings.Repeat(" ", leftW-lineW))
			}
		} else {
			b.WriteString(strings.Repeat(" ", leftW))
		}

		b.WriteString("  ") // gap

		if i < len(right) {
			line := right[i]
			lineW := lipgloss.Width(line)
			b.WriteString(line)
			if lineW < rightW {
				b.WriteString(strings.Repeat(" ", rightW-lineW))
			}
		}

		b.WriteByte('\n')
	}
}

// Fetch command

func fetchTopStatsCmd(c *client.Client) tea.Cmd {
	return func() tea.Msg {
		stats, err := fetchTopStats(c)

		return topFetchedMsg{stats: stats, err: err}
	}
}

func fetchTopStats(c *client.Client) (topStats, error) {
	var ts topStats

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	// Fetch /api/v1/status (unified endpoint).
	status, err := c.Status(ctx)
	if err != nil {
		return ts, fmt.Errorf("connect: %w", err)
	}

	ts.Version = status.Version
	if ts.Version == "" {
		ts.Version = "dev"
	}
	ts.UptimeSeconds = float64(status.UptimeSeconds)
	ts.Health = status.Health
	if ts.Health == "" {
		ts.Health = "unknown"
	}
	ts.StorageBytes = status.Storage.UsedBytes
	ts.TotalEvents = status.Events.Total
	ts.EventsToday = status.Events.Today
	ts.ActiveQueries = status.Queries.Active
	ts.MVTotal = status.Views.Total
	ts.MVActive = status.Views.Active

	if status.Tail != nil {
		ts.TailSessions = status.Tail.ActiveSessions
	}

	if status.Retention != nil {
		ts.OldestEvent = status.Retention.OldestEvent
	}

	// Fetch /api/v1/stats for source info and segment counts.
	ts.CacheHitRate = -1 // default: not available

	stats, statsErr := c.Stats(ctx)
	if statsErr == nil {
		ts.SegmentCount = stats.SegmentCount
		ts.BufferedEvts = stats.BufferedEvents
		ts.IndexCount = stats.IndexCount

		for _, s := range stats.Sources {
			if s.Name != "" {
				ts.Sources = append(ts.Sources, topSource{
					Name:  s.Name,
					Count: s.Count,
				})
			}
		}
	}

	// Sort sources by count descending.
	sort.Slice(ts.Sources, func(i, j int) bool {
		return ts.Sources[i].Count > ts.Sources[j].Count
	})

	// Fetch cache hit rate.
	cacheStats, cacheErr := c.CacheStats(ctx)
	if cacheErr == nil {
		if v, ok := cacheStats["hit_rate"].(float64); ok {
			ts.CacheHitRate = v
		}
	}

	return ts, nil
}

func runTop(interval time.Duration) error {
	m := newTopModel(globalServer, interval)
	p := tea.NewProgram(m)

	if _, err := p.Run(); err != nil {
		return err
	}

	return nil
}
