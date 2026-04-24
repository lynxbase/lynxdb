package shell

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"
	zone "github.com/lrstanley/bubblezone/v2"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
)

// Zone ID prefixes for mouse click detection.
const (
	zoneFieldPrefix   = "sidebar-field:"
	zoneHistPrefix    = "sidebar-hist:"
	zoneSectionPrefix = "sidebar-sec:"
)

// SidebarSection identifies a collapsible sidebar section.
type SidebarSection int

const (
	SectionServer SidebarSection = iota
	SectionIndexes
	SectionFields
	SectionQueryPlan
	SectionQueryStats
	SectionHistory
)

// Sidebar renders the right-side information panel.
// It is read-only — no keyboard focus. Mouse interaction is added later.
type Sidebar struct {
	width  int
	height int

	compactMode bool
	hasQueried  bool // switches section priority after first query
	collapsed   map[SidebarSection]bool
	mode        string // "server" or "file"

	// Data (populated by sidebar_data.go messages).
	serverInfo *client.ServerStatus
	indexes    []client.IndexInfo
	fields     []client.FieldInfo
	queryPlan  *client.ExplainResult // live query plan from debounced explain
	planState  planDisplayState      // typing, valid, invalid, empty
	queryStats *QueryStatsSnapshot
	history    []SidebarHistoryEntry
}

// planDisplayState tracks the query plan section's display state.
type planDisplayState int

const (
	planEmpty   planDisplayState = iota // editor is empty
	planTyping                          // debounce pending
	planValid                           // valid plan received
	planInvalid
)

// QueryStatsSnapshot holds post-query statistics for sidebar display.
type QueryStatsSnapshot struct {
	Query        string
	Elapsed      string
	RowsReturned int
	RowsScanned  int64
	Segments     int
	BloomSkipped int
	Accelerated  string // MV name or ""
}

// SidebarHistoryEntry holds a single history item for sidebar display.
type SidebarHistoryEntry struct {
	Query    string
	Time     string // formatted HH:MM (empty until Phase 6 JSONL)
	Duration string // formatted duration (empty until Phase 6 JSONL)
}

// NewSidebar creates a sidebar for the given mode.
func NewSidebar(mode string) Sidebar {
	// Default collapsed state depends on context (before first query).
	collapsed := map[SidebarSection]bool{
		SectionServer:     true,  // collapsed by default
		SectionIndexes:    false, // expanded
		SectionFields:     false, // expanded
		SectionQueryPlan:  true,  // auto-expands when plan data arrives
		SectionQueryStats: true,  // collapsed until first query
		SectionHistory:    true,  // collapsed until first query
	}

	return Sidebar{
		width:     sidebarDefaultWidth,
		height:    20,
		collapsed: collapsed,
		mode:      mode,
	}
}

// SetSize updates the sidebar dimensions.
func (s *Sidebar) SetSize(w, h int) {
	s.width = w
	s.height = h
}

// SetCompact sets compact mode (section headers only).
func (s *Sidebar) SetCompact(compact bool) {
	s.compactMode = compact
}

// ToggleSection collapses or expands a section.
func (s *Sidebar) ToggleSection(sec SidebarSection) {
	s.collapsed[sec] = !s.collapsed[sec]
}

// SetServerInfo updates the server info section data.
func (s *Sidebar) SetServerInfo(info *client.ServerStatus) {
	s.serverInfo = info
}

// SetIndexes updates the indexes section data.
func (s *Sidebar) SetIndexes(indexes []client.IndexInfo) {
	s.indexes = indexes
}

// SetFields updates the fields section data.
func (s *Sidebar) SetFields(fields []client.FieldInfo) {
	s.fields = fields
}

// SetQueryPlan updates the live query plan section.
func (s *Sidebar) SetQueryPlan(result *client.ExplainResult) {
	s.queryPlan = result

	if result == nil {
		s.planState = planEmpty
		s.collapsed[SectionQueryPlan] = true

		return
	}

	if result.IsValid {
		s.planState = planValid
	} else {
		s.planState = planInvalid
	}

	s.collapsed[SectionQueryPlan] = false
}

// SetPlanTyping marks the plan section as waiting for debounce.
func (s *Sidebar) SetPlanTyping() {
	s.planState = planTyping
	s.collapsed[SectionQueryPlan] = false
}

// ClearQueryPlan resets the plan section to empty.
func (s *Sidebar) ClearQueryPlan() {
	s.queryPlan = nil
	s.planState = planEmpty
	s.collapsed[SectionQueryPlan] = true
}

// SetQueryStats updates the query stats section after a query.
func (s *Sidebar) SetQueryStats(stats *QueryStatsSnapshot) {
	s.queryStats = stats

	// Switch to post-query section priority.
	if !s.hasQueried {
		s.hasQueried = true
		s.collapsed[SectionQueryStats] = false
		s.collapsed[SectionHistory] = false
	}
}

// SetHistory updates the history section entries.
func (s *Sidebar) SetHistory(entries []SidebarHistoryEntry) {
	s.history = entries
}

// View renders the sidebar panel.
func (s Sidebar) View() string {
	if s.width <= 0 || s.height <= 0 {
		return ""
	}

	contentW := s.width - 1 // 1 char left padding

	var sections []string

	// Section ordering depends on whether we've queried yet.
	if s.hasQueried {
		sections = s.renderPostQuerySections(contentW)
	} else {
		sections = s.renderPreQuerySections(contentW)
	}

	content := strings.Join(sections, "\n")

	// Truncate to fit height.
	lines := strings.Split(content, "\n")
	if len(lines) > s.height {
		lines = lines[:s.height]
	}
	// Pad to exact height so the panel doesn't collapse.
	for len(lines) < s.height {
		lines = append(lines, strings.Repeat(" ", s.width))
	}

	// Apply left padding and width.
	style := lipgloss.NewStyle().
		Width(s.width).
		PaddingLeft(1)

	return style.Render(strings.Join(lines, "\n"))
}

// renderPreQuerySections renders sections before the first query.
// Priority: Indexes, Fields, QueryPlan, Server (collapsed).
func (s Sidebar) renderPreQuerySections(w int) []string {
	var out []string

	if s.mode == "server" {
		out = append(out, s.renderSection(SectionIndexes, w)...)
	}

	out = append(out, s.renderSection(SectionFields, w)...)

	if s.mode == "server" {
		out = append(out, s.renderSection(SectionQueryPlan, w)...)
		out = append(out, s.renderSection(SectionServer, w)...)
	}

	return out
}

// renderPostQuerySections renders sections after the first query.
// Priority: QueryStats, QueryPlan, Fields, History, Indexes, Server.
func (s Sidebar) renderPostQuerySections(w int) []string {
	var out []string

	out = append(out, s.renderSection(SectionQueryStats, w)...)

	if s.mode == "server" {
		out = append(out, s.renderSection(SectionQueryPlan, w)...)
	}

	out = append(out, s.renderSection(SectionFields, w)...)
	out = append(out, s.renderSection(SectionHistory, w)...)

	if s.mode == "server" {
		out = append(out, s.renderSection(SectionIndexes, w)...)
		out = append(out, s.renderSection(SectionServer, w)...)
	}

	return out
}

// renderSection renders a single section with header and optional content.
func (s Sidebar) renderSection(sec SidebarSection, w int) []string {
	title := sectionTitle(sec, s)
	collapsed := s.collapsed[sec]

	headerStyle := lipgloss.NewStyle().Foreground(ui.ColorInfo()).Bold(true)

	arrow := "▼"
	if collapsed || s.compactMode {
		arrow = "▶"
	}

	header := headerStyle.Render(fmt.Sprintf("%s %s", arrow, title))
	header = zone.Mark(zoneSectionPrefix+sectionKey(sec), header)
	lines := []string{header}

	if collapsed || s.compactMode {
		return lines
	}

	content := s.renderSectionContent(sec, w)
	lines = append(lines, content...)
	lines = append(lines, "") // blank line separator

	return lines
}

// sectionTitle returns the display title for a section.
func sectionTitle(sec SidebarSection, s Sidebar) string {
	switch sec {
	case SectionServer:
		return "Server"
	case SectionIndexes:
		if len(s.indexes) > 0 {
			return fmt.Sprintf("Indexes (%d)", len(s.indexes))
		}
		return "Indexes"
	case SectionFields:
		if len(s.fields) > 0 {
			return fmt.Sprintf("Fields (%d)", len(s.fields))
		}
		return "Fields"
	case SectionQueryPlan:
		return "Query Plan"
	case SectionQueryStats:
		return "Last Query"
	case SectionHistory:
		if len(s.history) > 0 {
			return fmt.Sprintf("Recent (%d)", len(s.history))
		}
		return "Recent"
	default:
		return ""
	}
}

// renderSectionContent renders the body of a section.
// Phase 1: placeholder text. Phase 2 will wire real data.
func (s Sidebar) renderSectionContent(sec SidebarSection, w int) []string {
	dim := lipgloss.NewStyle().Foreground(ui.ColorDim())

	switch sec {
	case SectionServer:
		return s.renderServerContent(w)
	case SectionIndexes:
		return s.renderIndexesContent(w)
	case SectionFields:
		return s.renderFieldsContent(w)
	case SectionQueryPlan:
		return s.renderQueryPlanContent(w)
	case SectionQueryStats:
		return s.renderQueryStatsContent(w)
	case SectionHistory:
		return s.renderHistoryContent(w)
	default:
		return []string{dim.Render("  (no data)")}
	}
}

func (s Sidebar) renderServerContent(w int) []string {
	dim := lipgloss.NewStyle().Foreground(ui.ColorDim())

	if s.serverInfo == nil {
		return []string{dim.Render("  connecting...")}
	}

	info := s.serverInfo
	lines := []string{
		fmt.Sprintf("  %s · up %s",
			info.Version,
			formatUptimeShort(info.UptimeSeconds)),
	}

	evtStr := formatCountShell(info.Events.Total)
	storStr := formatBytesShort(info.Storage.UsedBytes)
	lines = append(lines, fmt.Sprintf("  %s events · %s", evtStr, storStr))

	if info.Events.IngestRate > 0 {
		lines = append(lines, dim.Render(
			fmt.Sprintf("  ingest: %s ev/s", formatCountShell(int64(info.Events.IngestRate)))))
	}

	return lines
}

func (s Sidebar) renderIndexesContent(w int) []string {
	dim := lipgloss.NewStyle().Foreground(ui.ColorDim())

	if len(s.indexes) == 0 {
		return []string{dim.Render("  (none)")}
	}

	lines := make([]string, 0, len(s.indexes))
	nameW := w - 4 // "  " prefix + padding
	if nameW < 8 {
		nameW = 8
	}

	for _, idx := range s.indexes {
		name := truncateStr(idx.Name, nameW)
		lines = append(lines, fmt.Sprintf("  %s", name))
	}

	return lines
}

func (s Sidebar) renderFieldsContent(w int) []string {
	dim := lipgloss.NewStyle().Foreground(ui.ColorDim())

	if len(s.fields) == 0 {
		return []string{dim.Render("  (none)")}
	}

	// Show up to 15 fields in compact name+type format.
	maxShow := 15
	if len(s.fields) < maxShow {
		maxShow = len(s.fields)
	}

	lines := make([]string, 0, maxShow+1)

	// Calculate name column width: leave room for type.
	nameW := w - 8 // "  " prefix + "  " gap + 4-char type
	if nameW < 8 {
		nameW = 8
	}

	for i := 0; i < maxShow; i++ {
		f := s.fields[i]
		name := truncateStr(f.Name, nameW)
		ftype := shortType(f.Type)

		typeStyle := lipgloss.NewStyle().Foreground(ui.ColorDim())
		line := fmt.Sprintf("  %-*s  %s", nameW, name, typeStyle.Render(ftype))
		line = zone.Mark(zoneFieldPrefix+f.Name, line)
		lines = append(lines, line)
	}

	if len(s.fields) > maxShow {
		lines = append(lines, dim.Render(fmt.Sprintf("  ... %d more", len(s.fields)-maxShow)))
	}

	return lines
}

func (s Sidebar) renderQueryPlanContent(w int) []string {
	dim := lipgloss.NewStyle().Foreground(ui.ColorDim())

	switch s.planState {
	case planEmpty:
		return []string{dim.Render("  type a query")}
	case planTyping:
		return []string{dim.Render("  (typing...)")}
	case planInvalid:
		if s.queryPlan != nil && len(s.queryPlan.Errors) > 0 {
			msg := s.queryPlan.Errors[0].Message
			errStyle := lipgloss.NewStyle().Foreground(ui.ColorError())
			return []string{errStyle.Render("  " + truncateStr(msg, w-4))}
		}
		return []string{dim.Render("  (invalid syntax)")}
	case planValid:
		if s.queryPlan == nil || s.queryPlan.Parsed == nil {
			return []string{dim.Render("  (no plan)")}
		}

		parsed := s.queryPlan.Parsed
		lines := make([]string, 0, len(parsed.Pipeline)+2)

		for i, stage := range parsed.Pipeline {
			cmd := truncateStr(stage.Command, w-7) // "  N. " prefix
			lines = append(lines, fmt.Sprintf("  %d. %s", i+1, cmd))
		}

		if parsed.EstimatedCost != "" {
			lines = append(lines, dim.Render(fmt.Sprintf("  est: %s", parsed.EstimatedCost)))
		}

		if s.queryPlan.Acceleration != nil && s.queryPlan.Acceleration.Available {
			accel := lipgloss.NewStyle().Foreground(ui.ColorSuccess())
			mvInfo := s.queryPlan.Acceleration.View
			if s.queryPlan.Acceleration.EstimatedSpeedup != "" {
				mvInfo += " (" + s.queryPlan.Acceleration.EstimatedSpeedup + ")"
			}
			lines = append(lines, accel.Render("  ⚡ "+truncateStr(mvInfo, w-6)))
		}

		if len(lines) == 0 {
			return []string{dim.Render("  (empty pipeline)")}
		}

		return lines
	}

	return []string{dim.Render("  type a query")}
}

func (s Sidebar) renderQueryStatsContent(w int) []string {
	dim := lipgloss.NewStyle().Foreground(ui.ColorDim())

	if s.queryStats == nil {
		return []string{dim.Render("  run a query")}
	}

	qs := s.queryStats
	lines := []string{
		fmt.Sprintf("  time:     %s", qs.Elapsed),
		fmt.Sprintf("  rows:     %d", qs.RowsReturned),
	}

	if qs.RowsScanned > 0 {
		lines = append(lines, fmt.Sprintf("  scanned:  %s", formatCountShell(qs.RowsScanned)))
	}

	if qs.Segments > 0 {
		segLine := fmt.Sprintf("  segments: %d", qs.Segments)
		if qs.BloomSkipped > 0 {
			segLine += fmt.Sprintf(" (%d skipped)", qs.BloomSkipped)
		}
		lines = append(lines, segLine)
	}

	if qs.Accelerated != "" {
		accel := lipgloss.NewStyle().Foreground(ui.ColorSuccess())
		lines = append(lines, accel.Render(fmt.Sprintf("  ⚡ %s", qs.Accelerated)))
	}

	return lines
}

func (s Sidebar) renderHistoryContent(w int) []string {
	dim := lipgloss.NewStyle().Foreground(ui.ColorDim())

	if len(s.history) == 0 {
		return []string{dim.Render("  (empty)")}
	}

	lines := make([]string, 0, len(s.history))

	for i, h := range s.history {
		var line string

		if h.Time != "" || h.Duration != "" {
			// Rich format with time and duration (Phase 6+).
			queryW := w - 14 // "  HH:MM  123ms  " prefix
			if queryW < 10 {
				queryW = 10
			}

			q := truncateStr(h.Query, queryW)
			timeStyle := lipgloss.NewStyle().Foreground(ui.ColorDim())
			line = fmt.Sprintf("  %s  %s  %s",
				timeStyle.Render(h.Time),
				timeStyle.Render(fmt.Sprintf("%5s", h.Duration)),
				q)
		} else {
			// Simple format — query text only.
			queryW := w - 4 // "  " prefix + margin
			if queryW < 10 {
				queryW = 10
			}

			q := truncateStr(h.Query, queryW)
			line = dim.Render(fmt.Sprintf("  %s", q))
		}

		line = zone.Mark(fmt.Sprintf("%s%d", zoneHistPrefix, i), line)
		lines = append(lines, line)
	}

	return lines
}

// truncateStr truncates a string to maxLen, adding "…" if truncated.
func truncateStr(s string, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}

	if len(s) <= maxLen {
		return s
	}

	if maxLen <= 1 {
		return "…"
	}

	return s[:maxLen-1] + "…"
}

// sectionKey returns a stable string key for a section (used in zone IDs).
func sectionKey(sec SidebarSection) string {
	switch sec {
	case SectionServer:
		return "server"
	case SectionIndexes:
		return "indexes"
	case SectionFields:
		return "fields"
	case SectionQueryPlan:
		return "plan"
	case SectionQueryStats:
		return "stats"
	case SectionHistory:
		return "history"
	default:
		return "unknown"
	}
}

// formatUptimeShort returns a compact human-readable uptime string.
func formatUptimeShort(seconds int) string {
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	}

	if seconds < 3600 {
		return fmt.Sprintf("%dm", seconds/60)
	}

	hours := seconds / 3600
	if hours < 24 {
		mins := (seconds % 3600) / 60
		if mins > 0 {
			return fmt.Sprintf("%dh %dm", hours, mins)
		}
		return fmt.Sprintf("%dh", hours)
	}

	days := hours / 24
	remH := hours % 24
	if remH > 0 {
		return fmt.Sprintf("%dd %dh", days, remH)
	}
	return fmt.Sprintf("%dd", days)
}

// formatBytesShort returns a compact human-readable byte size.
func formatBytesShort(bytes int64) string {
	switch {
	case bytes >= 1<<40:
		return fmt.Sprintf("%.1f TB", float64(bytes)/(1<<40))
	case bytes >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(bytes)/(1<<30))
	case bytes >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(bytes)/(1<<20))
	case bytes >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(bytes)/(1<<10))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// shortType returns a short type abbreviation for sidebar display.
func shortType(t string) string {
	switch strings.ToLower(t) {
	case "string", "str", "text":
		return "str"
	case "integer", "int", "long":
		return "int"
	case "float", "double", "number":
		return "num"
	case "boolean", "bool":
		return "bool"
	case "datetime", "timestamp", "time", "date":
		return "time"
	default:
		if len(t) > 4 {
			return t[:4]
		}
		return t
	}
}
