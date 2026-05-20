package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
)

type explainReportOptions struct {
	Analyze bool
	Plain   bool
	Theme   *ui.Theme
	Width   int
}

type explainViewModel struct {
	result *client.ExplainResult
	opts   explainReportOptions
}

func (m explainViewModel) Init() tea.Cmd {
	return nil
}

func (m explainViewModel) Update(tea.Msg) (tea.Model, tea.Cmd) {
	return m, tea.Quit
}

func (m explainViewModel) View() tea.View {
	return tea.NewView(renderExplainReportString(m.result, m.opts))
}

func renderExplainReport(w io.Writer, result *client.ExplainResult, opts explainReportOptions) error {
	if opts.Theme == nil {
		opts.Theme = ui.Stdout
	}
	if opts.Width == 0 {
		opts.Width = effectiveExplainWidth()
	}
	m := explainViewModel{result: result, opts: opts}
	out := m.View().Content
	if !strings.HasSuffix(out, "\n") {
		out += "\n"
	}
	_, err := fmt.Fprint(w, out)

	return err
}

func renderExplainReportString(result *client.ExplainResult, opts explainReportOptions) string {
	r := newExplainRenderer(opts)
	if result == nil {
		return r.section("Query plan") + r.item("Status", "no explain result")
	}

	var b strings.Builder
	b.WriteString(r.section("Query plan"))
	if opts.Analyze {
		b.WriteString(r.dim("Static plan, followed by actual execution profile"))
		b.WriteString("\n\n")
	}

	if !result.IsValid {
		b.WriteString(r.kvBlock([][2]string{
			{"Status", r.err("invalid")},
			{"Result", "not planned"},
		}))
		b.WriteByte('\n')
		b.WriteString(r.renderDiagnostics(result.Errors))
		b.WriteString(r.renderHints(result))

		return strings.TrimRight(b.String(), "\n") + "\n"
	}

	parsed := result.Parsed
	if parsed == nil {
		b.WriteString(r.kvBlock([][2]string{
			{"Status", r.warn("valid, no parsed plan")},
		}))

		return strings.TrimRight(b.String(), "\n") + "\n"
	}

	b.WriteString(r.renderSummary(parsed))
	b.WriteString(r.renderPipeline(parsed))
	b.WriteString(r.renderPhysicalStrategy(parsed))
	b.WriteString(r.renderOptimizer(parsed))
	b.WriteString(r.renderScanDetails(parsed))
	b.WriteString(r.renderAcceleration(result.Acceleration))
	b.WriteString(r.renderHints(result))

	return strings.TrimRight(b.String(), "\n") + "\n"
}

type explainRenderer struct {
	styles explainStyles
	chars  explainChars
}

type explainStyles struct {
	title lipgloss.Style
	label lipgloss.Style
	dim   lipgloss.Style
	ok    lipgloss.Style
	warn  lipgloss.Style
	err   lipgloss.Style
}

type explainChars struct {
	bullet string
	branch string
	last   string
	ok     string
	warn   string
	err    string
}

func newExplainRenderer(opts explainReportOptions) explainRenderer {
	t := opts.Theme
	if t == nil {
		t = ui.Stdout
	}
	if t == nil {
		t = ui.NewTheme(io.Discard, true)
	}
	plain := opts.Plain
	if os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
		plain = true
	}
	if opts.Width <= 0 {
		opts.Width = effectiveExplainWidth()
	}

	styles := explainStyles{
		title: t.SectionTitle,
		label: t.Label,
		dim:   t.Dim,
		ok:    t.Success,
		warn:  t.Warning,
		err:   t.Error,
	}
	chars := explainChars{
		bullet: "•",
		branch: "├─",
		last:   "└─",
		ok:     "✓",
		warn:   "!",
		err:    "✕",
	}
	if plain {
		plainStyle := lipgloss.NewStyle()
		styles = explainStyles{
			title: plainStyle,
			label: plainStyle,
			dim:   plainStyle,
			ok:    plainStyle,
			warn:  plainStyle,
			err:   plainStyle,
		}
		chars = explainChars{
			bullet: "-",
			branch: "|-",
			last:   "`-",
			ok:     "OK",
			warn:   "!",
			err:    "X",
		}
	}

	return explainRenderer{styles: styles, chars: chars}
}

func effectiveExplainWidth() int {
	if globalMaxWidth > 0 {
		return globalMaxWidth
	}

	return ui.TerminalWidth()
}

func explainPlainMode() bool {
	return globalNoColor || strings.EqualFold(globalTheme, string(ui.ThemePlain)) ||
		os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb"
}

func (r explainRenderer) section(title string) string {
	return r.styles.title.Render(title) + "\n"
}

func (r explainRenderer) item(label, value string) string {
	return fmt.Sprintf("  %s: %s\n", r.styles.label.Render(label), value)
}

func (r explainRenderer) kvBlock(rows [][2]string) string {
	var b strings.Builder
	for _, row := range rows {
		if row[1] == "" {
			continue
		}
		b.WriteString(r.item(row[0], row[1]))
	}

	return b.String()
}

func (r explainRenderer) ok(s string) string {
	return r.styles.ok.Render(s)
}

func (r explainRenderer) warn(s string) string {
	return r.styles.warn.Render(s)
}

func (r explainRenderer) err(s string) string {
	return r.styles.err.Render(s)
}

func (r explainRenderer) dim(s string) string {
	return r.styles.dim.Render(s)
}

func (r explainRenderer) renderSummary(parsed *client.ExplainParsed) string {
	scanStatus := "bounded scan"
	if parsed.UsesFullScan && !parsed.HasTimeBounds {
		scanStatus = r.warn("full scan, unbounded time")
	} else if parsed.UsesFullScan {
		scanStatus = r.warn("full scan, time bounded")
	}

	rows := [][2]string{
		{"Status", r.ok("valid")},
		{"Result", nonEmpty(parsed.ResultType, "unknown")},
		{"Cost", nonEmpty(parsed.EstimatedCost, "not estimated")},
		{"Source", sourceScopeSummary(parsed.SourceScope)},
		{"Scan", scanStatus},
	}

	return r.kvBlock(rows) + "\n"
}

func (r explainRenderer) renderPipeline(parsed *client.ExplainParsed) string {
	var b strings.Builder
	b.WriteString(r.section("Pipeline"))
	if len(parsed.Pipeline) == 0 {
		b.WriteString("  (no stages)\n\n")

		return b.String()
	}

	for i, stage := range parsed.Pipeline {
		prefix := r.chars.branch
		if i == len(parsed.Pipeline)-1 {
			prefix = r.chars.last
		}
		head := fmt.Sprintf("%s %d. %s", prefix, i+1, stage.Command)
		if stage.Description != "" && stage.Description != stage.Command {
			head += " - " + stage.Description
		}
		b.WriteString("  " + head + "\n")
		for _, line := range stageFieldLines(stage) {
			b.WriteString("     " + r.dim(line) + "\n")
		}
	}
	b.WriteByte('\n')

	return b.String()
}

func (r explainRenderer) renderPhysicalStrategy(parsed *client.ExplainParsed) string {
	var lines []string
	pp := parsed.PhysicalPlan
	if pp != nil {
		if pp.TopKAgg {
			topK := ""
			if pp.TopK > 0 {
				topK = fmt.Sprintf(" (%d)", pp.TopK)
			}
			lines = append(lines, "TopK heap optimization"+topK+": keep only best candidates during aggregation")
		}
		if pp.PartialAgg {
			lines = append(lines, "Partial aggregation: aggregate per segment, then merge")
		}
		if pp.CountStarOnly {
			lines = append(lines, "count(*) metadata shortcut: no event scan needed")
		}
		if pp.JoinStrategy != "" {
			lines = append(lines, "Join strategy: "+pp.JoinStrategy)
		}
	}
	if len(lines) == 0 {
		lines = append(lines, "Standard scan/filter pipeline")
	}

	return r.renderBulletSection("Physical strategy", lines)
}

func (r explainRenderer) renderOptimizer(parsed *client.ExplainParsed) string {
	var b strings.Builder
	b.WriteString(r.section("Optimizer"))

	totalFirings := 0
	for _, rd := range parsed.OptimizerRules {
		totalFirings += rd.Count
	}
	if totalFirings == 0 {
		for _, count := range parsed.OptimizerStats {
			totalFirings += count
		}
	}

	totalRules := parsed.TotalRules
	if totalRules == 0 {
		totalRules = len(parsed.OptimizerRules)
	}
	b.WriteString(r.kvBlock([][2]string{
		{"Rules", fmt.Sprintf("%d registered, %d fired", totalRules, totalFirings)},
		{"Timing", optimizerTiming(parsed)},
	}))

	for _, line := range topOptimizerRuleLines(parsed) {
		b.WriteString("  " + r.chars.bullet + " " + line + "\n")
	}
	for _, msg := range parsed.OptimizerMessages {
		b.WriteString("  " + r.chars.ok + " " + msg + "\n")
	}
	for _, warning := range parsed.OptimizerWarnings {
		b.WriteString("  " + r.chars.warn + " " + r.warn(warning) + "\n")
	}
	b.WriteByte('\n')

	return b.String()
}

func (r explainRenderer) renderScanDetails(parsed *client.ExplainParsed) string {
	var b strings.Builder
	b.WriteString(r.section("Scan details"))
	b.WriteString(r.kvBlock([][2]string{
		{"Source scope", sourceScopeSummary(parsed.SourceScope)},
		{"Fields read", listOrNone(parsed.FieldsRead)},
		{"Search terms", listOrNone(parsed.SearchTerms)},
		{"Time bounds", timeBoundsSummary(parsed)},
	}))
	if len(parsed.RangePredicates) > 0 {
		b.WriteString("  Range predicates:\n")
		for _, pred := range parsed.RangePredicates {
			b.WriteString("    " + r.chars.bullet + " " + rangePredicateSummary(pred) + "\n")
		}
	}
	b.WriteByte('\n')

	return b.String()
}

func (r explainRenderer) renderAcceleration(accel *client.ExplainAccel) string {
	if accel == nil || (!accel.Available && accel.Reason == "") {
		return ""
	}

	var lines []string
	if accel.Available {
		line := "available"
		if accel.View != "" {
			line += ": " + accel.View
		}
		if accel.EstimatedSpeedup != "" {
			line += " (" + accel.EstimatedSpeedup + ")"
		}
		lines = append(lines, line)
	} else if accel.Reason != "" {
		lines = append(lines, "not available: "+accel.Reason)
	}

	return r.renderBulletSection("MV acceleration", lines)
}

func (r explainRenderer) renderDiagnostics(errors []client.ExplainError) string {
	var b strings.Builder
	b.WriteString(r.section("Diagnostics"))
	if len(errors) == 0 {
		b.WriteString("  No diagnostics returned.\n\n")

		return b.String()
	}
	for _, e := range errors {
		b.WriteString("  " + r.chars.err + " " + r.err(e.Message) + "\n")
		if e.Suggestion != "" {
			b.WriteString("    suggestion: " + e.Suggestion + "\n")
		}
	}
	b.WriteByte('\n')

	return b.String()
}

func (r explainRenderer) renderHints(result *client.ExplainResult) string {
	hints := explainHints(result)
	if len(hints) == 0 {
		return ""
	}

	return r.renderBulletSection("Hints", hints)
}

func (r explainRenderer) renderBulletSection(title string, lines []string) string {
	var b strings.Builder
	b.WriteString(r.section(title))
	if len(lines) == 0 {
		b.WriteString("  (none)\n\n")

		return b.String()
	}
	for _, line := range lines {
		b.WriteString("  " + r.chars.bullet + " " + line + "\n")
	}
	b.WriteByte('\n')

	return b.String()
}

func stageFieldLines(stage client.ExplainStage) []string {
	var lines []string
	if len(stage.FieldsAdded) > 0 && !suppressUnknownFieldList(stage, stage.FieldsAdded) {
		lines = append(lines, "adds: "+summarizeFieldList(stage.FieldsAdded))
	}
	if len(stage.FieldsRemoved) > 0 {
		lines = append(lines, "removes: "+summarizeRemovedFields(stage))
	}
	if len(stage.FieldsOut) > 0 && !stage.FieldsUnknown {
		lines = append(lines, "fields out: "+summarizeFieldList(stage.FieldsOut))
	}
	if len(stage.FieldsOptional) > 0 {
		lines = append(lines, "optional: "+summarizeFieldList(stage.FieldsOptional))
	}
	if stage.FieldsUnknown {
		lines = append(lines, "fields: schema-on-read, not fully known")
	}

	return lines
}

func suppressUnknownFieldList(stage client.ExplainStage, fields []string) bool {
	return stage.FieldsUnknown && (stage.Command == "source" || len(fields) > 12)
}

func summarizeFieldList(fields []string) string {
	const maxFields = 12
	if len(fields) <= maxFields {
		return strings.Join(fields, ", ")
	}

	return fmt.Sprintf("%s, ... (+%d more)", strings.Join(fields[:maxFields], ", "), len(fields)-maxFields)
}

func summarizeRemovedFields(stage client.ExplainStage) string {
	if len(stage.FieldsRemoved) > 24 {
		return "previous schema-on-read field set"
	}

	return summarizeFieldList(stage.FieldsRemoved)
}

func topOptimizerRuleLines(parsed *client.ExplainParsed) []string {
	if len(parsed.OptimizerRules) > 0 {
		lines := make([]string, 0, minInt(5, len(parsed.OptimizerRules)))
		for i, rd := range parsed.OptimizerRules {
			if i >= 5 {
				break
			}
			line := rd.Name
			if rd.Count > 1 {
				line += fmt.Sprintf(" x%d", rd.Count)
			}
			if rd.Description != "" {
				line += " - " + rd.Description
			}
			lines = append(lines, line)
		}

		return lines
	}

	if len(parsed.OptimizerStats) == 0 {
		return nil
	}
	type stat struct {
		name  string
		count int
	}
	stats := make([]stat, 0, len(parsed.OptimizerStats))
	for name, count := range parsed.OptimizerStats {
		stats = append(stats, stat{name: name, count: count})
	}
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].count == stats[j].count {
			return stats[i].name < stats[j].name
		}

		return stats[i].count > stats[j].count
	})

	lines := make([]string, 0, minInt(5, len(stats)))
	for i, st := range stats {
		if i >= 5 {
			break
		}
		lines = append(lines, fmt.Sprintf("%s x%d", st.name, st.count))
	}

	return lines
}

func optimizerTiming(parsed *client.ExplainParsed) string {
	if parsed.ParseMS <= 0 && parsed.OptimizeMS <= 0 {
		return ""
	}

	return fmt.Sprintf("parse %.2fms, optimize %.2fms", parsed.ParseMS, parsed.OptimizeMS)
}

func sourceScopeSummary(scope *client.ExplainSourceScope) string {
	if scope == nil {
		return "all sources"
	}
	switch scope.Type {
	case "single":
		if len(scope.Sources) > 0 {
			return scope.Sources[0]
		}
	case "multi":
		if len(scope.Sources) > 0 {
			return fmt.Sprintf("%s (%d sources)", strings.Join(scope.Sources, ", "), len(scope.Sources))
		}
	case "glob":
		if scope.Pattern != "" && scope.TotalSourcesAvailable > 0 {
			return fmt.Sprintf("%s (%d sources available)", scope.Pattern, scope.TotalSourcesAvailable)
		}
		if scope.Pattern != "" {
			return scope.Pattern
		}
	}
	if len(scope.Sources) > 0 {
		return strings.Join(scope.Sources, ", ")
	}
	if scope.Type != "" {
		return scope.Type
	}

	return "all sources"
}

func timeBoundsSummary(parsed *client.ExplainParsed) string {
	if parsed.HasTimeBounds {
		return "bounded"
	}
	if parsed.UsesFullScan {
		return "unbounded"
	}

	return "not required"
}

func rangePredicateSummary(pred client.ExplainRangePredicate) string {
	var parts []string
	if pred.Min != "" {
		parts = append(parts, "min="+pred.Min)
	}
	if pred.Max != "" {
		parts = append(parts, "max="+pred.Max)
	}
	if pred.LoweredToBSI {
		parts = append(parts, "bsi")
	}
	if pred.RGFilterStrategy != "" {
		parts = append(parts, "row-group="+pred.RGFilterStrategy)
	}
	if pred.RowVMStrategy != "" {
		parts = append(parts, "row="+pred.RowVMStrategy)
	}
	if len(parts) == 0 {
		return pred.Field
	}

	return pred.Field + ": " + strings.Join(parts, ", ")
}

func explainHints(result *client.ExplainResult) []string {
	if result == nil {
		return nil
	}
	if !result.IsValid {
		hints := make([]string, 0, len(result.Errors))
		for _, e := range result.Errors {
			if e.Suggestion != "" {
				hints = append(hints, e.Suggestion)
			}
		}

		return dedupeStrings(hints)
	}
	parsed := result.Parsed
	if parsed == nil {
		return nil
	}

	var hints []string
	if parsed.UsesFullScan && !parsed.HasTimeBounds {
		hints = append(hints, "Add a time range to avoid scanning the full retention window.")
	}
	if parsed.SourceScope != nil {
		switch parsed.SourceScope.Type {
		case "glob":
			hints = append(hints, "Use an explicit source when possible; globs can expand to many sources.")
		case "multi":
			if len(parsed.SourceScope.Sources) > 1 {
				hints = append(hints, "Limit sources for repeated queries to reduce segment fan-out.")
			}
		}
	}
	if parsed.UsesFullScan && len(parsed.SearchTerms) == 0 {
		hints = append(hints, "Add selective search terms or indexed predicates before expensive pipeline commands.")
	}
	if result.Acceleration != nil && result.Acceleration.Available {
		hint := "Materialized view acceleration is available"
		if result.Acceleration.View != "" {
			hint += " via " + result.Acceleration.View
		}
		hints = append(hints, hint+".")
	}
	hints = append(hints, "Use --analyze to see actual timings, row counts, and runtime optimizations.")

	return dedupeStrings(hints)
}

func listOrNone(values []string) string {
	if len(values) == 0 {
		return "none"
	}

	return strings.Join(values, ", ")
}

func nonEmpty(value, fallback string) string {
	if value == "" {
		return fallback
	}

	return value
}

func minInt(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func dedupeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}

	return out
}
