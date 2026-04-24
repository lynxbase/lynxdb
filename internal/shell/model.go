package shell

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	zone "github.com/lrstanley/bubblezone/v2"

	"github.com/lynxbase/lynxdb/internal/output"
	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage"
	"github.com/lynxbase/lynxdb/pkg/timerange"
)

// Model is the root Bubble Tea model composing all shell sub-components.
type Model struct {
	header    Header
	editor    Editor
	results   Results
	sidebar   Sidebar
	statusBar StatusBar

	session *Session

	fields     []string
	fieldInfos []client.FieldInfo
	completer  *Completer
	history    *History
	keys       keyMap

	width, height int
	focus         Focus
	running       bool
	startTime     time.Time

	// Sidebar state.
	sidebarOpen   bool
	sidebarLay    sidebarLayout // cached layout from last WindowSizeMsg
	explainSeq    int           // debounce sequence counter for explain
	popupSeq      int           // debounce sequence counter for auto-popup
	lastEditorVal string        // tracks editor content for change detection

	// Async query state.
	jobID     string       // non-empty while polling an async job
	jobQuery  string       // the query text for the active job
	jobHints  string       // compat hints for the active job
	progress  *progressMsg // latest progress from async job
	pollCount int          // number of poll iterations

	// Live tail state.
	tailActive bool
	tailCancel context.CancelFunc
	tailCount  int
}

// NewModel creates the root shell model from RunOpts.
func NewModel(mode string, opts RunOpts) Model {
	history := NewHistory()
	completer := NewCompleter()

	prompt := "lynxdb> "
	if mode == "file" {
		prompt = "lynxdb[file]> "
	}
	// Align continuation prompt to same visual width as main prompt.
	pw := lipgloss.Width(prompt)
	contPrompt := strings.Repeat(" ", pw-4) + "...> "

	header := NewHeader(mode, opts.Server, opts.File, opts.Events)
	editor := NewEditor(prompt, contPrompt, history, completer)
	results := NewResults(80, 20)
	sidebar := NewSidebar(mode)
	statusBar := NewStatusBar(mode)

	sess := &Session{
		Mode:   mode,
		Server: opts.Server,
		Client: opts.Client,
		Engine: opts.Engine,
		Since:  opts.Since,
		Format: output.FormatTable,
		Timing: true,
	}

	return Model{
		header:      header,
		editor:      editor,
		results:     results,
		sidebar:     sidebar,
		statusBar:   statusBar,
		session:     sess,
		sidebarOpen: mode == "server", // sidebar open by default in server mode
		completer:   completer,
		history:     history,
		keys:        defaultKeyMap(),
		focus:       EditorFocus,
	}
}

// Init is the Bubble Tea initialization command.
func (m Model) Init() tea.Cmd {
	cmds := []tea.Cmd{
		m.statusBar.spinner.Tick,
	}

	// Fetch fields for autocomplete and sidebar data in server mode.
	if m.session.Mode == "server" && m.session.Client != nil {
		cmds = append(cmds,
			fetchFieldsCmd(m.session.Client),
			fetchSidebarDataCmd(m.session.Client),
			sidebarRefreshTickCmd(),
		)
	}

	return tea.Batch(cmds...)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.header.SetWidth(msg.Width)
		m.statusBar.SetWidth(msg.Width)
		m.editor.SetWidth(msg.Width) // editor spans full width

		m.recalcLayout()

		return m, nil

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.statusBar.spinner, cmd = m.statusBar.spinner.Update(msg)

		return m, cmd

	case fieldsLoadedMsg:
		m.fields = msg.fields
		m.fieldInfos = msg.fieldInfo
		m.completer.SetFields(msg.fields)
		m.completer.SetFieldValues(msg.fieldInfo)
		m.sidebar.SetFields(msg.fieldInfo)

		if len(msg.sources) > 0 {
			m.completer.SetSources(msg.sources)
		}

		return m, nil

	case sidebarDataMsg:
		m.sidebar.SetServerInfo(msg.server)
		m.sidebar.SetIndexes(msg.indexes)

		return m, nil

	case sidebarRefreshTickMsg:
		var cmds []tea.Cmd
		if m.session.Mode == "server" && m.session.Client != nil {
			cmds = append(cmds, fetchSidebarDataCmd(m.session.Client))
		}
		cmds = append(cmds, sidebarRefreshTickCmd())

		return m, tea.Batch(cmds...)

	case explainDebounceMsg:
		// Ignore stale debounce ticks.
		if msg.seq != m.explainSeq {
			return m, nil
		}

		query := strings.TrimSpace(m.editor.Value())
		if query == "" || strings.HasPrefix(query, "/") {
			m.sidebar.ClearQueryPlan()

			return m, nil
		}

		if m.session.Mode != "server" || m.session.Client == nil {
			return m, nil
		}

		return m, fetchExplainCmd(m.session.Client, query)

	case explainResultMsg:
		if msg.err != nil {
			m.sidebar.ClearQueryPlan()
		} else {
			m.sidebar.SetQueryPlan(msg.result)
		}

		return m, nil

	case popupDebounceMsg:
		if msg.seq != m.popupSeq {
			return m, nil
		}

		m.editor.TriggerAutoPopup()

		return m, nil

	case jobCreatedMsg:
		m.jobID = msg.jobID
		m.startTime = time.Now()
		m.pollCount = 0

		return m, pollJobCmd(m.session.Client, msg.jobID, m.jobQuery, m.jobHints, 0, m.startTime)

	case progressMsg:
		m.progress = &msg
		m.pollCount++

		return m, pollJobCmd(m.session.Client, m.jobID, m.jobQuery, m.jobHints, m.pollCount, m.startTime)

	case pollTickMsg:
		m.pollCount++

		return m, pollJobCmd(m.session.Client, m.jobID, m.jobQuery, m.jobHints, m.pollCount, m.startTime)

	case tailStartMsg:
		if m.tailActive {
			m.results.AppendText("  Tail already active. Ctrl+C to stop.")
			return m, nil
		}

		ctx, cancel := context.WithCancel(context.Background())
		m.tailActive = true
		m.tailCancel = cancel
		m.tailCount = 0
		m.results.AppendText("  Live tail: " + msg.query)

		eventCh := make(chan map[string]interface{}, 64)
		errCh := make(chan error, 1)
		c := m.session.Client
		from := ""

		if m.session.Since != "" {
			from = "-" + m.session.Since
		}

		go func() {
			err := c.Tail(ctx, msg.query, from, 100, func(evt client.SSEEvent) error {
				if evt.Event == "result" {
					var event map[string]interface{}
					if json.Unmarshal(evt.Data, &event) == nil {
						eventCh <- event
					}
				}

				return nil
			})
			errCh <- err
			close(eventCh)
		}()

		return m, waitForTailEvent(eventCh, errCh)

	case tailEventMsg:
		if !m.tailActive {
			return m, nil
		}

		m.tailCount++
		tableStr := renderResultRows([]map[string]interface{}{msg.event}, m.sidebarLay.mainW, m.session.Format)
		m.results.AppendText(strings.TrimRight(tableStr, "\n"))

		return m, waitForTailEvent(msg.eventCh, msg.errCh)

	case tailDoneMsg:
		m.tailActive = false
		m.tailCancel = nil

		if msg.err != nil && !errors.Is(msg.err, context.Canceled) {
			m.results.AppendText(fmt.Sprintf("  Tail ended: %v", msg.err))
		} else {
			m.results.AppendText(fmt.Sprintf("  Tail ended. %d events received.", m.tailCount))
		}

		return m, nil

	case querySubmitMsg:
		m.running = true
		m.startTime = time.Now()
		m.focus = EditorFocus

		// Handle _ as previous result reference.
		query := msg.query
		trimmed := strings.TrimSpace(query)
		if trimmed == "_" || strings.HasPrefix(trimmed, "_ |") || strings.HasPrefix(trimmed, "_|") {
			if len(m.session.LastRows) == 0 {
				m.running = false
				m.results.AppendText("  No previous results — run a query first.")
				return m, nil
			}
			if trimmed == "_" {
				// Re-display last results.
				m.running = false
				rw := m.sidebarLay.mainW
				if rw == 0 {
					rw = m.width
				}
				m.results.AppendResult(query, m.session.LastRows, 0, nil,
					rw, m.session.Format, false, "", nil)
				return m, nil
			}
			// "_ | <pipeline>" — execute pipeline on last results.
			// Execute against in-memory copy of last results, not the server.
			pipeline := rewriteUnderscoreQuery(trimmed, m.session.LastRows)
			return m, func() tea.Msg {
				start := time.Now()
				rows, err := executeUnderscoreQuery(pipeline, m.session.LastRows)
				return queryResultMsg{
					query:   msg.query,
					rows:    rows,
					elapsed: time.Since(start),
					err:     err,
				}
			}
		}

		m.jobQuery = query

		// Pre-compute compat hints for server mode (needed if async path is taken).
		if m.session.Mode == "server" {
			if hints := spl2.DetectCompatHints(query); len(hints) > 0 {
				m.jobHints = spl2.FormatCompatHints(hints)
			} else {
				m.jobHints = ""
			}
		}

		return m, m.executeQueryCmd(query)

	case queryResultMsg:
		m.running = false
		m.jobID = ""
		m.progress = nil
		m.session.LastQuery = msg.query
		m.session.LastRows = msg.rows

		// Extract field names from results and add to autocomplete.
		if len(msg.rows) > 0 {
			m.completer.MergeResultFields(extractFieldNames(msg.rows))
		}

		var zeroCtx *ZeroResultContext
		if len(msg.rows) == 0 && msg.err == nil && m.session.Mode == "server" {
			zeroCtx = &ZeroResultContext{Session: m.session, FieldInfos: m.fieldInfos}
		}

		resultW := m.sidebarLay.mainW
		if resultW == 0 {
			resultW = m.width
		}
		m.results.AppendResult(msg.query, msg.rows, msg.elapsed, msg.err, resultW,
			m.session.Format, m.session.Timing, msg.hints, zeroCtx)

		// Update sidebar with query stats and history.
		if msg.err == nil {
			m.sidebar.SetQueryStats(buildQueryStats(msg.query, msg.rows, msg.elapsed, msg.meta))
		}
		m.history.SetLastDuration(msg.elapsed)
		m.sidebar.SetHistory(m.history.RecentEntries(10))

		return m, nil

	case savedQueryRunMsg:
		if msg.err != nil {
			m.results.AppendText(fmt.Sprintf("  Error: %v", msg.err))
			return m, nil
		}

		m.results.AppendText(fmt.Sprintf("  Running saved query %q...", msg.name))

		return m, func() tea.Msg { return querySubmitMsg{query: msg.query} }

	case slashCommandMsg:
		if msg.quit {
			_ = m.history.Save()

			return m, tea.Quit
		}

		// Check if it was a /clear//cls command (output field contains the raw command).
		if msg.output == "" && !msg.clear {
			// Async command dispatched — no immediate output.
			return m, nil
		}

		if msg.clear {
			m.results.Clear()

			return m, nil
		}

		m.results.AppendText(msg.output)

		return m, nil

	case tea.PasteMsg:
		cmd, _, _ := m.editor.Update(msg)

		return m, cmd

	case tea.MouseClickMsg:
		return m.handleMouseClick(msg)

	case tea.MouseWheelMsg:
		cmd := m.results.Update(msg)

		return m, cmd

	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}

	return m, nil
}

func (m Model) handleKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	// Toggle sidebar — F2.
	if key.Matches(msg, m.keys.ToggleSidebar) {
		m.sidebarOpen = !m.sidebarOpen
		m.recalcLayout()

		return m, nil
	}

	// Global quit — Ctrl+D on empty input.
	if key.Matches(msg, m.keys.Quit) && m.editor.Value() == "" && !m.editor.InMultiLine() {
		_ = m.history.Save()

		return m, tea.Quit
	}

	// Cancel live tail.
	if key.Matches(msg, m.keys.Cancel) && m.tailActive {
		m.tailCancel()
		m.tailActive = false

		return m, nil
	}

	// Cancel running query.
	if key.Matches(msg, m.keys.Cancel) && m.running {
		// Fire-and-forget cancel for async jobs.
		if m.jobID != "" && m.session.Client != nil {
			jobID := m.jobID
			c := m.session.Client
			go func() { _ = c.CancelJob(context.Background(), jobID) }()
		}

		m.running = false
		m.jobID = ""
		m.progress = nil

		return m, nil
	}

	// Clear screen.
	if key.Matches(msg, m.keys.ClearScr) {
		m.results.Clear()

		return m, nil
	}

	// Focus switching.
	if key.Matches(msg, m.keys.FocusBack) && m.focus == ResultsFocus {
		m.focus = EditorFocus

		return m, nil
	}

	if key.Matches(msg, m.keys.ScrollUp) || key.Matches(msg, m.keys.ScrollDn) {
		m.focus = ResultsFocus
		cmd := m.results.Update(msg)

		return m, cmd
	}

	// Route to focused component.
	if m.focus == ResultsFocus {
		// Copy results to clipboard.
		if key.Matches(msg, m.keys.CopyResults) || key.Matches(msg, m.keys.CopyResultsMD) {
			markdown := key.Matches(msg, m.keys.CopyResultsMD)
			m.copyLastResults(markdown)

			return m, nil
		}

		// Pass scroll keys to viewport.
		cmd := m.results.Update(msg)

		return m, cmd
	}

	// Editor focus — handle input.
	cmd, submitMsg, slashMsg := m.editor.Update(msg)

	// Recalculate layout when editor height may have changed.
	m.recalcLayout()

	// Detect editor content changes for live query plan debounce and auto-popup.
	var extraCmds []tea.Cmd
	if newVal := m.editor.Value(); newVal != m.lastEditorVal {
		m.lastEditorVal = newVal
		m.explainSeq++
		m.popupSeq++

		if strings.TrimSpace(newVal) == "" {
			m.sidebar.ClearQueryPlan()
		} else {
			if m.session.Mode == "server" && m.session.Client != nil {
				m.sidebar.SetPlanTyping()
				extraCmds = append(extraCmds, explainDebounceCmd(m.explainSeq))
			}

			extraCmds = append(extraCmds, popupDebounceCmd(m.popupSeq))
		}
	}

	if submitMsg != nil {
		m.running = true
		m.startTime = time.Now()
		m.jobQuery = submitMsg.query

		// Clear plan on submit — the query is being executed now.
		m.sidebar.ClearQueryPlan()

		if m.session.Mode == "server" {
			if hints := spl2.DetectCompatHints(submitMsg.query); len(hints) > 0 {
				m.jobHints = spl2.FormatCompatHints(hints)
			} else {
				m.jobHints = ""
			}
		}

		return m, m.executeQueryCmd(submitMsg.query)
	}

	if slashMsg != nil {
		if slashMsg.quit {
			_ = m.history.Save()

			return m, tea.Quit
		}

		// Route slash command through ExecuteSlashCommand.
		output, asyncCmd, quit := ExecuteSlashCommand(slashMsg.output, m.session, m.history)
		if quit {
			_ = m.history.Save()

			return m, tea.Quit
		}

		// Handle /clear//cls.
		cmdLower := strings.ToLower(strings.Fields(slashMsg.output)[0])
		if cmdLower == "/clear" || cmdLower == "/cls" {
			m.results.Clear()

			return m, nil
		}

		if output != "" {
			m.results.AppendText(output)
		}

		if asyncCmd != nil {
			return m, asyncCmd
		}

		return m, nil
	}

	// Batch the editor command with debounce commands if present.
	if len(extraCmds) > 0 {
		all := append([]tea.Cmd{cmd}, extraCmds...)
		return m, tea.Batch(all...)
	}

	return m, cmd
}

// handleMouseClick processes mouse click events for sidebar zones.
func (m Model) handleMouseClick(msg tea.MouseClickMsg) (tea.Model, tea.Cmd) {
	for _, f := range m.sidebar.fields {
		if z := zone.Get(zoneFieldPrefix + f.Name); z != nil && z.InBounds(msg) {
			m.editor.InsertAtCursor(f.Name)
			m.focus = EditorFocus

			return m, nil
		}
	}

	for i, h := range m.sidebar.history {
		if z := zone.Get(fmt.Sprintf("%s%d", zoneHistPrefix, i)); z != nil && z.InBounds(msg) {
			m.editor.SetValue(h.Query)
			m.focus = EditorFocus

			return m, nil
		}
	}

	// Check sidebar section header clicks (toggle collapse).
	for _, sec := range []SidebarSection{
		SectionServer, SectionIndexes, SectionFields,
		SectionQueryPlan, SectionQueryStats, SectionHistory,
	} {
		if z := zone.Get(zoneSectionPrefix + sectionKey(sec)); z != nil && z.InBounds(msg) {
			m.sidebar.ToggleSection(sec)

			return m, nil
		}
	}

	return m, nil
}

// View renders the full-screen TUI.
func (m Model) View() tea.View {
	var b strings.Builder

	// Header (1 line, full width).
	b.WriteString(m.header.View())
	b.WriteByte('\n')

	// Main area: results + optional sidebar.
	resultsView := m.results.View()

	if m.sidebarOpen && m.sidebarLay.sidebarW > 0 {
		editorH := m.editor.EditorHeight()
		mainH := m.height - 1 - editorH - 1
		if mainH < 1 {
			mainH = 1
		}

		separator := renderVerticalSeparator(mainH)
		sidebarView := m.sidebar.View()
		mainArea := lipgloss.JoinHorizontal(lipgloss.Top, resultsView, separator, sidebarView)
		b.WriteString(mainArea)
	} else {
		b.WriteString(resultsView)
	}

	b.WriteByte('\n')

	// Editor (full width).
	b.WriteString(m.editor.View())
	b.WriteByte('\n')

	// Status bar (full width).
	var elapsed time.Duration
	if m.running {
		elapsed = time.Since(m.startTime)
	}
	b.WriteString(m.statusBar.View(m.focus, m.running, m.editor.InMultiLine(), elapsed, m.progress, m.tailActive, m.sidebarOpen))

	output := b.String()

	// Overlay autocomplete popup on top of the rendered output.
	if m.editor.PopupVisible() {
		popupStr := m.editor.PopupView(m.width / 2)
		if popupStr != "" {
			popupH := strings.Count(popupStr, "\n") + 1
			anchorCol := m.editor.PopupAnchorCol()
			if anchorCol < 0 {
				anchorCol = 0
			}

			// Position: bottom of the output minus editor height minus statusbar minus popup height.
			editorH := m.editor.EditorHeight()
			popupY := m.height - editorH - 1 - popupH // above the editor
			if popupY < 1 {
				popupY = 1
			}

			output = placeOverlay(output, popupStr, anchorCol, popupY, m.width, m.height)
		}
	}

	v := tea.NewView(zone.Scan(output))
	v.AltScreen = true
	v.MouseMode = tea.MouseModeCellMotion

	return v
}

// placeOverlay paints overlayStr on top of baseStr at the given (x, y) position.
func placeOverlay(baseStr, overlayStr string, x, y, totalW, totalH int) string {
	baseLines := strings.Split(baseStr, "\n")
	overlayLines := strings.Split(overlayStr, "\n")

	for i, oLine := range overlayLines {
		row := y + i
		if row < 0 || row >= len(baseLines) {
			continue
		}

		baseLine := baseLines[row]
		baseRunes := []rune(baseLine)

		// Ensure base line is wide enough.
		for len(baseRunes) < x {
			baseRunes = append(baseRunes, ' ')
		}

		// Replace characters at position x with overlay content.
		oRunes := []rune(oLine)
		oLen := len(oRunes)

		var result []rune
		result = append(result, baseRunes[:x]...)
		result = append(result, oRunes...)

		if x+oLen < len(baseRunes) {
			result = append(result, baseRunes[x+oLen:]...)
		}

		baseLines[row] = string(result)
	}

	return strings.Join(baseLines, "\n")
}

// recalcLayout recomputes panel sizes based on current terminal dimensions and sidebar state.
func (m *Model) recalcLayout() {
	if m.width == 0 || m.height == 0 {
		return
	}

	lay := computeSidebarLayout(m.width, m.sidebarOpen)
	m.sidebarLay = lay

	// If sidebar can't fit, force it closed.
	if m.sidebarOpen && lay.sidebarW == 0 {
		m.sidebarOpen = false
	}

	editorH := m.editor.EditorHeight()
	mainH := m.height - 1 - editorH - 1 // header(1) + statusbar(1)
	if mainH < 1 {
		mainH = 1
	}

	m.results.SetSize(lay.mainW, mainH)
	m.sidebar.SetSize(lay.sidebarW, mainH)
	m.sidebar.SetCompact(lay.compactMode)
}

// copyLastResults copies the last query results to the clipboard via OSC 52.
func (m *Model) copyLastResults(markdown bool) {
	rows := m.session.LastRows
	if len(rows) == 0 {
		m.statusBar.SetFlash("  No results to copy", 3*time.Second)
		return
	}

	var content string
	if markdown {
		content = renderMarkdownTable(rows)
	} else {
		resultW := m.sidebarLay.mainW
		if resultW == 0 {
			resultW = m.width
		}
		content = renderResultRows(rows, resultW, m.session.Format)
	}

	// Write to clipboard via OSC 52.
	encoded := base64Encode(content)
	osc := fmt.Sprintf("\033]52;c;%s\a", encoded)
	fmt.Fprint(os.Stderr, osc)

	word := "rows"
	if len(rows) == 1 {
		word = "row"
	}

	format := "table"
	if markdown {
		format = "markdown"
	}

	m.statusBar.SetFlash(
		fmt.Sprintf("  Copied %d %s as %s", len(rows), word, format),
		3*time.Second,
	)
}

// renderMarkdownTable renders rows as a GitHub-flavored markdown table.
func renderMarkdownTable(rows []map[string]interface{}) string {
	if len(rows) == 0 {
		return ""
	}

	// Collect columns deterministically.
	colSet := make(map[string]struct{})
	for _, row := range rows {
		for k := range row {
			colSet[k] = struct{}{}
		}
	}

	cols := make([]string, 0, len(colSet))
	for k := range colSet {
		cols = append(cols, k)
	}

	// Sort: builtin fields first, then alphabetical.
	sortColumns(cols)

	var b strings.Builder

	// Header.
	b.WriteString("| ")
	for i, c := range cols {
		if i > 0 {
			b.WriteString(" | ")
		}
		b.WriteString(c)
	}
	b.WriteString(" |\n")

	// Separator.
	b.WriteString("|")
	for range cols {
		b.WriteString(" --- |")
	}
	b.WriteByte('\n')

	// Rows.
	for _, row := range rows {
		b.WriteString("| ")
		for i, c := range cols {
			if i > 0 {
				b.WriteString(" | ")
			}
			if v, ok := row[c]; ok {
				b.WriteString(fmt.Sprintf("%v", v))
			}
		}
		b.WriteString(" |\n")
	}

	return b.String()
}

// sortColumns sorts column names with builtin fields first, then alphabetical.
func sortColumns(cols []string) {
	rank := map[string]int{
		"_time": 0, "_raw": 1, "index": 2, "source": 3,
		"_source": 4, "sourcetype": 5, "_sourcetype": 6, "host": 7,
	}

	for i := 1; i < len(cols); i++ {
		for j := i; j > 0; j-- {
			ri, oki := rank[cols[j]]
			rj, okj := rank[cols[j-1]]

			if oki && okj {
				if ri < rj {
					cols[j], cols[j-1] = cols[j-1], cols[j]
				}
			} else if oki {
				cols[j], cols[j-1] = cols[j-1], cols[j]
			} else if !okj && cols[j] < cols[j-1] {
				cols[j], cols[j-1] = cols[j-1], cols[j]
			}
		}
	}
}

// base64Encode returns the base64 encoding of a string.
func base64Encode(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

// Query execution commands.

func (m Model) executeQueryCmd(query string) tea.Cmd {
	if m.session.Mode == "file" {
		return m.executeFileQueryCmd(query)
	}

	return m.executeServerQueryCmd(query)
}

func (m Model) executeFileQueryCmd(query string) tea.Cmd {
	engine := m.session.Engine

	return func() tea.Msg {
		start := time.Now()

		normalizedQuery := spl2.NormalizeQuery(query)

		if ucErr := spl2.CheckUnsupportedCommands(normalizedQuery); ucErr != nil {
			return queryResultMsg{
				query:   query,
				elapsed: time.Since(start),
				err:     ucErr,
			}
		}

		var hintText string
		if hints := spl2.DetectCompatHints(normalizedQuery); len(hints) > 0 {
			hintText = spl2.FormatCompatHints(hints)
		}

		ctx := context.Background()

		result, _, err := engine.Query(ctx, normalizedQuery, storage.QueryOpts{})
		elapsed := time.Since(start)

		if err != nil {
			errMsg := spl2.FormatParseError(err, normalizedQuery)

			return queryResultMsg{
				query:   query,
				elapsed: elapsed,
				err:     errors.New(errMsg),
				hints:   hintText,
			}
		}

		return queryResultMsg{
			query:   query,
			rows:    result.Rows,
			elapsed: elapsed,
			hints:   hintText,
		}
	}
}

func (m Model) executeServerQueryCmd(query string) tea.Cmd {
	c := m.session.Client
	since := m.session.Since
	hintText := m.jobHints // pre-computed in querySubmitMsg handler
	start := time.Now()

	return func() tea.Msg {
		if ucErr := spl2.CheckUnsupportedCommands(query); ucErr != nil {
			return queryResultMsg{
				query:   query,
				elapsed: time.Since(start),
				err:     ucErr,
				hints:   hintText,
			}
		}

		var from, to string
		if since != "" {
			tr, err := timerange.FromSince(strings.TrimPrefix(since, "-"), time.Now())
			if err == nil {
				from = tr.Earliest.Format(time.RFC3339Nano)
				to = tr.Latest.Format(time.RFC3339Nano)
			}
		}

		ctx := context.Background()

		// Try async path first for progress reporting.
		job, err := c.QueryAsync(ctx, query, from, to)
		if err == nil && job != nil {
			return jobCreatedMsg{jobID: job.JobID}
		}

		// Fallback to sync if async is not supported or fails.
		start := time.Now()

		result, syncErr := c.QuerySync(ctx, query, from, to)
		elapsed := time.Since(start)

		if syncErr != nil {
			return queryResultMsg{
				query:   query,
				elapsed: elapsed,
				err:     syncErr,
				hints:   hintText,
			}
		}

		rows := queryResultToRows(result)
		meta := result.Meta

		return queryResultMsg{
			query:   query,
			rows:    rows,
			elapsed: elapsed,
			meta:    &meta,
			hints:   hintText,
		}
	}
}

func pollJobCmd(c *client.Client, jobID, query, hints string, pollCount int, start time.Time) tea.Cmd {
	return func() tea.Msg {
		// Progressive backoff: start fast, cap at 80ms.
		switch {
		case pollCount == 0:
			// No delay on first poll.
		case pollCount < 3:
			time.Sleep(30 * time.Millisecond)
		default:
			time.Sleep(80 * time.Millisecond)
		}

		ctx := context.Background()

		job, err := c.GetJob(ctx, jobID)
		if err != nil {
			return queryResultMsg{
				query:   query,
				elapsed: time.Since(start),
				err:     err,
				hints:   hints,
			}
		}

		switch strings.ToLower(job.Status) {
		case "done", "complete":
			rows := jobResultToRows(job)
			elapsed := jobElapsed(job)
			if elapsed == 0 {
				elapsed = time.Since(start)
			}

			return queryResultMsg{
				query:   query,
				rows:    rows,
				elapsed: elapsed,
				meta:    &job.Meta,
				hints:   hints,
			}
		case "error", "failed", "canceled":
			errMsg := "query failed"
			if job.Error != nil {
				errMsg = job.Error.Message
			}

			return queryResultMsg{
				query:   query,
				elapsed: time.Since(start),
				err:     errors.New(errMsg),
				hints:   hints,
			}
		default:
			// Still running — extract progress if available.
			if job.Results != nil {
				// Server returned results even though status isn't "done".
				rows := jobResultToRows(job)
				elapsed := jobElapsed(job)
				if elapsed == 0 {
					elapsed = time.Since(start)
				}

				return queryResultMsg{
					query:   query,
					rows:    rows,
					elapsed: elapsed,
					meta:    &job.Meta,
					hints:   hints,
				}
			}

			if job.Progress != nil {
				p := job.Progress

				return progressMsg{
					phase:           p.Phase,
					segmentsTotal:   p.SegmentsTotal,
					segmentsScanned: p.SegmentsScanned,
					segmentsSkipped: p.SegmentsSkippedIndex + p.SegmentsSkippedTime + p.SegmentsSkippedBloom,
					rowsReadSoFar:   p.RowsReadSoFar,
				}
			}

			return pollTickMsg{}
		}
	}
}

func jobElapsed(job *client.JobResult) time.Duration {
	if job.Progress != nil && job.Progress.ElapsedMS > 0 {
		return time.Duration(job.Progress.ElapsedMS) * time.Millisecond
	}

	return 0
}

func fetchFieldsCmd(c *client.Client) tea.Cmd {
	return func() tea.Msg {
		fields, err := c.Fields(context.Background())
		if err != nil {
			return fieldsLoadedMsg{}
		}

		names := make([]string, 0, len(fields))
		for _, f := range fields {
			if f.Name != "" {
				names = append(names, f.Name)
			}
		}

		// Also fetch sources for autocomplete (e.g. after FROM).
		var sourceNames []string

		sources, srcErr := c.Sources(context.Background())
		if srcErr == nil {
			sourceNames = make([]string, 0, len(sources))
			for _, s := range sources {
				if s.Name != "" {
					sourceNames = append(sourceNames, s.Name)
				}
			}
		}

		return fieldsLoadedMsg{fields: names, fieldInfo: fields, sources: sourceNames}
	}
}

func waitForTailEvent(eventCh <-chan map[string]interface{}, errCh <-chan error) tea.Cmd {
	return func() tea.Msg {
		select {
		case event, ok := <-eventCh:
			if !ok {
				err := <-errCh
				return tailDoneMsg{err: err}
			}

			return tailEventMsg{event: event, eventCh: eventCh, errCh: errCh}
		case err := <-errCh:
			return tailDoneMsg{err: err}
		}
	}
}

// welcomeBanner returns the startup message with example queries.
func welcomeBanner() string {
	t := ui.Stdout

	var b strings.Builder
	b.WriteString("\n")
	b.WriteString("  " + t.Bold.Render("Welcome to LynxDB Shell") + "\n")
	b.WriteString("\n")
	b.WriteString("  " + t.Dim.Render("Try a query:") + "\n")
	b.WriteString("    " + t.Accent.Render("source=nginx | stats count by status") + "\n")
	b.WriteString("    " + t.Accent.Render("level=error | timechart count span=5m") + "\n")
	b.WriteString("    " + t.Accent.Render("| top 10 uri") + "\n")
	b.WriteString("\n")
	b.WriteString("  " + t.Dim.Render("Enter:run • ↑↓:history • Tab:complete • F1:help") + "\n")

	return b.String()
}

// rewriteUnderscoreQuery handles "_ | <pipeline>" queries by extracting
// the pipeline part and wrapping it for execution against the last results.
// For now, it just strips the "_ |" prefix and returns the pipeline,
// since the server can re-execute it against the same data source.
func rewriteUnderscoreQuery(query string, lastRows []map[string]interface{}) string {
	trimmed := strings.TrimSpace(query)
	// Strip "_ |" or "_|" prefix.
	if strings.HasPrefix(trimmed, "_ |") {
		return strings.TrimSpace(trimmed[3:])
	}
	if strings.HasPrefix(trimmed, "_|") {
		return strings.TrimSpace(trimmed[2:])
	}

	return trimmed
}
