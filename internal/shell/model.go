package shell

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

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
		header:    header,
		editor:    editor,
		results:   results,
		statusBar: statusBar,
		session:   sess,
		completer: completer,
		history:   history,
		keys:      defaultKeyMap(),
		focus:     EditorFocus,
	}
}

// Init is the Bubble Tea initialization command.
func (m Model) Init() tea.Cmd {
	cmds := []tea.Cmd{
		m.statusBar.spinner.Tick,
	}

	// Fetch fields for autocomplete in server mode.
	if m.session.Mode == "server" && m.session.Client != nil {
		cmds = append(cmds, fetchFieldsCmd(m.session.Client))
	}

	return tea.Batch(cmds...)
}

// Update handles all messages.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.header.SetWidth(msg.Width)
		m.statusBar.SetWidth(msg.Width)
		m.editor.SetWidth(msg.Width)
		// Results viewport: remaining height minus header(1) + editor(dynamic) + statusbar(1).
		editorH := m.editor.EditorHeight()
		resultsHeight := msg.Height - 1 - editorH - 1
		if resultsHeight < 1 {
			resultsHeight = 1
		}
		m.results.SetSize(msg.Width, resultsHeight)

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

		if len(msg.sources) > 0 {
			m.completer.SetSources(msg.sources)
		}

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
		tableStr := renderResultRows([]map[string]interface{}{msg.event}, m.width, m.session.Format)
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
		m.jobQuery = msg.query

		// Pre-compute compat hints for server mode (needed if async path is taken).
		if m.session.Mode == "server" {
			if hints := spl2.DetectCompatHints(msg.query); len(hints) > 0 {
				m.jobHints = spl2.FormatCompatHints(hints)
			} else {
				m.jobHints = ""
			}
		}

		return m, m.executeQueryCmd(msg.query)

	case queryResultMsg:
		m.running = false
		m.jobID = ""
		m.progress = nil
		m.session.LastQuery = msg.query

		// Extract field names from results and add to autocomplete.
		if len(msg.rows) > 0 {
			m.completer.MergeResultFields(extractFieldNames(msg.rows))
		}

		var zeroCtx *ZeroResultContext
		if len(msg.rows) == 0 && msg.err == nil && m.session.Mode == "server" {
			zeroCtx = &ZeroResultContext{Session: m.session, FieldInfos: m.fieldInfos}
		}

		m.results.AppendResult(msg.query, msg.rows, msg.elapsed, msg.err, m.width,
			m.session.Format, m.session.Timing, msg.hints, zeroCtx)

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

	case tea.MouseWheelMsg:
		cmd := m.results.Update(msg)

		return m, cmd

	case tea.KeyPressMsg:
		return m.handleKey(msg)
	}

	return m, nil
}

func (m Model) handleKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
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
		// Pass scroll keys to viewport.
		cmd := m.results.Update(msg)

		return m, cmd
	}

	// Editor focus — handle input.
	cmd, submitMsg, slashMsg := m.editor.Update(msg)

	// Recalculate layout when editor height may have changed.
	editorH := m.editor.EditorHeight()
	resultsH := m.height - 1 - editorH - 1
	if resultsH < 1 {
		resultsH = 1
	}
	m.results.SetSize(m.width, resultsH)

	if submitMsg != nil {
		m.running = true
		m.startTime = time.Now()
		m.jobQuery = submitMsg.query

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

	return m, cmd
}

// View renders the full-screen TUI.
func (m Model) View() tea.View {
	var b strings.Builder

	// Header (1 line).
	b.WriteString(m.header.View())
	b.WriteByte('\n')

	// Results viewport (fills remaining space).
	b.WriteString(m.results.View())
	b.WriteByte('\n')

	// Editor (1 line).
	b.WriteString(m.editor.View())
	b.WriteByte('\n')

	// Status bar (1 line).
	var elapsed time.Duration
	if m.running {
		elapsed = time.Since(m.startTime)
	}
	b.WriteString(m.statusBar.View(m.focus, m.running, m.editor.InMultiLine(), elapsed, m.progress, m.tailActive))

	v := tea.NewView(b.String())
	v.AltScreen = true
	v.MouseMode = tea.MouseModeCellMotion

	return v
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
				err:     fmt.Errorf("%s", errMsg),
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
				err:     fmt.Errorf("%s", errMsg),
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

// welcomeBanner returns the startup message.
// The header already shows version + connection info, so this only has the usage hint.
func welcomeBanner() string {
	t := ui.Stdout

	return fmt.Sprintf("\n  Type %s for commands, %s for autocomplete, %s/%s for history, %s to exit.\n",
		t.Accent.Render("/help"),
		t.Accent.Render("Tab"),
		t.Accent.Render("Ctrl+P"),
		t.Accent.Render("Ctrl+N"),
		t.Accent.Render("Ctrl+D"))
}
