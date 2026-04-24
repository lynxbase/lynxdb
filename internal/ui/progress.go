package ui

import (
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/progress"
	tea "charm.land/bubbletea/v2"
)

// ProgressModel is a bubbletea model wrapping bubbles/progress for file ingest.
type ProgressModel struct {
	progress progress.Model
	theme    *Theme
	label    string
	total    int64
	current  int64
	events   int64
	start    time.Time
	done     bool
	result   string
	err      error
}

// ProgressUpdateMsg updates the progress bar state.
type ProgressUpdateMsg struct {
	Current int64
	Events  int64
}

// ProgressDoneMsg signals completion.
type ProgressDoneMsg struct {
	Events int64
	Err    error
}

// NewProgressModel creates a new progress bar model.
func NewProgressModel(theme *Theme, label string, total int64) ProgressModel {
	p := progress.New(
		progress.WithDefaultBlend(),
		progress.WithWidth(30),
	)

	return ProgressModel{
		progress: p,
		theme:    theme,
		label:    label,
		total:    total,
		start:    time.Now(),
	}
}

// Init implements tea.Model.
func (m ProgressModel) Init() tea.Cmd {
	return nil
}

func (m ProgressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		if msg.String() == "ctrl+c" {
			m.done = true

			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		w := msg.Width - 20
		if w > 40 {
			w = 40
		}
		if w < 10 {
			w = 10
		}
		m.progress.SetWidth(w)
	case ProgressUpdateMsg:
		m.current = msg.Current
		m.events = msg.Events

		return m, nil
	case ProgressDoneMsg:
		m.done = true
		m.events = msg.Events
		m.err = msg.Err

		if m.err != nil {
			m.result = fmt.Sprintf("Error: %s", m.err)
		} else {
			elapsed := time.Since(m.start)
			eps := int64(0)
			if elapsed.Seconds() > 0 {
				eps = int64(float64(m.events) / elapsed.Seconds())
			}
			m.result = fmt.Sprintf("Ingested %d events in %s (%d events/sec)",
				m.events, elapsed.Round(time.Millisecond), eps)
		}

		return m, tea.Quit
	case progress.FrameMsg:
		var cmd tea.Cmd
		m.progress, cmd = m.progress.Update(msg)

		return m, cmd
	}

	return m, nil
}

// View implements tea.Model.
func (m ProgressModel) View() tea.View {
	if m.done {
		return tea.NewView("")
	}

	var b strings.Builder

	pct := 0.0
	if m.total > 0 {
		pct = float64(m.current) / float64(m.total)
		if pct > 1.0 {
			pct = 1.0
		}
	}

	fmt.Fprintf(&b, "  %s %s  %d events",
		m.label,
		m.progress.ViewAs(pct),
		m.events)

	return tea.NewView(b.String())
}
