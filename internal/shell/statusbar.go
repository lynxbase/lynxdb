package shell

import (
	"fmt"
	"time"

	"charm.land/bubbles/v2/help"
	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/spinner"
	"charm.land/lipgloss/v2"

	"github.com/lynxbase/lynxdb/internal/ui"
)

// Focus indicates which component has keyboard focus.
type Focus int

const (
	EditorFocus Focus = iota
	ResultsFocus
)

// StatusBar renders the bottom shortcut bar with context-dependent hints.
type StatusBar struct {
	spinner spinner.Model
	help    help.Model
	width   int
	mode    string

	// Transient flash message (e.g. "Copied!"), cleared after flashUntil.
	flashMsg   string
	flashUntil time.Time
}

// NewStatusBar creates a status bar.
func NewStatusBar(mode string) StatusBar {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = ui.Stdout.Accent

	h := help.New()
	h.ShowAll = false
	styles := h.Styles
	styles.ShortKey = lipgloss.NewStyle().Foreground(ui.ColorGray()).Bold(true)
	styles.ShortDesc = lipgloss.NewStyle().Foreground(ui.ColorDim())
	styles.ShortSeparator = lipgloss.NewStyle().Foreground(ui.ColorDim())
	h.Styles = styles

	return StatusBar{
		spinner: s,
		help:    h,
		width:   80,
		mode:    mode,
	}
}

// SetWidth updates the status bar width.
func (sb *StatusBar) SetWidth(w int) {
	sb.width = w
	sb.help.SetWidth(w - 2)
}

// SetFlash sets a transient message that displays for the given duration.
func (sb *StatusBar) SetFlash(msg string, d time.Duration) {
	sb.flashMsg = msg
	sb.flashUntil = time.Now().Add(d)
}

// View renders the status bar based on current state.
func (sb StatusBar) View(focus Focus, running bool, inMulti bool, popupOpen bool, elapsed time.Duration, progress *progressMsg, tailActive bool, sidebarOpen bool, keys keyMap) string {
	style := lipgloss.NewStyle().
		Width(sb.width).
		Foreground(ui.ColorDim()).
		PaddingLeft(1)

	// Flash message takes priority.
	if sb.flashMsg != "" && time.Now().Before(sb.flashUntil) {
		return style.Render(sb.flashMsg)
	}

	var content string

	switch {
	case tailActive:
		content = fmt.Sprintf("%s Live tail active    %s",
			sb.spinner.View(),
			sb.help.View(statusHelp{keys.Cancel}))

	case running && progress != nil && progress.segmentsTotal > 0:
		el := elapsed.Round(10 * time.Millisecond)
		content = fmt.Sprintf("%s %s  %s/%s segments  %s    %s",
			sb.spinner.View(),
			phaseDisplayName(progress.phase),
			formatCountShell(int64(progress.segmentsScanned)),
			formatCountShell(int64(progress.segmentsTotal)),
			ui.Stdout.Value.Render(el.String()),
			sb.help.View(statusHelp{keys.Cancel}))

	case running:
		el := elapsed.Round(10 * time.Millisecond)
		content = fmt.Sprintf("%s Executing... %s    %s",
			sb.spinner.View(),
			ui.Stdout.Value.Render(el.String()),
			sb.help.View(statusHelp{keys.Cancel}))

	case popupOpen:
		content = sb.help.View(statusHelp{
			keys.AcceptSugg,
			key.NewBinding(key.WithKeys("up", "down"), key.WithHelp("up/down", "move")),
			keys.FocusBack,
		})

	case focus == ResultsFocus:
		content = sb.help.View(statusHelp{
			key.NewBinding(key.WithKeys("j", "k"), key.WithHelp("j/k", "scroll")),
			keys.FocusBack,
			keys.ToggleSidebar,
			keys.CopyResults,
			keys.CopyResultsMD,
		})

	case inMulti:
		content = sb.help.View(statusHelp{
			keys.Submit,
			keys.InsertNewline,
			keys.Cancel,
			keys.ToggleSidebar,
		})

	default:
		content = sb.help.View(statusHelp{
			keys.Submit,
			key.NewBinding(key.WithKeys("up", "down"), key.WithHelp("up/down", "history")),
			keys.AcceptSugg,
			keys.CompletePopup,
			keys.ToggleSidebar,
		})
	}

	return style.Render(content)
}

type statusHelp []key.Binding

func (h statusHelp) ShortHelp() []key.Binding {
	return []key.Binding(h)
}

func (h statusHelp) FullHelp() [][]key.Binding {
	return [][]key.Binding{h.ShortHelp()}
}

// phaseDisplayName maps a query execution phase to a user-friendly label.
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
