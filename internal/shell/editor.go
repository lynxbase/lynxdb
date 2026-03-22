package shell

import (
	"fmt"
	"strings"

	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/textarea"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/lynxbase/lynxdb/internal/ui"
)

const editorMaxLines = 6

// Editor wraps textarea.Model with history navigation, ghost-text autocomplete,
// and dynamic height.
type Editor struct {
	input       textarea.Model
	history     *History
	completer   *Completer
	prompt      string
	contPrompt  string
	keys        keyMap
	ghostText   string // autocomplete ghost suffix
	multiLine   bool   // tracks multi-line state for prompt switching
	promptWidth int    // cached display width of the prompt
}

// NewEditor creates an editor with the given prompt strings.
func NewEditor(prompt, contPrompt string, history *History, completer *Completer) Editor {
	ta := textarea.New()
	ta.ShowLineNumbers = false
	ta.CharLimit = 0
	ta.MaxHeight = editorMaxLines
	ta.SetHeight(1)
	ta.EndOfBufferCharacter = ' '

	// Override textarea KeyMap so our shell-level bindings don't conflict.
	// InsertNewline: only shift+enter (we handle plain enter for submit).
	ta.KeyMap.InsertNewline = key.NewBinding(key.WithKeys("shift+enter"))
	// LinePrevious: only up arrow (we use ctrl+p for history).
	ta.KeyMap.LinePrevious = key.NewBinding(key.WithKeys("up"))
	// LineNext: only down arrow (we use ctrl+n for history).
	ta.KeyMap.LineNext = key.NewBinding(key.WithKeys("down"))

	promptWidth := lipgloss.Width(prompt)
	ta.SetPromptFunc(promptWidth, func(info textarea.PromptInfo) string {
		if info.LineNumber == 0 {
			return prompt
		}
		return contPrompt
	})

	// Minimal styling — no borders, no background, clear CursorLine highlight.
	styles := ta.Styles()
	styles.Focused.CursorLine = lipgloss.NewStyle()
	styles.Focused.Base = lipgloss.NewStyle()
	styles.Blurred.CursorLine = lipgloss.NewStyle()
	styles.Blurred.Base = lipgloss.NewStyle()
	ta.SetStyles(styles)

	ta.Focus()

	return Editor{
		input:       ta,
		history:     history,
		completer:   completer,
		prompt:      prompt,
		contPrompt:  contPrompt,
		keys:        defaultKeyMap(),
		promptWidth: promptWidth,
	}
}

// Value returns the current text input value.
func (e *Editor) Value() string {
	return e.input.Value()
}

// SetWidth updates the input width.
func (e *Editor) SetWidth(w int) {
	e.input.SetWidth(w)
}

// EditorHeight returns the current height of the textarea.
func (e *Editor) EditorHeight() int {
	return e.input.Height()
}

// InMultiLine reports whether the editor content spans multiple lines.
func (e *Editor) InMultiLine() bool {
	return e.input.LineCount() > 1
}

// Update handles key events and returns commands.
func (e *Editor) Update(msg tea.Msg) (tea.Cmd, *querySubmitMsg, *slashCommandMsg) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		switch {
		case key.Matches(msg, e.keys.Submit):
			return e.handleSubmit()

		case key.Matches(msg, e.keys.InsertNewline):
			e.input.InsertRune('\n')
			e.updateHeight()
			e.refreshSuggestions()
			return nil, nil, nil

		case key.Matches(msg, e.keys.Cancel):
			return e.handleCancel()

		case key.Matches(msg, e.keys.Quit):
			if e.input.Value() == "" {
				return nil, nil, &slashCommandMsg{quit: true}
			}
			// Non-empty: fall through to textarea (ctrl+d = delete forward).

		case key.Matches(msg, e.keys.AcceptSugg):
			if e.ghostText != "" {
				e.input.InsertString(e.ghostText)
				e.ghostText = ""
				e.updateHeight()
			}
			e.refreshSuggestions()
			return nil, nil, nil

		case key.Matches(msg, e.keys.HistPrev):
			if entry, ok := e.history.Prev(); ok {
				e.input.SetValue(entry)
				e.input.MoveToEnd()
				e.updateHeight()
			}
			e.refreshSuggestions()
			return nil, nil, nil

		case key.Matches(msg, e.keys.HistNext):
			if entry, ok := e.history.Next(); ok {
				e.input.SetValue(entry)
				e.input.MoveToEnd()
			} else {
				e.input.Reset()
			}
			e.updateHeight()
			e.refreshSuggestions()
			return nil, nil, nil
		}
	}

	// Default textarea update.
	var cmd tea.Cmd
	e.input, cmd = e.input.Update(msg)

	e.updateHeight()
	e.refreshSuggestions()

	return cmd, nil, nil
}

// refreshSuggestions recomputes ghost text when cursor is at the end of input.
func (e *Editor) refreshSuggestions() {
	if e.completer == nil {
		e.ghostText = ""
		return
	}

	value := e.input.Value()

	// Only suggest when cursor is at the absolute end of input.
	onLastLine := e.input.Line() == e.input.LineCount()-1
	lines := strings.Split(value, "\n")
	lastLine := ""
	if len(lines) > 0 {
		lastLine = lines[len(lines)-1]
	}
	atLineEnd := e.input.Column() >= len(lastLine)

	if !onLastLine || !atLineEnd {
		e.ghostText = ""
		return
	}

	suggestions := e.completer.Suggest(value)
	if len(suggestions) > 0 && len(suggestions[0]) > len(value) {
		e.ghostText = suggestions[0][len(value):]
	} else {
		e.ghostText = ""
	}
}

// updateHeight adjusts textarea height to match content, clamped to [1, editorMaxLines].
// When the editor transitions between single-line and multi-line, the prompt
// is updated to show/hide line numbers.
func (e *Editor) updateHeight() {
	h := e.input.LineCount()
	if h > editorMaxLines {
		h = editorMaxLines
	}
	if h < 1 {
		h = 1
	}
	e.input.SetHeight(h)

	isMulti := h > 1
	if isMulti != e.multiLine {
		e.multiLine = isMulti
		e.updatePrompt()
	}
}

// updatePrompt switches the textarea prompt between the normal prompt
// (single-line) and line-numbered prompt (multi-line).
func (e *Editor) updatePrompt() {
	pw := e.promptWidth

	if !e.multiLine {
		// Restore original prompt.
		prompt := e.prompt
		contPrompt := e.contPrompt
		e.input.SetPromptFunc(pw, func(info textarea.PromptInfo) string {
			if info.LineNumber == 0 {
				return prompt
			}
			return contPrompt
		})
		return
	}

	// Multi-line: show dimmed line numbers.
	dimStyle := lipgloss.NewStyle().Foreground(ui.ColorDim())
	e.input.SetPromptFunc(pw, func(info textarea.PromptInfo) string {
		num := fmt.Sprintf("%d", info.LineNumber+1)
		suffix := " │ "
		suffixWidth := lipgloss.Width(suffix)
		padWidth := pw - len(num) - suffixWidth
		if padWidth < 0 {
			padWidth = 0
		}
		return dimStyle.Render(strings.Repeat(" ", padWidth) + num + suffix)
	})
}

func (e *Editor) handleSubmit() (tea.Cmd, *querySubmitMsg, *slashCommandMsg) {
	value := strings.TrimSpace(e.input.Value())
	if value == "" {
		return nil, nil, nil
	}

	// Auto-continue: if the trimmed value ends with |, insert a newline.
	if strings.HasSuffix(value, "|") {
		e.input.InsertRune('\n')
		e.updateHeight()
		return nil, nil, nil
	}

	fullQuery := value
	e.input.Reset()
	e.updateHeight()
	e.ghostText = ""

	// Slash commands.
	if strings.HasPrefix(fullQuery, "/") {
		return nil, nil, &slashCommandMsg{output: fullQuery}
	}

	// Regular query.
	e.history.Add(fullQuery)
	e.history.Reset()

	return nil, &querySubmitMsg{query: fullQuery}, nil
}

func (e *Editor) handleCancel() (tea.Cmd, *querySubmitMsg, *slashCommandMsg) {
	if e.input.Value() != "" {
		e.input.Reset()
		e.updateHeight()
		e.ghostText = ""
		return nil, nil, nil
	}

	// Empty input + Ctrl+C → no-op hint.
	return nil, nil, nil
}

// View renders the editor with ghost text overlay.
//
// Without ghost text, this delegates to the textarea's own View which manages
// the virtual cursor and all ANSI rendering.
//
// With ghost text, we post-process the rendered output to append dimmed ghost
// text after the cursor. We never mutate textarea state in View() — calling
// SetValue() triggers Reset() internally which destroys cursor position,
// viewport scroll, and the value slice, corrupting state for the next Update().
func (e *Editor) View() string {
	v := e.input.View()
	if e.ghostText == "" {
		return v
	}

	// Append dimmed ghost text to the last content line of the rendered output.
	// The textarea pads each line with trailing plain spaces to fill the width;
	// we strip that padding, insert styled ghost text, then re-pad.
	ghostStyle := lipgloss.NewStyle().Foreground(ui.ColorDim())
	styledGhost := ghostStyle.Render(e.ghostText)
	ghostWidth := lipgloss.Width(e.ghostText)

	lines := strings.Split(v, "\n")

	// Find the last non-empty line — textarea.View() ends with \n producing
	// a trailing empty string, and end-of-buffer lines may be blank.
	lastIdx := len(lines) - 1
	for lastIdx > 0 && strings.TrimRight(lines[lastIdx], " ") == "" {
		lastIdx--
	}
	if lastIdx < 0 {
		return v
	}

	line := lines[lastIdx]
	trimmed := strings.TrimRight(line, " ")
	fullWidth := lipgloss.Width(line)
	trimmedWidth := lipgloss.Width(trimmed)
	available := fullWidth - trimmedWidth

	if available <= 0 {
		return v
	}

	// Truncate ghost text if it exceeds the available padding space.
	ghost := e.ghostText
	if ghostWidth > available {
		ghost = truncateToWidth(ghost, available)
		styledGhost = ghostStyle.Render(ghost)
		ghostWidth = lipgloss.Width(ghost)
	}

	remaining := available - ghostWidth
	if remaining < 0 {
		remaining = 0
	}

	lines[lastIdx] = trimmed + styledGhost + strings.Repeat(" ", remaining)

	return strings.Join(lines, "\n")
}

// truncateToWidth returns a prefix of s whose display width does not exceed maxWidth.
func truncateToWidth(s string, maxWidth int) string {
	if maxWidth <= 0 {
		return ""
	}

	w := 0
	for i, r := range s {
		rw := lipgloss.Width(string(r))
		if w+rw > maxWidth {
			return s[:i]
		}
		w += rw
	}

	return s
}
