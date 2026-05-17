package shell

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	zone "github.com/lrstanley/bubblezone/v2"

	"github.com/lynxbase/lynxdb/internal/output"
	"github.com/lynxbase/lynxdb/internal/ui"
)

var ansiRE = regexp.MustCompile(`\x1b\[[0-9;]*[A-Za-z]`)

func init() {
	ui.Init(true)
}

func plain(s string) string {
	return ansiRE.ReplaceAllString(s, "")
}

func TestAutocompletePopupRendersKindDetailAndScroll(t *testing.T) {
	var popup AutocompletePopup
	items := make([]CompletionItem, 0, 10)
	for i := 0; i < 10; i++ {
		items = append(items, CompletionItem{
			Text:   "field_name",
			Kind:   KindField,
			Detail: "string",
		})
	}
	popup.Show(items, 0)
	for i := 0; i < 8; i++ {
		popup.MoveDown()
	}

	got := plain(popup.View(48))
	for _, want := range []string{"field_name", "field", "string", "9/10 shown"} {
		if !strings.Contains(got, want) {
			t.Fatalf("popup missing %q in %q", want, got)
		}
	}
}

func TestStatusBarRendersPopupHelpMode(t *testing.T) {
	sb := NewStatusBar("server")
	sb.SetWidth(80)

	got := plain(sb.View(EditorFocus, false, false, true, 0, nil, false, true, defaultKeyMap()))
	for _, want := range []string{"tab", "complete", "up/down", "move", "esc", "back"} {
		if !strings.Contains(got, want) {
			t.Fatalf("status bar missing %q in %q", want, got)
		}
	}
}

func TestStatusBarRendersRunningHelpMode(t *testing.T) {
	sb := NewStatusBar("server")
	sb.SetWidth(80)

	got := plain(sb.View(EditorFocus, true, false, false, 120*time.Millisecond, nil, false, true, defaultKeyMap()))
	for _, want := range []string{"Executing", "ctrl+c", "cancel"} {
		if !strings.Contains(got, want) {
			t.Fatalf("status bar missing %q in %q", want, got)
		}
	}
}

func TestResultsRenderConnectionDiagnostic(t *testing.T) {
	results := NewResults(80, 20)
	results.AppendConnectionDiagnostic("http://localhost:3100", errors.New("connection refused"))

	got := plain(results.View())
	for _, want := range []string{
		"Cannot connect to LynxDB server",
		"http://localhost:3100",
		"connection refused",
		"lynxdb server",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("connection diagnostic missing %q in %q", want, got)
		}
	}
}

func TestModelViewCapturesMouseForResultsWheel(t *testing.T) {
	zone.NewGlobal()
	defer zone.Close()

	model := NewModel("server", RunOpts{Server: "http://localhost:3100"})
	model.width = 100
	model.height = 30
	model.recalcLayout()

	view := model.View()
	if view.MouseMode != tea.MouseModeCellMotion {
		t.Fatalf("shell view mouse mode = %v, want %v", view.MouseMode, tea.MouseModeCellMotion)
	}
}

func TestMouseWheelOnlyScrollsResultsPane(t *testing.T) {
	model := NewModel("server", RunOpts{Server: "http://localhost:3100"})
	model.width = 80
	model.height = 12
	model.recalcLayout()
	for i := 0; i < 20; i++ {
		model.results.AppendText("result line")
	}

	before := model.results.viewport.YOffset()
	next, _ := model.Update(tea.MouseWheelMsg{X: 2, Y: 2, Button: tea.MouseWheelUp})
	model = next.(Model)
	if model.focus != ResultsFocus {
		t.Fatal("mouse wheel over results should focus results")
	}
	if got := model.results.viewport.YOffset(); got >= before {
		t.Fatalf("results y offset = %d, want less than %d", got, before)
	}

	model.focus = EditorFocus
	before = model.results.viewport.YOffset()
	next, _ = model.Update(tea.MouseWheelMsg{X: 2, Y: model.height - 1, Button: tea.MouseWheelUp})
	model = next.(Model)
	if model.focus != EditorFocus {
		t.Fatal("mouse wheel outside results should keep editor focus")
	}
	if got := model.results.viewport.YOffset(); got != before {
		t.Fatalf("results y offset changed outside results: %d != %d", got, before)
	}
}

func TestModelViewKeepsEditorInsideScreen(t *testing.T) {
	zone.NewGlobal()
	defer zone.Close()

	model := NewModel("server", RunOpts{Server: "http://localhost:3100"})
	model.width = 72
	model.height = 18
	model.recalcLayout()
	for i := 0; i < 12; i++ {
		model.results.AppendText(strings.Repeat("result line with enough text to wrap ", 4))
	}

	view := model.View()
	lines := strings.Split(view.Content, "\n")
	if len(lines) != model.height {
		t.Fatalf("view height = %d, want %d\n%s", len(lines), model.height, plain(view.Content))
	}
	if !strings.Contains(plain(view.Content), "lynxdb>") {
		t.Fatalf("editor prompt is not visible in\n%s", plain(view.Content))
	}
}

func TestModelEditorStopsBeforeSidebar(t *testing.T) {
	zone.NewGlobal()
	defer zone.Close()

	model := NewModel("server", RunOpts{Server: "http://localhost:3100"})
	model.width = 120
	model.height = 20
	model.recalcLayout()

	got := model.View().Content
	lines := strings.Split(got, "\n")
	editorTop := 1 + model.mainHeight()
	if editorTop >= len(lines) {
		t.Fatalf("editor top line %d outside rendered view", editorTop)
	}
	if w := lipgloss.Width(lines[editorTop]); w != model.width {
		t.Fatalf("editor row width = %d, want %d in %q", w, model.width, plain(lines[editorTop]))
	}
	if !strings.Contains(plain(lines[editorTop]), "│") {
		t.Fatalf("editor row should include sidebar separator in %q", plain(lines[editorTop]))
	}
	if firstSep := strings.Index(plain(lines[editorTop]), "│"); firstSep < 0 {
		t.Fatalf("editor row should include sidebar separator in %q", plain(lines[editorTop]))
	} else if col := lipgloss.Width(plain(lines[editorTop])[:firstSep]); col != model.sidebarLay.mainW {
		t.Fatalf("editor separator column = %d, want %d in %q", col, model.sidebarLay.mainW, plain(lines[editorTop]))
	}
}

func TestEditorRendersFramedInputBlock(t *testing.T) {
	editor := NewEditor("lynxdb> ", "   ...> ", NewHistory(), NewCompleter())
	editor.SetWidth(40)

	got := plain(editor.View())
	lines := strings.Split(got, "\n")
	if editor.EditorHeight() != 3 {
		t.Fatalf("editor height = %d, want 3", editor.EditorHeight())
	}
	if len(lines) != 3 {
		t.Fatalf("rendered editor lines = %d, want 3 in %q", len(lines), got)
	}
	if !strings.Contains(got, "lynxdb>") || !strings.Contains(got, "─") {
		t.Fatalf("editor input block missing prompt or frame in %q", got)
	}
}

func TestRenderResultRowsFitsLogTableWidth(t *testing.T) {
	rows := []map[string]interface{}{
		{
			"timestamp": "2026-05-17T12:00:00Z",
			"level":     "error",
			"service":   "api",
			"host":      "localhost",
			"source":    "nginx",
			"trace_id":  "abc123",
			"message":   strings.Repeat("connection refused while dialing upstream ", 4),
		},
	}

	got := plain(renderResultRows(rows, 48, output.FormatTable))
	for _, line := range strings.Split(got, "\n") {
		if w := lipgloss.Width(line); w > 48 {
			t.Fatalf("rendered line width = %d, want <= 48 in %q\n%s", w, line, got)
		}
	}
}

func TestPlaceOverlayUsesDisplayWidth(t *testing.T) {
	base := "\x1b[31mabcdef\x1b[0m"
	got := placeOverlay(base, "ZZ", 2, 0, 10, 1)
	line := strings.Split(got, "\n")[0]

	if lipgloss.Width(line) != 10 {
		t.Fatalf("overlay line width = %d, want 10 in %q", lipgloss.Width(line), line)
	}
	if !strings.HasPrefix(plain(line), "abZZef") {
		t.Fatalf("overlay used byte offsets, got %q", plain(line))
	}
}

func TestPreflightViewRendersConnectionState(t *testing.T) {
	model := preflightModel{
		server: "http://localhost:3100",
		width:  80,
		height: 24,
		state:  preflightConnecting,
	}

	view := model.View()
	got := plain(view.Content)
	if !view.AltScreen {
		t.Fatal("preflight should render in alt screen")
	}
	for _, want := range []string{"Connecting to LynxDB server", "http://localhost:3100"} {
		if !strings.Contains(got, want) {
			t.Fatalf("preflight missing %q in %q", want, got)
		}
	}
	if strings.Contains(got, "Try:") {
		t.Fatalf("preflight should not render main shell welcome, got %q", got)
	}
}

func TestPreflightViewRendersFailureState(t *testing.T) {
	model := preflightModel{
		server: "http://localhost:3100",
		width:  80,
		height: 24,
		state:  preflightFailed,
		err:    errors.New("connection refused"),
	}

	got := plain(model.View().Content)
	for _, want := range []string{
		"LynxDB server is not reachable",
		"connection refused - no LynxDB server appears to be listening there",
		"Hint: start the server with lynxdb server",
		"Press r to retry or q to quit",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("preflight failure missing %q in %q", want, got)
		}
	}
}

func TestPreflightLynxAnimationUsesMultipleFrames(t *testing.T) {
	first := plain(preflightModel{state: preflightConnecting, frame: 0}.lynxFrame())
	second := plain(preflightModel{state: preflightConnecting, frame: 1}.lynxFrame())
	if first == second {
		t.Fatal("preflight lynx animation should change between frames")
	}
	if lipgloss.Width(first) != lipgloss.Width(second) {
		t.Fatalf("preflight frame width changed: %d != %d", lipgloss.Width(first), lipgloss.Width(second))
	}
}

func TestPreflightQQuits(t *testing.T) {
	model := preflightModel{
		state: preflightFailed,
		err:   errors.New("connection refused"),
		keys:  defaultKeyMap(),
	}

	next, cmd := model.Update(tea.KeyPressMsg{Code: 'q'})
	if _, ok := next.(preflightModel); !ok {
		t.Fatalf("unexpected model type %T", next)
	}
	if cmd == nil {
		t.Fatal("q should quit preflight")
	}
}
