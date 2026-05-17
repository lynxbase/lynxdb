package shell

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

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
