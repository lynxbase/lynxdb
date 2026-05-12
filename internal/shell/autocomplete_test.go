package shell

import "testing"

func TestCompleterCommandCatalogIncludesParserCommands(t *testing.T) {
	completer := NewCompleter()

	want := []string{
		"APPENDCOLS",
		"APPENDPIPE",
		"COMPARE",
		"FIELDFORMAT",
		"MAKERESULTS",
		"MVCOMBINE",
		"OUTLIERS",
		"PATTERNS",
		"TRACE",
		"UNION",
		"USE",
	}

	have := make(map[string]bool, len(completer.commands))
	for _, command := range completer.commands {
		have[command] = true
	}
	for _, command := range want {
		if !have[command] {
			t.Fatalf("missing command completion %q", command)
		}
	}
}

func TestCompleterSuggestsCompareCommand(t *testing.T) {
	completer := NewCompleter()
	got := completer.Suggest("com")

	for _, suggestion := range got {
		if suggestion == "COMPARE" {
			return
		}
	}

	t.Fatalf("COMPARE not suggested for prefix, got %v", got)
}
