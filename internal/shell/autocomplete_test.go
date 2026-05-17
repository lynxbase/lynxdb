package shell

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func TestCompleterCommandCatalogIncludesParserCommands(t *testing.T) {
	completer := NewCompleter()

	have := make(map[string]bool, len(completer.commands))
	for _, command := range completer.commands {
		have[strings.ToLower(command.Text)] = true
	}
	for _, command := range spl2.KnownCommands() {
		if !have[command] {
			t.Fatalf("missing command completion %q", command)
		}
	}
}

func TestCompleterFunctionCatalogIncludesParserFunctions(t *testing.T) {
	completer := NewCompleter()

	haveEval := make(map[string]bool, len(completer.evalFuncs))
	for _, fn := range completer.evalFuncs {
		haveEval[strings.ToLower(fn.Text)] = true
	}
	for _, fn := range appendCatalogs(spl2.KnownEvalFunctions(), spl2.KnownJSONFunctions()) {
		if !haveEval[fn] {
			t.Fatalf("missing eval function completion %q", fn)
		}
	}

	haveAgg := make(map[string]bool, len(completer.aggFuncs))
	for _, fn := range completer.aggFuncs {
		haveAgg[strings.ToLower(fn.Text)] = true
	}
	for _, fn := range spl2.KnownAggregateFunctions() {
		if !haveAgg[fn] {
			t.Fatalf("missing aggregate function completion %q", fn)
		}
	}
}

func TestCompleterSuggestsCompareCommand(t *testing.T) {
	completer := NewCompleter()
	got := completer.Suggest("com")

	for _, suggestion := range got {
		if suggestion == "compare" {
			return
		}
	}

	t.Fatalf("compare not suggested for prefix, got %v", got)
}

func TestCompleterSuggestsEvalFunctionsInEvalContext(t *testing.T) {
	completer := NewCompleter()
	got := completer.Suggest("| eval sha")

	for _, suggestion := range got {
		if suggestion == "| eval sha256" {
			return
		}
	}

	t.Fatalf("sha256 not suggested in eval context, got %v", got)
}

func TestCompleterSuggestsEvalFunctionsInWhereContext(t *testing.T) {
	completer := NewCompleter()
	got := completer.SuggestAll("| where strp")

	for _, item := range got {
		if item.Text == "strptime" && item.Kind == KindFunction {
			return
		}
	}

	t.Fatalf("strptime function item not suggested in where context, got %v", got)
}

func TestCompleterSuggestsJSONFunctionsInEvalContext(t *testing.T) {
	completer := NewCompleter()
	got := completer.Suggest("| eval json_ex")

	for _, suggestion := range got {
		if suggestion == "| eval json_extract" {
			return
		}
	}

	t.Fatalf("json_extract not suggested in eval context, got %v", got)
}

func TestCompleterSuggestsQueryTemplateAtStart(t *testing.T) {
	completer := NewCompleter()
	got := completer.SuggestAll("lat")

	assertItem(t, got, "latency duration_ms every 1m", KindTemplate)
}

func TestCompleterSuggestsSourcesAfterFrom(t *testing.T) {
	completer := NewCompleter()
	completer.SetIndexes([]string{"app", "audit"})
	completer.SetSources([]string{"api-gateway"})

	got := completer.SuggestAll("from ap")

	assertItem(t, got, "app", KindSource)
	assertItem(t, got, "api-gateway", KindSource)
}

func TestCompleterSuggestsTimeModifierValues(t *testing.T) {
	completer := NewCompleter()
	got := completer.SuggestAll("from app earliest=-")

	assertItem(t, got, "-15m", KindConstant)
}

func TestCompleterSuggestsSourceTimeTemplates(t *testing.T) {
	completer := NewCompleter()
	got := completer.Suggest("from app[")

	assertString(t, got, "from app[-15m]")
}

func TestCompleterSuggestsRegexSnippets(t *testing.T) {
	completer := NewCompleter()
	got := completer.SuggestAll(`message=~"`)

	assertItem(t, got, `"(?i)error|fatal"`, KindTemplate)
}

func TestCompleterSuggestsFieldsAfterComma(t *testing.T) {
	completer := NewCompleter()
	completer.SetFields([]string{"service", "status"})

	got := completer.SuggestAll("stats count by service, st")

	assertItem(t, got, "status", KindField)
}

func TestCompleterSuggestsAggregateFunctions(t *testing.T) {
	completer := NewCompleter()
	got := completer.SuggestAll("stats co")

	assertItem(t, got, "count", KindFunction)
}

func TestCompleterSuggestsLazyFieldValues(t *testing.T) {
	var hits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/fields/level/values" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.URL.Query().Get("limit") != "20" {
			t.Fatalf("missing limit query: %s", r.URL.RawQuery)
		}
		hits++
		fmt.Fprint(w, `{"data":{"field":"level","values":[{"value":"error","count":7},{"value":"info","count":3}]}}`)
	}))
	defer server.Close()

	completer := NewCompleter()
	completer.SetFields([]string{"level"})
	completer.SetClient(client.NewClient(client.WithBaseURL(server.URL)))
	now := time.Unix(100, 0)
	completer.now = func() time.Time { return now }

	got := completer.SuggestAll("level=er")
	assertItem(t, got, "error", KindValue)

	got = completer.SuggestAll("level=in")
	assertItem(t, got, "info", KindValue)

	if hits != 1 {
		t.Fatalf("expected cached field values after first fetch, got %d hits", hits)
	}
}

func assertString(t *testing.T, got []string, want string) {
	t.Helper()

	for _, s := range got {
		if s == want {
			return
		}
	}

	t.Fatalf("missing %q in %v", want, got)
}

func assertItem(t *testing.T, got []CompletionItem, text string, kind CompletionKind) {
	t.Helper()

	for _, item := range got {
		if item.Text == text && item.Kind == kind {
			return
		}
	}

	t.Fatalf("missing %q kind %v in %+v", text, kind, got)
}
