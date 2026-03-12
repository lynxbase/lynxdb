package spl2

import (
	"strings"
	"testing"
)

func TestSplunkCompat_Lookup(t *testing.T) {
	// "lookup" is a valid Lynx Flow command — should NOT produce an unsupported hint.
	hints := DetectCompatHints(`index=main | lookup users uid OUTPUT username`)
	for _, h := range hints {
		if h.Pattern == "lookup" && h.Unsupported {
			t.Errorf("lookup is a valid Lynx Flow command, should not be unsupported, got: %s", h.Suggestion)
		}
	}
}

func TestSplunkCompat_InputLookup(t *testing.T) {
	hints := DetectCompatHints(`| inputlookup users.csv`)
	if len(hints) == 0 {
		t.Fatal("expected hint for inputlookup")
	}
	if !hints[0].Unsupported {
		t.Error("inputlookup should be unsupported")
	}
}

func TestSplunkCompat_Chart(t *testing.T) {
	hints := DetectCompatHints(`index=main | chart count by host`)
	if len(hints) == 0 {
		t.Fatal("expected hint for chart")
	}
	found := false
	for _, h := range hints {
		if h.Pattern == "chart" {
			found = true
			if !strings.Contains(h.Suggestion, "timechart") {
				t.Errorf("expected timechart suggestion, got: %s", h.Suggestion)
			}
		}
	}
	if !found {
		t.Error("missing chart hint")
	}
}

func TestSplunkCompat_TimechartNotFlagged(t *testing.T) {
	// "| timechart" should NOT trigger a chart hint.
	hints := DetectCompatHints(`index=main | timechart count by host`)
	for _, h := range hints {
		if h.Pattern == "chart" {
			t.Error("timechart should not trigger chart hint")
		}
	}
}

func TestSplunkCompat_SourceType(t *testing.T) {
	hints := DetectCompatHints(`index=main sourcetype=access_combined | stats count by host`)
	if len(hints) == 0 {
		t.Fatal("expected hint for access_combined")
	}
	found := false
	for _, h := range hints {
		if strings.Contains(h.Pattern, "access_combined") {
			found = true
			if !strings.Contains(h.Suggestion, "nginx") {
				t.Errorf("expected nginx suggestion, got: %s", h.Suggestion)
			}
		}
	}
	if !found {
		t.Error("missing sourcetype hint")
	}
}

func TestSplunkCompat_NoHints(t *testing.T) {
	hints := DetectCompatHints(`FROM main | stats count by source`)
	if len(hints) != 0 {
		t.Errorf("expected no hints for native LynxDB query, got %d", len(hints))
	}
}

func TestFormatCompatHints(t *testing.T) {
	hints := []CompatHint{
		{Pattern: "lookup", Suggestion: "lookup is not yet supported.", Unsupported: true},
		{Pattern: "chart", Suggestion: "Try timechart instead."},
	}
	formatted := FormatCompatHints(hints)
	if !strings.Contains(formatted, "Warning:") || !strings.Contains(formatted, "lookup") {
		t.Errorf("expected Warning with lookup hint content, got:\n%s", formatted)
	}
	if !strings.Contains(formatted, "Info:") || !strings.Contains(formatted, "timechart") {
		t.Errorf("expected Info with timechart hint content, got:\n%s", formatted)
	}
}

func TestFormatCompatHints_Empty(t *testing.T) {
	formatted := FormatCompatHints(nil)
	if formatted != "" {
		t.Errorf("expected empty string, got: %s", formatted)
	}
}

func TestSplunkCompat_EarliestLatest(t *testing.T) {
	hints := DetectCompatHints(`earliest=-1h latest=now level=error | stats count`)
	found := false
	for _, h := range hints {
		if h.Pattern == "earliest=/latest=" {
			found = true
			if !strings.Contains(h.Suggestion, "--since") || !strings.Contains(h.Suggestion, "--from/--to") {
				t.Errorf("expected CLI flag suggestion, got: %s", h.Suggestion)
			}
		}
	}
	if !found {
		t.Error("missing earliest/latest hint")
	}
}

func TestSplunkCompat_EarliestOnly(t *testing.T) {
	hints := DetectCompatHints(`earliest=-24h level=error`)
	found := false
	for _, h := range hints {
		if h.Pattern == "earliest=/latest=" {
			found = true
		}
	}
	if !found {
		t.Error("missing earliest= hint when only earliest is present")
	}
}

func TestSplunkCompat_Bucket(t *testing.T) {
	// "bucket" is a valid Lynx Flow command — should NOT produce a compat hint.
	hints := DetectCompatHints(`index=main | bucket _time span=5m`)
	for _, h := range hints {
		if h.Pattern == "bucket" {
			t.Errorf("bucket is a valid Lynx Flow command, should not produce compat hint, got: %s", h.Suggestion)
		}
	}
}

func TestSplunkCompat_Makemv(t *testing.T) {
	hints := DetectCompatHints(`index=main | makemv delim="," field`)
	found := false
	for _, h := range hints {
		if h.Pattern == "makemv" && h.Unsupported {
			found = true
			if !strings.Contains(h.Suggestion, "mvappend") {
				t.Errorf("expected mvappend suggestion, got: %s", h.Suggestion)
			}
		}
	}
	if !found {
		t.Error("missing makemv hint")
	}
}

func TestSplunkCompat_Sistats(t *testing.T) {
	hints := DetectCompatHints(`index=main | sistats count by host`)
	found := false
	for _, h := range hints {
		if h.Pattern == "sistats" && h.Unsupported {
			found = true
			if !strings.Contains(h.Suggestion, "stats") {
				t.Errorf("expected stats suggestion, got: %s", h.Suggestion)
			}
		}
	}
	if !found {
		t.Error("missing sistats hint")
	}
}

func TestSplunkCompat_IndexEquals(t *testing.T) {
	hints := DetectCompatHints(`index=nginx level=error | stats count`)
	found := false
	for _, h := range hints {
		if h.Pattern == "index=" {
			found = true
			if !strings.Contains(h.Suggestion, "_source") {
				t.Errorf("expected _source mapping info, got: %s", h.Suggestion)
			}
			if !strings.Contains(h.Suggestion, "FROM") {
				t.Errorf("expected FROM suggestion, got: %s", h.Suggestion)
			}
		}
	}
	if !found {
		t.Error("missing index= compat hint")
	}
}

func TestSplunkCompat_IndexInternal(t *testing.T) {
	hints := DetectCompatHints(`index=_internal | stats count by component`)
	found := false
	for _, h := range hints {
		if h.Pattern == "index=_internal" {
			found = true
			if !strings.Contains(h.Suggestion, "does not have") {
				t.Errorf("expected 'does not have' message, got: %s", h.Suggestion)
			}
		}
	}
	if !found {
		t.Error("missing index=_internal hint")
	}
}

func TestSplunkCompat_IndexAudit(t *testing.T) {
	hints := DetectCompatHints(`index=_audit action=login | stats count by user`)
	found := false
	for _, h := range hints {
		if h.Pattern == "index=_audit" {
			found = true
		}
	}
	if !found {
		t.Error("missing index=_audit hint")
	}
}

func TestSplunkCompat_NoIndexHintForSource(t *testing.T) {
	// source= should NOT trigger the index= hint.
	hints := DetectCompatHints(`source=nginx level=error | stats count`)
	for _, h := range hints {
		if h.Pattern == "index=" {
			t.Error("source= should not trigger index= hint")
		}
	}
}

func TestDetectScopeHint_SimpleSearch(t *testing.T) {
	// Simple keyword search with many sources — should suggest narrowing.
	hint := DetectScopeHint("connection refused", 10)
	if hint == nil {
		t.Fatal("expected scope hint for simple search with many sources")
	}
	if !strings.Contains(hint.Suggestion, "source=nginx") {
		t.Errorf("expected source=nginx in suggestion, got: %s", hint.Suggestion)
	}
}

func TestDetectScopeHint_FewSources(t *testing.T) {
	// Few sources — no hint needed.
	hint := DetectScopeHint("connection refused", 3)
	if hint != nil {
		t.Error("expected no hint with few sources")
	}
}

func TestDetectScopeHint_HasSource(t *testing.T) {
	// Already has source= — no hint.
	hint := DetectScopeHint("source=nginx connection refused", 10)
	if hint != nil {
		t.Error("expected no hint when source= is present")
	}
}

func TestDetectScopeHint_HasIndex(t *testing.T) {
	// Already has index= — no hint.
	hint := DetectScopeHint("index=nginx error", 10)
	if hint != nil {
		t.Error("expected no hint when index= is present")
	}
}

func TestDetectScopeHint_HasFrom(t *testing.T) {
	// Already has FROM — no hint.
	hint := DetectScopeHint("FROM nginx | search error", 10)
	if hint != nil {
		t.Error("expected no hint when FROM is present")
	}
}

func TestDetectScopeHint_HasPipe(t *testing.T) {
	// Has a pipe (not a simple keyword search) — no hint.
	hint := DetectScopeHint("error | stats count", 10)
	if hint != nil {
		t.Error("expected no hint when query has a pipe")
	}
}

// =============================================================================
// DetectLynxFlowHints — cross-syntax migration hints (SPL2 → Lynx Flow)
// =============================================================================

func TestDetectLynxFlowHints_Eval(t *testing.T) {
	hints := DetectLynxFlowHints([]Command{&EvalCommand{Field: "x"}})
	if len(hints) != 1 || hints[0].Pattern != "eval" {
		t.Fatalf("expected eval hint, got %v", hints)
	}
	if !strings.Contains(hints[0].Suggestion, "let") {
		t.Errorf("expected 'let' suggestion, got: %s", hints[0].Suggestion)
	}
}

func TestDetectLynxFlowHints_Stats(t *testing.T) {
	hints := DetectLynxFlowHints([]Command{&StatsCommand{}})
	if len(hints) != 1 || hints[0].Pattern != "stats" {
		t.Fatalf("expected stats hint, got %v", hints)
	}
	if !strings.Contains(hints[0].Suggestion, "group") {
		t.Errorf("expected 'group' suggestion, got: %s", hints[0].Suggestion)
	}
}

func TestDetectLynxFlowHints_FieldsInclude(t *testing.T) {
	hints := DetectLynxFlowHints([]Command{&FieldsCommand{Remove: false}})
	if len(hints) != 1 || hints[0].Pattern != "fields" {
		t.Fatalf("expected fields hint, got %v", hints)
	}
	if !strings.Contains(hints[0].Suggestion, "keep") {
		t.Errorf("expected 'keep' suggestion, got: %s", hints[0].Suggestion)
	}
}

func TestDetectLynxFlowHints_FieldsExclude(t *testing.T) {
	hints := DetectLynxFlowHints([]Command{&FieldsCommand{Remove: true}})
	if len(hints) != 1 || hints[0].Pattern != "fields -" {
		t.Fatalf("expected 'fields -' hint, got %v", hints)
	}
	if !strings.Contains(hints[0].Suggestion, "omit") {
		t.Errorf("expected 'omit' suggestion, got: %s", hints[0].Suggestion)
	}
}

func TestDetectLynxFlowHints_Streamstats(t *testing.T) {
	hints := DetectLynxFlowHints([]Command{&StreamstatsCommand{}})
	if len(hints) != 1 || hints[0].Pattern != "streamstats" {
		t.Fatalf("expected streamstats hint, got %v", hints)
	}
	if !strings.Contains(hints[0].Suggestion, "running") {
		t.Errorf("expected 'running' suggestion, got: %s", hints[0].Suggestion)
	}
}

func TestDetectLynxFlowHints_Eventstats(t *testing.T) {
	hints := DetectLynxFlowHints([]Command{&EventstatsCommand{}})
	if len(hints) != 1 || hints[0].Pattern != "eventstats" {
		t.Fatalf("expected eventstats hint, got %v", hints)
	}
	if !strings.Contains(hints[0].Suggestion, "enrich") {
		t.Errorf("expected 'enrich' suggestion, got: %s", hints[0].Suggestion)
	}
}

func TestDetectLynxFlowHints_Rex(t *testing.T) {
	hints := DetectLynxFlowHints([]Command{&RexCommand{Field: "_raw", Pattern: "test"}})
	if len(hints) != 1 || hints[0].Pattern != "rex" {
		t.Fatalf("expected rex hint, got %v", hints)
	}
	if !strings.Contains(hints[0].Suggestion, "parse regex") {
		t.Errorf("expected 'parse regex' suggestion, got: %s", hints[0].Suggestion)
	}
}

func TestDetectLynxFlowHints_UnpackJSON(t *testing.T) {
	hints := DetectLynxFlowHints([]Command{&UnpackCommand{Format: "json"}})
	if len(hints) != 1 || hints[0].Pattern != "unpack_json" {
		t.Fatalf("expected unpack_json hint, got %v", hints)
	}
	if !strings.Contains(hints[0].Suggestion, "parse json") {
		t.Errorf("expected 'parse json' suggestion, got: %s", hints[0].Suggestion)
	}
}

func TestDetectLynxFlowHints_Head(t *testing.T) {
	hints := DetectLynxFlowHints([]Command{&HeadCommand{Count: 10}})
	if len(hints) != 1 || hints[0].Pattern != "head" {
		t.Fatalf("expected head hint, got %v", hints)
	}
	if !strings.Contains(hints[0].Suggestion, "take") {
		t.Errorf("expected 'take' suggestion, got: %s", hints[0].Suggestion)
	}
}

func TestDetectLynxFlowHints_Bin(t *testing.T) {
	hints := DetectLynxFlowHints([]Command{&BinCommand{Field: "_time", Span: "5m"}})
	if len(hints) != 1 || hints[0].Pattern != "bin" {
		t.Fatalf("expected bin hint, got %v", hints)
	}
	if !strings.Contains(hints[0].Suggestion, "bucket") {
		t.Errorf("expected 'bucket' suggestion, got: %s", hints[0].Suggestion)
	}
}

func TestDetectLynxFlowHints_Timechart(t *testing.T) {
	hints := DetectLynxFlowHints([]Command{&TimechartCommand{Span: "5m"}})
	if len(hints) != 1 || hints[0].Pattern != "timechart" {
		t.Fatalf("expected timechart hint, got %v", hints)
	}
	if !strings.Contains(hints[0].Suggestion, "every") {
		t.Errorf("expected 'every' suggestion, got: %s", hints[0].Suggestion)
	}
}

func TestDetectLynxFlowHints_Sort(t *testing.T) {
	hints := DetectLynxFlowHints([]Command{&SortCommand{}})
	if len(hints) != 1 || hints[0].Pattern != "sort" {
		t.Fatalf("expected sort hint, got %v", hints)
	}
	if !strings.Contains(hints[0].Suggestion, "order by") {
		t.Errorf("expected 'order by' suggestion, got: %s", hints[0].Suggestion)
	}
}

func TestDetectLynxFlowHints_Unroll(t *testing.T) {
	hints := DetectLynxFlowHints([]Command{&UnrollCommand{Field: "tags"}})
	if len(hints) != 1 || hints[0].Pattern != "unroll" {
		t.Fatalf("expected unroll hint, got %v", hints)
	}
	if !strings.Contains(hints[0].Suggestion, "explode") {
		t.Errorf("expected 'explode' suggestion, got: %s", hints[0].Suggestion)
	}
}

func TestDetectLynxFlowHints_PackJson(t *testing.T) {
	hints := DetectLynxFlowHints([]Command{&PackJsonCommand{Target: "out"}})
	if len(hints) != 1 || hints[0].Pattern != "pack_json" {
		t.Fatalf("expected pack_json hint, got %v", hints)
	}
	if !strings.Contains(hints[0].Suggestion, "pack") {
		t.Errorf("expected 'pack' suggestion, got: %s", hints[0].Suggestion)
	}
}

func TestDetectLynxFlowHints_NoDuplicates(t *testing.T) {
	// Multiple commands of same type should produce only one hint.
	hints := DetectLynxFlowHints([]Command{
		&EvalCommand{Field: "a"},
		&EvalCommand{Field: "b"},
	})
	count := 0
	for _, h := range hints {
		if h.Pattern == "eval" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected 1 eval hint, got %d", count)
	}
}

func TestDetectLynxFlowHints_Empty(t *testing.T) {
	hints := DetectLynxFlowHints(nil)
	if len(hints) != 0 {
		t.Errorf("expected no hints for nil commands, got %d", len(hints))
	}
}

func TestDetectLynxFlowHints_NoHintForLynxFlowCmds(t *testing.T) {
	// SelectCommand is Lynx Flow native — should produce no hint.
	hints := DetectLynxFlowHints([]Command{&SelectCommand{
		Columns: []SelectColumn{{Name: "f1"}},
	}})
	if len(hints) != 0 {
		t.Errorf("expected no hints for Lynx Flow-native SelectCommand, got %d", len(hints))
	}
}

func TestDetectLynxFlowHints_Multi(t *testing.T) {
	// A pipeline mixing multiple SPL2 commands should produce multiple hints.
	hints := DetectLynxFlowHints([]Command{
		&EvalCommand{Field: "x"},
		&StatsCommand{},
		&HeadCommand{Count: 10},
	})
	if len(hints) != 3 {
		t.Fatalf("expected 3 hints, got %d", len(hints))
	}
	patterns := make(map[string]bool)
	for _, h := range hints {
		patterns[h.Pattern] = true
	}
	for _, want := range []string{"eval", "stats", "head"} {
		if !patterns[want] {
			t.Errorf("missing hint for %q", want)
		}
	}
}
