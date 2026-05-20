package main

import (
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestExplainRenderer_FullScanHint(t *testing.T) {
	out := renderExplainReportString(explainFixture(func(parsed *client.ExplainParsed) {
		parsed.UsesFullScan = true
		parsed.HasTimeBounds = false
	}), explainTestOptions())

	if !strings.Contains(out, "Add a time range") {
		t.Fatalf("expected time range hint, got:\n%s", out)
	}
}

func TestExplainRenderer_TopKPhysicalStrategy(t *testing.T) {
	out := renderExplainReportString(explainFixture(func(parsed *client.ExplainParsed) {
		parsed.PhysicalPlan = &client.ExplainPhysicalPlan{TopKAgg: true, TopK: 10}
	}), explainTestOptions())

	if !strings.Contains(out, "TopK heap optimization (10)") {
		t.Fatalf("expected TopK strategy, got:\n%s", out)
	}
}

func TestExplainRenderer_RangePredicatesAndSourceScope(t *testing.T) {
	out := renderExplainReportString(explainFixture(func(parsed *client.ExplainParsed) {
		parsed.SourceScope = &client.ExplainSourceScope{
			Type:                  "glob",
			Pattern:               "app-*",
			TotalSourcesAvailable: 4,
		}
		parsed.RangePredicates = []client.ExplainRangePredicate{{
			Field:            "duration_ms",
			Min:              "100",
			RGFilterStrategy: "zone-map",
			RowVMStrategy:    "per-row",
		}}
	}), explainTestOptions())

	for _, want := range []string{"app-* (4 sources available)", "Range predicates", "duration_ms: min=100"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %q, got:\n%s", want, out)
		}
	}
}

func TestExplainRenderer_PlainModeHasNoANSI(t *testing.T) {
	out := renderExplainReportString(explainFixture(nil), explainTestOptions())

	if strings.Contains(out, "\x1b[") {
		t.Fatalf("plain explain output contains ANSI: %q", out)
	}
	if strings.Contains(out, "•") || strings.Contains(out, "└") || strings.Contains(out, "→") {
		t.Fatalf("plain explain output contains non-ascii decorations: %q", out)
	}
}

func TestExplainRenderer_UnknownSourceSuppressesCatalogFields(t *testing.T) {
	out := renderExplainReportString(explainFixture(func(parsed *client.ExplainParsed) {
		parsed.Pipeline[0] = client.ExplainStage{
			Command:       "source",
			Description:   "from app-json",
			FieldsAdded:   []string{"_time", "_raw", "_source", "_sourcetype", "message", "level", "114Z"},
			FieldsOut:     []string{"_time", "_raw", "_source", "_sourcetype", "message", "level", "114Z"},
			FieldsUnknown: true,
		}
	}), explainTestOptions())

	if strings.Contains(out, "114Z") {
		t.Fatalf("unknown source stage leaked catalog field 114Z:\n%s", out)
	}
	if !strings.Contains(out, "fields: schema-on-read, not fully known") {
		t.Fatalf("expected schema-on-read summary, got:\n%s", out)
	}
}

func TestExplainRenderer_LargeRemovedFieldSetIsSummarized(t *testing.T) {
	out := renderExplainReportString(explainFixture(func(parsed *client.ExplainParsed) {
		removed := []string{"_time", "_raw", "_source", "_sourcetype"}
		for i := 0; i < 32; i++ {
			removed = append(removed, "field_"+string(rune('a'+i%26)))
		}
		removed = append(removed, "114Z")
		parsed.Pipeline[1].FieldsRemoved = removed
	}), explainTestOptions())

	if strings.Contains(out, "114Z") {
		t.Fatalf("large removed field set leaked catalog field 114Z:\n%s", out)
	}
	if !strings.Contains(out, "previous schema-on-read field set") {
		t.Fatalf("expected removed-field summary, got:\n%s", out)
	}
}

func TestExplainRenderer_InvalidQueryDiagnostics(t *testing.T) {
	out := renderExplainReportString(&client.ExplainResult{
		IsValid: false,
		Errors: []client.ExplainError{{
			Message:    "expected command after pipe",
			Suggestion: "Add a command after '|'.",
		}},
	}, explainTestOptions())

	for _, want := range []string{"Diagnostics", "expected command after pipe", "Add a command after '|'."} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected %q, got:\n%s", want, out)
		}
	}
}

func explainFixture(mutate func(*client.ExplainParsed)) *client.ExplainResult {
	parsed := &client.ExplainParsed{
		Pipeline: []client.ExplainStage{
			{
				Command:       "search",
				Description:   "level=error",
				FieldsOut:     []string{"_time", "_raw", "level"},
				FieldsUnknown: true,
			},
			{
				Command:     "stats",
				Description: "stats count by host",
				FieldsAdded: []string{"count"},
				FieldsOut:   []string{"host", "count"},
			},
		},
		ResultType:     "aggregate",
		EstimatedCost:  "medium",
		UsesFullScan:   false,
		FieldsRead:     []string{"level", "host"},
		SearchTerms:    []string{"error"},
		HasTimeBounds:  true,
		OptimizerRules: []client.ExplainRuleDetail{{Name: "ProjectionPushdown", Count: 1}},
		TotalRules:     12,
		PhysicalPlan:   &client.ExplainPhysicalPlan{PartialAgg: true},
		OptimizerStats: map[string]int{"ProjectionPushdown": 1},
		OptimizerMessages: []string{
			"pushed projection into scan",
		},
	}
	if mutate != nil {
		mutate(parsed)
	}

	return &client.ExplainResult{
		IsValid: true,
		Parsed:  parsed,
		Errors:  []client.ExplainError{},
	}
}

func explainTestOptions() explainReportOptions {
	return explainReportOptions{
		Plain: true,
		Theme: ui.NewTheme(nil, true),
		Width: 100,
	}
}
