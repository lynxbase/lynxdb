package optimizer

import (
	"reflect"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func TestRegexExtract_SimpleLiteral(t *testing.T) {
	// "(?P<user>\w+)@example.com" → ["@example", "com"]
	// The "." in regex breaks the literal, so it splits.
	result := extractRegexLiterals(`(?P<user>\w+)@example.com`)
	if len(result) == 0 {
		t.Fatal("expected at least one literal")
	}
	found := false
	for _, s := range result {
		if s == "@example" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected '@example' in literals, got %v", result)
	}
}

func TestRegexExtract_MultipleLiterals(t *testing.T) {
	// "error: (?P<msg>.+) at line" → ["error: ", " at line"]
	// "." breaks the run, so "error: " and " at line" are extracted separately.
	result := extractRegexLiterals(`error: (?P<msg>.+) at line`)
	if len(result) < 2 {
		t.Fatalf("expected at least 2 literals, got %v", result)
	}
	has := func(s string) bool {
		for _, r := range result {
			if r == s {
				return true
			}
		}

		return false
	}
	if !has("error: ") {
		t.Errorf("expected 'error: ' in %v", result)
	}
	if !has(" at line") {
		t.Errorf("expected ' at line' in %v", result)
	}
}

func TestRegexExtract_NoLiterals(t *testing.T) {
	result := extractRegexLiterals(`.*`)
	if len(result) != 0 {
		t.Errorf("expected no literals, got %v", result)
	}
}

func TestRegexExtract_ShortLiterals(t *testing.T) {
	// Literals shorter than 3 chars are excluded.
	result := extractRegexLiterals(`a.b`)
	if len(result) != 0 {
		t.Errorf("expected no literals (both are <3 chars), got %v", result)
	}
}

func TestRegexExtract_EscapedChars(t *testing.T) {
	// Escaped characters like \/ should be treated as literal.
	result := extractRegexLiterals(`\/archives\/edgar\/data\/`)
	if len(result) == 0 {
		t.Fatal("expected literals from escaped path")
	}
	// Should extract "/archives/edgar/data/" or segments thereof
	found := false
	for _, s := range result {
		if len(s) >= 3 {
			found = true
		}
	}
	if !found {
		t.Errorf("expected at least one literal >= 3 chars, got %v", result)
	}
}

func TestRegexLiteralExtractionRule_Annotates(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.RexCommand{
				Pattern: `(?P<user>\w+)@example.com`,
			},
		},
	}
	rule := &regexLiteralExtractionRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("rule should have fired")
	}
	ann, ok := result.GetAnnotation("rexPreFilter")
	if !ok {
		t.Fatal("rexPreFilter annotation not set")
	}
	lits, ok := ann.([]string)
	if !ok {
		t.Fatal("rexPreFilter should be []string")
	}
	if len(lits) == 0 {
		t.Error("expected at least one pre-filter literal")
	}
}

func TestRegexLiteralExtractionRule_NoRex(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "x"},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "5"},
				},
			},
		},
	}
	rule := &regexLiteralExtractionRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("rule should not fire when no rex commands")
	}
}

func TestRegexExtract_MetaclassBreaks(t *testing.T) {
	// \d and \w break literal runs
	result := extractRegexLiterals(`abc\ddef\wghi`)
	want := []string{"abc", "def", "ghi"}
	if !reflect.DeepEqual(result, want) {
		t.Errorf("got %v, want %v", result, want)
	}
}
