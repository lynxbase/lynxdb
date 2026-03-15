package spl2

import (
	"testing"
)

func TestExtractQueryHints_SearchTerms(t *testing.T) {
	prog, err := ParseProgram(`FROM main | search "error timeout"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)

	if hints.IndexName != "main" {
		t.Errorf("IndexName: got %q, want %q", hints.IndexName, "main")
	}
	if len(hints.SearchTerms) == 0 {
		t.Fatal("expected search terms")
	}
	// "error timeout" tokenizes to ["error", "timeout"]
	found := map[string]bool{}
	for _, term := range hints.SearchTerms {
		found[term] = true
	}
	if !found["error"] {
		t.Error("missing search term 'error'")
	}
	if !found["timeout"] {
		t.Error("missing search term 'timeout'")
	}
}

func TestExtractQueryHints_IndexFromSearch(t *testing.T) {
	prog, err := ParseProgram(`FROM main | search index=security "error"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if hints.IndexName != "security" {
		t.Errorf("IndexName: got %q, want %q", hints.IndexName, "security")
	}
}

func TestExtractQueryHints_HeadLimit(t *testing.T) {
	prog, err := ParseProgram(`FROM main | search "error" | head 10`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if hints.Limit != 10 {
		t.Errorf("Limit: got %d, want 10", hints.Limit)
	}
}

func TestExtractQueryHints_NoLimit(t *testing.T) {
	prog, err := ParseProgram(`FROM main | search "error"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if hints.Limit != 0 {
		t.Errorf("Limit: got %d, want 0", hints.Limit)
	}
}

func TestExtractQueryHints_WherePredicate(t *testing.T) {
	prog, err := ParseProgram(`FROM main | where status >= 500`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if len(hints.FieldPredicates) != 1 {
		t.Fatalf("FieldPredicates: got %d, want 1", len(hints.FieldPredicates))
	}
	fp := hints.FieldPredicates[0]
	if fp.Field != "status" || fp.Op != ">=" || fp.Value != "500" {
		t.Errorf("FieldPredicate: got %+v, want {status >= 500}", fp)
	}
}

func TestExtractQueryHints_WhereTimeBounds(t *testing.T) {
	prog, err := ParseProgram(`FROM main | where _time >= 1704067200 | where _time <= 1704153600`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if hints.TimeBounds == nil {
		t.Fatal("expected TimeBounds")
	}
	if hints.TimeBounds.Earliest.IsZero() {
		t.Fatal("Earliest should not be zero")
	}
	if hints.TimeBounds.Earliest.Unix() != 1704067200 {
		t.Errorf("Earliest Unix: got %d, want 1704067200", hints.TimeBounds.Earliest.Unix())
	}
	if hints.TimeBounds.Latest.IsZero() {
		t.Fatal("Latest should not be zero")
	}
	if hints.TimeBounds.Latest.Unix() != 1704153600 {
		t.Errorf("Latest Unix: got %d, want 1704153600", hints.TimeBounds.Latest.Unix())
	}
}

func TestExtractQueryHints_RequiredCols(t *testing.T) {
	prog, err := ParseProgram(`FROM main | search "error" | where status >= 500 | fields host, status`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	colSet := map[string]bool{}
	for _, c := range hints.RequiredCols {
		colSet[c] = true
	}
	// Should contain at least _time, _raw (from search), status, host
	for _, expected := range []string{"_time", "_raw", "status", "host"} {
		if !colSet[expected] {
			t.Errorf("missing required column %q", expected)
		}
	}
}

func TestExtractQueryHints_WhereAnd(t *testing.T) {
	prog, err := ParseProgram(`FROM main | where status >= 500 and host = "web-01"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if len(hints.FieldPredicates) != 2 {
		t.Fatalf("FieldPredicates: got %d, want 2", len(hints.FieldPredicates))
	}
}

func TestExtractQueryHints_WhereOr(t *testing.T) {
	// OR predicates should NOT be extracted (not safe to push down).
	prog, err := ParseProgram(`FROM main | where status = 500 or status = 503`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if len(hints.FieldPredicates) != 0 {
		t.Errorf("FieldPredicates: got %d, want 0 (OR not safe to push down)", len(hints.FieldPredicates))
	}
}

func TestExtractQueryHints_WhereWildcard(t *testing.T) {
	// Wildcard patterns in WHERE (e.g., message="*ssh*") are compiled to
	// OpStrMatch (regex) by the VM. They must NOT be extracted as literal
	// FieldPredicates because the segment reader uses exact string comparison,
	// which would compare val == "*ssh*" (always false) and drop all events.
	prog, err := ParseProgram(`FROM main | where message="*ssh*"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if len(hints.FieldPredicates) != 0 {
		t.Errorf("FieldPredicates: got %d, want 0 (wildcard should not be pushed down)", len(hints.FieldPredicates))
	}
}

func TestExtractQueryHints_WhereWildcardQuestion(t *testing.T) {
	prog, err := ParseProgram(`FROM main | where host="web-??"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if len(hints.FieldPredicates) != 0 {
		t.Errorf("FieldPredicates: got %d, want 0 (? wildcard should not be pushed down)", len(hints.FieldPredicates))
	}
}

func TestExtractQueryHints_WhereNoWildcard(t *testing.T) {
	// Non-wildcard string equality should still be pushed down.
	prog, err := ParseProgram(`FROM main | where source="nginx"`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if len(hints.FieldPredicates) != 1 {
		t.Fatalf("FieldPredicates: got %d, want 1", len(hints.FieldPredicates))
	}
	if hints.FieldPredicates[0].Value != "nginx" {
		t.Errorf("value: got %q, want %q", hints.FieldPredicates[0].Value, "nginx")
	}
}

func TestExtractQueryHints_NilProgram(t *testing.T) {
	hints := ExtractQueryHints(nil)
	if hints == nil {
		t.Fatal("hints should not be nil")
	}
	if len(hints.SearchTerms) != 0 {
		t.Errorf("SearchTerms: got %v, want empty", hints.SearchTerms)
	}
}

func TestIsStreamable(t *testing.T) {
	tests := []struct {
		query        string
		wantStream   bool
		wantTerminal string
	}{
		{`FROM main | search "error"`, true, "search"},
		{`FROM main | search "error" | stats count`, true, "stats"},
		{`FROM main | search "error" | head 10`, true, "head"},
		{`FROM main | search "error" | sort -_time`, false, "sort"},
		{`FROM main | search "error" | tail 10`, false, "tail"},
		{`FROM main | stats count by source`, true, "stats"},
		{`FROM main | where status >= 500`, true, "where"},
		{`FROM main | search "error" | dedup source`, true, "dedup"},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			prog, err := ParseProgram(tt.query)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			hints := ExtractQueryHints(prog)
			if hints.IsStreamable() != tt.wantStream {
				t.Errorf("IsStreamable: got %v, want %v", hints.IsStreamable(), tt.wantStream)
			}
			if hints.TerminalCommand() != tt.wantTerminal {
				t.Errorf("TerminalCommand: got %q, want %q", hints.TerminalCommand(), tt.wantTerminal)
			}
		})
	}
}

func TestCanPushdownToReader(t *testing.T) {
	tests := []struct {
		query    string
		wantPush bool
		wantLits int // expected number of pre-filter literals
	}{
		{`FROM main | search "*/user_*"`, true, 1},
		{`FROM main | search "error"`, true, 1},
		{`FROM main | search "error" "timeout"`, true, 2},               // AND → pushdown safe
		{`FROM main | search "err" OR "warn"`, false, 0},                // OR → not safe
		{`FROM main | search NOT "error"`, false, 0},                    // NOT → not safe
		{`FROM main | stats count`, false, 0},                           // no search → not safe
		{`FROM main | where status >= 500`, false, 0},                   // field predicate → not safe
		{`FROM main | search "error" | stats count by source`, true, 1}, // search then stats → safe
		{`FROM main | search status=500`, false, 0},                     // field comparison → not safe
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			prog, err := ParseProgram(tt.query)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			hints := ExtractQueryHints(prog)
			gotPush := hints.CanPushdownToReader()
			if gotPush != tt.wantPush {
				t.Errorf("CanPushdownToReader: got %v, want %v", gotPush, tt.wantPush)
			}
			if gotPush {
				lits := hints.CollectPreFilterBytes()
				if len(lits) != tt.wantLits {
					t.Errorf("CollectPreFilterBytes: got %d literals, want %d", len(lits), tt.wantLits)
				}
			}
		})
	}
}

func TestExtractQueryHints_TailLimit(t *testing.T) {
	prog, err := ParseProgram(`FROM main | search "error" | tail 5`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if hints.TailLimit != 5 {
		t.Errorf("TailLimit: got %d, want 5", hints.TailLimit)
	}
	// Limit (head) should be 0 — tail does not set Limit.
	if hints.Limit != 0 {
		t.Errorf("Limit: got %d, want 0 (tail should not set Limit)", hints.Limit)
	}
}

func TestExtractQueryHints_TailLimitNotTerminal(t *testing.T) {
	// tail followed by another command → TailLimit should be 0.
	prog, err := ParseProgram(`FROM main | tail 5 | stats count`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if hints.TailLimit != 0 {
		t.Errorf("TailLimit: got %d, want 0 (tail is not terminal)", hints.TailLimit)
	}
}

func TestExtractQueryHints_ReverseScanFromAnnotation(t *testing.T) {
	// Manually set the tailScanOptimization annotation to simulate the optimizer.
	q := &Query{
		Source:   &SourceClause{Index: "main"},
		Commands: []Command{&TailCommand{Count: 3}},
	}
	q.Annotate("tailScanOptimization", 3)

	prog := &Program{Main: q}
	hints := ExtractQueryHints(prog)

	if hints.TailLimit != 3 {
		t.Errorf("TailLimit: got %d, want 3", hints.TailLimit)
	}
	if !hints.ReverseScan {
		t.Error("ReverseScan should be true when tailScanOptimization annotation is present")
	}
}

func TestExtractQueryHints_WhereIn(t *testing.T) {
	prog, err := ParseProgram(`FROM main | where status IN (200, 404, 500)`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if len(hints.InPredicates) != 1 {
		t.Fatalf("InPredicates: got %d, want 1", len(hints.InPredicates))
	}
	ip := hints.InPredicates[0]
	if ip.Field != "status" {
		t.Errorf("Field: got %q, want status", ip.Field)
	}
	if len(ip.Values) != 3 {
		t.Fatalf("Values: got %d, want 3", len(ip.Values))
	}
	wantValues := []string{"200", "404", "500"}
	for i, want := range wantValues {
		if ip.Values[i] != want {
			t.Errorf("Values[%d]: got %q, want %q", i, ip.Values[i], want)
		}
	}
}

func TestExtractQueryHints_WhereNotIn(t *testing.T) {
	// NOT IN predicates should NOT be extracted (not safe to prune segments).
	prog, err := ParseProgram(`FROM main | where status NOT IN (200, 404)`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if len(hints.InPredicates) != 0 {
		t.Errorf("InPredicates: got %d, want 0 (NOT IN should not be extracted)", len(hints.InPredicates))
	}
}

func TestExtractQueryHints_WhereInAndFieldPred(t *testing.T) {
	// IN combined with a simple field predicate via AND.
	prog, err := ParseProgram(`FROM main | where source IN ("nginx", "redis") and status >= 500`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if len(hints.InPredicates) != 1 {
		t.Fatalf("InPredicates: got %d, want 1", len(hints.InPredicates))
	}
	if hints.InPredicates[0].Field != "source" {
		t.Errorf("InPredicates[0].Field: got %q, want source", hints.InPredicates[0].Field)
	}
	if len(hints.FieldPredicates) != 1 {
		t.Fatalf("FieldPredicates: got %d, want 1", len(hints.FieldPredicates))
	}
	if hints.FieldPredicates[0].Field != "status" {
		t.Errorf("FieldPredicates[0].Field: got %q, want status", hints.FieldPredicates[0].Field)
	}
}

func TestExtractQueryHints_WhereInGeneratedField(t *testing.T) {
	// IN predicate on a field created by EVAL — should be filtered out.
	prog, err := ParseProgram(`FROM main | eval category = "test" | where category IN ("a", "b")`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	hints := ExtractQueryHints(prog)
	if len(hints.InPredicates) != 0 {
		t.Errorf("InPredicates: got %d, want 0 (eval-generated field should be filtered)", len(hints.InPredicates))
	}
}

func TestCollectAllIndexNames(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string // expected index names (order-independent)
	}{
		{
			name:  "single index from FROM",
			query: `FROM main | search "error"`,
			want:  []string{"main"},
		},
		{
			name:  "APPEND with two different indexes",
			query: `FROM logs_a | search "*" | APPEND [FROM logs_b | search "*"]`,
			want:  []string{"logs_a", "logs_b"},
		},
		{
			name:  "JOIN with two different indexes",
			query: `FROM web | where status>=500 | JOIN type=inner client_ip [FROM threats | search "*"]`,
			want:  []string{"web", "threats"},
		},
		{
			name:  "SEARCH index= overrides FROM",
			query: `FROM main | search index=security "error"`,
			want:  []string{"main", "security"},
		},
		{
			name:  "single index (no duplication)",
			query: `FROM main | search "*" | APPEND [FROM main | search "error"]`,
			want:  []string{"main"},
		},
		{
			name:  "nil program",
			query: "", // will use nil Program
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var prog *Program
			if tt.query != "" {
				var err error
				prog, err = ParseProgram(tt.query)
				if err != nil {
					t.Fatalf("parse: %v", err)
				}
			}

			got := CollectAllIndexNames(prog)

			// Compare as sets.
			gotSet := make(map[string]bool, len(got))
			for _, name := range got {
				gotSet[name] = true
			}
			wantSet := make(map[string]bool, len(tt.want))
			for _, name := range tt.want {
				wantSet[name] = true
			}

			if len(gotSet) != len(wantSet) {
				t.Fatalf("CollectAllIndexNames: got %v, want %v", got, tt.want)
			}
			for name := range wantSet {
				if !gotSet[name] {
					t.Errorf("missing index name %q; got %v", name, got)
				}
			}
		})
	}
}

func TestCollectAllIndexNames_CTE(t *testing.T) {
	// Build a program with CTE datasets manually since CTE parsing
	// may vary. This tests the AST walk directly.
	prog := &Program{
		Datasets: []DatasetDef{
			{
				Name: "threats",
				Query: &Query{
					Source:   &SourceClause{Index: "idx_threats"},
					Commands: []Command{&SearchCommand{Term: "sqli"}},
				},
			},
		},
		Main: &Query{
			Source:   &SourceClause{Index: "idx_audit"},
			Commands: []Command{&SearchCommand{Term: "login"}},
		},
	}

	got := CollectAllIndexNames(prog)
	gotSet := make(map[string]bool, len(got))
	for _, name := range got {
		gotSet[name] = true
	}

	for _, want := range []string{"idx_threats", "idx_audit"} {
		if !gotSet[want] {
			t.Errorf("missing index %q; got %v", want, got)
		}
	}
	if len(gotSet) != 2 {
		t.Errorf("expected 2 indexes, got %v", got)
	}
}

func TestExtractQueryHints_UnpackPrefixFieldPredicatesExcluded(t *testing.T) {
	// Predicates on fields generated by UnpackCommand (with prefix) must NOT
	// be pushed to the segment reader — they don't exist in segment data.
	tests := []struct {
		name            string
		query           string
		wantFieldPreds  int
		wantInPreds     int
	}{
		{
			name:           "unpack prefix compare",
			query:          `FROM main | parse postgres(message) as pg | where pg.duration_ms > 0`,
			wantFieldPreds: 0,
			wantInPreds:    0,
		},
		{
			name:           "unpack prefix equality",
			query:          `FROM main | parse json(message) as j | where j.status = 500`,
			wantFieldPreds: 0,
			wantInPreds:    0,
		},
		{
			name:           "unpack prefix AND",
			query:          `FROM main | parse json(message) as j | where j.status >= 500 and j.host = "web-01"`,
			wantFieldPreds: 0,
			wantInPreds:    0,
		},
		{
			name:           "unpack prefix IN",
			query:          `FROM main | parse json(message) as j | where j.level IN ("ERROR", "WARN")`,
			wantFieldPreds: 0,
			wantInPreds:    0,
		},
		{
			name:           "unpack prefix mixed with segment field",
			query:          `FROM main | parse json(message) as j | where j.status >= 500 and status >= 200`,
			wantFieldPreds: 1, // only "status >= 200" should survive
			wantInPreds:    0,
		},
		{
			name:           "non-prefixed segment field not affected",
			query:          `FROM main | parse json(message) as j | where status >= 500`,
			wantFieldPreds: 1, // "status" is NOT unpack-generated
			wantInPreds:    0,
		},
		{
			name:           "unpack with explicit Fields — known names excluded",
			query:          `FROM main | parse json(message) as j | where j.status >= 500`,
			wantFieldPreds: 0,
			wantInPreds:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prog, err := ParseProgram(tt.query)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			hints := ExtractQueryHints(prog)
			if len(hints.FieldPredicates) != tt.wantFieldPreds {
				t.Errorf("FieldPredicates: got %d, want %d; preds=%+v",
					len(hints.FieldPredicates), tt.wantFieldPreds, hints.FieldPredicates)
			}
			if len(hints.InPredicates) != tt.wantInPreds {
				t.Errorf("InPredicates: got %d, want %d; preds=%+v",
					len(hints.InPredicates), tt.wantInPreds, hints.InPredicates)
			}
		})
	}
}
