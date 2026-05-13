package spl2

import (
	"strings"
	"testing"
)

func TestPrepareQueryLints_AnnotatesAndSorts(t *testing.T) {
	lints := []QueryLint{
		{Code: LintDoubleQuotedName, Message: "canon", Position: 2},
		{Code: LintLeadingWildcard, Message: "slow", Position: 10},
		{Code: LintRawExactCompare, Message: "raw", Position: 1},
		{Code: LintDefaultSource, Message: "default", Position: 0},
	}

	got := PrepareQueryLints(lints)
	wantCodes := []string{
		LintRawExactCompare,
		LintLeadingWildcard,
		LintDefaultSource,
		LintDoubleQuotedName,
	}
	if len(got) != len(wantCodes) {
		t.Fatalf("len(PrepareQueryLints) = %d, want %d", len(got), len(wantCodes))
	}
	for i, want := range wantCodes {
		if got[i].Code != want {
			t.Fatalf("got[%d].Code = %q, want %q; lints=%+v", i, got[i].Code, want, got)
		}
		if got[i].Reason == "" {
			t.Fatalf("got[%d].Reason is empty: %+v", i, got[i])
		}
		if got[i].Severity == "" {
			t.Fatalf("got[%d].Severity is empty: %+v", i, got[i])
		}
	}
	if got[0].Severity != LintSeverityWarning || got[0].Reason != "canon" {
		t.Fatalf("first lint annotation = severity %q reason %q, want warning/canon", got[0].Severity, got[0].Reason)
	}
	if lints[0].Reason != "" || lints[0].Severity != "" {
		t.Fatalf("PrepareQueryLints mutated input: %+v", lints[0])
	}
}

func TestSuggestionsFromLints_ShortcutSuggestions(t *testing.T) {
	lints := PrepareQueryLints([]QueryLint{
		{
			Code:     LintShortcutAvailable,
			Message:  "Equivalent: `errors by service` (shorter by 8 tokens)",
			Position: 0,
		},
		{
			Code:     LintLeadingWildcard,
			Message:  "Leading wildcard slows the query; consider an anchor",
			Position: 1,
		},
	})

	got := SuggestionsFromLints(lints)
	if len(got) != 1 {
		t.Fatalf("len(SuggestionsFromLints) = %d, want 1: %+v", len(got), got)
	}
	if got[0].Text != "errors by service" {
		t.Fatalf("suggestion text = %q, want errors by service", got[0].Text)
	}
	if got[0].Reason != "shortcut" || got[0].SourceCode != LintShortcutAvailable {
		t.Fatalf("suggestion metadata = %+v, want shortcut/%s", got[0], LintShortcutAvailable)
	}
}

func TestLintQuery_CountWithoutParens(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "stats count",
			query:     `from app | stats count by service`,
			wantCodes: []string{LintCountWithoutParens},
		},
		{
			name:      "timechart count",
			query:     `from app | timechart span=5m count by service`,
			wantCodes: []string{LintCountWithoutParens},
		},
		{
			name:      "streamstats count with options",
			query:     `from app | streamstats current=false window=5 count as n`,
			wantCodes: []string{LintCountWithoutParens},
		},
		{
			name:      "running count",
			query:     `from app | running count as n`,
			wantCodes: []string{LintCountWithoutParens},
		},
		{
			name:      "count with parens",
			query:     `from app | stats count() by count`,
			wantCodes: nil,
		},
		{
			name:      "count alias",
			query:     `from app | timechart span=1m count() as count`,
			wantCodes: nil,
		},
		{
			name:      "group by count field",
			query:     `from app | stats avg(duration_ms) by count`,
			wantCodes: nil,
		},
		{
			name:      "non aggregation command",
			query:     `from app | where count > 0`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
				if lints[i].Position < 0 {
					t.Fatalf("lints[%d].Position: got %d, want non-negative", i, lints[i].Position)
				}
			}
		})
	}
}

func TestLintQuery_DefaultSource(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "leading pipeline",
			query:     `| head 1`,
			wantCodes: []string{LintDefaultSource},
		},
		{
			name:      "bare command",
			query:     `head 1`,
			wantCodes: []string{LintDefaultSource},
		},
		{
			name:      "explicit source",
			query:     `from app | head 1`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintQuery_LeadingWildcard(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "search keyword",
			query:     `from app | search "*error"`,
			wantCodes: []string{LintLeadingWildcard},
		},
		{
			name:      "search comparison",
			query:     `from app | search host=*web`,
			wantCodes: []string{LintLeadingWildcard},
		},
		{
			name:      "search in list",
			query:     `from app | search host IN (api, *web)`,
			wantCodes: []string{LintLeadingWildcard},
		},
		{
			name:      "where like glob-style",
			query:     `from app | where host like "*web"`,
			wantCodes: []string{LintLeadingWildcard},
		},
		{
			name:      "where like sql-style",
			query:     `from app | where host like "%web"`,
			wantCodes: []string{LintLeadingWildcard},
		},
		{
			name:      "search like sql-style",
			query:     `from app | search host LIKE "%web"`,
			wantCodes: []string{LintLeadingWildcard},
		},
		{
			name:      "anchored wildcard",
			query:     `from app | search host=web* | where path like "api*"`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintQuery_IndexRewrite(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "bare index equals",
			query:     `index=main error`,
			wantCodes: []string{LintIndexRewrite},
		},
		{
			name:      "quoted index equals",
			query:     `index="main" error`,
			wantCodes: []string{LintIndexRewrite, LintDoubleQuotedName},
		},
		{
			name:      "index space source",
			query:     `index main | search error`,
			wantCodes: []string{LintIndexRewrite},
		},
		{
			name:      "index in source list",
			query:     `index IN ("main", "audit") | head 1`,
			wantCodes: []string{LintIndexRewrite},
		},
		{
			name:      "index not in source list",
			query:     `index NOT IN ("internal") | head 1`,
			wantCodes: []string{LintIndexRewrite},
		},
		{
			name:      "index not equals",
			query:     `index!=internal level=error`,
			wantCodes: []string{LintIndexRewrite},
		},
		{
			name:      "canonical from",
			query:     `from main | search error`,
			wantCodes: nil,
		},
		{
			name:      "search index predicate",
			query:     `from main | search index=security error`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintQuery_RawExactCompare(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "where raw equality",
			query:     `from app | where _raw = "panic"`,
			wantCodes: []string{LintRawExactCompare},
		},
		{
			name:      "search raw equality",
			query:     `from app | search _raw="panic"`,
			wantCodes: []string{LintRawExactCompare},
		},
		{
			name:      "raw like",
			query:     `from app | where _raw like "%panic%"`,
			wantCodes: []string{LintLeadingWildcard},
		},
		{
			name:      "other field equality",
			query:     `from app | where message = "panic"`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintQuery_StatsCountWideRange(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "count without by and no time bounds",
			query:     `from app | stats count()`,
			wantCodes: []string{LintStatsCountWide},
		},
		{
			name:      "count without parens also warns about wide count",
			query:     `from app | stats count`,
			wantCodes: []string{LintStatsCountWide, LintCountWithoutParens},
		},
		{
			name:      "grouped count",
			query:     `from app | stats count() by service`,
			wantCodes: nil,
		},
		{
			name:      "source time range",
			query:     `from app[-1h] | stats count()`,
			wantCodes: nil,
		},
		{
			name:      "where time range before stats",
			query:     `from app | where _time >= "2025-01-01T00:00:00Z" | stats count()`,
			wantCodes: nil,
		},
		{
			name:      "search time range before stats",
			query:     `from app | search _time>=2025-01-01T00:00:00Z | stats count()`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintQuery_DoubleQuotedNames(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "quoted source",
			query:     `from "my logs" | head 1`,
			wantCodes: []string{LintDoubleQuotedName},
		},
		{
			name:      "quoted source in list",
			query:     `from app, "my logs" | head 1`,
			wantCodes: []string{LintDoubleQuotedName},
		},
		{
			name:      "quoted index equals",
			query:     `index="my logs" | head 1`,
			wantCodes: []string{LintIndexRewrite, LintDoubleQuotedName},
		},
		{
			name:      "chart split field",
			query:     `from app | chart count() over "host name" by "status code"`,
			wantCodes: []string{LintDoubleQuotedName, LintDoubleQuotedName},
		},
		{
			name:      "fieldformat field",
			query:     `from app | fieldformat "display name" = tostring(status)`,
			wantCodes: []string{LintDoubleQuotedName},
		},
		{
			name:      "eval target",
			query:     `from app | eval "display name" = status`,
			wantCodes: []string{LintDoubleQuotedName},
		},
		{
			name:      "let target",
			query:     `from app | let "display name" = status`,
			wantCodes: []string{LintDoubleQuotedName},
		},
		{
			name:      "fields command",
			query:     `from app | fields "user id", status`,
			wantCodes: []string{LintDoubleQuotedName},
		},
		{
			name:      "table command",
			query:     `from app | table _time, "user id"`,
			wantCodes: []string{LintDoubleQuotedName},
		},
		{
			name:      "dedup command",
			query:     `from app | dedup 2 "user id", host`,
			wantCodes: []string{LintDoubleQuotedName},
		},
		{
			name:      "rename command",
			query:     `from app | rename "old name" as "new name"`,
			wantCodes: []string{LintDoubleQuotedName, LintDoubleQuotedName},
		},
		{
			name:      "stats group by",
			query:     `from app | stats count() by "user id", host`,
			wantCodes: []string{LintDoubleQuotedName},
		},
		{
			name:      "field option",
			query:     `from app | json field="message"`,
			wantCodes: []string{LintDoubleQuotedName},
		},
		{
			name:      "unpack field list",
			query:     `from app | unpack_json fields ("host")`,
			wantCodes: []string{LintDoubleQuotedName},
		},
		{
			name:      "string value",
			query:     `from app | where level = "ERROR"`,
			wantCodes: nil,
		},
		{
			name:      "search phrase after from word",
			query:     `from app | search from "quoted phrase"`,
			wantCodes: nil,
		},
		{
			name:      "single quoted source",
			query:     `from 'my logs' | head 1`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintQuery_DefaultMetricField(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "slowest row default",
			query:     `from app | slowest`,
			wantCodes: []string{LintDefaultMetricField},
		},
		{
			name:      "slowest group default",
			query:     `from app | slowest 20 uri`,
			wantCodes: []string{LintDefaultMetricField},
		},
		{
			name:      "slowest explicit metric",
			query:     `from app | slowest 20 uri by latency_ms`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintQuery_DeprecatedSortSyntax(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "sort field desc",
			query:     `from app | sort duration_ms desc`,
			wantCodes: []string{LintDeprecatedSort},
		},
		{
			name:      "sort multiple fields",
			query:     `from app | sort status asc, duration_ms desc`,
			wantCodes: []string{LintDeprecatedSort, LintDeprecatedSort},
		},
		{
			name:      "prefix canonical",
			query:     `from app | sort -duration_ms, +status`,
			wantCodes: nil,
		},
		{
			name:      "sort by form",
			query:     `from app | sort by duration_ms desc`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintQuery_OptionAfterArg(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "transaction option after field",
			query:     `from app | transaction session_id maxspan=30m`,
			wantCodes: []string{LintOptionAfterArg},
		},
		{
			name:      "transaction canonical option order",
			query:     `from app | transaction maxspan=30m session_id`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintQuery_AmbiguousDedupArgs(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "space separated fields",
			query:     `from app | dedup host source`,
			wantCodes: []string{LintAmbiguousDedupArgs},
		},
		{
			name:      "trailing limit",
			query:     `from app | dedup host source 2`,
			wantCodes: []string{LintAmbiguousDedupArgs},
		},
		{
			name:      "leading limit with space separated fields",
			query:     `from app | dedup 2 host source`,
			wantCodes: []string{LintAmbiguousDedupArgs},
		},
		{
			name:      "canonical comma fields",
			query:     `from app | dedup 2 host, source`,
			wantCodes: nil,
		},
		{
			name:      "single field",
			query:     `from app | dedup host`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintQuery_ReservedFieldNames(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "stats group field",
			query:     `from app | stats count() by order`,
			wantCodes: []string{LintReservedFieldName},
		},
		{
			name:      "sort field",
			query:     `from app | sort -order`,
			wantCodes: []string{LintReservedFieldName},
		},
		{
			name:      "fields list",
			query:     `from app | fields order`,
			wantCodes: []string{LintReservedFieldName},
		},
		{
			name:      "single quoted field",
			query:     `from app | stats count() by 'order'`,
			wantCodes: nil,
		},
		{
			name:      "sort direction",
			query:     `from app | sort by duration_ms desc`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintQuery_TautologicalSearchWideRange(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "search star",
			query:     `from app | search *`,
			wantCodes: []string{LintTautologicalSearch},
		},
		{
			name:      "freehand star",
			query:     `*`,
			wantCodes: []string{LintTautologicalSearch},
		},
		{
			name:      "source time range",
			query:     `from app[-1h] | search *`,
			wantCodes: nil,
		},
		{
			name:      "time predicate before search",
			query:     `from app | where _time >= "2025-01-01T00:00:00Z" | search *`,
			wantCodes: nil,
		},
		{
			name:      "field exists is not tautological",
			query:     `from app | search status=*`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintQuery_UnquotedOperatorValues(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "path value",
			query:     `from app | search path=/api/v1`,
			wantCodes: []string{LintUnquotedOpValue},
		},
		{
			name:      "in list value",
			query:     `from app | search service IN (api+worker, web)`,
			wantCodes: []string{LintUnquotedOpValue},
		},
		{
			name:      "quoted path value",
			query:     `from app | search path="/api/v1"`,
			wantCodes: nil,
		},
		{
			name:      "hyphenated value",
			query:     `from app | search host=web-1`,
			wantCodes: nil,
		},
		{
			name:      "where arithmetic is not a search literal value",
			query:     `from app | where duration_ms > latency_ms/2`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintQuery_LynxFlowShortcutAvailable(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		wantCodes   []string
		wantMessage string
	}{
		{
			name:        "errors by field",
			query:       `from app | where level IN ("error", "fatal") | stats count() by service`,
			wantCodes:   []string{LintShortcutAvailable},
			wantMessage: "Equivalent: `errors by service`",
		},
		{
			name:        "errors lower level",
			query:       `from app[-1h] | where lower(level) IN ("fatal", "error") | stats count()`,
			wantCodes:   []string{LintShortcutAvailable},
			wantMessage: "Equivalent: `errors`",
		},
		{
			name:      "already lynxflow",
			query:     `from app | errors by service`,
			wantCodes: nil,
		},
		{
			name:      "custom aggregate remains explicit",
			query:     `from app | where level IN ("error", "fatal") | stats dc(user_id) by service`,
			wantCodes: nil,
		},
		{
			name:      "custom error definition remains explicit",
			query:     `from app | where status >= 500 | stats count() by service`,
			wantCodes: nil,
		},
		{
			name:        "rate default span",
			query:       `from app | timechart span=1m count() as rate`,
			wantCodes:   []string{LintShortcutAvailable},
			wantMessage: "Equivalent: `rate`",
		},
		{
			name:        "rate span by field",
			query:       `from app | timechart span=5m count() as rate by service`,
			wantCodes:   []string{LintShortcutAvailable},
			wantMessage: "Equivalent: `rate per 5m by service`",
		},
		{
			name:      "already rate",
			query:     `from app | rate per 5m by service`,
			wantCodes: nil,
		},
		{
			name:      "timechart count without rate alias keeps output name",
			query:     `from app | timechart span=1m count()`,
			wantCodes: nil,
		},
		{
			name:        "every count",
			query:       `from app | bin _time span=5m | stats count() by _time`,
			wantCodes:   []string{LintShortcutAvailable},
			wantMessage: "Equivalent: `every 5m compute count()`",
		},
		{
			name:        "every count by field",
			query:       `from app | bin _time span=5m | stats count() by service, _time`,
			wantCodes:   []string{LintShortcutAvailable},
			wantMessage: "Equivalent: `every 5m by service compute count()`",
		},
		{
			name:      "already every",
			query:     `from app | every 5m by service compute count()`,
			wantCodes: nil,
		},
		{
			name:      "bin alias keeps explicit form",
			query:     `from app | bin _time span=5m as minute | stats count() by minute`,
			wantCodes: nil,
		},
		{
			name:        "latency default aggs",
			query:       `from app | timechart span=1m perc50(duration_ms) as p50, perc95(duration_ms) as p95, perc99(duration_ms) as p99, count() as count`,
			wantCodes:   []string{LintShortcutAvailable},
			wantMessage: "Equivalent: `latency duration_ms every 1m`",
		},
		{
			name:        "latency default aggs by field",
			query:       `from app | timechart span=5m perc50(dur) as p50, perc95(dur) as p95, perc99(dur) as p99, count() as count by service`,
			wantCodes:   []string{LintShortcutAvailable},
			wantMessage: "Equivalent: `latency dur every 5m by service`",
		},
		{
			name:      "already latency",
			query:     `from app | latency dur every 5m by service`,
			wantCodes: nil,
		},
		{
			name:      "latency custom aggs stay explicit",
			query:     `from app | timechart span=5m perc50(dur) as p50, perc99(dur) as p99, count() as count by service`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
				if tt.wantMessage != "" && !strings.Contains(lints[i].Message, tt.wantMessage) {
					t.Fatalf("lints[%d].Message = %q, want to contain %q", i, lints[i].Message, tt.wantMessage)
				}
			}
		})
	}
}

func TestLintQuery_NoExtractablePattern(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "raw regex wildcard only",
			query:     `from app | where _raw =~ ".*"`,
			wantCodes: []string{LintNoExtractablePattern},
		},
		{
			name:      "raw regex character class only",
			query:     `from app | where _raw =~ "^[0-9]+$"`,
			wantCodes: []string{LintNoExtractablePattern},
		},
		{
			name:      "raw regex with literal",
			query:     `from app | where _raw =~ ".*error.*"`,
			wantCodes: nil,
		},
		{
			name:      "field regex without literal is not raw",
			query:     `from app | where message =~ ".*"`,
			wantCodes: nil,
		},
		{
			name:      "search raw glob without ngram",
			query:     `from app | search *e*`,
			wantCodes: []string{LintLeadingWildcard, LintNoExtractablePattern},
		},
		{
			name:      "raw like without ngram",
			query:     `from app | where _raw like "%e%"`,
			wantCodes: []string{LintLeadingWildcard, LintNoExtractablePattern},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintQuery_PCRE2RegexFeatures(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "regex command lookahead",
			query:     `from app | regex "error(?= fatal)"`,
			wantCodes: []string{LintPCRE2RegexFeature},
		},
		{
			name:      "where regex lookbehind",
			query:     `from app | where _raw =~ "(?<=error) fatal"`,
			wantCodes: []string{LintPCRE2RegexFeature},
		},
		{
			name:      "rex backreference",
			query:     `from app | rex field=_raw "(error)\1"`,
			wantCodes: []string{LintPCRE2RegexFeature},
		},
		{
			name:      "possessive quantifier",
			query:     `from app | regex "error.*+"`,
			wantCodes: []string{LintPCRE2RegexFeature},
		},
		{
			name:      "linear regex",
			query:     `from app | regex "error|fatal"`,
			wantCodes: nil,
		},
		{
			name:      "named capture stays linear",
			query:     `from app | rex field=_raw "(?P<level>ERROR|WARN)"`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintProgram_RequiresSuccessfulParse(t *testing.T) {
	_, err := LintQuery(`from app | stats count(`)
	if err == nil {
		t.Fatal("expected parse error")
	}
}

func TestLintQuery_MixedSearchAndOr(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "explicit mixed operators",
			query:     `from app | search error OR timeout AND fatal`,
			wantCodes: []string{LintMixedSearchAndOr},
		},
		{
			name:      "implicit and with or",
			query:     `from app | search error timeout OR fatal`,
			wantCodes: []string{LintMixedSearchAndOr},
		},
		{
			name:      "field comparisons",
			query:     `from app | search status=500 OR status=503 host=web`,
			wantCodes: []string{LintMixedSearchAndOr},
		},
		{
			name:      "freehand normalized search",
			query:     `error OR timeout AND fatal`,
			wantCodes: []string{LintDefaultSource, LintMixedSearchAndOr},
		},
		{
			name:      "parenthesized or",
			query:     `from app | search (error OR timeout) AND fatal`,
			wantCodes: nil,
		},
		{
			name:      "parenthesized and",
			query:     `from app | search error OR (timeout AND fatal)`,
			wantCodes: nil,
		},
		{
			name:      "where context",
			query:     `from app | where error OR timeout AND fatal`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}

func TestLintQuery_DeepSearchNesting(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "deep not chain",
			query:     `from app | search NOT NOT NOT NOT NOT NOT error`,
			wantCodes: []string{LintDeepSearchNesting},
		},
		{
			name:      "deep binary tree",
			query:     `from app | search a AND (b AND (c AND (d AND (e AND (f AND g)))))`,
			wantCodes: []string{LintDeepSearchNesting},
		},
		{
			name:      "shallow search",
			query:     `from app | search error OR timeout`,
			wantCodes: nil,
		},
		{
			name:      "where context",
			query:     `from app | where NOT NOT NOT NOT NOT NOT error`,
			wantCodes: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lints, err := LintQuery(tt.query)
			if err != nil {
				t.Fatalf("LintQuery: %v", err)
			}
			if len(lints) != len(tt.wantCodes) {
				t.Fatalf("lints: got %+v, want codes %v", lints, tt.wantCodes)
			}
			for i, want := range tt.wantCodes {
				if lints[i].Code != want {
					t.Fatalf("lints[%d].Code: got %q, want %q", i, lints[i].Code, want)
				}
			}
		})
	}
}
