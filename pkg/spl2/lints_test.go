package spl2

import "testing"

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
			query:     `| stats count()`,
			wantCodes: []string{LintDefaultSource},
		},
		{
			name:      "bare command",
			query:     `stats count()`,
			wantCodes: []string{LintDefaultSource},
		},
		{
			name:      "explicit source",
			query:     `from app | stats count()`,
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
			name:      "where like",
			query:     `from app | where host like "*web"`,
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
			name:      "canonical from",
			query:     `from main | search error`,
			wantCodes: nil,
		},
		{
			name:      "index command without equals",
			query:     `index main | search error`,
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
			wantCodes: nil,
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

func TestLintQuery_DoubleQuotedNames(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantCodes []string
	}{
		{
			name:      "quoted source",
			query:     `from "my logs" | stats count()`,
			wantCodes: []string{LintDoubleQuotedName},
		},
		{
			name:      "quoted index equals",
			query:     `index="my logs" | stats count()`,
			wantCodes: []string{LintIndexRewrite, LintDoubleQuotedName},
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
			name:      "single quoted source",
			query:     `from 'my logs' | stats count()`,
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
