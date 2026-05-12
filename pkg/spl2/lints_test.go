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
