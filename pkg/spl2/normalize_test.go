package spl2

import (
	"strings"
	"testing"
	"time"
)

func TestNormalizeQuery(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty", input: "", want: ""},
		{name: "whitespace only", input: "   ", want: ""},
		{name: "FROM clause unchanged", input: "FROM main | stats count", want: "FROM main | stats count"},
		{name: "FROM lowercase unchanged", input: "from main | stats count", want: "from main | stats count"},
		{name: "pipe-prefixed gets FROM main", input: "| stats count by level", want: "FROM main | stats count by level"},
		{name: "CTE variable unchanged", input: "$threats = FROM idx | fields ip", want: "$threats = FROM idx | fields ip"},
		{name: "known command search", input: "search level=error", want: "FROM main | search level=error"},
		{name: "known command where", input: "where status>=500", want: "FROM main | where status>=500"},
		{name: "known command stats", input: "stats count by host", want: "FROM main | stats count by host"},
		{name: "implicit search bare field=value", input: "level=error | stats count", want: "FROM main | search level=error | stats count"},
		{name: "implicit search bare field", input: "level=error", want: "FROM main | search level=error"},
		{name: "implicit search text", input: "connection refused", want: "FROM main | search connection refused"},
		{name: "implicit search quoted", input: `"connection refused"`, want: `FROM main | search "connection refused"`},
		{name: "implicit search field>value", input: "status>=500 | top 10 uri", want: "FROM main | search status>=500 | top 10 uri"},
		{name: "trims whitespace", input: "  level=error  ", want: "FROM main | search level=error"},
		{name: "FROM with leading spaces", input: "  FROM main | search error", want: "FROM main | search error"},

		// Splunk-style index= selection
		{name: "index=name pipe stats", input: "index=2xlog | stats count", want: "FROM 2xlog | stats count"},
		{name: "index space name pipe stats", input: "index 2xlog | stats count", want: "FROM 2xlog | stats count"},
		{name: "index=name with search terms", input: "index=2xlog level=error | stats count", want: "FROM 2xlog | search level=error | stats count"},
		{name: "index space name with search terms", input: "index 2xlog level=error", want: "FROM 2xlog | search level=error"},
		{name: "index=name alone", input: "index=2xlog", want: "FROM 2xlog"},
		{name: "INDEX=name uppercase", input: "INDEX=foo", want: "FROM foo"},
		{name: "index=quoted name", input: `index="my-logs" | stats count`, want: "FROM my-logs | stats count"},
		{name: "index space name pipe where", input: "index 2xlog | where status>=500", want: "FROM 2xlog | where status>=500"},
		{name: "index space known command falls through", input: "index stats", want: "FROM main | search index stats"},

		// Wildcard and multi-source via index=
		{name: "index=* all sources", input: "index=*", want: "FROM *"},
		{name: "index=* with pipe", input: "index=* | stats count", want: "FROM * | stats count"},
		{name: "index=logs* glob", input: "index=logs*", want: "FROM logs*"},
		{name: "index=logs* with pipe", input: "index=logs* | stats count by source", want: "FROM logs* | stats count by source"},
		{name: "index=logs* with search", input: "index=logs* level=error", want: "FROM logs* | search level=error"},

		// index IN (...) rewriting
		{name: "index IN quoted", input: `index IN ("nginx", "postgres")`, want: "FROM nginx, postgres"},
		{name: "index IN with pipe", input: `index IN ("nginx", "postgres") | stats count`, want: "FROM nginx, postgres | stats count"},
		{name: "index IN unquoted", input: "index IN (nginx, postgres)", want: "FROM nginx, postgres"},
		{name: "index IN with search", input: `index IN ("a","b") level=error`, want: "FROM a, b | search level=error"},
		{name: "index NOT IN", input: `index NOT IN ("internal", "audit")`, want: `FROM * | where _source NOT IN ("internal", "audit")`},
		{name: "INDEX IN uppercase", input: `INDEX IN ("a","b")`, want: "FROM a, b"},
		{name: "source IN", input: `source IN ("nginx","postgres")`, want: `FROM main | where _source IN ("nginx", "postgres")`},
		{name: "source IN with pipe", input: `source IN ("nginx","postgres") | stats count`, want: `FROM main | where _source IN ("nginx", "postgres") | stats count`},
		{name: "source NOT IN", input: `source NOT IN ("internal")`, want: `FROM * | where _source NOT IN ("internal")`},
		{name: "index NOT IN with pipe", input: `index NOT IN ("a") | stats count`, want: `FROM * | where _source NOT IN ("a") | stats count`},
		{name: "index IN with known cmd", input: `index IN ("a","b") stats count`, want: "FROM a, b | stats count"},

		// index!= negation rewriting
		{name: "index!=value", input: "index!=internal", want: `FROM * | where _source!="internal"`},
		{name: "index!= with pipe", input: "index!=internal | stats count", want: `FROM * | where _source!="internal" | stats count`},
		{name: "index!= with search", input: "index!=internal level=error", want: `FROM * | where _source!="internal" | search level=error`},
		{name: "source!=value", input: "source!=internal", want: `FROM * | where _source!="internal"`},
		{name: "index!= with known cmd", input: "index!=internal stats count", want: `FROM * | where _source!="internal" | stats count`},

		// source= is a field filter — scans all indexes, filters by _source
		{name: "source=nginx", input: "source=nginx", want: `FROM * | where _source="nginx"`},
		{name: "source=nginx with pipe", input: "source=nginx | stats count", want: `FROM * | where _source="nginx" | stats count`},
		{name: "source=logs*", input: "source=logs*", want: `FROM * | where _source="logs*"`},
		{name: "source=* all", input: "source=*", want: `FROM * | where _source="*"`},
		{name: "source=nginx with search", input: "source=nginx level=error", want: `FROM * | where _source="nginx" | search level=error`},
		{name: "source=nginx with known cmd", input: "source=nginx stats count", want: `FROM * | where _source="nginx" | stats count`},
		{name: "SOURCE=nginx uppercase", input: "SOURCE=nginx | stats count", want: `FROM * | where _source="nginx" | stats count`},
		{name: "source=quoted", input: `source="my-app" | stats count`, want: `FROM * | where _source="my-app" | stats count`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeQuery(tt.input)
			if got != tt.want {
				t.Errorf("NormalizeQuery(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsKnownCommand(t *testing.T) {
	if !isKnownCommand("search") {
		t.Error("expected 'search' to be a known command")
	}
	if !isKnownCommand("stats") {
		t.Error("expected 'stats' to be a known command")
	}
	if isKnownCommand("level") {
		t.Error("'level' should not be a known command")
	}
	if isKnownCommand("") {
		t.Error("empty string should not be a known command")
	}
}

func TestFirstToken(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"level=error", "level"},
		{"stats count", "stats"},
		{"status>=500", "status"},
		{"|stats count", ""},
		{"search foo", "search"},
		{"x", "x"},
		{"", ""},
	}

	for _, tt := range tests {
		got := firstToken(tt.input)
		if got != tt.want {
			t.Errorf("firstToken(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestResolveTimeLiterals_Between(t *testing.T) {
	now := time.Date(2025, 3, 23, 14, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		input string
	}{
		{name: "between durations", input: "from nginx | where _time between -24h and -1h"},
		{name: "between with spaces", input: "from nginx | where _time between  -7d  and  -1d"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveTimeLiterals(tt.input, now)
			// Should NOT contain raw durations anymore.
			if strings.Contains(got, "between -") {
				t.Errorf("expected durations to be resolved, got: %s", got)
			}
			// Should contain "between" with quoted timestamps.
			if !strings.Contains(got, "between \"") {
				t.Errorf("expected quoted timestamps after 'between', got: %s", got)
			}
			if !strings.Contains(got, "\" and \"") {
				t.Errorf("expected 'and' with quoted timestamps, got: %s", got)
			}
		})
	}
}

func TestResolveTimeLiterals_AtDateLiterals(t *testing.T) {
	now := time.Date(2025, 3, 23, 14, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		input    string
		contains string
	}{
		{
			name:     "date literal in where",
			input:    "from nginx | where _time > @2025-01-15",
			contains: `"2025-01-15T00:00:00Z"`,
		},
		{
			name:     "date literal with time",
			input:    "from nginx | where _time > @2025-01-15T10:30:00",
			contains: `"2025-01-15T10:30:00Z"`,
		},
		{
			name:     "between with date literals",
			input:    "from nginx | where _time between @2025-01-01 and @2025-02-01",
			contains: `"2025-01-01T00:00:00Z"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveTimeLiterals(tt.input, now)
			if !strings.Contains(got, tt.contains) {
				t.Errorf("expected %q in output, got: %s", tt.contains, got)
			}
			// Should NOT contain raw @date.
			if strings.Contains(got, "@2025-") {
				t.Errorf("expected @date to be resolved, got: %s", got)
			}
		})
	}
}

func TestSubstituteParams(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		params map[string]string
		want   string
	}{
		{
			name:   "no params returns unchanged",
			query:  "level=error | stats count",
			params: nil,
			want:   "level=error | stats count",
		},
		{
			name:   "empty params returns unchanged",
			query:  "level=${lvl}",
			params: map[string]string{},
			want:   "level=${lvl}",
		},
		{
			name:   "string param auto-quoted",
			query:  "level=${lvl} | stats count",
			params: map[string]string{"lvl": "error"},
			want:   `level="error" | stats count`,
		},
		{
			name:   "numeric param not quoted",
			query:  "status>=${threshold}",
			params: map[string]string{"threshold": "500"},
			want:   "status>=500",
		},
		{
			name:   "negative numeric param",
			query:  "_time > ${offset}",
			params: map[string]string{"offset": "-3600"},
			want:   "_time > -3600",
		},
		{
			name:   "float numeric param",
			query:  "rate>${r}",
			params: map[string]string{"r": "3.14"},
			want:   "rate>3.14",
		},
		{
			name:   "multiple params",
			query:  "source=${src} AND status>=${code}",
			params: map[string]string{"src": "nginx", "code": "400"},
			want:   `source="nginx" AND status>=400`,
		},
		{
			name:   "unknown param left as-is",
			query:  "field=${unknown}",
			params: map[string]string{"other": "val"},
			want:   "field=${unknown}",
		},
		{
			name:   "CTE $name not touched",
			query:  "$threats = FROM idx | where ip=${ip}",
			params: map[string]string{"ip": "1.2.3.4"},
			want:   `$threats = FROM idx | where ip="1.2.3.4"`,
		},
		{
			name:   "incomplete ${ at end",
			query:  "field=${",
			params: map[string]string{"x": "y"},
			want:   "field=${",
		},
		{
			name:   "empty braces ${}",
			query:  "field=${}",
			params: map[string]string{"": "val"},
			want:   `field="val"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SubstituteParams(tt.query, tt.params)
			if got != tt.want {
				t.Errorf("SubstituteParams() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsNumericParam(t *testing.T) {
	tests := []struct {
		val  string
		want bool
	}{
		{"0", true},
		{"42", true},
		{"-1", true},
		{"3.14", true},
		{"-0.5", true},
		{"", false},
		{"abc", false},
		{"12abc", false},
		{"1.2.3", false},
		{"-", false},
	}
	for _, tt := range tests {
		t.Run(tt.val, func(t *testing.T) {
			if got := isNumericParam(tt.val); got != tt.want {
				t.Errorf("isNumericParam(%q) = %v, want %v", tt.val, got, tt.want)
			}
		})
	}
}

func TestParseParamFlags(t *testing.T) {
	flags := []string{"level=error", "threshold=500", "host=web-01"}
	got := ParseParamFlags(flags)
	want := map[string]string{"level": "error", "threshold": "500", "host": "web-01"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("got[%q] = %q, want %q", k, got[k], v)
		}
	}
}
