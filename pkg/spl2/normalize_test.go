package spl2

import "testing"

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

		// source= is a field filter, not an index selector — falls through to implicit search
		{name: "source=nginx", input: "source=nginx", want: "FROM main | search source=nginx"},
		{name: "source=nginx with pipe", input: "source=nginx | stats count", want: "FROM main | search source=nginx | stats count"},
		{name: "source=logs*", input: "source=logs*", want: "FROM main | search source=logs*"},
		{name: "source=* all", input: "source=*", want: "FROM main | search source=*"},
		{name: "source=nginx with search", input: "source=nginx level=error", want: "FROM main | search source=nginx level=error"},
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
