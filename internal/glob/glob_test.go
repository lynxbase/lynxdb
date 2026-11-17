package glob

import "testing"

func TestMatch(t *testing.T) {
	tests := []struct {
		pattern, text string
		caseIns       bool
		want          bool
	}{
		{"*", "anything", false, true},
		{"foo*", "foobar", false, true},
		{"foo*", "bazfoo", false, false},
		{"*bar", "foobar", false, true},
		{"*bar", "barbaz", false, false},
		{"f?o", "foo", false, true},
		{"f?o", "fxo", false, true},
		{"f?o", "fo", false, false},
		{"[abc]", "a", false, true},
		{"[abc]", "d", false, false},
		{"[!abc]", "d", false, true},
		{"[!abc]", "a", false, false},
		{"{foo,bar}", "foo", false, true},
		{"{foo,bar}", "bar", false, true},
		{"{foo,bar}", "baz", false, false},
		{"FOO", "foo", true, true},
		{"FOO", "foo", false, false},
		{"logs*", "logs-nginx", false, true},
		{"logs*", "other", false, false},
	}

	for _, tt := range tests {
		got := Match(tt.pattern, tt.text, tt.caseIns)
		if got != tt.want {
			t.Errorf("Match(%q, %q, %v) = %v, want %v",
				tt.pattern, tt.text, tt.caseIns, got, tt.want)
		}
	}
}
