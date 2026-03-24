package pipeline

import (
	"testing"
)

func TestDrainTokenize(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{
			"GET /api/v1/users 200 1234",
			[]string{"GET", "/api/v1/users", "<*>", "<*>"},
		},
		{
			"Connection from 192.168.1.1 refused",
			[]string{"Connection", "from", "<*>", "refused"},
		},
		{
			"User 42 logged in at 2024-01-15",
			[]string{"User", "<*>", "logged", "in", "at", "2024-01-15"},
		},
		{
			"Simple text message",
			[]string{"Simple", "text", "message"},
		},
		{
			"Error 0xDEADBEEF detected",
			[]string{"Error", "<*>", "detected"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := drainTokenize(tt.input)
			if len(got) != len(tt.expected) {
				t.Errorf("drainTokenize(%q) = %v, want %v", tt.input, got, tt.expected)
				return
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("drainTokenize(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func TestDrainTokenize_VariableDetection(t *testing.T) {
	// Numeric values should be detected as variables.
	if got := drainTokenize("status 500"); len(got) != 2 || got[1] != "<*>" {
		t.Errorf("expected numeric 500 to be <**>, got %v", got)
	}

	// IP addresses should be detected as variables.
	if got := drainTokenize("from 10.0.0.1"); len(got) != 2 || got[1] != "<*>" {
		t.Errorf("expected IP 10.0.0.1 to be <**>, got %v", got)
	}

	// UUID should be detected as variable.
	uuid := "550e8400-e29b-41d4-a716-446655440000"
	if got := drainTokenize("id " + uuid); len(got) != 2 || got[1] != "<*>" {
		t.Errorf("expected UUID to be <**>, got %v", got)
	}
}

func TestIsNumeric(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"123", true},
		{"45.67", true},
		{"-3.14", true},
		{"0", true},
		{"abc", false},
		{"12abc", false},
		{"", false},
		{"-", false},
	}

	for _, tt := range tests {
		if got := isNumeric(tt.input); got != tt.expected {
			t.Errorf("isNumeric(%q) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}

func TestIsIPAddress(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"192.168.1.1", true},
		{"10.0.0.1", true},
		{"255.255.255.255", true},
		{"not.an.ip", false},
		{"192.168.1", false},
		{"192.168.1.1.1", false},
	}

	for _, tt := range tests {
		if got := isIPAddress(tt.input); got != tt.expected {
			t.Errorf("isIPAddress(%q) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}

func TestDrainTree_BasicClustering(t *testing.T) {
	tree := newDrainTree(4, 0.4)

	// Insert same template with different values.
	tree.insert("Connection from 192.168.1.1 refused")
	tree.insert("Connection from 10.0.0.1 refused")
	tree.insert("Connection from 172.16.0.1 refused")

	templates := tree.allTemplates()
	if len(templates) != 1 {
		t.Fatalf("expected 1 template, got %d", len(templates))
	}
	if templates[0].Count != 3 {
		t.Errorf("expected count 3, got %d", templates[0].Count)
	}
	expected := "Connection from <*> refused"
	if templates[0].Pattern != expected {
		t.Errorf("expected pattern %q, got %q", expected, templates[0].Pattern)
	}
}

func TestDrainTree_MultipleTemplates(t *testing.T) {
	tree := newDrainTree(4, 0.4)

	// Insert distinct patterns that cluster well with the simplified Drain.
	// Numeric tokens get replaced with <*>, so "42" and "99" both become <*>
	// and cluster into one template.
	tree.insert("User 42 logged in")
	tree.insert("User 99 logged in")
	tree.insert("Connection from 10.0.0.1 refused")
	tree.insert("Connection from 192.168.1.1 refused")

	templates := tree.allTemplates()
	// "User <*> logged in" and "Connection from <*> refused" — 2 templates.
	if len(templates) != 2 {
		t.Errorf("expected 2 templates, got %d", len(templates))
		for i, tmpl := range templates {
			t.Logf("  [%d] pattern=%q count=%d", i, tmpl.Pattern, tmpl.Count)
		}
	}

	// Both should have count 2.
	for _, tmpl := range templates {
		if tmpl.Count != 2 {
			t.Errorf("expected count 2 for %q, got %d", tmpl.Pattern, tmpl.Count)
		}
	}
}

func TestDrainTree_MaxTemplates(t *testing.T) {
	tree := newDrainTree(4, 0.4)

	// Insert many distinct patterns.
	for i := 0; i < 100; i++ {
		tree.insert("Pattern " + string(rune('A'+i%26)) + " detected")
	}

	templates := tree.allTemplates()
	// The tree itself doesn't enforce max — that's done in buildOutput.
	if len(templates) == 0 {
		t.Error("expected some templates")
	}
}

func TestDrainTree_EmptyInput(t *testing.T) {
	tree := newDrainTree(4, 0.4)
	tree.insert("")

	templates := tree.allTemplates()
	if len(templates) != 0 {
		t.Errorf("expected 0 templates for empty input, got %d", len(templates))
	}
}

func TestTemplateMatches(t *testing.T) {
	tests := []struct {
		template []string
		line     []string
		expected bool
	}{
		{
			[]string{"Connection", "from", "<*>", "refused"},
			[]string{"Connection", "from", "10.0.0.1", "refused"},
			true,
		},
		{
			[]string{"Connection", "from", "<*>", "refused"},
			[]string{"Connection", "from", "10.0.0.1", "accepted"},
			false,
		},
		{
			[]string{"A", "B", "C"},
			[]string{"A", "B"},
			false,
		},
	}

	for _, tt := range tests {
		if got := templateMatches(tt.template, tt.line); got != tt.expected {
			t.Errorf("templateMatches(%v, %v) = %v, want %v", tt.template, tt.line, got, tt.expected)
		}
	}
}

func TestBuildPattern(t *testing.T) {
	tokens := []string{"Connection", "from", "<*>", "refused"}
	expected := "Connection from <*> refused"
	if got := buildPattern(tokens); got != expected {
		t.Errorf("buildPattern(%v) = %q, want %q", tokens, got, expected)
	}
}

func TestTemplateSimilarity(t *testing.T) {
	tests := []struct {
		name     string
		a, b     []string
		expected float64
	}{
		{
			"identical",
			[]string{"User", "<*>", "logged", "in"},
			[]string{"User", "<*>", "logged", "in"},
			1.0,
		},
		{
			"one diff",
			[]string{"User", "<*>", "logged", "in"},
			[]string{"User", "<*>", "logged", "out"},
			0.75,
		},
		{
			"wildcard matches anything",
			[]string{"User", "<*>", "logged", "in"},
			[]string{"User", "42", "logged", "in"},
			1.0,
		},
		{
			"length mismatch",
			[]string{"A", "B", "C"},
			[]string{"A", "B"},
			0,
		},
		{
			"empty",
			[]string{},
			[]string{},
			1.0,
		},
		{
			"all different",
			[]string{"A", "B"},
			[]string{"X", "Y"},
			0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := templateSimilarity(tt.a, tt.b)
			if got != tt.expected {
				t.Errorf("templateSimilarity(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.expected)
			}
		})
	}
}

func TestMergeTokens(t *testing.T) {
	tests := []struct {
		name     string
		a, b     []string
		expected []string
	}{
		{
			"identical stays literal",
			[]string{"User", "42", "logged", "in"},
			[]string{"User", "42", "logged", "in"},
			[]string{"User", "42", "logged", "in"},
		},
		{
			"mismatch becomes wildcard",
			[]string{"User", "42", "logged", "in"},
			[]string{"User", "99", "logged", "out"},
			[]string{"User", "<*>", "logged", "<*>"},
		},
		{
			"wildcard preserved",
			[]string{"User", "<*>", "logged", "in"},
			[]string{"User", "42", "logged", "in"},
			[]string{"User", "<*>", "logged", "in"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeTokens(tt.a, tt.b)
			if len(got) != len(tt.expected) {
				t.Fatalf("mergeTokens(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.expected)
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("mergeTokens[%d] = %q, want %q", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func TestDrainTree_SimilarityMerge(t *testing.T) {
	tree := newDrainTree(4, 0.4)

	// First 4 tokens (depth routing) are identical. 5th token differs.
	// With depth=4, both lines land in the same leaf. The 5th token
	// is beyond tree routing, so it's not a wildcard — similarity merge kicks in.
	tree.insert("Error reading config file /etc/app.conf from disk")
	tree.insert("Error reading config file /etc/db.conf from disk")

	templates := tree.allTemplates()
	if len(templates) != 1 {
		t.Fatalf("expected 1 template after similarity merge, got %d", len(templates))
		for i, tmpl := range templates {
			t.Logf("  [%d] pattern=%q count=%d", i, tmpl.Pattern, tmpl.Count)
		}
	}
	if templates[0].Count != 2 {
		t.Errorf("expected count 2, got %d", templates[0].Count)
	}
}

func TestDrainTree_NoMergeBelowThreshold(t *testing.T) {
	tree := newDrainTree(4, 0.9)

	// With high threshold (0.9), these should NOT merge.
	tree.insert("Connection from 10.0.0.1 refused")
	tree.insert("Connection timeout on 10.0.0.1 port 443")

	templates := tree.allTemplates()
	if len(templates) != 2 {
		t.Errorf("expected 2 templates with high threshold, got %d", len(templates))
	}
}
