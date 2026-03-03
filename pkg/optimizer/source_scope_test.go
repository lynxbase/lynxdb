package optimizer

import (
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
)

func TestSourceORtoIN_TwoSources(t *testing.T) {
	// WHERE source="nginx" OR source="postgres" → WHERE source IN ("nginx","postgres")
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.CompareExpr{
						Left: &spl2.FieldExpr{Name: "source"}, Op: "=",
						Right: &spl2.LiteralExpr{Value: "nginx"},
					},
					Op: "or",
					Right: &spl2.CompareExpr{
						Left: &spl2.FieldExpr{Name: "source"}, Op: "=",
						Right: &spl2.LiteralExpr{Value: "postgres"},
					},
				},
			},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	if opt.Stats["SourceORtoIN"] == 0 {
		t.Fatal("SourceORtoIN rule should have fired")
	}
	// Check that the WHERE now contains an InExpr.
	if len(result.Commands) == 0 {
		t.Fatal("expected at least one command")
	}
	w, ok := result.Commands[0].(*spl2.WhereCommand)
	if !ok {
		t.Fatalf("expected WhereCommand, got %T", result.Commands[0])
	}
	in, ok := w.Expr.(*spl2.InExpr)
	if !ok {
		t.Fatalf("expected InExpr, got %T (%s)", w.Expr, w.Expr)
	}
	if len(in.Values) != 2 {
		t.Fatalf("expected 2 values in IN, got %d", len(in.Values))
	}
}

func TestSourceORtoIN_ThreeSources(t *testing.T) {
	// WHERE index="a" OR index="b" OR index="c" → WHERE index IN ("a","b","c")
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.BinaryExpr{
						Left: &spl2.CompareExpr{
							Left: &spl2.FieldExpr{Name: "index"}, Op: "=",
							Right: &spl2.LiteralExpr{Value: "a"},
						},
						Op: "or",
						Right: &spl2.CompareExpr{
							Left: &spl2.FieldExpr{Name: "index"}, Op: "=",
							Right: &spl2.LiteralExpr{Value: "b"},
						},
					},
					Op: "or",
					Right: &spl2.CompareExpr{
						Left: &spl2.FieldExpr{Name: "index"}, Op: "=",
						Right: &spl2.LiteralExpr{Value: "c"},
					},
				},
			},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	// SourceORtoIN fires for source fields with threshold 2, but InListRewrite
	// also fires for 3+ items — SourceORtoIN runs first (lower threshold).
	if opt.Stats["SourceORtoIN"] == 0 {
		t.Fatal("SourceORtoIN rule should have fired")
	}
	w := result.Commands[0].(*spl2.WhereCommand)
	in, ok := w.Expr.(*spl2.InExpr)
	if !ok {
		t.Fatalf("expected InExpr, got %T", w.Expr)
	}
	if len(in.Values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(in.Values))
	}
}

func TestSourceORtoIN_NonSourceField(t *testing.T) {
	// WHERE level="error" OR level="warn" → should NOT fire (not a source field)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.CompareExpr{
						Left: &spl2.FieldExpr{Name: "level"}, Op: "=",
						Right: &spl2.LiteralExpr{Value: "error"},
					},
					Op: "or",
					Right: &spl2.CompareExpr{
						Left: &spl2.FieldExpr{Name: "level"}, Op: "=",
						Right: &spl2.LiteralExpr{Value: "warn"},
					},
				},
			},
		},
	}
	opt := New()
	opt.Optimize(q)
	if opt.Stats["SourceORtoIN"] > 0 {
		t.Fatal("SourceORtoIN should NOT fire for non-source fields")
	}
}

func TestSourceScopeAnnotation_FromSingle(t *testing.T) {
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: "error"},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	if opt.Stats["SourceScopeAnnotation"] == 0 {
		t.Fatal("SourceScopeAnnotation rule should have fired")
	}
	anno, ok := result.Annotations["sourceScope"]
	if !ok {
		t.Fatal("expected sourceScope annotation")
	}
	m := anno.(map[string]interface{})
	if m["type"] != "single" {
		t.Fatalf("expected scope type 'single', got %v", m["type"])
	}
	sources := m["sources"].([]string)
	if len(sources) != 1 || sources[0] != "nginx" {
		t.Fatalf("expected sources=[nginx], got %v", sources)
	}
}

func TestSourceScopeAnnotation_FromMulti(t *testing.T) {
	q := &spl2.Query{
		Source: &spl2.SourceClause{
			Index:   "nginx",
			Indices: []string{"nginx", "postgres", "redis"},
		},
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: "error"},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	anno := result.Annotations["sourceScope"].(map[string]interface{})
	if anno["type"] != "list" {
		t.Fatalf("expected scope type 'list', got %v", anno["type"])
	}
	sources := anno["sources"].([]string)
	if len(sources) != 3 {
		t.Fatalf("expected 3 sources, got %d", len(sources))
	}
}

func TestSourceScopeAnnotation_FromGlob(t *testing.T) {
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "logs*", IsGlob: true},
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: "error"},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	anno := result.Annotations["sourceScope"].(map[string]interface{})
	if anno["type"] != "glob" {
		t.Fatalf("expected scope type 'glob', got %v", anno["type"])
	}
	if anno["pattern"] != "logs*" {
		t.Fatalf("expected pattern 'logs*', got %v", anno["pattern"])
	}
}

func TestSourceScopeAnnotation_FromAll(t *testing.T) {
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "*", IsGlob: true},
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: "error"},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	anno := result.Annotations["sourceScope"].(map[string]interface{})
	if anno["type"] != "all" {
		t.Fatalf("expected scope type 'all', got %v", anno["type"])
	}
}

func TestSourceScopeAnnotation_SearchExpr(t *testing.T) {
	// search index=nginx level=error → scope should be single "nginx"
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SearchCommand{
				Expression: &spl2.SearchAndExpr{
					Left: &spl2.SearchCompareExpr{
						Field: "index", Op: spl2.OpEq, Value: "nginx",
					},
					Right: &spl2.SearchCompareExpr{
						Field: "level", Op: spl2.OpEq, Value: "error",
					},
				},
			},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	anno, ok := result.Annotations["sourceScope"]
	if !ok {
		t.Fatal("expected sourceScope annotation from search expr")
	}
	m := anno.(map[string]interface{})
	if m["type"] != "single" {
		t.Fatalf("expected scope type 'single', got %v", m["type"])
	}
}

func TestSourceScopeAnnotation_WhereExpr(t *testing.T) {
	// WHERE _source="nginx" → scope should be single "nginx"
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "_source"},
					Op:    "=",
					Right: &spl2.LiteralExpr{Value: "nginx"},
				},
			},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	anno, ok := result.Annotations["sourceScope"]
	if !ok {
		t.Fatal("expected sourceScope annotation from WHERE")
	}
	m := anno.(map[string]interface{})
	if m["type"] != "single" {
		t.Fatalf("expected scope type 'single', got %v", m["type"])
	}
}

func TestSourceScope_MatchesSource(t *testing.T) {
	tests := []struct {
		name   string
		scope  SourceScope
		source string
		want   bool
	}{
		{"all matches anything", SourceScope{Type: "all"}, "nginx", true},
		{"single match", SourceScope{Type: "single", Sources: []string{"nginx"}}, "nginx", true},
		{"single no match", SourceScope{Type: "single", Sources: []string{"nginx"}}, "postgres", false},
		{"list match first", SourceScope{Type: "list", Sources: []string{"nginx", "postgres"}}, "nginx", true},
		{"list match second", SourceScope{Type: "list", Sources: []string{"nginx", "postgres"}}, "postgres", true},
		{"list no match", SourceScope{Type: "list", Sources: []string{"nginx", "postgres"}}, "redis", false},
		{"glob match", SourceScope{Type: "glob", Pattern: "logs*"}, "logs-web", true},
		{"glob no match", SourceScope{Type: "glob", Pattern: "logs*"}, "nginx", false},
		{"glob question mark", SourceScope{Type: "glob", Pattern: "log?"}, "logs", true},
		{"glob question no match", SourceScope{Type: "glob", Pattern: "log?"}, "logging", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.scope.MatchesSource(tt.source)
			if got != tt.want {
				t.Errorf("MatchesSource(%q) = %v, want %v", tt.source, got, tt.want)
			}
		})
	}
}

// ─── BUG-8: SearchOrExpr source scope extraction ────────────────────────────

func TestExtractSourceScope_SearchOrExpr(t *testing.T) {
	// search index=nginx OR index=postgres
	// Should produce a "list" scope with ["nginx", "postgres"].
	expr := &spl2.SearchOrExpr{
		Left: &spl2.SearchCompareExpr{
			Field: "index", Op: spl2.OpEq, Value: "nginx",
		},
		Right: &spl2.SearchCompareExpr{
			Field: "index", Op: spl2.OpEq, Value: "postgres",
		},
	}

	scope := extractSourceScopeFromSearch(expr)
	if scope == nil {
		t.Fatal("expected non-nil scope from index=nginx OR index=postgres")
	}
	if scope.Type != scopeList {
		t.Errorf("expected scope type %q, got %q", scopeList, scope.Type)
	}
	if len(scope.Sources) != 2 {
		t.Fatalf("expected 2 sources, got %d", len(scope.Sources))
	}

	sourceSet := make(map[string]bool)
	for _, s := range scope.Sources {
		sourceSet[s] = true
	}
	if !sourceSet["nginx"] {
		t.Error("expected 'nginx' in sources")
	}
	if !sourceSet["postgres"] {
		t.Error("expected 'postgres' in sources")
	}
}

func TestExtractSourceScope_SearchAndExpr(t *testing.T) {
	// search index=nginx error → SearchAndExpr of index compare + keyword
	// Should extract "single" scope for nginx.
	expr := &spl2.SearchAndExpr{
		Left: &spl2.SearchCompareExpr{
			Field: "index", Op: spl2.OpEq, Value: "nginx",
		},
		Right: &spl2.SearchKeywordExpr{Value: "error"},
	}

	scope := extractSourceScopeFromSearch(expr)
	if scope == nil {
		t.Fatal("expected non-nil scope from index=nginx AND keyword")
	}
	if scope.Type != scopeSingle {
		t.Errorf("expected scope type %q, got %q", scopeSingle, scope.Type)
	}
	if len(scope.Sources) != 1 || scope.Sources[0] != "nginx" {
		t.Errorf("expected sources=[nginx], got %v", scope.Sources)
	}
}

func TestExtractSourceScope_SearchOrExprMixed(t *testing.T) {
	// search index=nginx OR error → OR where one branch has no scope
	// Should return nil (can't restrict scope).
	expr := &spl2.SearchOrExpr{
		Left: &spl2.SearchCompareExpr{
			Field: "index", Op: spl2.OpEq, Value: "nginx",
		},
		Right: &spl2.SearchKeywordExpr{Value: "error"},
	}

	scope := extractSourceScopeFromSearch(expr)
	if scope != nil {
		t.Errorf("expected nil scope from OR with keyword branch, got %+v", scope)
	}
}

// ─── BUG-9: SourceScope lazy set for large lists ─────────────────────────────

func TestSourceScope_LargeList_MatchesSource(t *testing.T) {
	// Create a scope with >16 sources to trigger set optimization.
	sources := make([]string, 20)
	for i := range sources {
		sources[i] = "idx-" + string(rune('a'+i))
	}
	scope := SourceScope{Type: scopeList, Sources: sources}

	// First call should match (and build the set internally).
	if !scope.MatchesSource("idx-a") {
		t.Error("expected match for idx-a")
	}
	// Second call uses cached set.
	if !scope.MatchesSource("idx-e") {
		t.Error("expected match for idx-e")
	}
	if scope.MatchesSource("idx-z") {
		t.Error("expected no match for idx-z (not in list)")
	}
}
