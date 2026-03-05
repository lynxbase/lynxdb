package optimizer

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

type staticCatalog struct {
	views []ViewInfo
}

func (s *staticCatalog) ListViews() []ViewInfo { return s.views }

func TestMVRewrite_MatchingView(t *testing.T) {
	catalog := &staticCatalog{
		views: []ViewInfo{
			{
				Name:         "mv_nginx_stats",
				Filter:       "nginx",
				GroupBy:      []string{"host", "status"},
				Aggregations: []string{"count", "sum"},
				Status:       "active",
			},
		},
	}
	rule := NewMVRewriteRule(catalog)

	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{
					{Func: "count"},
				},
				GroupBy: []string{"host"},
			},
		},
	}

	newQ, applied := rule.Apply(q)
	if !applied {
		t.Fatal("expected rule to apply")
	}

	if len(newQ.Commands) < 1 {
		t.Fatal("expected at least 1 command")
	}
	from, ok := newQ.Commands[0].(*spl2.FromCommand)
	if !ok {
		t.Fatalf("expected FromCommand, got %T", newQ.Commands[0])
	}
	if from.ViewName != "mv_nginx_stats" {
		t.Errorf("view name: got %q, want %q", from.ViewName, "mv_nginx_stats")
	}

	// Stats command should be preserved.
	if len(newQ.Commands) < 2 {
		t.Fatal("expected stats command to be preserved")
	}
	if _, ok := newQ.Commands[1].(*spl2.StatsCommand); !ok {
		t.Fatalf("expected StatsCommand at [1], got %T", newQ.Commands[1])
	}
}

func TestMVRewrite_NoStatsCommand(t *testing.T) {
	catalog := &staticCatalog{
		views: []ViewInfo{
			{Name: "mv_test", Filter: "nginx", Status: "active"},
		},
	}
	rule := NewMVRewriteRule(catalog)

	q := &spl2.Query{
		Source:   &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{&spl2.HeadCommand{Count: 10}},
	}

	_, applied := rule.Apply(q)
	if applied {
		t.Error("expected rule to NOT apply without stats command")
	}
}

func TestMVRewrite_FilterMismatch(t *testing.T) {
	catalog := &staticCatalog{
		views: []ViewInfo{
			{
				Name:         "mv_api",
				Filter:       "api",
				GroupBy:      []string{"host"},
				Aggregations: []string{"count"},
				Status:       "active",
			},
		},
	}
	rule := NewMVRewriteRule(catalog)

	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count"}},
				GroupBy:      []string{"host"},
			},
		},
	}

	_, applied := rule.Apply(q)
	if applied {
		t.Error("expected rule to NOT apply with filter mismatch")
	}
}

func TestMVRewrite_GroupByMismatch(t *testing.T) {
	catalog := &staticCatalog{
		views: []ViewInfo{
			{
				Name:         "mv_narrow",
				Filter:       "nginx",
				GroupBy:      []string{"host"},
				Aggregations: []string{"count"},
				Status:       "active",
			},
		},
	}
	rule := NewMVRewriteRule(catalog)

	// Query groups by "host" AND "status", but MV only has "host".
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count"}},
				GroupBy:      []string{"host", "status"},
			},
		},
	}

	_, applied := rule.Apply(q)
	if applied {
		t.Error("expected rule to NOT apply when query GROUP BY is not a subset of MV GROUP BY")
	}
}

func TestMVRewrite_AggMismatch(t *testing.T) {
	catalog := &staticCatalog{
		views: []ViewInfo{
			{
				Name:         "mv_count_only",
				Filter:       "nginx",
				GroupBy:      []string{"host"},
				Aggregations: []string{"count"},
				Status:       "active",
			},
		},
	}
	rule := NewMVRewriteRule(catalog)

	// Query wants "avg" which the MV doesn't have.
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "avg"}},
				GroupBy:      []string{"host"},
			},
		},
	}

	_, applied := rule.Apply(q)
	if applied {
		t.Error("expected rule to NOT apply with missing aggregation")
	}
}

func TestMVRewrite_InactiveView(t *testing.T) {
	catalog := &staticCatalog{
		views: []ViewInfo{
			{
				Name:         "mv_building",
				Filter:       "nginx",
				GroupBy:      []string{"host"},
				Aggregations: []string{"count"},
				Status:       "disabled",
			},
		},
	}
	rule := NewMVRewriteRule(catalog)

	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count"}},
				GroupBy:      []string{"host"},
			},
		},
	}

	_, applied := rule.Apply(q)
	if applied {
		t.Error("expected rule to NOT apply for disabled view")
	}
}

func TestMVRewrite_BackfillView(t *testing.T) {
	catalog := &staticCatalog{
		views: []ViewInfo{
			{
				Name:         "mv_backfilling",
				Filter:       "nginx",
				GroupBy:      []string{"host"},
				Aggregations: []string{"count"},
				Status:       "backfill",
				Rows:         5000,
			},
		},
	}
	rule := NewMVRewriteRule(catalog)

	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count"}},
				GroupBy:      []string{"host"},
			},
		},
	}

	newQ, applied := rule.Apply(q)
	if !applied {
		t.Fatal("expected rule to apply for backfilling view")
	}

	// Verify annotation status.
	ann, ok := newQ.GetAnnotation("mvAccelerated")
	if !ok {
		t.Fatal("expected mvAccelerated annotation")
	}
	mvAnn := ann.(*MVAccelAnnotation)
	if mvAnn.Status != "backfilling" {
		t.Errorf("expected status 'backfilling', got %q", mvAnn.Status)
	}
}

func TestMVRewrite_NilCatalog(t *testing.T) {
	rule := NewMVRewriteRule(nil)

	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count"}},
			},
		},
	}

	_, applied := rule.Apply(q)
	if applied {
		t.Error("expected rule to NOT apply with nil catalog")
	}
}

func TestMVRewrite_EmptyMVFilter(t *testing.T) {
	catalog := &staticCatalog{
		views: []ViewInfo{
			{
				Name:         "mv_all",
				Filter:       "", // matches everything
				GroupBy:      []string{"host"},
				Aggregations: []string{"count"},
				Status:       "active",
			},
		},
	}
	rule := NewMVRewriteRule(catalog)

	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count"}},
				GroupBy:      []string{"host"},
			},
		},
	}

	_, applied := rule.Apply(q)
	if !applied {
		t.Error("expected rule to apply with empty MV filter (matches everything)")
	}
}

func TestMVRewrite_PreservesPostStatsCommands(t *testing.T) {
	catalog := &staticCatalog{
		views: []ViewInfo{
			{
				Name:         "mv_test",
				Filter:       "nginx",
				GroupBy:      []string{"host"},
				Aggregations: []string{"count"},
				Status:       "active",
			},
		},
	}
	rule := NewMVRewriteRule(catalog)

	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "nginx"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count"}},
				GroupBy:      []string{"host"},
			},
			&spl2.HeadCommand{Count: 10},
		},
	}

	newQ, applied := rule.Apply(q)
	if !applied {
		t.Fatal("expected rule to apply")
	}

	if len(newQ.Commands) != 3 {
		t.Fatalf("expected 3 commands, got %d", len(newQ.Commands))
	}
	if _, ok := newQ.Commands[0].(*spl2.FromCommand); !ok {
		t.Errorf("expected FromCommand at [0], got %T", newQ.Commands[0])
	}
	if _, ok := newQ.Commands[1].(*spl2.StatsCommand); !ok {
		t.Errorf("expected StatsCommand at [1], got %T", newQ.Commands[1])
	}
	if _, ok := newQ.Commands[2].(*spl2.HeadCommand); !ok {
		t.Errorf("expected HeadCommand at [2], got %T", newQ.Commands[2])
	}
}

func TestHelpers_StringSliceSubset(t *testing.T) {
	tests := []struct {
		sub, super []string
		want       bool
	}{
		{nil, nil, true},
		{nil, []string{"a"}, true},
		{[]string{"a"}, []string{"a", "b"}, true},
		{[]string{"a", "b"}, []string{"a"}, false},
		{[]string{"c"}, []string{"a", "b"}, false},
	}
	for _, tt := range tests {
		got := stringSliceSubset(tt.sub, tt.super)
		if got != tt.want {
			t.Errorf("stringSliceSubset(%v, %v) = %v, want %v", tt.sub, tt.super, got, tt.want)
		}
	}
}

func TestHelpers_FilterIsSubset(t *testing.T) {
	tests := []struct {
		mvFilter, queryFilter string
		want                  bool
	}{
		{"", "nginx", true},
		{"nginx", "nginx", true},
		{"api", "nginx", false},
	}
	for _, tt := range tests {
		got := filterIsSubset(tt.mvFilter, tt.queryFilter)
		if got != tt.want {
			t.Errorf("filterIsSubset(%q, %q) = %v, want %v", tt.mvFilter, tt.queryFilter, got, tt.want)
		}
	}
}

func TestHelpers_AggsAreMergeable(t *testing.T) {
	tests := []struct {
		queryAggs map[string]bool
		mvAggs    []string
		want      bool
	}{
		{map[string]bool{"count": true}, []string{"count", "sum"}, true},
		{map[string]bool{"avg": true}, []string{"count", "sum"}, false},
		{map[string]bool{}, []string{"count"}, true},
	}
	for _, tt := range tests {
		got := aggsAreMergeable(tt.queryAggs, tt.mvAggs)
		if got != tt.want {
			t.Errorf("aggsAreMergeable(%v, %v) = %v, want %v", tt.queryAggs, tt.mvAggs, got, tt.want)
		}
	}
}

func TestExprImplies_StructuralEquality(t *testing.T) {
	// source = "nginx" implies source = "nginx"
	query := &spl2.CompareExpr{
		Left: &spl2.FieldExpr{Name: "source"}, Op: "=",
		Right: &spl2.LiteralExpr{Value: "nginx"},
	}
	mv := &spl2.CompareExpr{
		Left: &spl2.FieldExpr{Name: "source"}, Op: "=",
		Right: &spl2.LiteralExpr{Value: "nginx"},
	}
	if !exprImplies(query, mv) {
		t.Error("expected source=nginx to imply source=nginx")
	}
}

func TestExprImplies_RangeTightening_LowerBound(t *testing.T) {
	// status >= 500 implies status >= 400
	query := &spl2.CompareExpr{
		Left: &spl2.FieldExpr{Name: "status"}, Op: ">=",
		Right: &spl2.LiteralExpr{Value: "500"},
	}
	mv := &spl2.CompareExpr{
		Left: &spl2.FieldExpr{Name: "status"}, Op: ">=",
		Right: &spl2.LiteralExpr{Value: "400"},
	}
	if !exprImplies(query, mv) {
		t.Error("expected status>=500 to imply status>=400")
	}
}

func TestExprImplies_RangeTightening_UpperBound(t *testing.T) {
	// status <= 300 implies status <= 400
	query := &spl2.CompareExpr{
		Left: &spl2.FieldExpr{Name: "status"}, Op: "<=",
		Right: &spl2.LiteralExpr{Value: "300"},
	}
	mv := &spl2.CompareExpr{
		Left: &spl2.FieldExpr{Name: "status"}, Op: "<=",
		Right: &spl2.LiteralExpr{Value: "400"},
	}
	if !exprImplies(query, mv) {
		t.Error("expected status<=300 to imply status<=400")
	}
}

func TestExprImplies_RangeTightening_Negative(t *testing.T) {
	// status >= 300 does NOT imply status >= 500
	query := &spl2.CompareExpr{
		Left: &spl2.FieldExpr{Name: "status"}, Op: ">=",
		Right: &spl2.LiteralExpr{Value: "300"},
	}
	mv := &spl2.CompareExpr{
		Left: &spl2.FieldExpr{Name: "status"}, Op: ">=",
		Right: &spl2.LiteralExpr{Value: "500"},
	}
	if exprImplies(query, mv) {
		t.Error("expected status>=300 to NOT imply status>=500")
	}
}

func TestExprImplies_EqualityImpliesRange(t *testing.T) {
	// status = 500 implies status >= 400
	query := &spl2.CompareExpr{
		Left: &spl2.FieldExpr{Name: "status"}, Op: "=",
		Right: &spl2.LiteralExpr{Value: "500"},
	}
	mv := &spl2.CompareExpr{
		Left: &spl2.FieldExpr{Name: "status"}, Op: ">=",
		Right: &spl2.LiteralExpr{Value: "400"},
	}
	if !exprImplies(query, mv) {
		t.Error("expected status=500 to imply status>=400")
	}
}

func TestExprImplies_INContainment(t *testing.T) {
	// status IN (200, 404) implies status IN (200, 404, 500)
	query := &spl2.InExpr{
		Field: &spl2.FieldExpr{Name: "status"},
		Values: []spl2.Expr{
			&spl2.LiteralExpr{Value: "200"},
			&spl2.LiteralExpr{Value: "404"},
		},
	}
	mv := &spl2.InExpr{
		Field: &spl2.FieldExpr{Name: "status"},
		Values: []spl2.Expr{
			&spl2.LiteralExpr{Value: "200"},
			&spl2.LiteralExpr{Value: "404"},
			&spl2.LiteralExpr{Value: "500"},
		},
	}
	if !exprImplies(query, mv) {
		t.Error("expected IN(200,404) to imply IN(200,404,500)")
	}
}

func TestExprImplies_INContainment_Negative(t *testing.T) {
	// status IN (200, 999) does NOT imply status IN (200, 404)
	query := &spl2.InExpr{
		Field: &spl2.FieldExpr{Name: "status"},
		Values: []spl2.Expr{
			&spl2.LiteralExpr{Value: "200"},
			&spl2.LiteralExpr{Value: "999"},
		},
	}
	mv := &spl2.InExpr{
		Field: &spl2.FieldExpr{Name: "status"},
		Values: []spl2.Expr{
			&spl2.LiteralExpr{Value: "200"},
			&spl2.LiteralExpr{Value: "404"},
		},
	}
	if exprImplies(query, mv) {
		t.Error("expected IN(200,999) to NOT imply IN(200,404)")
	}
}

func TestExprImplies_ConjunctionSubset(t *testing.T) {
	// (source = "nginx" AND status >= 500) implies (source = "nginx")
	query := &spl2.BinaryExpr{
		Left: &spl2.CompareExpr{
			Left: &spl2.FieldExpr{Name: "source"}, Op: "=",
			Right: &spl2.LiteralExpr{Value: "nginx"},
		},
		Op: "and",
		Right: &spl2.CompareExpr{
			Left: &spl2.FieldExpr{Name: "status"}, Op: ">=",
			Right: &spl2.LiteralExpr{Value: "500"},
		},
	}
	mv := &spl2.CompareExpr{
		Left: &spl2.FieldExpr{Name: "source"}, Op: "=",
		Right: &spl2.LiteralExpr{Value: "nginx"},
	}
	if !exprImplies(query, mv) {
		t.Error("expected (source=nginx AND status>=500) to imply source=nginx")
	}
}

func TestExprImplies_DifferentField(t *testing.T) {
	// source = "nginx" does NOT imply host = "nginx"
	query := &spl2.CompareExpr{
		Left: &spl2.FieldExpr{Name: "source"}, Op: "=",
		Right: &spl2.LiteralExpr{Value: "nginx"},
	}
	mv := &spl2.CompareExpr{
		Left: &spl2.FieldExpr{Name: "host"}, Op: "=",
		Right: &spl2.LiteralExpr{Value: "nginx"},
	}
	if exprImplies(query, mv) {
		t.Error("expected source=nginx to NOT imply host=nginx")
	}
}

func TestMVRewrite_WithFilterExpr(t *testing.T) {
	// Test AST-based matching with FilterExpr field.
	catalog := &staticCatalog{
		views: []ViewInfo{
			{
				Name: "mv_errors",
				FilterExpr: &spl2.CompareExpr{
					Left: &spl2.FieldExpr{Name: "level"}, Op: "=",
					Right: &spl2.LiteralExpr{Value: "error"},
				},
				GroupBy:      []string{"source"},
				Aggregations: []string{"count"},
				Status:       "active",
			},
		},
	}
	rule := NewMVRewriteRule(catalog)

	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left: &spl2.FieldExpr{Name: "level"}, Op: "=",
					Right: &spl2.LiteralExpr{Value: "error"},
				},
			},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count"}},
				GroupBy:      []string{"source"},
			},
		},
	}

	newQ, applied := rule.Apply(q)
	if !applied {
		t.Fatal("expected rule to apply with AST-based FilterExpr matching")
	}
	from, ok := newQ.Commands[0].(*spl2.FromCommand)
	if !ok {
		t.Fatalf("expected FromCommand, got %T", newQ.Commands[0])
	}
	if from.ViewName != "mv_errors" {
		t.Errorf("view name: got %q, want %q", from.ViewName, "mv_errors")
	}
}

func TestParseViewFilter(t *testing.T) {
	tests := []struct {
		filter  string
		wantNil bool
	}{
		{"status >= 500", false},
		{"level = \"error\" AND source = \"nginx\"", false},
		{"", true},
	}

	for _, tt := range tests {
		expr := parseViewFilter(tt.filter)
		if tt.wantNil && expr != nil {
			t.Errorf("parseViewFilter(%q): expected nil, got %v", tt.filter, expr)
		}
		if !tt.wantNil && expr == nil {
			t.Errorf("parseViewFilter(%q): expected non-nil", tt.filter)
		}
	}
}
