package pipeline

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func TestAnalyzeVecExpr_SimpleCompare(t *testing.T) {
	// field >= 500
	expr := &spl2.CompareExpr{
		Left:  &spl2.FieldExpr{Name: "status"},
		Op:    ">=",
		Right: &spl2.LiteralExpr{Value: "500"},
	}

	node := analyzeVecExpr(expr)
	if node == nil {
		t.Fatal("expected non-nil vecNode for simple compare")
	}
	if _, ok := node.(*vecCompareNode); !ok {
		t.Fatalf("expected *vecCompareNode, got %T", node)
	}
}

func TestAnalyzeVecExpr_CompoundAnd(t *testing.T) {
	// status >= 500 AND level = "ERROR"
	expr := &spl2.BinaryExpr{
		Left: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    ">=",
			Right: &spl2.LiteralExpr{Value: "500"},
		},
		Op: "and",
		Right: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "level"},
			Op:    "=",
			Right: &spl2.LiteralExpr{Value: "ERROR"},
		},
	}

	node := analyzeVecExpr(expr)
	if node == nil {
		t.Fatal("expected non-nil vecNode for AND expression")
	}
	and, ok := node.(*vecAndNode)
	if !ok {
		t.Fatalf("expected *vecAndNode, got %T", node)
	}
	if _, ok := and.left.(*vecCompareNode); !ok {
		t.Errorf("left child: expected *vecCompareNode, got %T", and.left)
	}
	if _, ok := and.right.(*vecCompareNode); !ok {
		t.Errorf("right child: expected *vecCompareNode, got %T", and.right)
	}
}

func TestAnalyzeVecExpr_CompoundOr(t *testing.T) {
	// status = 200 OR status = 404
	expr := &spl2.BinaryExpr{
		Left: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    "=",
			Right: &spl2.LiteralExpr{Value: "200"},
		},
		Op: "or",
		Right: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    "=",
			Right: &spl2.LiteralExpr{Value: "404"},
		},
	}

	node := analyzeVecExpr(expr)
	if node == nil {
		t.Fatal("expected non-nil vecNode for OR expression")
	}
	if _, ok := node.(*vecOrNode); !ok {
		t.Fatalf("expected *vecOrNode, got %T", node)
	}
}

func TestAnalyzeVecExpr_Not(t *testing.T) {
	// NOT status = 200
	expr := &spl2.NotExpr{
		Expr: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    "=",
			Right: &spl2.LiteralExpr{Value: "200"},
		},
	}

	node := analyzeVecExpr(expr)
	if node == nil {
		t.Fatal("expected non-nil vecNode for NOT expression")
	}
	if _, ok := node.(*vecNotNode); !ok {
		t.Fatalf("expected *vecNotNode, got %T", node)
	}
}

func TestAnalyzeVecExpr_In(t *testing.T) {
	// status IN (200, 404, 500)
	expr := &spl2.InExpr{
		Field: &spl2.FieldExpr{Name: "status"},
		Values: []spl2.Expr{
			&spl2.LiteralExpr{Value: "200"},
			&spl2.LiteralExpr{Value: "404"},
			&spl2.LiteralExpr{Value: "500"},
		},
	}

	node := analyzeVecExpr(expr)
	if node == nil {
		t.Fatal("expected non-nil vecNode for IN expression")
	}
	in, ok := node.(*vecInNode)
	if !ok {
		t.Fatalf("expected *vecInNode, got %T", node)
	}
	if in.negated {
		t.Error("expected negated=false")
	}
	if len(in.intSet) != 3 {
		t.Errorf("expected 3 int values, got %d", len(in.intSet))
	}
	if len(in.strSet) != 3 {
		t.Errorf("expected 3 str values, got %d", len(in.strSet))
	}
}

func TestAnalyzeVecExpr_InNegated(t *testing.T) {
	// status NOT IN (200)
	expr := &spl2.InExpr{
		Field:   &spl2.FieldExpr{Name: "status"},
		Values:  []spl2.Expr{&spl2.LiteralExpr{Value: "200"}},
		Negated: true,
	}

	node := analyzeVecExpr(expr)
	if node == nil {
		t.Fatal("expected non-nil vecNode for NOT IN expression")
	}
	in := node.(*vecInNode)
	if !in.negated {
		t.Error("expected negated=true")
	}
}

func TestAnalyzeVecExpr_Isnull(t *testing.T) {
	// isnull(host)
	expr := &spl2.FuncCallExpr{
		Name: "isnull",
		Args: []spl2.Expr{&spl2.FieldExpr{Name: "host"}},
	}

	node := analyzeVecExpr(expr)
	if node == nil {
		t.Fatal("expected non-nil vecNode for isnull")
	}
	nc, ok := node.(*vecNullCheckNode)
	if !ok {
		t.Fatalf("expected *vecNullCheckNode, got %T", node)
	}
	if !nc.wantNull {
		t.Error("expected wantNull=true")
	}
	if nc.field != "host" {
		t.Errorf("expected field 'host', got %q", nc.field)
	}
}

func TestAnalyzeVecExpr_Isnotnull(t *testing.T) {
	// isnotnull(host)
	expr := &spl2.FuncCallExpr{
		Name: "isnotnull",
		Args: []spl2.Expr{&spl2.FieldExpr{Name: "host"}},
	}

	node := analyzeVecExpr(expr)
	if node == nil {
		t.Fatal("expected non-nil vecNode for isnotnull")
	}
	nc := node.(*vecNullCheckNode)
	if nc.wantNull {
		t.Error("expected wantNull=false")
	}
}

func TestAnalyzeVecExpr_Like(t *testing.T) {
	// uri LIKE "/api/%"
	expr := &spl2.CompareExpr{
		Left:  &spl2.FieldExpr{Name: "uri"},
		Op:    "like",
		Right: &spl2.LiteralExpr{Value: "/api/%"},
	}

	node := analyzeVecExpr(expr)
	if node == nil {
		t.Fatal("expected non-nil vecNode for LIKE")
	}
	ln, ok := node.(*vecLikeNode)
	if !ok {
		t.Fatalf("expected *vecLikeNode, got %T", node)
	}
	if ln.kind != "prefix" {
		t.Errorf("expected kind 'prefix', got %q", ln.kind)
	}
	if ln.literal != "/api/" {
		t.Errorf("expected literal '/api/', got %q", ln.literal)
	}
}

func TestAnalyzeVecExpr_LikeContains(t *testing.T) {
	// uri LIKE "%error%"
	expr := &spl2.CompareExpr{
		Left:  &spl2.FieldExpr{Name: "uri"},
		Op:    "like",
		Right: &spl2.LiteralExpr{Value: "%error%"},
	}

	node := analyzeVecExpr(expr)
	if node == nil {
		t.Fatal("expected non-nil vecNode for LIKE contains")
	}
	ln := node.(*vecLikeNode)
	if ln.kind != "contains" {
		t.Errorf("expected kind 'contains', got %q", ln.kind)
	}
}

func TestAnalyzeVecExpr_BetweenFusion(t *testing.T) {
	// status >= 400 AND status <= 599
	expr := &spl2.BinaryExpr{
		Left: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    ">=",
			Right: &spl2.LiteralExpr{Value: "400"},
		},
		Op: "and",
		Right: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    "<=",
			Right: &spl2.LiteralExpr{Value: "599"},
		},
	}

	node := analyzeVecExpr(expr)
	if node == nil {
		t.Fatal("expected non-nil vecNode for BETWEEN")
	}
	rn, ok := node.(*vecRangeNode)
	if !ok {
		t.Fatalf("expected *vecRangeNode, got %T", node)
	}
	if rn.field != "status" {
		t.Errorf("expected field 'status', got %q", rn.field)
	}
	if rn.minVal != "400" || rn.maxVal != "599" {
		t.Errorf("range values: got min=%q max=%q, want 400/599", rn.minVal, rn.maxVal)
	}
	if rn.minOp != ">=" || rn.maxOp != "<=" {
		t.Errorf("range ops: got min=%q max=%q, want >=/<=", rn.minOp, rn.maxOp)
	}
}

func TestAnalyzeVecExpr_BetweenFusion_Reversed(t *testing.T) {
	// status <= 599 AND status >= 400
	expr := &spl2.BinaryExpr{
		Left: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    "<=",
			Right: &spl2.LiteralExpr{Value: "599"},
		},
		Op: "and",
		Right: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    ">=",
			Right: &spl2.LiteralExpr{Value: "400"},
		},
	}

	node := analyzeVecExpr(expr)
	if node == nil {
		t.Fatal("expected non-nil vecNode for reversed BETWEEN")
	}
	if _, ok := node.(*vecRangeNode); !ok {
		t.Fatalf("expected *vecRangeNode, got %T", node)
	}
}

func TestAnalyzeVecExpr_NonVectorizable(t *testing.T) {
	tests := []struct {
		name string
		expr spl2.Expr
	}{
		{
			name: "glob wildcard",
			expr: &spl2.CompareExpr{
				Left:  &spl2.FieldExpr{Name: "host"},
				Op:    "=",
				Right: &spl2.LiteralExpr{Value: "web-*"},
			},
		},
		{
			name: "field vs field",
			expr: &spl2.CompareExpr{
				Left:  &spl2.FieldExpr{Name: "a"},
				Op:    "=",
				Right: &spl2.FieldExpr{Name: "b"},
			},
		},
		{
			name: "literal vs literal",
			expr: &spl2.CompareExpr{
				Left:  &spl2.LiteralExpr{Value: "1"},
				Op:    "=",
				Right: &spl2.LiteralExpr{Value: "2"},
			},
		},
		{
			name: "unknown function",
			expr: &spl2.FuncCallExpr{
				Name: "cidrmatch",
				Args: []spl2.Expr{
					&spl2.LiteralExpr{Value: "10.0.0.0/8"},
					&spl2.FieldExpr{Name: "ip"},
				},
			},
		},
		{
			name: "arithmetic",
			expr: &spl2.ArithExpr{
				Left:  &spl2.FieldExpr{Name: "a"},
				Op:    "+",
				Right: &spl2.LiteralExpr{Value: "1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := analyzeVecExpr(tt.expr)
			if node != nil {
				t.Errorf("expected nil vecNode for non-vectorizable %s, got %T", tt.name, node)
			}
		})
	}
}

func TestAnalyzeVecExpr_NestedAndOr(t *testing.T) {
	// (status >= 500 AND level = "ERROR") OR host = "web-01"
	expr := &spl2.BinaryExpr{
		Left: &spl2.BinaryExpr{
			Left: &spl2.CompareExpr{
				Left:  &spl2.FieldExpr{Name: "status"},
				Op:    ">=",
				Right: &spl2.LiteralExpr{Value: "500"},
			},
			Op: "and",
			Right: &spl2.CompareExpr{
				Left:  &spl2.FieldExpr{Name: "level"},
				Op:    "=",
				Right: &spl2.LiteralExpr{Value: "ERROR"},
			},
		},
		Op: "or",
		Right: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "host"},
			Op:    "=",
			Right: &spl2.LiteralExpr{Value: "web-01"},
		},
	}

	node := analyzeVecExpr(expr)
	if node == nil {
		t.Fatal("expected non-nil vecNode for nested AND/OR")
	}
	orNode, ok := node.(*vecOrNode)
	if !ok {
		t.Fatalf("expected *vecOrNode, got %T", node)
	}
	// Left side should be an AND (or fused range if on same field, but fields differ).
	if _, ok := orNode.left.(*vecAndNode); !ok {
		t.Errorf("left child: expected *vecAndNode, got %T", orNode.left)
	}
	if _, ok := orNode.right.(*vecCompareNode); !ok {
		t.Errorf("right child: expected *vecCompareNode, got %T", orNode.right)
	}
}

func TestAnalyzeVecExpr_PartiallyNonVectorizable(t *testing.T) {
	// status >= 500 AND cidrmatch("10.0.0.0/8", ip)
	// Left is vectorizable, right is not → entire tree returns nil.
	expr := &spl2.BinaryExpr{
		Left: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    ">=",
			Right: &spl2.LiteralExpr{Value: "500"},
		},
		Op: "and",
		Right: &spl2.FuncCallExpr{
			Name: "cidrmatch",
			Args: []spl2.Expr{
				&spl2.LiteralExpr{Value: "10.0.0.0/8"},
				&spl2.FieldExpr{Name: "ip"},
			},
		},
	}

	node := analyzeVecExpr(expr)
	if node != nil {
		t.Error("expected nil vecNode when one branch is non-vectorizable")
	}
}

func TestClassifyLikePattern(t *testing.T) {
	tests := []struct {
		pattern  string
		wantKind string
		wantLit  string
	}{
		{"/api/%", "prefix", "/api/"},
		{"%error", "suffix", "error"},
		{"%error%", "contains", "error"},
		{"exact", "exact", "exact"},
		{"%", "general", ""},
		{"%a%b%", "general", "%a%b%"},
		{"_%test", "general", "_%test"},
	}

	for _, tt := range tests {
		kind, lit := classifyLikePatternForVec(tt.pattern)
		if kind != tt.wantKind || lit != tt.wantLit {
			t.Errorf("classifyLikePatternForVec(%q): got (%q, %q), want (%q, %q)",
				tt.pattern, kind, lit, tt.wantKind, tt.wantLit)
		}
	}
}
