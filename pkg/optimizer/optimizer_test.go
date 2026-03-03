package optimizer

import (
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
)

func TestConstantFolding(t *testing.T) {
	// WHERE 1+2 > 3 → WHERE 3 > 3 → WHERE false
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.ArithExpr{Left: &spl2.LiteralExpr{Value: "1"}, Op: "+", Right: &spl2.LiteralExpr{Value: "2"}},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "3"},
				},
			},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	// After folding 1+2→3, then 3>3→false, then dead code elimination
	if len(result.Commands) > 0 {
		if w, ok := result.Commands[0].(*spl2.WhereCommand); ok {
			if lit, ok := w.Expr.(*spl2.LiteralExpr); ok {
				if lit.Value != "false" {
					t.Errorf("expected false, got %s", lit.Value)
				}
			}
		}
	}
	if opt.Stats["ConstantFolding"] == 0 {
		t.Error("ConstantFolding rule should have been applied")
	}
}

func TestConstantPropagation(t *testing.T) {
	// eval x=5 | WHERE x>3 → WHERE 5>3 → WHERE true → eliminated
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.EvalCommand{Field: "x", Expr: &spl2.LiteralExpr{Value: "5"}},
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "x"},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "3"},
				},
			},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	// After constant propagation (x→5), constant folding (5>3→true), dead code elimination (WHERE true removed)
	hasWhere := false
	for _, cmd := range result.Commands {
		if _, ok := cmd.(*spl2.WhereCommand); ok {
			hasWhere = true
		}
	}
	if hasWhere {
		t.Error("WHERE should have been eliminated after constant propagation")
	}
}

func TestPredicateSimplification(t *testing.T) {
	tests := []struct {
		name string
		expr spl2.Expr
		want string
	}{
		{
			"x>5 AND x>5 → x>5",
			&spl2.BinaryExpr{
				Left:  &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "x"}, Op: ">", Right: &spl2.LiteralExpr{Value: "5"}},
				Op:    "and",
				Right: &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "x"}, Op: ">", Right: &spl2.LiteralExpr{Value: "5"}},
			},
			"(x > 5)",
		},
		{
			"true OR x>5 → true",
			&spl2.BinaryExpr{
				Left:  &spl2.LiteralExpr{Value: "true"},
				Op:    "or",
				Right: &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "x"}, Op: ">", Right: &spl2.LiteralExpr{Value: "5"}},
			},
			"true",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &spl2.Query{Commands: []spl2.Command{&spl2.WhereCommand{Expr: tt.expr}}}
			opt := New()
			result := opt.Optimize(q)
			if len(result.Commands) == 0 {
				return // WHERE true → eliminated by dead code
			}
			if w, ok := result.Commands[0].(*spl2.WhereCommand); ok {
				got := w.Expr.String()
				if got != tt.want {
					t.Errorf("got %s, want %s", got, tt.want)
				}
			}
		})
	}
}

func TestNegationPushdown(t *testing.T) {
	// NOT(x > 5) → x <= 5
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.NotExpr{
					Expr: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "x"},
						Op:    ">",
						Right: &spl2.LiteralExpr{Value: "5"},
					},
				},
			},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	if w, ok := result.Commands[0].(*spl2.WhereCommand); ok {
		if cmp, ok := w.Expr.(*spl2.CompareExpr); ok {
			if cmp.Op != "<=" {
				t.Errorf("expected <=, got %s", cmp.Op)
			}
		} else {
			t.Error("expected CompareExpr")
		}
	}
}

func TestDeadCodeElimination(t *testing.T) {
	// WHERE false | stats count → just WHERE false (rest is dead)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{Expr: &spl2.LiteralExpr{Value: "false"}},
			&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	if len(result.Commands) != 1 {
		t.Errorf("expected 1 command (WHERE false), got %d", len(result.Commands))
	}
}

func TestPredicatePushdown(t *testing.T) {
	// eval y=1 | WHERE status>400 → WHERE status>400 | eval y=1
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.EvalCommand{Field: "y", Expr: &spl2.LiteralExpr{Value: "1"}},
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "status"},
					Op:    ">",
					Right: &spl2.LiteralExpr{Value: "400"},
				},
			},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	// WHERE should now be before EVAL
	if _, ok := result.Commands[0].(*spl2.WhereCommand); !ok {
		t.Error("WHERE should have been pushed before EVAL")
	}
	if _, ok := result.Commands[1].(*spl2.EvalCommand); !ok {
		t.Error("EVAL should be after WHERE")
	}
}

func TestStrengthReduction(t *testing.T) {
	// eval y = x * 2 → eval y = x + x
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.EvalCommand{
				Field: "y",
				Expr: &spl2.ArithExpr{
					Left:  &spl2.FieldExpr{Name: "x"},
					Op:    "*",
					Right: &spl2.LiteralExpr{Value: "2"},
				},
			},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	if e, ok := result.Commands[0].(*spl2.EvalCommand); ok {
		if a, ok := e.Expr.(*spl2.ArithExpr); ok {
			if a.Op != "+" {
				t.Errorf("expected +, got %s", a.Op)
			}
		}
	}
}

func TestOptimizerFixedPoint(t *testing.T) {
	// Verify optimizer converges (no infinite loop)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left:  &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "x"}, Op: ">", Right: &spl2.LiteralExpr{Value: "5"}},
					Op:    "and",
					Right: &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "x"}, Op: ">", Right: &spl2.LiteralExpr{Value: "5"}},
				},
			},
			&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	// Should simplify x>5 AND x>5 → x>5 without infinite loop
	if result == nil {
		t.Fatal("optimizer returned nil")
	}
	totalApplied := 0
	for _, count := range opt.Stats {
		totalApplied += count
	}
	// Should have applied at least PredicateSimplification
	if totalApplied == 0 {
		t.Error("expected at least one rule application")
	}
}

func TestOptimizerExplain(t *testing.T) {
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.NotExpr{
					Expr: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "x"},
						Op:    ">",
						Right: &spl2.LiteralExpr{Value: "5"},
					},
				},
			},
		},
	}
	opt := New()
	opt.Optimize(q)
	explain := opt.Explain()
	if !containsStr(explain, "NegationPushdown") {
		t.Error("explain should mention NegationPushdown")
	}
}

func TestPredicateFusion(t *testing.T) {
	tests := []struct {
		name     string
		commands []spl2.Command
		wantLen  int
		fused    bool
	}{
		{
			"where+where fused",
			[]spl2.Command{
				&spl2.WhereCommand{Expr: &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "a"}, Op: ">", Right: &spl2.LiteralExpr{Value: "1"}}},
				&spl2.WhereCommand{Expr: &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "b"}, Op: "<", Right: &spl2.LiteralExpr{Value: "5"}}},
			},
			1,
			true,
		},
		{
			"where+stats+where not fused",
			[]spl2.Command{
				&spl2.WhereCommand{Expr: &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "a"}, Op: ">", Right: &spl2.LiteralExpr{Value: "1"}}},
				&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "cnt"}}, GroupBy: []string{"source"}},
				&spl2.WhereCommand{Expr: &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "cnt"}, Op: ">", Right: &spl2.LiteralExpr{Value: "5"}}},
			},
			3,
			false,
		},
		{
			"where+stats not fused",
			[]spl2.Command{
				&spl2.WhereCommand{Expr: &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "a"}, Op: ">", Right: &spl2.LiteralExpr{Value: "1"}}},
				&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
			},
			2,
			false,
		},
		{
			"three consecutive where → two fusions",
			[]spl2.Command{
				&spl2.WhereCommand{Expr: &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "a"}, Op: ">", Right: &spl2.LiteralExpr{Value: "1"}}},
				&spl2.WhereCommand{Expr: &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "b"}, Op: "<", Right: &spl2.LiteralExpr{Value: "5"}}},
				&spl2.WhereCommand{Expr: &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "c"}, Op: "=", Right: &spl2.LiteralExpr{Value: "x"}}},
			},
			// First pass fuses [0]+[1]→one, then [2] remains = 2 commands
			// Second pass fuses those two → 1 command
			1,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &spl2.Query{Commands: tt.commands}
			opt := New()
			result := opt.Optimize(q)
			if len(result.Commands) != tt.wantLen {
				t.Errorf("expected %d commands, got %d", tt.wantLen, len(result.Commands))
			}
			if tt.fused && opt.Stats["PredicateFusion"] == 0 {
				t.Error("expected PredicateFusion to fire")
			}
			if !tt.fused && opt.Stats["PredicateFusion"] > 0 {
				t.Error("expected PredicateFusion NOT to fire")
			}
		})
	}
}

func containsStr(s, sub string) bool {
	return s != "" && sub != "" && contains(s, sub)
}

func contains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}

	return false
}
