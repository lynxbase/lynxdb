package optimizer

import (
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
)

func TestReorder_EqualityFirst(t *testing.T) {
	// WHERE message LIKE "%" AND status=500
	// → status=500 should come first (equality on field is cheaper)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.FuncCallExpr{
						Name: "like",
						Args: []spl2.Expr{
							&spl2.FieldExpr{Name: "message"},
							&spl2.LiteralExpr{Value: "%"},
						},
					},
					Op: "and",
					Right: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "status"},
						Op:    "=",
						Right: &spl2.LiteralExpr{Value: "500"},
					},
				},
			},
		},
	}
	rule := &predicateReorderingRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("predicateReorderingRule should have reordered")
	}

	w := result.Commands[0].(*spl2.WhereCommand)
	// The first predicate in the AND should be the equality comparison.
	preds := flattenAND(w.Expr)
	if len(preds) != 2 {
		t.Fatalf("expected 2 predicates, got %d", len(preds))
	}
	// First should be the equality on status (cost ~20).
	if _, ok := preds[0].(*spl2.CompareExpr); !ok {
		t.Errorf("expected CompareExpr first (equality), got %T", preds[0])
	}
	// Second should be the like function call (cost ~80).
	if _, ok := preds[1].(*spl2.FuncCallExpr); !ok {
		t.Errorf("expected FuncCallExpr second (like), got %T", preds[1])
	}
}

func TestReorder_IndexedFieldFirst(t *testing.T) {
	// WHERE status > 500 AND host = "web01"
	// → host="web01" first (indexed equality, cost 10) before range comparison (cost 30)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "status"},
						Op:    ">",
						Right: &spl2.LiteralExpr{Value: "500"},
					},
					Op: "and",
					Right: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "host"},
						Op:    "=",
						Right: &spl2.LiteralExpr{Value: "web01"},
					},
				},
			},
		},
	}
	rule := &predicateReorderingRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("predicateReorderingRule should have reordered")
	}

	w := result.Commands[0].(*spl2.WhereCommand)
	preds := flattenAND(w.Expr)
	if len(preds) != 2 {
		t.Fatalf("expected 2 predicates, got %d", len(preds))
	}
	// First should be host="web01" (indexed equality).
	cmp, ok := preds[0].(*spl2.CompareExpr)
	if !ok {
		t.Fatalf("expected CompareExpr first, got %T", preds[0])
	}
	field := cmp.Left.(*spl2.FieldExpr)
	if field.Name != "host" {
		t.Errorf("expected host first, got %s", field.Name)
	}
}

func TestReorder_NoChangeWhenAlreadyOptimal(t *testing.T) {
	// WHERE host="web01" AND status > 500
	// Already optimal — should not change.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.BinaryExpr{
					Left: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "host"},
						Op:    "=",
						Right: &spl2.LiteralExpr{Value: "web01"},
					},
					Op: "and",
					Right: &spl2.CompareExpr{
						Left:  &spl2.FieldExpr{Name: "status"},
						Op:    ">",
						Right: &spl2.LiteralExpr{Value: "500"},
					},
				},
			},
		},
	}
	rule := &predicateReorderingRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("should not have reordered already-optimal predicates")
	}
}

func TestReorder_SinglePredicate(t *testing.T) {
	// Single predicate — nothing to reorder.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{
				Expr: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "status"},
					Op:    "=",
					Right: &spl2.LiteralExpr{Value: "200"},
				},
			},
		},
	}
	rule := &predicateReorderingRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Error("should not reorder single predicate")
	}
}
