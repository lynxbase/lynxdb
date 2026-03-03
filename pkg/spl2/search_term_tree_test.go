package spl2

import (
	"testing"
)

func TestExtractSearchTermTree_SingleKeyword(t *testing.T) {
	// "*.html" → Leaf{["html"]}
	expr, err := ParseSearchExpression(`"*.html"`)
	if err != nil {
		t.Fatal(err)
	}

	tree := ExtractSearchTermTree(expr)
	if tree == nil {
		t.Fatal("expected non-nil tree for *.html")
	}
	if tree.Op != SearchTermLeaf {
		t.Fatalf("expected Leaf, got op=%d", tree.Op)
	}
	if len(tree.Terms) == 0 {
		t.Fatal("expected at least one term")
	}

	found := false
	for _, term := range tree.Terms {
		if term == "html" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected term 'html' in %v", tree.Terms)
	}
}

func TestExtractSearchTermTree_OR(t *testing.T) {
	// "*.png" OR "*.htm" → Or{Leaf{["png"]}, Leaf{["htm"]}}
	expr, err := ParseSearchExpression(`"*.png" OR "*.htm"`)
	if err != nil {
		t.Fatal(err)
	}

	tree := ExtractSearchTermTree(expr)
	if tree == nil {
		t.Fatal("expected non-nil tree for OR expression")
	}
	if tree.Op != SearchTermOr {
		t.Fatalf("expected Or, got op=%d", tree.Op)
	}
	if len(tree.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(tree.Children))
	}

	// Check both children are leaves.
	for i, child := range tree.Children {
		if child.Op != SearchTermLeaf {
			t.Errorf("child[%d]: expected Leaf, got op=%d", i, child.Op)
		}
		if len(child.Terms) == 0 {
			t.Errorf("child[%d]: expected at least one term", i)
		}
	}
}

func TestExtractSearchTermTree_AND(t *testing.T) {
	// "error" "timeout" → And{Leaf{["error"]}, Leaf{["timeout"]}}
	expr, err := ParseSearchExpression(`error timeout`)
	if err != nil {
		t.Fatal(err)
	}

	tree := ExtractSearchTermTree(expr)
	if tree == nil {
		t.Fatal("expected non-nil tree for AND expression")
	}
	if tree.Op != SearchTermAnd {
		t.Fatalf("expected And, got op=%d", tree.Op)
	}
	if len(tree.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(tree.Children))
	}
}

func TestExtractSearchTermTree_NestedORinAND(t *testing.T) {
	// "error" ("timeout" OR "*.png")
	// → And{Leaf{["error"]}, Or{Leaf{["timeout"]}, Leaf{["png"]}}}
	expr, err := ParseSearchExpression(`error (timeout OR "*.png")`)
	if err != nil {
		t.Fatal(err)
	}

	tree := ExtractSearchTermTree(expr)
	if tree == nil {
		t.Fatal("expected non-nil tree")
	}
	if tree.Op != SearchTermAnd {
		t.Fatalf("expected And at root, got op=%d", tree.Op)
	}
	if len(tree.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(tree.Children))
	}

	// One child should be a Leaf, the other an Or.
	var hasLeaf, hasOr bool
	for _, child := range tree.Children {
		if child.Op == SearchTermLeaf {
			hasLeaf = true
		}
		if child.Op == SearchTermOr {
			hasOr = true
		}
	}
	if !hasLeaf || !hasOr {
		t.Errorf("expected one Leaf and one Or child, got children ops: %d, %d",
			tree.Children[0].Op, tree.Children[1].Op)
	}
}

func TestExtractSearchTermTree_NOT_returns_nil(t *testing.T) {
	// NOT "error" → nil
	expr, err := ParseSearchExpression(`NOT error`)
	if err != nil {
		t.Fatal(err)
	}

	tree := ExtractSearchTermTree(expr)
	if tree != nil {
		t.Fatalf("expected nil tree for NOT expression, got op=%d", tree.Op)
	}
}

func TestExtractSearchTermTree_OR_with_nil_branch_returns_nil(t *testing.T) {
	// "*.png" OR NOT "error" → nil (one branch can't produce terms)
	expr, err := ParseSearchExpression(`"*.png" OR NOT error`)
	if err != nil {
		t.Fatal(err)
	}

	tree := ExtractSearchTermTree(expr)
	if tree != nil {
		t.Fatalf("expected nil tree when OR has nil branch, got op=%d", tree.Op)
	}
}

func TestExtractSearchTermTree_WildcardStar_returns_nil(t *testing.T) {
	// "*" → nil (match-all, no useful literal)
	expr, err := ParseSearchExpression(`"*"`)
	if err != nil {
		t.Fatal(err)
	}

	tree := ExtractSearchTermTree(expr)
	if tree != nil {
		t.Fatalf("expected nil tree for match-all wildcard, got op=%d", tree.Op)
	}
}

func TestExtractSearchTermTree_nil_expr(t *testing.T) {
	tree := ExtractSearchTermTree(nil)
	if tree != nil {
		t.Fatal("expected nil tree for nil expr")
	}
}

func TestExtractSearchTermTree_FieldComparison_returns_nil(t *testing.T) {
	// status=200 → nil (field comparisons use InvertedIndexPredicates)
	expr, err := ParseSearchExpression(`status=200`)
	if err != nil {
		t.Fatal(err)
	}

	tree := ExtractSearchTermTree(expr)
	if tree != nil {
		t.Fatalf("expected nil tree for field comparison, got op=%d", tree.Op)
	}
}
