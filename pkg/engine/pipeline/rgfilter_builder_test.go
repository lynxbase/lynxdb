package pipeline

import (
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment"
)

func TestBuildRGFilter_NilHints(t *testing.T) {
	if BuildRGFilter(nil) != nil {
		t.Error("expected nil for nil hints")
	}
}

func TestBuildRGFilter_EmptyHints(t *testing.T) {
	if BuildRGFilter(&SegmentStreamHints{}) != nil {
		t.Error("expected nil for empty hints")
	}
}

func TestBuildRGFilter_SingleSearchTerm(t *testing.T) {
	node := BuildRGFilter(&SegmentStreamHints{
		SearchTerms: []string{"error"},
	})
	if node == nil {
		t.Fatal("expected non-nil node")
	}
	if node.Op != segment.RGFilterTerm {
		t.Errorf("Op: got %d, want RGFilterTerm(%d)", node.Op, segment.RGFilterTerm)
	}
	if len(node.Terms) != 1 || node.Terms[0] != "error" {
		t.Errorf("Terms: got %v, want [error]", node.Terms)
	}
}

func TestBuildRGFilter_MultipleSearchTerms(t *testing.T) {
	node := BuildRGFilter(&SegmentStreamHints{
		SearchTerms: []string{"error", "timeout"},
	})
	if node == nil {
		t.Fatal("expected non-nil node")
	}
	// Multiple terms → AND of Term nodes.
	if node.Op != segment.RGFilterAnd {
		t.Errorf("Op: got %d, want RGFilterAnd(%d)", node.Op, segment.RGFilterAnd)
	}
	if len(node.Children) != 2 {
		t.Fatalf("Children: got %d, want 2", len(node.Children))
	}
	for _, child := range node.Children {
		if child.Op != segment.RGFilterTerm {
			t.Errorf("child Op: got %d, want RGFilterTerm", child.Op)
		}
	}
}

func TestBuildRGFilter_FieldPredEq(t *testing.T) {
	node := BuildRGFilter(&SegmentStreamHints{
		FieldPreds: []spl2.FieldPredicate{
			{Field: "source", Op: "=", Value: "nginx"},
		},
	})
	if node == nil {
		t.Fatal("expected non-nil node")
	}
	if node.Op != segment.RGFilterFieldEq {
		t.Errorf("Op: got %d, want RGFilterFieldEq(%d)", node.Op, segment.RGFilterFieldEq)
	}
	// Verify field normalization: source → _source.
	if node.Field != "_source" {
		t.Errorf("Field: got %q, want _source", node.Field)
	}
	if node.Value != "nginx" {
		t.Errorf("Value: got %q, want nginx", node.Value)
	}
	if len(node.Terms) == 0 {
		t.Error("expected pre-tokenized terms for bloom")
	}
}

func TestBuildRGFilter_FieldPredNeq(t *testing.T) {
	node := BuildRGFilter(&SegmentStreamHints{
		FieldPreds: []spl2.FieldPredicate{
			{Field: "level", Op: "!=", Value: "debug"},
		},
	})
	if node == nil {
		t.Fatal("expected non-nil node")
	}
	if node.Op != segment.RGFilterFieldNeq {
		t.Errorf("Op: got %d, want RGFilterFieldNeq(%d)", node.Op, segment.RGFilterFieldNeq)
	}
	if node.Field != "level" {
		t.Errorf("Field: got %q, want level", node.Field)
	}
}

func TestBuildRGFilter_FieldPredRange(t *testing.T) {
	node := BuildRGFilter(&SegmentStreamHints{
		FieldPreds: []spl2.FieldPredicate{
			{Field: "status", Op: ">=", Value: "500"},
		},
	})
	if node == nil {
		t.Fatal("expected non-nil node")
	}
	if node.Op != segment.RGFilterFieldRange {
		t.Errorf("Op: got %d, want RGFilterFieldRange(%d)", node.Op, segment.RGFilterFieldRange)
	}
	if node.RangeOp != ">=" {
		t.Errorf("RangeOp: got %q, want >=", node.RangeOp)
	}
	if node.RangeVal != "500" {
		t.Errorf("RangeVal: got %q, want 500", node.RangeVal)
	}
}

func TestBuildRGFilter_RangePreds(t *testing.T) {
	node := BuildRGFilter(&SegmentStreamHints{
		RangePreds: []spl2.RangePredicate{
			{Field: "duration", Min: "100", Max: "500"},
		},
	})
	if node == nil {
		t.Fatal("expected non-nil node")
	}
	// Min+Max → AND of two FieldRange nodes.
	if node.Op != segment.RGFilterAnd {
		t.Errorf("Op: got %d, want RGFilterAnd(%d)", node.Op, segment.RGFilterAnd)
	}
	if len(node.Children) != 2 {
		t.Fatalf("Children: got %d, want 2", len(node.Children))
	}
	if node.Children[0].RangeOp != ">=" || node.Children[0].RangeVal != "100" {
		t.Errorf("child0: got %q %q, want >= 100", node.Children[0].RangeOp, node.Children[0].RangeVal)
	}
	if node.Children[1].RangeOp != "<=" || node.Children[1].RangeVal != "500" {
		t.Errorf("child1: got %q %q, want <= 500", node.Children[1].RangeOp, node.Children[1].RangeVal)
	}
}

func TestBuildRGFilter_InvertedPreds(t *testing.T) {
	node := BuildRGFilter(&SegmentStreamHints{
		InvertedPreds: []spl2.InvertedIndexPredicate{
			{Field: "source", Value: "nginx"},
		},
	})
	if node == nil {
		t.Fatal("expected non-nil node")
	}
	if node.Op != segment.RGFilterFieldEq {
		t.Errorf("Op: got %d, want RGFilterFieldEq", node.Op)
	}
	if node.Field != "_source" {
		t.Errorf("Field: got %q, want _source (normalized)", node.Field)
	}
}

func TestBuildRGFilter_SearchTermTree_Or(t *testing.T) {
	tree := &spl2.SearchTermTree{
		Op: spl2.SearchTermOr,
		Children: []*spl2.SearchTermTree{
			{Op: spl2.SearchTermLeaf, Terms: []string{"error"}},
			{Op: spl2.SearchTermLeaf, Terms: []string{"warn"}},
		},
	}
	node := BuildRGFilter(&SegmentStreamHints{
		SearchTermTree: tree,
	})
	if node == nil {
		t.Fatal("expected non-nil node")
	}
	if node.Op != segment.RGFilterOr {
		t.Errorf("Op: got %d, want RGFilterOr(%d)", node.Op, segment.RGFilterOr)
	}
	if len(node.Children) != 2 {
		t.Fatalf("Children: got %d, want 2", len(node.Children))
	}
}

func TestBuildRGFilter_SearchTermTree_PreferredOverFlat(t *testing.T) {
	// When both SearchTermTree and SearchTerms are set, tree takes precedence.
	tree := &spl2.SearchTermTree{
		Op:    spl2.SearchTermLeaf,
		Terms: []string{"from_tree"},
	}
	node := BuildRGFilter(&SegmentStreamHints{
		SearchTermTree: tree,
		SearchTerms:    []string{"from_flat"},
	})
	if node == nil {
		t.Fatal("expected non-nil node")
	}
	if node.Op != segment.RGFilterTerm {
		t.Errorf("Op: got %d, want RGFilterTerm", node.Op)
	}
	if len(node.Terms) != 1 || node.Terms[0] != "from_tree" {
		t.Errorf("Terms: got %v, want [from_tree]", node.Terms)
	}
}

func TestBuildRGFilter_Combined(t *testing.T) {
	node := BuildRGFilter(&SegmentStreamHints{
		SearchTerms: []string{"timeout"},
		FieldPreds: []spl2.FieldPredicate{
			{Field: "source", Op: "=", Value: "nginx"},
			{Field: "status", Op: ">=", Value: "500"},
		},
	})
	if node == nil {
		t.Fatal("expected non-nil node")
	}
	// 1 term + 2 field preds = 3 children under AND.
	if node.Op != segment.RGFilterAnd {
		t.Errorf("Op: got %d, want RGFilterAnd", node.Op)
	}
	if len(node.Children) != 3 {
		t.Errorf("Children: got %d, want 3", len(node.Children))
	}
}

func TestBuildRGFilter_FieldNormalization(t *testing.T) {
	node := BuildRGFilter(&SegmentStreamHints{
		FieldPreds: []spl2.FieldPredicate{
			{Field: "sourcetype", Op: "=", Value: "json"},
		},
	})
	if node == nil {
		t.Fatal("expected non-nil node")
	}
	if node.Field != "_sourcetype" {
		t.Errorf("Field: got %q, want _sourcetype", node.Field)
	}
}
