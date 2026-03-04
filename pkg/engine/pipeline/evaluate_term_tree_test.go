package pipeline

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

// buildTestSerializedIndex creates a SerializedIndex with the given term->docIDs mapping.
func buildTestSerializedIndex(t *testing.T, termDocs map[string][]uint32) *index.SerializedIndex {
	t.Helper()
	idx := index.NewInvertedIndex()
	for term, docs := range termDocs {
		for _, docID := range docs {
			idx.Add(docID, term) // Add indexes all tokens from text; using single-token terms here.
		}
	}
	data, err := idx.Encode()
	if err != nil {
		t.Fatalf("encode inverted index: %v", err)
	}
	si, err := index.DecodeInvertedIndex(data)
	if err != nil {
		t.Fatalf("decode inverted index: %v", err)
	}

	return si
}

func TestEvaluateTermTree_Leaf(t *testing.T) {
	si := buildTestSerializedIndex(t, map[string][]uint32{
		"error": {1, 2, 3, 5},
	})

	tree := &spl2.SearchTermTree{
		Op:    spl2.SearchTermLeaf,
		Terms: []string{"error"},
	}

	bm := evaluateTermTree(tree, si)
	if bm == nil {
		t.Fatal("expected non-nil bitmap")
	}
	if bm.GetCardinality() != 4 {
		t.Errorf("expected 4 hits, got %d", bm.GetCardinality())
	}
}

func TestEvaluateTermTree_LeafMultipleTerms(t *testing.T) {
	// Leaf with multiple terms — AND within the leaf.
	si := buildTestSerializedIndex(t, map[string][]uint32{
		"error":   {1, 2, 3, 5},
		"timeout": {2, 3, 4},
	})

	tree := &spl2.SearchTermTree{
		Op:    spl2.SearchTermLeaf,
		Terms: []string{"error", "timeout"},
	}

	bm := evaluateTermTree(tree, si)
	if bm == nil {
		t.Fatal("expected non-nil bitmap")
	}
	// Intersection of {1,2,3,5} and {2,3,4} = {2,3}
	if bm.GetCardinality() != 2 {
		t.Errorf("expected 2 hits, got %d", bm.GetCardinality())
	}
}

func TestEvaluateTermTree_Or(t *testing.T) {
	si := buildTestSerializedIndex(t, map[string][]uint32{
		"png": {1, 2},
		"htm": {3, 4},
	})

	tree := &spl2.SearchTermTree{
		Op: spl2.SearchTermOr,
		Children: []*spl2.SearchTermTree{
			{Op: spl2.SearchTermLeaf, Terms: []string{"png"}},
			{Op: spl2.SearchTermLeaf, Terms: []string{"htm"}},
		},
	}

	bm := evaluateTermTree(tree, si)
	if bm == nil {
		t.Fatal("expected non-nil bitmap")
	}
	// Union of {1,2} and {3,4} = {1,2,3,4}
	if bm.GetCardinality() != 4 {
		t.Errorf("expected 4 hits, got %d", bm.GetCardinality())
	}
}

func TestEvaluateTermTree_And(t *testing.T) {
	si := buildTestSerializedIndex(t, map[string][]uint32{
		"error":   {1, 2, 3, 5},
		"timeout": {2, 3, 4},
	})

	tree := &spl2.SearchTermTree{
		Op: spl2.SearchTermAnd,
		Children: []*spl2.SearchTermTree{
			{Op: spl2.SearchTermLeaf, Terms: []string{"error"}},
			{Op: spl2.SearchTermLeaf, Terms: []string{"timeout"}},
		},
	}

	bm := evaluateTermTree(tree, si)
	if bm == nil {
		t.Fatal("expected non-nil bitmap")
	}
	// Intersection of {1,2,3,5} and {2,3,4} = {2,3}
	if bm.GetCardinality() != 2 {
		t.Errorf("expected 2 hits, got %d", bm.GetCardinality())
	}
}

func TestEvaluateTermTree_NestedORinAND(t *testing.T) {
	si := buildTestSerializedIndex(t, map[string][]uint32{
		"error":   {1, 2, 3},
		"timeout": {2, 3, 4},
		"png":     {3, 5, 6},
	})

	// (error AND timeout) OR png = {2,3} ∪ {3,5,6} = {2,3,5,6}
	tree := &spl2.SearchTermTree{
		Op: spl2.SearchTermOr,
		Children: []*spl2.SearchTermTree{
			{
				Op: spl2.SearchTermAnd,
				Children: []*spl2.SearchTermTree{
					{Op: spl2.SearchTermLeaf, Terms: []string{"error"}},
					{Op: spl2.SearchTermLeaf, Terms: []string{"timeout"}},
				},
			},
			{Op: spl2.SearchTermLeaf, Terms: []string{"png"}},
		},
	}

	bm := evaluateTermTree(tree, si)
	if bm == nil {
		t.Fatal("expected non-nil bitmap")
	}
	if bm.GetCardinality() != 4 {
		t.Errorf("expected 4 hits ({2,3,5,6}), got %d", bm.GetCardinality())
	}
	for _, expected := range []uint32{2, 3, 5, 6} {
		if !bm.Contains(expected) {
			t.Errorf("expected bitmap to contain %d", expected)
		}
	}
}

func TestEvaluateTermTree_NilChild_Or(t *testing.T) {
	si := buildTestSerializedIndex(t, map[string][]uint32{
		"png": {1, 2},
	})

	// OR with a nil child → return nil (match everything).
	tree := &spl2.SearchTermTree{
		Op: spl2.SearchTermOr,
		Children: []*spl2.SearchTermTree{
			{Op: spl2.SearchTermLeaf, Terms: []string{"png"}},
			nil, // nil child
		},
	}

	bm := evaluateTermTree(tree, si)
	if bm != nil {
		t.Errorf("expected nil bitmap when OR has nil child, got cardinality=%d", bm.GetCardinality())
	}
}

func TestEvaluateTermTree_Nil(t *testing.T) {
	si := buildTestSerializedIndex(t, map[string][]uint32{
		"error": {1},
	})

	bm := evaluateTermTree(nil, si)
	if bm != nil {
		t.Error("expected nil bitmap for nil tree")
	}
}

func TestEvaluateTermTree_EmptyLeaf(t *testing.T) {
	si := buildTestSerializedIndex(t, map[string][]uint32{
		"error": {1, 2},
	})

	// Leaf with empty terms → nil result (no constraint).
	tree := &spl2.SearchTermTree{
		Op:    spl2.SearchTermLeaf,
		Terms: []string{},
	}

	bm := evaluateTermTree(tree, si)
	if bm != nil {
		t.Errorf("expected nil bitmap for empty leaf, got cardinality=%d", bm.GetCardinality())
	}
}
