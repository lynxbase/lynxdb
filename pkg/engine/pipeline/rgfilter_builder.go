package pipeline

import (
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

// BuildRGFilter translates SegmentStreamHints into an RGFilterNode tree for
// row-group-level pruning. Called once per query during iterator construction.
// Returns nil when no prunable predicates exist (callers skip RG filtering).
//
// All predicate sources are merged under a single AND root: SearchTermTree
// (recursive bloom check), flat SearchTerms fallback, FieldPreds, RangePreds,
// and InvertedPreds (field=value with bloom terms).
func BuildRGFilter(hints *SegmentStreamHints) *segment.RGFilterNode {
	if hints == nil {
		return nil
	}

	var children []segment.RGFilterNode

	// Structured search term tree (supports OR/AND).
	if hints.SearchTermTree != nil {
		if node := convertSearchTermTree(hints.SearchTermTree); node != nil {
			children = append(children, *node)
		}
	} else if len(hints.SearchTerms) > 0 {
		// Flat search terms fallback (AND semantics on _raw bloom).
		for _, term := range hints.SearchTerms {
			children = append(children, segment.RGFilterNode{
				Op:    segment.RGFilterTerm,
				Terms: []string{term},
			})
		}
	}

	// Field predicates from WHERE clauses.
	for _, fp := range hints.FieldPreds {
		physField := normalizeField(fp.Field)
		switch fp.Op {
		case "=", "==":
			children = append(children, segment.RGFilterNode{
				Op:    segment.RGFilterFieldEq,
				Field: physField,
				Value: fp.Value,
				Terms: index.TokenizeUnique(fp.Value),
			})
		case "!=":
			children = append(children, segment.RGFilterNode{
				Op:    segment.RGFilterFieldNeq,
				Field: physField,
				Value: fp.Value,
			})
		case ">", ">=", "<", "<=":
			children = append(children, segment.RGFilterNode{
				Op:       segment.RGFilterFieldRange,
				Field:    physField,
				RangeOp:  fp.Op,
				RangeVal: fp.Value,
			})
		}
	}

	// Range predicates (Min/Max bounds).
	for _, rp := range hints.RangePreds {
		physField := normalizeField(rp.Field)
		if rp.Min != "" {
			children = append(children, segment.RGFilterNode{
				Op:       segment.RGFilterFieldRange,
				Field:    physField,
				RangeOp:  ">=",
				RangeVal: rp.Min,
			})
		}
		if rp.Max != "" {
			children = append(children, segment.RGFilterNode{
				Op:       segment.RGFilterFieldRange,
				Field:    physField,
				RangeOp:  "<=",
				RangeVal: rp.Max,
			})
		}
	}

	// IN predicates from WHERE field IN (val1, val2, ...).
	for _, ip := range hints.InPreds {
		physField := normalizeField(ip.Field)
		// Tokenize all values and deduplicate for bloom filter terms.
		var allTerms []string
		for _, v := range ip.Values {
			allTerms = append(allTerms, index.TokenizeUnique(v)...)
		}
		children = append(children, segment.RGFilterNode{
			Op:     segment.RGFilterFieldIn,
			Field:  physField,
			Values: ip.Values,
			Terms:  deduplicateStrings(allTerms),
		})
	}

	// Inverted index field=value predicates (field bloom check).
	for _, ip := range hints.InvertedPreds {
		physField := normalizeField(ip.Field)
		children = append(children, segment.RGFilterNode{
			Op:    segment.RGFilterFieldEq,
			Field: physField,
			Value: ip.Value,
			Terms: index.TokenizeUnique(ip.Value),
		})
	}

	if len(children) == 0 {
		return nil
	}
	if len(children) == 1 {
		return &children[0]
	}

	return &segment.RGFilterNode{
		Op:       segment.RGFilterAnd,
		Children: children,
	}
}

// convertSearchTermTree recursively converts a spl2.SearchTermTree into an
// RGFilterNode tree. Leaf nodes become RGFilterTerm (checking _raw bloom).
// Returns nil when the subtree produces no constraint.
func convertSearchTermTree(tree *spl2.SearchTermTree) *segment.RGFilterNode {
	if tree == nil {
		return nil
	}

	switch tree.Op {
	case spl2.SearchTermLeaf:
		if len(tree.Terms) == 0 {
			return nil
		}

		return &segment.RGFilterNode{
			Op:    segment.RGFilterTerm,
			Terms: tree.Terms,
		}

	case spl2.SearchTermAnd:
		var children []segment.RGFilterNode
		for _, child := range tree.Children {
			node := convertSearchTermTree(child)
			if node != nil {
				children = append(children, *node)
			}
		}
		if len(children) == 0 {
			return nil
		}
		if len(children) == 1 {
			return &children[0]
		}

		return &segment.RGFilterNode{
			Op:       segment.RGFilterAnd,
			Children: children,
		}

	case spl2.SearchTermOr:
		var children []segment.RGFilterNode
		for _, child := range tree.Children {
			node := convertSearchTermTree(child)
			if node == nil {
				// One OR branch matches everything → whole OR is unconstrained.
				return nil
			}
			children = append(children, *node)
		}
		if len(children) == 0 {
			return nil
		}
		if len(children) == 1 {
			return &children[0]
		}

		return &segment.RGFilterNode{
			Op:       segment.RGFilterOr,
			Children: children,
		}
	}

	return nil
}

// normalizeField maps virtual field aliases to physical column names.
// "source" → "_source" (matching physical column names in segments).
func normalizeField(name string) string {
	if name == "source" {
		return "_source"
	}
	if name == "sourcetype" {
		return "_sourcetype"
	}

	return name
}

// deduplicateStrings returns a new slice with duplicate strings removed.
// Preserves order of first occurrence.
func deduplicateStrings(ss []string) []string {
	if len(ss) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(ss))
	result := make([]string, 0, len(ss))
	for _, s := range ss {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			result = append(result, s)
		}
	}

	return result
}
