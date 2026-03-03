package optimizer

import (
	"strings"

	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
)

// invertedIndexPruningRule extracts field=value predicates from WHERE for indexed fields.
type invertedIndexPruningRule struct{}

func (r *invertedIndexPruningRule) Name() string { return "InvertedIndexPruning" }
func (r *invertedIndexPruningRule) Description() string {
	return "Extracts field=value predicates for inverted index lookup"
}

func (r *invertedIndexPruningRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["invertedIndexPredicates"]; done {
			return q, false
		}
	}

	// Fields that have inverted index entries.
	indexedFields := map[string]bool{
		"host": true, "source": true, "_source": true,
		"sourcetype": true, "_sourcetype": true, "index": true,
	}

	var preds []spl2.InvertedIndexPredicate
	for _, cmd := range q.Commands {
		w, ok := cmd.(*spl2.WhereCommand)
		if !ok {
			continue
		}
		extractInvertedIndexPreds(w.Expr, indexedFields, &preds)
	}

	if len(preds) == 0 {
		return q, false
	}

	q.Annotate("invertedIndexPredicates", preds)

	return q, true
}

func extractInvertedIndexPreds(expr spl2.Expr, indexed map[string]bool, preds *[]spl2.InvertedIndexPredicate) {
	switch e := expr.(type) {
	case *spl2.CompareExpr:
		if e.Op != "=" && e.Op != "==" {
			return // only equality
		}
		field, ok := e.Left.(*spl2.FieldExpr)
		if !ok || !indexed[field.Name] {
			return
		}
		lit, ok := e.Right.(*spl2.LiteralExpr)
		if !ok {
			return
		}
		*preds = append(*preds, spl2.InvertedIndexPredicate{
			Field: field.Name,
			Value: strings.ToLower(lit.Value),
		})
	case *spl2.BinaryExpr:
		if strings.EqualFold(e.Op, "and") {
			extractInvertedIndexPreds(e.Left, indexed, preds)
			extractInvertedIndexPreds(e.Right, indexed, preds)
		}
	}
}
