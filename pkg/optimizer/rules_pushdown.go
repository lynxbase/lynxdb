package optimizer

import (
	"strings"

	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment/index"
)

type predicatePushdownRule struct{}

func (r *predicatePushdownRule) Name() string { return "PredicatePushdown" }
func (r *predicatePushdownRule) Description() string {
	return "Moves WHERE filters before EVAL/RENAME to reduce rows early"
}
func (r *predicatePushdownRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	for i := 1; i < len(q.Commands); i++ {
		w, isWhere := q.Commands[i].(*spl2.WhereCommand)
		if !isWhere {
			continue
		}
		whereFields := collectFieldNames(w.Expr)
		prev := q.Commands[i-1]

		switch p := prev.(type) {
		case *spl2.EvalCommand:
			// Move WHERE before EVAL if WHERE doesn't depend on EVAL's output.
			if len(p.Assignments) > 0 {
				depends := false
				for _, a := range p.Assignments {
					if whereFields[a.Field] {
						depends = true

						break
					}
				}
				if depends {
					continue
				}
			} else if whereFields[p.Field] {
				continue
			}
			q.Commands[i-1] = w
			q.Commands[i] = p

			return q, true

		case *spl2.RenameCommand:
			// Push WHERE through RENAME by adjusting field references.
			// Build reverse rename map: new → old.
			reverseMap := make(map[string]string)
			for _, r := range p.Renames {
				reverseMap[r.New] = r.Old
			}
			// Skip if WHERE references a renamed field.
			usesRenamed := false
			for f := range whereFields {
				if _, ok := reverseMap[f]; ok {
					usesRenamed = true

					break
				}
			}
			if !usesRenamed {
				// No overlap — safe to swap directly.
				q.Commands[i-1] = w
				q.Commands[i] = p

				return q, true
			}
			// Adjust WHERE expr with reverse renames then swap.
			newExpr := substituteFieldNames(w.Expr, reverseMap)
			q.Commands[i-1] = &spl2.WhereCommand{Expr: newExpr}
			q.Commands[i] = p

			return q, true

		case *spl2.FillnullCommand:
			// Push WHERE through FILLNULL — fillnull doesn't filter or reorder.
			q.Commands[i-1] = w
			q.Commands[i] = p

			return q, true
		}
	}

	return q, false
}

// substituteFieldNames replaces field references using the given name map.
func substituteFieldNames(expr spl2.Expr, nameMap map[string]string) spl2.Expr {
	switch e := expr.(type) {
	case *spl2.FieldExpr:
		if newName, ok := nameMap[e.Name]; ok {
			return &spl2.FieldExpr{Name: newName}
		}

		return e
	case *spl2.CompareExpr:
		return &spl2.CompareExpr{
			Left:  substituteFieldNames(e.Left, nameMap),
			Op:    e.Op,
			Right: substituteFieldNames(e.Right, nameMap),
		}
	case *spl2.BinaryExpr:
		return &spl2.BinaryExpr{
			Left:  substituteFieldNames(e.Left, nameMap),
			Op:    e.Op,
			Right: substituteFieldNames(e.Right, nameMap),
		}
	case *spl2.ArithExpr:
		return &spl2.ArithExpr{
			Left:  substituteFieldNames(e.Left, nameMap),
			Op:    e.Op,
			Right: substituteFieldNames(e.Right, nameMap),
		}
	case *spl2.NotExpr:
		return &spl2.NotExpr{Expr: substituteFieldNames(e.Expr, nameMap)}
	case *spl2.FuncCallExpr:
		args := make([]spl2.Expr, len(e.Args))
		for i, arg := range e.Args {
			args[i] = substituteFieldNames(arg, nameMap)
		}

		return &spl2.FuncCallExpr{Name: e.Name, Args: args}
	default:
		return expr
	}
}

type projectionPushdownRule struct{}

func (r *projectionPushdownRule) Name() string { return "ProjectionPushdown" }
func (r *projectionPushdownRule) Description() string {
	return "Inserts early FIELDS before expensive operators to reduce column width"
}

// Apply is implemented in columns.go

type columnPruningRule struct{}

func (r *columnPruningRule) Name() string { return "ColumnPruning" }
func (r *columnPruningRule) Description() string {
	return "Computes minimal column set and skips unused columns during scan"
}

// Apply is implemented in columns.go

type filterPushdownIntoJoinRule struct{}

func (r *filterPushdownIntoJoinRule) Name() string { return "FilterPushdownIntoJoin" }
func (r *filterPushdownIntoJoinRule) Description() string {
	return "Pushes WHERE into JOIN subquery to reduce build-side rows"
}
func (r *filterPushdownIntoJoinRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	// Look for WHERE immediately after JOIN. If WHERE references only
	// the join field, push a copy into the subquery.
	for i := 1; i < len(q.Commands); i++ {
		w, isWhere := q.Commands[i].(*spl2.WhereCommand)
		if !isWhere {
			continue
		}
		j, isJoin := q.Commands[i-1].(*spl2.JoinCommand)
		if !isJoin || j.Subquery == nil {
			continue
		}
		// Push WHERE into subquery by appending it.
		j.Subquery.Commands = append(j.Subquery.Commands, w)
		// Keep WHERE in the outer pipeline too for correctness.
		return q, true
	}

	return q, false
}

type unionFilterPushdownRule struct{}

func (r *unionFilterPushdownRule) Name() string { return "UnionFilterPushdown" }
func (r *unionFilterPushdownRule) Description() string {
	return "Pushes WHERE into APPEND/MULTISEARCH branches"
}
func (r *unionFilterPushdownRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	// Look for WHERE after APPEND or MULTISEARCH and push into branches.
	for i := 1; i < len(q.Commands); i++ {
		w, isWhere := q.Commands[i].(*spl2.WhereCommand)
		if !isWhere {
			continue
		}
		switch c := q.Commands[i-1].(type) {
		case *spl2.AppendCommand:
			if c.Subquery != nil {
				c.Subquery.Commands = append(c.Subquery.Commands, w)

				return q, true
			}
		case *spl2.MultisearchCommand:
			for _, sub := range c.Searches {
				sub.Commands = append(sub.Commands, w)
			}

			return q, true
		}
	}

	return q, false
}

type timeRangePruningRule struct{}

func (r *timeRangePruningRule) Name() string { return "TimeRangePruning" }
func (r *timeRangePruningRule) Description() string {
	return "Extracts _time bounds for segment-level time range skip"
}
func (r *timeRangePruningRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["timeAnnotation"]; done {
			return q, false
		}
	}
	earliest, latest := "", ""
	found := false
	for _, cmd := range q.Commands {
		w, ok := cmd.(*spl2.WhereCommand)
		if !ok {
			continue
		}
		e, l, ok2 := extractTimeBoundsFromExpr(w.Expr)
		if ok2 {
			found = true
			if e != "" {
				earliest = e
			}
			if l != "" {
				latest = l
			}
		}
	}
	if !found {
		return q, false
	}
	q.Annotate("timeAnnotation", map[string]string{
		"earliest": earliest,
		"latest":   latest,
	})

	return q, true
}

// extractTimeBoundsFromExpr returns earliest, latest, and whether any were found.
func extractTimeBoundsFromExpr(expr spl2.Expr) (earliest, latest string, found bool) {
	switch e := expr.(type) {
	case *spl2.CompareExpr:
		field, ok := e.Left.(*spl2.FieldExpr)
		if !ok || field.Name != "_time" {
			return "", "", false
		}
		lit, ok := e.Right.(*spl2.LiteralExpr)
		if !ok {
			return "", "", false
		}
		switch e.Op {
		case ">=", ">":
			return lit.Value, "", true
		case "<=", "<":
			return "", lit.Value, true
		case "=", "==":
			return lit.Value, lit.Value, true
		}
	case *spl2.BinaryExpr:
		if strings.EqualFold(e.Op, "and") {
			e1, l1, f1 := extractTimeBoundsFromExpr(e.Left)
			e2, l2, f2 := extractTimeBoundsFromExpr(e.Right)
			if e2 != "" {
				e1 = e2
			}
			if l2 != "" {
				l1 = l2
			}

			return e1, l1, f1 || f2
		}
	}

	return "", "", false
}

type bloomFilterPruningRule struct{}

func (r *bloomFilterPruningRule) Name() string { return "BloomFilterPruning" }
func (r *bloomFilterPruningRule) Description() string {
	return "Extracts bloom filter terms for segment-level skip"
}
func (r *bloomFilterPruningRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["bloomTerms"]; done {
			return q, false
		}
	}

	// Fields whose values are bloom-indexed.
	bloomFields := map[string]bool{
		"_raw": true, "host": true, "source": true, "_source": true,
		"sourcetype": true, "_sourcetype": true,
	}

	var terms []string
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *spl2.WhereCommand:
			extractBloomTermsFromExpr(c.Expr, bloomFields, &terms)
		case *spl2.SearchCommand:
			// Search terms are already extracted by hints — also capture
			// field-specific comparisons from SearchExpr.
			if c.Expression != nil {
				extractBloomTermsFromSearch(c.Expression, bloomFields, &terms)
			}
			// Also extract from SearchCommand.Term via tokenization.
			if c.Term != "" {
				tokens := index.Tokenize(c.Term)
				terms = append(terms, tokens...)
			}
		}
	}
	if len(terms) == 0 {
		return q, false
	}
	q.Annotate("bloomTerms", terms)

	return q, true
}

// extractBloomTermsFromExpr extracts bloom-eligible terms from WHERE expressions.
func extractBloomTermsFromExpr(expr spl2.Expr, bloomFields map[string]bool, terms *[]string) {
	switch e := expr.(type) {
	case *spl2.CompareExpr:
		if e.Op == "=" || e.Op == "==" {
			if field, ok := e.Left.(*spl2.FieldExpr); ok {
				if bloomFields[field.Name] {
					if lit, ok := e.Right.(*spl2.LiteralExpr); ok {
						*terms = append(*terms, strings.ToLower(lit.Value))
					}
				}
			}
		}
	case *spl2.BinaryExpr:
		if strings.EqualFold(e.Op, "and") {
			extractBloomTermsFromExpr(e.Left, bloomFields, terms)
			extractBloomTermsFromExpr(e.Right, bloomFields, terms)
		}
		// For OR, we can't safely extract terms for bloom pruning.
	}
}

// extractBloomTermsFromSearch extracts bloom-eligible terms from search expressions.
func extractBloomTermsFromSearch(expr spl2.SearchExpr, bloomFields map[string]bool, terms *[]string) {
	switch e := expr.(type) {
	case *spl2.SearchAndExpr:
		extractBloomTermsFromSearch(e.Left, bloomFields, terms)
		extractBloomTermsFromSearch(e.Right, bloomFields, terms)
	case *spl2.SearchCompareExpr:
		if e.Op == spl2.OpEq && !e.HasWildcard && bloomFields[e.Field] {
			*terms = append(*terms, strings.ToLower(e.Value))
		}
	}
}

type columnStatsPruningRule struct{}

func (r *columnStatsPruningRule) Name() string { return "ColumnStatsPruning" }
func (r *columnStatsPruningRule) Description() string {
	return "Uses column min/max stats to skip segments that cannot match"
}
func (r *columnStatsPruningRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["fieldPredicates"]; done {
			return q, false
		}
	}

	var preds []FieldPredInfo
	for _, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *spl2.WhereCommand:
			extractFieldPredicatesFromExpr(c.Expr, &preds)
		case *spl2.SearchCommand:
			if c.Expression != nil {
				extractFieldPredicatesFromSearch(c.Expression, &preds)
			}
		}
	}
	if len(preds) == 0 {
		return q, false
	}
	q.Annotate("fieldPredicates", preds)

	return q, true
}

// FieldPredInfo holds a simple field op literal predicate for annotation.
type FieldPredInfo struct {
	Field string
	Op    string
	Value string
}

// extractFieldPredicatesFromSearch walks a SearchExpr and extracts field=value
// and field op value comparisons that can be used for segment column stats pruning.
// Only extracts from AND branches (conjunctive). OR and NOT are skipped because
// they cannot be safely pushed as conjunctive predicates.
func extractFieldPredicatesFromSearch(expr spl2.SearchExpr, preds *[]FieldPredInfo) {
	switch e := expr.(type) {
	case *spl2.SearchAndExpr:
		extractFieldPredicatesFromSearch(e.Left, preds)
		extractFieldPredicatesFromSearch(e.Right, preds)
	case *spl2.SearchCompareExpr:
		// Skip wildcard values — they cannot be used for min/max pruning.
		if e.HasWildcard {
			return
		}
		*preds = append(*preds, FieldPredInfo{
			Field: e.Field,
			Op:    e.Op.String(),
			Value: e.Value,
		})
	case *spl2.SearchOrExpr:
		// OR cannot be pushed as conjunctive predicates — skip.
	case *spl2.SearchNotExpr:
		// NOT complicates min/max checks — skip.
	}
}

func extractFieldPredicatesFromExpr(expr spl2.Expr, preds *[]FieldPredInfo) {
	switch e := expr.(type) {
	case *spl2.CompareExpr:
		field, ok := e.Left.(*spl2.FieldExpr)
		if !ok {
			return
		}
		lit, ok := e.Right.(*spl2.LiteralExpr)
		if !ok {
			return
		}
		if field.Name == "_time" {
			return // time handled by time range rule
		}
		*preds = append(*preds, FieldPredInfo{
			Field: field.Name, Op: e.Op, Value: lit.Value,
		})
	case *spl2.BinaryExpr:
		if strings.EqualFold(e.Op, "and") {
			extractFieldPredicatesFromExpr(e.Left, preds)
			extractFieldPredicatesFromExpr(e.Right, preds)
		}
	}
}

type partitionPruningRule struct{}

func (r *partitionPruningRule) Name() string { return "PartitionPruning" }
func (r *partitionPruningRule) Description() string {
	return "Skips partitions that cannot match the query predicates"
}
func (r *partitionPruningRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	return q, false // Not yet partitioned
}
