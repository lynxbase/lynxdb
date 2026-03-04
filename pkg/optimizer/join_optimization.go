package optimizer

import (
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// JoinAnnotation stores join optimization hints.
type JoinAnnotation struct {
	Strategy     string   // "hash", "bloom_semi", "in_list"
	BuildLeft    bool     // true if left side is smaller
	InListValues []string // for in_list strategy
}

// GetStrategy returns the join strategy string.
func (ja *JoinAnnotation) GetStrategy() string { return ja.Strategy }

// joinSizeEstimationRule estimates join input sizes and annotates.
type joinSizeEstimationRule struct{}

func (r *joinSizeEstimationRule) Name() string { return "JoinSizeEstimation" }
func (r *joinSizeEstimationRule) Description() string {
	return "Estimates join input sizes and selects hash/bloom/in-list strategy"
}

func (r *joinSizeEstimationRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["joinStrategy"]; done {
			return q, false
		}
	}

	for _, cmd := range q.Commands {
		j, ok := cmd.(*spl2.JoinCommand)
		if !ok || j.Subquery == nil {
			continue
		}

		// Estimate subquery size from commands.
		strategy := estimateJoinStrategy(j.Subquery)
		q.Annotate("joinStrategy", &JoinAnnotation{
			Strategy:  strategy,
			BuildLeft: false, // default: build right (subquery)
		})

		return q, true
	}

	return q, false
}

func estimateJoinStrategy(subquery *spl2.Query) string {
	// Check if subquery has a head/limit that makes it small.
	for _, cmd := range subquery.Commands {
		if h, ok := cmd.(*spl2.HeadCommand); ok {
			if h.Count <= 1000 {
				return "in_list" // small enough for IN-list rewrite
			}
		}
	}
	// Default to hash join with bloom pre-filter for large subqueries.
	return "bloom_semi"
}
