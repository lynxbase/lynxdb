package optimizer

import (
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// TopKAggAnnotation is stored in q.Annotations["topKAgg"].
type TopKAggAnnotation struct {
	K          int
	SortFields []spl2.SortField
	StatsIdx   int
}

// topKAggRule detects stats+sort+head → emits topKAgg annotation.
type topKAggRule struct{}

func (r *topKAggRule) Name() string { return "TopKAggregation" }
func (r *topKAggRule) Description() string {
	return "Detects stats+sort+head pattern for heap-based TopK execution"
}

func (r *topKAggRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["topKAgg"]; done {
			return q, false
		}
	}

	// Look for stats followed by sort followed by head pattern.
	if len(q.Commands) < 3 {
		return q, false
	}

	for i := 0; i+2 < len(q.Commands); i++ {
		stats, isStats := q.Commands[i].(*spl2.StatsCommand)
		sortCmd, isSort := q.Commands[i+1].(*spl2.SortCommand)
		head, isHead := q.Commands[i+2].(*spl2.HeadCommand)

		if !isStats || !isSort || !isHead {
			continue
		}

		// Check that sort fields are aggregation outputs.
		if len(sortCmd.Fields) == 0 || head.Count <= 0 {
			continue
		}

		// All aggs must be monotonic for TopK optimization.
		allMonotonic := true
		for _, agg := range stats.Aggregations {
			if !isMonotonicAgg(agg.Func) {
				allMonotonic = false

				break
			}
		}

		if !allMonotonic {
			continue
		}

		q.Annotate("topKAgg", &TopKAggAnnotation{
			K:          head.Count,
			SortFields: sortCmd.Fields,
			StatsIdx:   i,
		})

		return q, true
	}

	return q, false
}

func isMonotonicAgg(name string) bool {
	switch name {
	case "count", "sum", "min", "max": //nolint:goconst // domain literals in switch, constant adds no clarity
		return true
	default:
		return false
	}
}
