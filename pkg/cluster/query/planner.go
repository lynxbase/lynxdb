package query

import (
	"fmt"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
)

// MergeStrategy describes how shard results are combined.
type MergeStrategy int

const (
	MergeConcat MergeStrategy = iota
	MergePartialAgg
	MergeTopK
)

func (s MergeStrategy) String() string {
	switch s {
	case MergeConcat:
		return "concat"
	case MergePartialAgg:
		return "partial_agg"
	case MergeTopK:
		return "topk"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// DistributedPlan is what the coordinator fans out to shards: the query
// text they compile and the merge strategy for their results.
type DistributedPlan struct {
	ShardQuery     string
	Strategy       MergeStrategy
	PartialAggSpec *pipeline.PartialAggSpec
	TopK           int
	TopKSortFields []pipeline.SortField
	SplitIndex     int
}
