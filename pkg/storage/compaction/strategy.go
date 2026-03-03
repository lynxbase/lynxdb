package compaction

// Strategy produces compaction plans for a set of segments.
type Strategy interface {
	// Plan returns zero or more compaction plans for the given segments.
	Plan(segments []*SegmentInfo) []*Plan
}

// JobPriority determines the scheduling order for compaction jobs.
type JobPriority int

const (
	PriorityL0ToL1    JobPriority = 0 // highest — flush pressure
	PriorityL1ToL2Hot JobPriority = 1 // hot data, recent queries
	PriorityL1ToL2    JobPriority = 2 // warm data
	PriorityMaint     JobPriority = 3 // lowest — maintenance
)

// Job wraps a Plan with scheduling metadata.
type Job struct {
	Plan     *Plan
	Priority JobPriority
	Index    string
}
