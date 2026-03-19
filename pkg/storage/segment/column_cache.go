package segment

// ColumnCache provides decoded column caching to avoid repeated decompression.
// When set on a Reader, column read methods check the cache before decompressing.
//
// Keys are (segmentID, rowGroupIndex, columnName). The segmentID is set by the
// caller via SetColumnCache so the cache can distinguish columns across segments.
type ColumnCache interface {
	GetStrings(segID string, rgIdx int, col string) ([]string, bool)
	PutStrings(segID string, rgIdx int, col string, data []string)
	GetInt64s(segID string, rgIdx int, col string) ([]int64, bool)
	PutInt64s(segID string, rgIdx int, col string, data []int64)
	GetFloat64s(segID string, rgIdx int, col string) ([]float64, bool)
	PutFloat64s(segID string, rgIdx int, col string, data []float64)
	InvalidateSegment(segID string)
}
