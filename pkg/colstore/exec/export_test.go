package exec

// Pushdown exposes the scan options derived from a filter.
var Pushdown = pushdown

// SetOrderedChunk changes how many rows of a block the aggregation in key
// order loads at once, and returns a function that restores it.
func SetOrderedChunk(rows int) (restore func()) {
	old := orderedChunk
	orderedChunk = rows

	return func() { orderedChunk = old }
}

// SetLimits changes when a worker's table spills and when workers' tables
// are merged in partitions, and returns a function that restores both.
func SetLimits(tableGroups, serial int) (restore func()) {
	oldTable, oldSerial := maxTableGroups, serialGroups
	maxTableGroups, serialGroups = tableGroups, serial

	return func() { maxTableGroups, serialGroups = oldTable, oldSerial }
}
