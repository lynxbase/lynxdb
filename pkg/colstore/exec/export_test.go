package exec

// Pushdown exposes the scan options derived from a filter.
var Pushdown = pushdown

// SetOrderedTokenShare changes the share of blocks that token filters must
// admit for aggregation in key order, and returns a function that restores
// it.
func SetOrderedTokenShare(share float64) (restore func()) {
	old := orderedTokenShare
	orderedTokenShare = share

	return func() { orderedTokenShare = old }
}

// SetLimits changes when a worker's table spills and when workers' tables
// are merged in partitions, and returns a function that restores both.
func SetLimits(tableGroups, serial int) (restore func()) {
	oldTable, oldSerial := maxTableGroups, serialGroups
	maxTableGroups, serialGroups = tableGroups, serial

	return func() { maxTableGroups, serialGroups = oldTable, oldSerial }
}
