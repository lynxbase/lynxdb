package exec

// Pushdown exposes the scan options derived from a filter.
var Pushdown = pushdown

// SetLimits changes when a worker's table spills and when workers' tables
// are merged in partitions, and returns a function that restores both.
func SetLimits(tableGroups, serial int) (restore func()) {
	oldTable, oldSerial := maxTableGroups, serialGroups
	maxTableGroups, serialGroups = tableGroups, serial

	return func() { maxTableGroups, serialGroups = oldTable, oldSerial }
}
