package vm

import "sync/atomic"

// WarningCounters counts runtime warnings per category; it is safe for
// concurrent use.
type WarningCounters struct {
	counts [warningCategoryCount]int64
}

type warningCategory int

const (
	warnIncompatibleTypes warningCategory = iota
	warnNotOnNonBool
	warnModOnNonInt
	warnStringArithmetic
	warnTypeError
	warnCategoryCount
)

const warningCategoryCount = int(warnCategoryCount)

var warningCategoryNames = [warningCategoryCount]string{
	warnIncompatibleTypes: "incompatible_type_comparison",
	warnNotOnNonBool:      "not_on_non_bool",
	warnModOnNonInt:       "mod_on_non_int",
	warnStringArithmetic:  "string_arithmetic",
	warnTypeError:         "type_error",
}

// Increment adds one to a warning counter.
func (w *WarningCounters) Increment(cat warningCategory) {
	atomic.AddInt64(&w.counts[cat], 1)
}

// Counts returns the non-zero counters by category name.
func (w *WarningCounters) Counts() map[string]int64 {
	result := make(map[string]int64)
	for i := 0; i < warningCategoryCount; i++ {
		if v := atomic.LoadInt64(&w.counts[i]); v > 0 {
			result[warningCategoryNames[i]] = v
		}
	}
	return result
}

// Reset zeroes all counters.
func (w *WarningCounters) Reset() {
	for i := 0; i < warningCategoryCount; i++ {
		atomic.StoreInt64(&w.counts[i], 0)
	}
}
