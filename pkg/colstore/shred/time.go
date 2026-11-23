package shred

import (
	"math"
	"strconv"
	"unsafe"

	"github.com/lynxbase/lynxdb/pkg/timestamp"
)

// epochFloat converts an epoch with a fraction, in the unit that its size
// tells as EpochNanos does, to nanoseconds. A float holds about sixteen
// digits, so the fraction is taken to the microsecond.
func epochFloat(v float64) (int64, bool) {
	abs := math.Abs(v)
	if abs != abs || abs > 9e18 {
		return 0, false
	}
	whole, frac := math.Modf(v)
	switch {
	case abs < 1e11:
		return int64(whole)*1e9 + int64(math.Round(frac*1e6))*1e3, true
	case abs < 1e14:
		return int64(whole)*1e6 + int64(math.Round(frac*1e3))*1e3, true
	case abs < 1e17:
		return int64(whole)*1e3 + int64(math.Round(frac))*1e3, true
	}

	return int64(whole), true
}

// ParseTime reads a time as text, in a form that timestamp.Parse reads, or
// a number as an epoch.
func ParseTime(s []byte) (int64, bool) {
	text := unsafe.String(unsafe.SliceData(s), len(s))
	if t, ok := timestamp.Parse(text); ok {
		return t, true
	}
	if len(s) == 0 || len(s) > 40 {
		return 0, false
	}
	if n, err := strconv.ParseInt(text, 10, 64); err == nil {
		return EpochNanos(n), true
	}
	if f, err := strconv.ParseFloat(text, 64); err == nil {
		return epochFloat(f)
	}

	return 0, false
}
