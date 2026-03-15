package usecases

import (
	"math"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
)

// ParseTimeParam parses a relative or absolute time string.
// Relative strings use a leading "-" followed by a duration (e.g. "-1h", "-30d").
// The duration supports all Go units (ns, us, ms, s, m, h) plus "d" for days.
func ParseTimeParam(s string, now time.Time) (time.Time, error) {
	if s == "now" {
		return now, nil
	}
	if s != "" && s[0] == '-' {
		dur, err := config.ParseDuration(s[1:])
		if err != nil {
			return time.Time{}, err
		}

		return now.Add(-dur.Duration()), nil
	}

	return time.Parse(time.RFC3339, s)
}

// SnapInterval rounds an interval to the nearest standard bucket size.
func SnapInterval(d time.Duration) time.Duration {
	standards := []time.Duration{
		time.Second,
		5 * time.Second,
		30 * time.Second,
		time.Minute,
		5 * time.Minute,
		15 * time.Minute,
		time.Hour,
		6 * time.Hour,
		24 * time.Hour,
	}
	best := standards[0]
	bestDiff := math.Abs(float64(d - best))
	for _, s := range standards[1:] {
		diff := math.Abs(float64(d - s))
		if diff < bestDiff {
			best = s
			bestDiff = diff
		}
	}

	return best
}
