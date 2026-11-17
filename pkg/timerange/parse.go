package timerange

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// TimeRange represents a time window for query filtering.
type TimeRange struct {
	Earliest time.Time
	Latest   time.Time
}

// ParseRelative parses relative time strings: "15m", "1h", "4h", "1d", "7d", "30d", "1w".
// Returns the duration to subtract from now.
func ParseRelative(s string) (time.Duration, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if len(s) < 2 {
		return 0, fmt.Errorf("invalid relative time: %q", s)
	}
	numStr := s[:len(s)-1]
	unit := s[len(s)-1]
	n, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, fmt.Errorf("invalid relative time: %q: %w", s, err)
	}
	switch unit {
	case 's':
		return time.Duration(n) * time.Second, nil
	case 'm':
		return time.Duration(n) * time.Minute, nil
	case 'h':
		return time.Duration(n) * time.Hour, nil
	case 'd':
		return time.Duration(n) * 24 * time.Hour, nil
	case 'w':
		return time.Duration(n) * 7 * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unknown time unit: %c in %q", unit, s)
	}
}

// ParseAbsolute parses absolute time strings in various formats.
func ParseAbsolute(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse time: %q", s)
}

// Parse converts a relative or absolute time string to time.Time.
// Relative: "-1h", "-7d", "-30m", "now", "-5m".
// Absolute: ISO 8601 ("2026-02-14T14:00:00Z").
// Snap-to: "-1h@h" (start of hour), "-1d@d" (start of day).
func Parse(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty time string")
	}

	// "now" returns the current time directly.
	if strings.EqualFold(s, "now") {
		return now, nil
	}

	// Relative time with leading "-".
	if s[0] == '-' {
		body := s[1:]

		var snapUnit byte
		if idx := strings.LastIndex(body, "@"); idx >= 0 {
			snapPart := body[idx+1:]
			if len(snapPart) != 1 {
				return time.Time{}, fmt.Errorf("invalid snap-to unit in %q", s)
			}
			snapUnit = snapPart[0]
			body = body[:idx]
		}

		dur, err := ParseRelative(body)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse relative time %q: %w", s, err)
		}
		result := now.Add(-dur)

		if snapUnit != 0 {
			result = snapTo(result, snapUnit)
		}

		return result, nil
	}

	// Try absolute parse.
	return ParseAbsolute(s)
}

// ParseRange parses from/to strings into a TimeRange, applying defaults.
// Default from: "-15m", default to: "now".
func ParseRange(from, to string, now time.Time) (*TimeRange, error) {
	from = strings.TrimSpace(from)
	to = strings.TrimSpace(to)
	if from == "" {
		from = "-15m"
	}
	if to == "" {
		to = "now"
	}
	earliest, err := Parse(from, now)
	if err != nil {
		return nil, fmt.Errorf("parse from %q: %w", from, err)
	}
	latest, err := Parse(to, now)
	if err != nil {
		return nil, fmt.Errorf("parse to %q: %w", to, err)
	}

	return &TimeRange{
		Earliest: earliest,
		Latest:   latest,
	}, nil
}

// ParseOptionalRange parses optional from/to strings without applying defaults.
// If both inputs are empty it returns nil. If only one side is provided, the
// other side remains the zero time.
func ParseOptionalRange(from, to string, now time.Time) (*TimeRange, error) {
	from = strings.TrimSpace(from)
	to = strings.TrimSpace(to)
	if from == "" && to == "" {
		return nil, nil
	}

	var tr TimeRange
	if from != "" {
		t, err := Parse(from, now)
		if err != nil {
			return nil, fmt.Errorf("parse from %q: %w", from, err)
		}
		tr.Earliest = t
	}
	if to != "" {
		t, err := Parse(to, now)
		if err != nil {
			return nil, fmt.Errorf("parse to %q: %w", to, err)
		}
		tr.Latest = t
	}
	if !tr.Earliest.IsZero() && !tr.Latest.IsZero() && tr.Earliest.After(tr.Latest) {
		return nil, fmt.Errorf("from must be before to")
	}

	return &tr, nil
}

// snapTo truncates a time to the start of the given unit.
// Supported units: 's' (second), 'm' (minute), 'h' (hour), 'd' (day UTC), 'w' (week, Monday UTC).
func snapTo(t time.Time, unit byte) time.Time {
	switch unit {
	case 's':
		return t.Truncate(time.Second)
	case 'm':
		return t.Truncate(time.Minute)
	case 'h':
		return t.Truncate(time.Hour)
	case 'd':
		y, m, d := t.UTC().Date()

		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	case 'w':
		t = t.UTC()
		wd := t.Weekday()
		if wd == time.Sunday {
			wd = 7
		}
		t = t.AddDate(0, 0, -int(wd-time.Monday))
		y, m, d := t.Date()

		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	default:
		return t
	}
}

// FromAbsoluteRange creates a TimeRange from --from/--to flag values.
func FromAbsoluteRange(from, to string) (*TimeRange, error) {
	earliest, err := ParseAbsolute(from)
	if err != nil {
		return nil, fmt.Errorf("parse --from: %w", err)
	}
	latest, err := ParseAbsolute(to)
	if err != nil {
		return nil, fmt.Errorf("parse --to: %w", err)
	}

	return &TimeRange{
		Earliest: earliest,
		Latest:   latest,
	}, nil
}
