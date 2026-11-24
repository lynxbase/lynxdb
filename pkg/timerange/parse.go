package timerange

import (
	"fmt"
	"math"
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

// A ParseError reports text that is not a time.
type ParseError struct {
	// Name says what the text was given as, such as from.
	Name string
	// Text is the text as it was given.
	Text string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("invalid %s %q: use now, a relative time (-15m, -1h, -7d, -2w), "+
		"a snapped time (-1d@d, @d) or a date or time (2026-11-24, 2026-11-24T09:00:00Z)", e.Name, e.Text)
}

// Parse reads s as a time:
//
//	time     = "now" | offset [snap] | snap | absolute
//	offset   = "-" digits unit
//	snap     = "@" unit
//	unit     = "s" | "m" | "h" | "d" | "w"
//
// An offset counts back from now. A snap truncates to the start of its unit
// in UTC, where a week starts on Monday; a snap alone truncates now. An
// absolute time is RFC 3339, with or without a fraction of a second, or one
// of the layouts 2006-01-02T15:04:05, 2006-01-02 15:04:05, 2006-01-02 15:04
// and 2006-01-02 in UTC. The word now and the units are read in either case,
// and spaces around s are ignored. Any other text is a *ParseError.
func Parse(s string, now time.Time) (time.Time, error) {
	return parseNamed("time", s, now)
}

// parseNamed reads s as Parse does; name says what s was given as.
func parseNamed(name, s string, now time.Time) (time.Time, error) {
	t := strings.TrimSpace(s)
	if strings.EqualFold(t, "now") {
		return now, nil
	}
	if strings.HasPrefix(t, "-") || strings.HasPrefix(t, "@") {
		if r, ok := parseRelative(strings.ToLower(t), now); ok {
			return r, nil
		}
	} else if a, err := ParseAbsolute(t); err == nil {
		return a, nil
	}

	return time.Time{}, &ParseError{Name: name, Text: s}
}

// units are the spans of the time units.
var units = map[byte]time.Duration{
	's': time.Second,
	'm': time.Minute,
	'h': time.Hour,
	'd': 24 * time.Hour,
	'w': 7 * 24 * time.Hour,
}

// parseRelative reads the lower-case text offset [snap] or snap.
func parseRelative(s string, now time.Time) (time.Time, bool) {
	t := now
	if rest, ok := strings.CutPrefix(s, "-"); ok {
		digits := len(rest) - len(strings.TrimLeft(rest, "0123456789"))
		if digits == 0 || digits == len(rest) {
			return time.Time{}, false
		}
		n, err := strconv.ParseInt(rest[:digits], 10, 64)
		unit, ok := units[rest[digits]]
		if err != nil || !ok || n > math.MaxInt64/int64(unit) {
			return time.Time{}, false
		}
		t = now.Add(-time.Duration(n) * unit)
		s = rest[digits+1:]
		if s == "" {
			return t, true
		}
	}
	if len(s) != 2 || s[0] != '@' {
		return time.Time{}, false
	}

	return snapTo(t, s[1])
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
	earliest, err := parseNamed("from", from, now)
	if err != nil {
		return nil, err
	}
	latest, err := parseNamed("to", to, now)
	if err != nil {
		return nil, err
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
		t, err := parseNamed("from", from, now)
		if err != nil {
			return nil, err
		}
		tr.Earliest = t
	}
	if to != "" {
		t, err := parseNamed("to", to, now)
		if err != nil {
			return nil, err
		}
		tr.Latest = t
	}
	if !tr.Earliest.IsZero() && !tr.Latest.IsZero() && tr.Earliest.After(tr.Latest) {
		return nil, fmt.Errorf("from must be before to")
	}

	return &tr, nil
}

// snapTo truncates a time to the start of the given unit: 's' (second), 'm'
// (minute), 'h' (hour), 'd' (day UTC) or 'w' (week, Monday UTC). It reports
// false for any other unit.
func snapTo(t time.Time, unit byte) (time.Time, bool) {
	switch unit {
	case 's':
		return t.Truncate(time.Second), true
	case 'm':
		return t.Truncate(time.Minute), true
	case 'h':
		return t.Truncate(time.Hour), true
	case 'd':
		y, m, d := t.UTC().Date()

		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC), true
	case 'w':
		t = t.UTC()
		wd := t.Weekday()
		if wd == time.Sunday {
			wd = 7
		}
		t = t.AddDate(0, 0, -int(wd-time.Monday))
		y, m, d := t.Date()

		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC), true
	default:
		return time.Time{}, false
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
