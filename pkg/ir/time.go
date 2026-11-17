package ir

import (
	"fmt"
	"strings"
	"time"
)

// TimeUnit is a calendar or clock unit used by relative time bounds.
type TimeUnit uint8

const (
	UnitSecond TimeUnit = iota + 1
	UnitMinute
	UnitHour
	UnitDay
	UnitWeek
	UnitMonth
	UnitQuarter
	UnitYear
)

var timeUnitNames = map[TimeUnit]string{
	UnitSecond: "s", UnitMinute: "m", UnitHour: "h", UnitDay: "d",
	UnitWeek: "w", UnitMonth: "mon", UnitQuarter: "q", UnitYear: "y",
}

func (u TimeUnit) String() string { return timeUnitNames[u] }

// TimeStep is one component of a relative time bound: either an offset of N
// units or, when Snap is set, rounding down to the start of the unit. A week
// snap rounds to the most recent Weekday (0 = Sunday).
type TimeStep struct {
	Snap    bool
	N       int
	Unit    TimeUnit
	Weekday int
}

// TimeBound is one end of a time range. An absolute bound is a fixed
// instant; a relative bound starts at the query's reference time ("now") and
// applies Steps in order.
type TimeBound struct {
	Abs   *time.Time
	Steps []TimeStep
}

// AbsoluteBound returns a bound at a fixed instant.
func AbsoluteBound(t time.Time) *TimeBound {
	u := t.UTC()
	return &TimeBound{Abs: &u}
}

// NowBound returns the bound at the query's reference time.
func NowBound() *TimeBound { return &TimeBound{} }

// Resolve returns the instant the bound denotes for reference time now.
// Calendar arithmetic and snapping use UTC.
func (b *TimeBound) Resolve(now time.Time) time.Time {
	if b.Abs != nil {
		return *b.Abs
	}
	t := now.UTC()
	for _, s := range b.Steps {
		if s.Snap {
			t = snapTime(t, s.Unit, s.Weekday)
			continue
		}
		t = addUnits(t, s.N, s.Unit)
	}
	return t
}

// IsRelative reports whether the bound depends on the reference time.
func (b *TimeBound) IsRelative() bool { return b.Abs == nil }

func (b *TimeBound) String() string {
	if b.Abs != nil {
		return b.Abs.Format(time.RFC3339Nano)
	}
	if len(b.Steps) == 0 {
		return "now"
	}
	var sb strings.Builder
	sb.WriteString("now")
	for _, s := range b.Steps {
		if s.Snap {
			sb.WriteString("@" + s.Unit.String())
			if s.Unit == UnitWeek {
				fmt.Fprintf(&sb, "%d", s.Weekday)
			}
			continue
		}
		fmt.Fprintf(&sb, "%+d%s", s.N, s.Unit)
	}
	return sb.String()
}

func addUnits(t time.Time, n int, u TimeUnit) time.Time {
	switch u {
	case UnitSecond:
		return t.Add(time.Duration(n) * time.Second)
	case UnitMinute:
		return t.Add(time.Duration(n) * time.Minute)
	case UnitHour:
		return t.Add(time.Duration(n) * time.Hour)
	case UnitDay:
		return t.AddDate(0, 0, n)
	case UnitWeek:
		return t.AddDate(0, 0, 7*n)
	case UnitMonth:
		return t.AddDate(0, n, 0)
	case UnitQuarter:
		return t.AddDate(0, 3*n, 0)
	case UnitYear:
		return t.AddDate(n, 0, 0)
	}
	return t
}

func snapTime(t time.Time, u TimeUnit, weekday int) time.Time {
	y, mo, d := t.Date()
	switch u {
	case UnitSecond:
		return t.Truncate(time.Second)
	case UnitMinute:
		return t.Truncate(time.Minute)
	case UnitHour:
		return t.Truncate(time.Hour)
	case UnitDay:
		return time.Date(y, mo, d, 0, 0, 0, 0, time.UTC)
	case UnitWeek:
		day := time.Date(y, mo, d, 0, 0, 0, 0, time.UTC)
		back := (int(day.Weekday()) - weekday + 7) % 7
		return day.AddDate(0, 0, -back)
	case UnitMonth:
		return time.Date(y, mo, 1, 0, 0, 0, 0, time.UTC)
	case UnitQuarter:
		q := (int(mo) - 1) / 3 * 3
		return time.Date(y, time.Month(q+1), 1, 0, 0, 0, 0, time.UTC)
	case UnitYear:
		return time.Date(y, time.January, 1, 0, 0, 0, 0, time.UTC)
	}
	return t
}

// TimeRange restricts rows by event time: Earliest is inclusive, Latest is
// exclusive, and a nil bound is open.
type TimeRange struct {
	Earliest *TimeBound
	Latest   *TimeBound
}

// IsZero reports whether neither bound is set.
func (r *TimeRange) IsZero() bool { return r == nil || (r.Earliest == nil && r.Latest == nil) }

func (r *TimeRange) String() string {
	if r.IsZero() {
		return ""
	}
	e, l := "-inf", "+inf"
	if r.Earliest != nil {
		e = r.Earliest.String()
	}
	if r.Latest != nil {
		l = r.Latest.String()
	}
	return "[" + e + ", " + l + ")"
}

// Resolve returns the absolute bounds for reference time now; zero values
// mean an open end.
func (r *TimeRange) Resolve(now time.Time) (earliest, latest time.Time) {
	if r == nil {
		return
	}
	if r.Earliest != nil {
		earliest = r.Earliest.Resolve(now)
	}
	if r.Latest != nil {
		latest = r.Latest.Resolve(now)
	}
	return
}
