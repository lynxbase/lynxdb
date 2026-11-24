package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"github.com/lynxbase/lynxdb/pkg/timerange"
)

// timeRange is the range of event times, [from, to), that the time flags of
// a command select. Its bounds are text that a server resolves on its own
// clock and that a local query resolves after its input loads; an empty
// bound is open.
type timeRange struct {
	from, to string
	// since is the --since or default_since that selects the range, as given.
	since string
}

// timeRangeOf returns the range that the flags --since, --from and --to
// given on f select; defaultSince applies when none of them is given. The
// bounds are resolved at now to check their order.
func timeRangeOf(f *pflag.FlagSet, defaultSince string, now time.Time) (timeRange, error) {
	since, sinceGiven := givenFlag(f, "since")
	from, fromGiven := givenFlag(f, "from")
	to, toGiven := givenFlag(f, "to")
	switch {
	case sinceGiven && fromGiven:
		return timeRange{}, usageError("--since and --from cannot be used together")
	case sinceGiven && toGiven:
		return timeRange{}, usageError("--since and --to cannot be used together")
	case sinceGiven:
		return sinceRange("--since", since, now)
	case !fromGiven && !toGiven:
		if defaultSince == "" {
			return timeRange{}, nil
		}
		return sinceRange("default_since", defaultSince, now)
	}

	tr := timeRange{from: strings.TrimSpace(from), to: "now"}
	start, end := time.Time{}, now
	var err error
	if fromGiven {
		if start, err = parseTimeFlag("--from", from, now); err != nil {
			return timeRange{}, err
		}
	}
	if toGiven {
		if end, err = parseTimeFlag("--to", to, now); err != nil {
			return timeRange{}, err
		}
		tr.to = strings.TrimSpace(to)
	}
	switch {
	case !fromGiven || !start.After(end):
		return tr, nil
	case toGiven:
		return timeRange{}, usageError(fmt.Sprintf("--from %q is after --to %q", from, to))
	default:
		return timeRange{}, usageError(fmt.Sprintf("--from %q is after now", from))
	}
}

// sinceRange returns the range from since until now; name is what since was
// given as. An offset may leave out its leading -, as in 15m.
func sinceRange(name, since string, now time.Time) (timeRange, error) {
	from := strings.TrimSpace(since)
	t, err := timerange.Parse(from, now)
	if err != nil {
		from = "-" + from
		t, err = timerange.Parse(from, now)
	}
	if err != nil {
		return timeRange{}, usageError(fmt.Sprintf("%v; a relative time may leave out its -, as in 15m",
			&timerange.ParseError{Name: name, Text: since}))
	}
	if t.After(now) {
		return timeRange{}, usageError(fmt.Sprintf("%s %q is after now", name, since))
	}

	return timeRange{from: from, to: "now", since: since}, nil
}

// parseTimeFlag reads the value of the time flag name.
func parseTimeFlag(name, value string, now time.Time) (time.Time, error) {
	t, err := timerange.Parse(value, now)
	if err != nil {
		return time.Time{}, usageError((&timerange.ParseError{Name: name, Text: value}).Error())
	}

	return t, nil
}

// givenFlag returns the value of the flag name and whether the command line
// gives it.
func givenFlag(f *pflag.FlagSet, name string) (string, bool) {
	if !f.Changed(name) {
		return "", false
	}

	return f.Lookup(name).Value.String(), true
}
