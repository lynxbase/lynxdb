package main

import (
	"errors"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

// timeFlagsNow is the time the tests of timeRangeOf run at.
var timeFlagsNow = time.Date(2026, 11, 24, 14, 45, 30, 0, time.UTC)

// timeFlagSet returns the time flags of a command, parsed from args.
func timeFlagSet(t *testing.T, args ...string) *pflag.FlagSet {
	t.Helper()
	f := pflag.NewFlagSet("query", pflag.ContinueOnError)
	f.StringP("since", "s", "", "")
	f.String("from", "", "")
	f.String("to", "", "")
	if err := f.Parse(args); err != nil {
		t.Fatal(err)
	}

	return f
}

func TestTimeRangeOf(t *testing.T) {
	for _, c := range []struct {
		args         []string
		defaultSince string
		want         timeRange
	}{
		{nil, "", timeRange{}},
		{[]string{"--since", "15m"}, "", timeRange{from: "-15m", to: "now", since: "15m"}},
		{[]string{"--since", "-15m"}, "", timeRange{from: "-15m", to: "now", since: "-15m"}},
		{[]string{"-s", "1h"}, "", timeRange{from: "-1h", to: "now", since: "1h"}},
		{[]string{"--since", "2w"}, "", timeRange{from: "-2w", to: "now", since: "2w"}},
		{[]string{"--since", " 15m "}, "", timeRange{from: "-15m", to: "now", since: " 15m "}},
		{[]string{"--since", "1d@d"}, "", timeRange{from: "-1d@d", to: "now", since: "1d@d"}},
		{[]string{"--since", "-1d@d"}, "", timeRange{from: "-1d@d", to: "now", since: "-1d@d"}},
		{[]string{"--since", "@d"}, "", timeRange{from: "@d", to: "now", since: "@d"}},
		{[]string{"--since", "now"}, "", timeRange{from: "now", to: "now", since: "now"}},
		{[]string{"--since", "2026-11-24"}, "", timeRange{from: "2026-11-24", to: "now", since: "2026-11-24"}},
		{[]string{"--from", "2026-11-24T09:00:00Z"}, "", timeRange{from: "2026-11-24T09:00:00Z", to: "now"}},
		{[]string{"--from", "-1h"}, "", timeRange{from: "-1h", to: "now"}},
		{[]string{"--to", "2026-11-24T09:00:00Z"}, "", timeRange{to: "2026-11-24T09:00:00Z"}},
		{[]string{"--to", "@d"}, "", timeRange{to: "@d"}},
		{[]string{"--from", "-1h", "--to", "now"}, "", timeRange{from: "-1h", to: "now"}},
		{[]string{"--from", "-1d@d", "--to", "@d"}, "", timeRange{from: "-1d@d", to: "@d"}},
		{[]string{"--from", "2026-11-24", "--to", "2026-11-24"}, "", timeRange{from: "2026-11-24", to: "2026-11-24"}},
		{nil, "6h", timeRange{from: "-6h", to: "now", since: "6h"}},
		{[]string{"--since", "1h"}, "6h", timeRange{from: "-1h", to: "now", since: "1h"}},
		{[]string{"--from", "-1d"}, "6h", timeRange{from: "-1d", to: "now"}},
		{[]string{"--to", "-1d"}, "6h", timeRange{to: "-1d"}},
		{[]string{"--from", "-1d"}, "banana", timeRange{from: "-1d", to: "now"}},
	} {
		got, err := timeRangeOf(timeFlagSet(t, c.args...), c.defaultSince, timeFlagsNow)
		if err != nil {
			t.Errorf("%q, default_since %q: %v", c.args, c.defaultSince, err)
			continue
		}
		if got != c.want {
			t.Errorf("%q, default_since %q: got %+v, want %+v", c.args, c.defaultSince, got, c.want)
		}
	}
}

// timeForms is how the error of a time that is not valid ends.
const timeForms = `: use now, a relative time (-15m, -1h, -7d, -2w), ` +
	`a snapped time (-1d@d, @d) or a date or time (2026-11-24, 2026-11-24T09:00:00Z)`

// sinceForms is how the error of a --since that is not valid ends.
const sinceForms = timeForms + "; a relative time may leave out its -, as in 15m"

func TestTimeRangeOfRefuses(t *testing.T) {
	for _, c := range []struct {
		args         []string
		defaultSince string
		want         string
	}{
		{[]string{"--since", "15m", "--from", "-1h"}, "", "--since and --from cannot be used together"},
		{[]string{"-s", "15m", "--from", "-1h"}, "", "--since and --from cannot be used together"},
		{[]string{"--since", "15m", "--to", "now"}, "", "--since and --to cannot be used together"},
		{[]string{"--since", "15m", "--from", "-1h", "--to", "now"}, "", "--since and --from cannot be used together"},
		{[]string{"--since", "banana", "--from", "banana"}, "", "--since and --from cannot be used together"},
		{[]string{"--since", "banana"}, "", `invalid --since "banana"` + sinceForms},
		{[]string{"--since", "1h30m"}, "", `invalid --since "1h30m"` + sinceForms},
		{[]string{"--since", "yesterday"}, "", `invalid --since "yesterday"` + sinceForms},
		{[]string{"--since", "-1h@z"}, "", `invalid --since "-1h@z"` + sinceForms},
		{[]string{"--since", "--5m"}, "", `invalid --since "--5m"` + sinceForms},
		{[]string{"--since", "9999999999d"}, "", `invalid --since "9999999999d"` + sinceForms},
		{[]string{"--since", ""}, "", `invalid --since ""` + sinceForms},
		{[]string{"--from", "banana"}, "", `invalid --from "banana"` + timeForms},
		{[]string{"--from", "15m"}, "", `invalid --from "15m"` + timeForms},
		{[]string{"--from", "-1h", "--to", "Tomorrow"}, "", `invalid --to "Tomorrow"` + timeForms},
		{[]string{"--to", "+1h"}, "", `invalid --to "+1h"` + timeForms},
		{nil, "banana", `invalid default_since "banana"` + sinceForms},
		{[]string{"--from", "2026-11-24T10:00:00Z", "--to", "2026-11-24T09:00:00Z"}, "",
			`--from "2026-11-24T10:00:00Z" is after --to "2026-11-24T09:00:00Z"`},
		{[]string{"--from", "-1h", "--to", "-2h"}, "", `--from "-1h" is after --to "-2h"`},
		{[]string{"--from", "2030-01-01"}, "", `--from "2030-01-01" is after now`},
		{[]string{"--since", "2030-01-01"}, "", `--since "2030-01-01" is after now`},
		{nil, "2030-01-01", `default_since "2030-01-01" is after now`},
	} {
		_, err := timeRangeOf(timeFlagSet(t, c.args...), c.defaultSince, timeFlagsNow)
		var usage usageError
		if !errors.As(err, &usage) || err.Error() != c.want {
			t.Errorf("%q, default_since %q: got %v, want the usage error %q", c.args, c.defaultSince, err, c.want)
		}
	}
}
