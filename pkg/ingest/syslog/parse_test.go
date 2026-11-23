package syslog

import (
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestDetect(t *testing.T) {
	for line, want := range map[string]string{
		"<34>1 2003-10-11T22:14:15.003Z host app - - - hi": RFC5424,
		"<13>Feb  5 17:32:18 host app: hi":                 RFC3164,
		"Feb  5 17:32:18 host app: hi":                     Raw,
		"<>1 x":                                            Raw,
	} {
		if got := Detect(line); got != want {
			t.Errorf("%q: %s, want %s", line, got, want)
		}
	}
}

func TestParseReadsTheRFC5424TimeAsRFC3339(t *testing.T) {
	at := time.Date(2003, 10, 11, 22, 14, 15, 3_000_000, time.UTC)
	for stamp, want := range map[string]time.Time{
		"2003-10-11T22:14:15.003Z":        at,
		"2003-10-11T15:14:15.003-07:00":   at,
		"2003-10-11T23:14:15.003000+0100": at,
		"2003-10-11T22:14:15.003":         at,
	} {
		e := event.NewEvent(time.Time{}, "")
		line := "<34>1 " + stamp + " host app - - - hi"
		if err := Parse(e, line, RFC5424, time.UTC, 2026); err != nil {
			t.Fatalf("%s: %v", stamp, err)
		}
		if !e.Time.Equal(want) {
			t.Errorf("%s: time %v, want %v", stamp, e.Time, want)
		}
	}
	received := time.Unix(10, 0)
	e := event.NewEvent(received, "")
	if err := Parse(e, "<34>1 - host app - - - hi", RFC5424, time.UTC, 2026); err != nil || !e.Time.Equal(received) {
		t.Fatalf("nil time: %v, time %v", err, e.Time)
	}
}

func TestParseLeavesTheEventAsItWasOnAnError(t *testing.T) {
	at := time.Unix(10, 0)
	for _, line := range []string{
		"not syslog",
		"<13>Feb  5 17:32:18",
		`<34>1 2003-10-11T22:14:15.003Z host app - - [unterminated`,
		"<34>1 yesterday host app - - - message",
	} {
		e := event.NewEvent(at, line)
		e.Host = "given"
		if err := Parse(e, line, Detect(line), time.UTC, 2026); err == nil {
			t.Errorf("%q: no error", line)
		}
		if !e.Time.Equal(at) || e.Host != "given" || len(e.Fields) != 0 {
			t.Errorf("%q: event changed: time %v host %q fields %v", line, e.Time, e.Host, e.Fields)
		}
	}
}

func TestParseTakesTheYearAndZoneOfAnRFC3164Time(t *testing.T) {
	e := event.NewEvent(time.Time{}, "")
	line := "<13>Feb 29 17:32:18 host app: leap"
	if err := Parse(e, line, Detect(line), time.UTC, 2024); err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2024, 2, 29, 17, 32, 18, 0, time.UTC); !e.Time.Equal(want) {
		t.Fatalf("time %v, want %v", e.Time, want)
	}
}
