package timestamp

import (
	"strings"
	"testing"
	"time"
)

func TestParseReadsEveryForm(t *testing.T) {
	at := time.Date(2025, 1, 15, 8, 0, 0, 0, time.UTC)
	year := time.Now().Year()
	for _, tc := range []struct {
		text string
		want time.Time
	}{
		{"2025-01-15T08:00:00Z", at},
		{"2025-01-15T08:00:00.1Z", at.Add(100 * time.Millisecond)},
		{"2025-01-15T08:00:00.12Z", at.Add(120 * time.Millisecond)},
		{"2025-01-15T08:00:00.105Z", at.Add(105 * time.Millisecond)},
		{"2025-01-15T08:00:00.123456789Z", at.Add(123456789)},
		{"2025-01-15T08:00:00.1234567891234Z", at.Add(123456789)},
		{"2025-01-15T08:00:00,5Z", at.Add(500 * time.Millisecond)},
		{"2025-01-15t08:00:00z", at},
		{"2025-01-15 08:00:00", at},
		{"2025-01-15 08:00:00.250", at.Add(250 * time.Millisecond)},
		{"2025-01-15T08:00:00", at},
		{"2025-01-15T10:00:00+02:00", at},
		{"2025-01-15T10:00:00+0200", at},
		{"2025-01-15T02:30:00.5-05:30", at.Add(500 * time.Millisecond)},
		{"2025-01-15T02:30:00-0530", at},
		{"2024-02-29T23:59:59Z", time.Date(2024, 2, 29, 23, 59, 59, 0, time.UTC)},
		{"1969-12-31T23:59:59.5Z", time.Unix(-1, 500_000_000)},
		{"Jan 15 08:00:00", time.Date(year, 1, 15, 8, 0, 0, 0, time.UTC)},
		{"Jan  5 08:00:00", time.Date(year, 1, 5, 8, 0, 0, 0, time.UTC)},
		{"Jan 5 08:00:00", time.Date(year, 1, 5, 8, 0, 0, 0, time.UTC)},
		{"Jan 05 08:00:00", time.Date(year, 1, 5, 8, 0, 0, 0, time.UTC)},
		{"Dec 31 23:59:59.75", time.Date(year, 12, 31, 23, 59, 59, 750_000_000, time.UTC)},
		{"15/Jan/2025:08:00:00 +0000", at},
		{"15/Jan/2025:10:00:00 +0200", at},
		{"15/Jan/2025:03:00:00 -05:00", at},
		{"15/Jan/2025:08:00:00", at},
		{"15/Jan/2025:08:00:00.5 +0000", at.Add(500 * time.Millisecond)},
	} {
		got, ok := Parse(tc.text)
		if !ok || got != tc.want.UnixNano() {
			t.Errorf("%q: %v %v, want %v", tc.text, time.Unix(0, got).UTC(), ok, tc.want)
		}
	}
}

func TestParseRejectsOtherText(t *testing.T) {
	for _, text := range []string{
		"", "yesterday", "2025-01-15", "2025-01-15T08:00", "25-01-15T08:00:00Z",
		"2025-13-01T00:00:00Z", "2023-02-29T00:00:00Z", "2025-01-15T24:00:00Z", "2025-01-15T08:60:00Z", "2025-01-15T08:00:60Z",
		"2025-01-15T08:00:00.Z", "2025-01-15T08:00:00+2", "2025-01-15T08:00:00+02", "2025-01-15T08:00:00Zz",
		"2025-01-15T08:00:00+25:00", "2025-01-15T08:00:00+02:60", "2025-01-15T08:00:00 +0200", "2025-01-15T6:00:00Z",
		"Jan 32 08:00:00", "Jun 31 08:00:00", "Foo 15 08:00:00", "JAN 15 08:00:00", "Jan 15 8:00:00", "Jan 15 08:00:00 2025",
		"15/Foo/2025:08:00:00 +0000", "32/Jan/2025:08:00:00 +0000", "15/Jan/2025:08:00:00 +2400", "5/Jan/2025:08:00:00 +0000",
		"1732205367",
	} {
		if got, ok := Parse(text); ok {
			t.Errorf("%q read as %v", text, time.Unix(0, got).UTC())
		}
	}
}

func TestFindLooksAfterSeparators(t *testing.T) {
	at := time.Date(2025, 1, 15, 8, 0, 0, 0, time.UTC).UnixNano()
	year := time.Now().Year()
	for _, tc := range []struct {
		line string
		want int64
	}{
		{"2025-01-15T08:00:00Z level=a", at},
		{"2025-01-15T08:00:00.1Z level=a", at + 1e8},
		{"2025-01-15T08:00:00.12Z level=a", at + 12e7},
		{"2025-01-15 08:00:00,000 INFO main started", at},
		{"2025-01-15 08:00:00,INFO,main", at},
		{"INFO 2025-01-15T08:00:00Z started", at},
		{"\t2025-01-15T08:00:00Z", at},
		{`{"timestamp":"2025-01-15T08:00:00Z","level":"info"}`, at},
		{`msg='2025-01-15T08:00:00Z'`, at},
		{"[2025-01-15T08:00:00Z] started", at},
		{"(2025-01-15 08:00:00) started", at},
		{"time=2025-01-15T08:00:00Z level=info msg=hi", at},
		{`127.0.0.1 - frank [15/Jan/2025:08:00:00 +0000] "GET / HTTP/1.1" 200 2326`, at},
		{"Jan 15 08:00:00 web-01 sshd[42]: accepted", time.Date(year, 1, 15, 8, 0, 0, 0, time.UTC).UnixNano()},
		{"<34>Jan 15 08:00:00 web-01 su: failed", time.Date(year, 1, 15, 8, 0, 0, 0, time.UTC).UnixNano()},
		{"<165>1 2025-01-15T08:00:00.000Z host app - - - hi", at},
		{"seen 2025-01-15T08:00:00Z then 2026-01-15T08:00:00Z", at},
		{"id=7 2024-13-15T08:00:00Z 2025-01-15T08:00:00Z", at},
	} {
		got, ok := Find(tc.line, year)
		if !ok || got != tc.want {
			t.Errorf("%q: %v %v, want %v", tc.line, time.Unix(0, got).UTC(), ok, time.Unix(0, tc.want).UTC())
		}
	}
}

func TestFindMissesTimesItShouldNotRead(t *testing.T) {
	for _, line := range []string{
		"plain words",
		"build-2025-01-15T08:00:00Z",
		"x2025-01-15T08:00:00Z",
		"1732205367 started",
		strings.Repeat("a", 100) + " 2025-01-15T08:00:00Z",
	} {
		if got, ok := Find(line, 2025); ok {
			t.Errorf("%q read as %v", line, time.Unix(0, got).UTC())
		}
	}
}

func TestSyslogTimeTakesTheYearGiven(t *testing.T) {
	got, ok := Find("Feb 29 12:00:00 host app: leap", 2024)
	if want := time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC).UnixNano(); !ok || got != want {
		t.Fatalf("got %v %v, want %v", time.Unix(0, got).UTC(), ok, time.Unix(0, want).UTC())
	}
	if _, ok := Find("Feb 29 12:00:00 host app: leap", 2025); ok {
		t.Fatal("Feb 29 read in 2025")
	}
}

// strictRFC3339 reports whether s has the two-digit fields, the offset in
// range and the second below 60 that the forms of this package require,
// beyond what the layout of the time package checks.
func strictRFC3339(s string) bool {
	if len(s) < 20 || s[13] != ':' || s[16] != ':' || s[17] == '6' {
		return false
	}
	if z := s[len(s)-6:]; (z[0] == '+' || z[0] == '-') && z[3] == ':' {
		return (z[1] < '2' || z[1] == '2' && z[2] < '4') && z[4] < '6'
	}

	return true
}

// FuzzParse requires the time that the time package reads from text in the
// layout of RFC 3339 that has the fields of this package, and no panic for
// any other text.
func FuzzParse(f *testing.F) {
	for _, s := range []string{"2024-11-21T16:09:27.095Z", "2024-11-21T18:09:27+02:00", "1969-12-31T23:59:59.999999999-23:59", "2024-02-29 00:00:00", "Jan  5 08:00:00", "15/Jan/2025:08:00:00 +0000"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		got, ok := Parse(s)
		if _, n, ok := At(s, 2025); ok && (n <= 0 || n > len(s)) {
			t.Fatalf("%q: length %d", s, n)
		}
		Find(s, 2025)
		want, err := time.Parse(time.RFC3339Nano, s)
		if err != nil || want.Year() < 1 || want.Year() > 9999 {
			return
		}
		if !ok {
			if strictRFC3339(s) {
				t.Fatalf("%q is %d for the time package and not read", s, want.UnixNano())
			}
			return
		}
		if lo, hi := time.Date(1700, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2250, 1, 1, 0, 0, 0, 0, time.UTC); want.After(lo) && want.Before(hi) && got != want.UnixNano() {
			t.Fatalf("%q read as %d, the time package says %d", s, got, want.UnixNano())
		}
	})
}

func BenchmarkParse(b *testing.B) {
	for b.Loop() {
		if _, ok := Parse("2024-11-21T16:09:27.095123Z"); !ok {
			b.Fatal("not read")
		}
	}
}

func BenchmarkFind(b *testing.B) {
	for _, line := range []string{
		"2025-01-15T08:00:00.105Z host=web-03 level=ERROR method=GET path=/api/v1/users status=502",
		`{"timestamp":"2026-02-14T14:23:01.345Z","level":"error","source":"api-gw","msg":"connection refused"}`,
		`192.168.1.42 - john [10/Feb/2026:08:12:03 +0000] "GET /x HTTP/1.1" 200 1243 "-" "curl/8"`,
		"Feb 14 14:23:01 prod-app-01 api-gw[1234]: connection refused to /user_service/health status=502",
		"a line of text that carries no time at all, only words and numbers like 42 and 7, for a hundred bytes or more",
	} {
		b.Run(line[:8], func(b *testing.B) {
			for b.Loop() {
				Find(line, 2026)
			}
		})
	}
}
