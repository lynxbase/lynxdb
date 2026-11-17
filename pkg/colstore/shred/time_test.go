package shred

import (
	"testing"
	"time"
)

func TestParseTimeReadsTheCommonForms(t *testing.T) {
	at := time.Date(2024, 11, 21, 16, 9, 27, 95_000_000, time.UTC)
	for text, want := range map[string]time.Time{
		"2024-11-21T16:09:27.095Z":           at,
		"2024-11-21 16:09:27.095":            at,
		"2024-11-21T16:09:27,095":            at,
		"2024-11-21T18:09:27.095+02:00":      at,
		"2024-11-21T10:39:27.095-0530":       at,
		"2024-11-21T16:09:27Z":               at.Truncate(time.Second),
		"2024-11-21T16:09:27.095123456Z":     at.Add(123456),
		"2024-11-21T16:09:27.0951234567891Z": at.Add(123456),
		"2024-02-29T23:59:59Z":               time.Date(2024, 2, 29, 23, 59, 59, 0, time.UTC),
		"1969-12-31T23:59:59.5Z":             time.Unix(-1, 500_000_000),
		"0001-01-01T00:00:00Z":               time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		"1732205367":                         time.Unix(1732205367, 0),
		"1732205367095":                      time.Unix(1732205367, 95_000_000),
		"1732205367.095":                     time.Unix(1732205367, 95_000_000),
		"2024-11-21t16:09:27.095z":           at,
	} {
		got, ok := ParseTime([]byte(text))
		if !ok || got != want.UnixNano() {
			t.Errorf("%s: %d %v, want %d", text, got, ok, want.UnixNano())
		}
	}
	for _, text := range []string{"", "yesterday", "2024-13-01T00:00:00Z", "2023-02-29T00:00:00Z", "2024-11-21T24:00:00Z", "2024-11-21T16:09:27.Z",
		"2024-11-21T16:09:27+2", "2024-11-21", "2024-11-21T16:09", "24-11-21T16:09:27Z", "2024-11-21T16:09:27Zz", "2024-11-21T16:09:27+25:00"} {
		if got, ok := ParseTime([]byte(text)); ok {
			t.Errorf("%q read as %d", text, got)
		}
	}
}

func TestRowTimeComesFromTheFirstFieldThatHoldsOne(t *testing.T) {
	b := NewBatch(Options{TimeField: "ts", TimeFields: []string{"@timestamp", "meta.time"}})
	at := time.Date(2024, 11, 21, 16, 9, 27, 0, time.UTC)
	docs := []struct {
		doc  string
		want int64
	}{
		{`{"msg":"no time"}`, 7},
		{`{"@timestamp":"2024-11-21T16:09:27Z","msg":"text"}`, at.UnixNano()},
		{`{"ts":1732205367000,"@timestamp":"2020-01-01T00:00:00Z"}`, at.UnixNano()},
		{`{"meta":{"time":1732205367.25}}`, at.UnixNano() + 250_000_000},
		{`{"ts":"not a time","meta":{"time":"2024-11-21 16:09:27"}}`, at.UnixNano()},
		{`{"ts":true,"@timestamp":null}`, 7},
		{`{"@timestamp":"1732205367"}`, at.UnixNano()},
	}
	for _, d := range docs {
		if err := b.AddJSON([]byte(d.doc), 7); err != nil {
			t.Fatal(err)
		}
	}
	for i, d := range docs {
		if b.Times[i] != d.want {
			t.Errorf("%s: time %d, want %d", d.doc, b.Times[i], d.want)
		}
	}
}

// FuzzParseTime requires the time that the library reads from any text it
// takes in the layout of RFC 3339, and no panic for any other.
func FuzzParseTime(f *testing.F) {
	for _, s := range []string{"2024-11-21T16:09:27.095Z", "2024-11-21T18:09:27+02:00", "1969-12-31T23:59:59.999999999-23:59", "2024-02-29 00:00:00", "17322053670"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		got, ok := ParseTime([]byte(s))
		want, err := time.Parse(time.RFC3339Nano, s)
		if err != nil || want.Year() < 1 || want.Year() > 9999 {
			return
		}
		// The library takes a second of 60 where the minute is 59; it is not a time of day here.
		if !ok {
			if len(s) > 18 && s[17] == '6' {
				return
			}
			t.Fatalf("%q is %d for the library and not read", s, want.UnixNano())
		}
		if lo, hi := time.Date(1700, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2250, 1, 1, 0, 0, 0, 0, time.UTC); want.After(lo) && want.Before(hi) && got != want.UnixNano() {
			t.Fatalf("%q read as %d, the library says %d", s, got, want.UnixNano())
		}
	})
}

func BenchmarkParseTime(b *testing.B) {
	s := []byte("2024-11-21T16:09:27.095123Z")
	for i := 0; i < b.N; i++ {
		if _, ok := ParseTime(s); !ok {
			b.Fatal("not read")
		}
	}
}
