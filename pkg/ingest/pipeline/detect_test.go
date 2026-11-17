package pipeline

import "testing"

func TestExtractFormatName(t *testing.T) {
	cases := map[DetectedFormat]string{
		FormatCombined: "combined",
		FormatSyslog:   "syslog",
		FormatJSON:     "",
		FormatCSV:      "",
		FormatTSV:      "",
	}
	for format, want := range cases {
		if got := ExtractFormatName(format); got != want {
			t.Errorf("ExtractFormatName(%s) = %q, want %q", format, got, want)
		}
	}
}
