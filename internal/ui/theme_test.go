package ui

import (
	"bytes"
	"strings"
	"testing"
)

func TestNewTheme_NoColor_StripsStyles(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	// All styled renders should produce plain text (no ANSI escapes).
	got := theme.Bold.Render("hello")
	if strings.Contains(got, "\033[") {
		t.Errorf("Bold.Render with noColor should not contain ANSI: %q", got)
	}

	got = theme.Success.Render("ok")
	if strings.Contains(got, "\033[") {
		t.Errorf("Success.Render with noColor should not contain ANSI: %q", got)
	}

	got = theme.Error.Render("fail")
	if strings.Contains(got, "\033[") {
		t.Errorf("Error.Render with noColor should not contain ANSI: %q", got)
	}

	got = theme.JSONKey.Render(`"key"`)
	if strings.Contains(got, "\033[") {
		t.Errorf("JSONKey.Render with noColor should not contain ANSI: %q", got)
	}
}

func TestNewTheme_NoColor_PreservesText(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	tests := []struct {
		name   string
		render func(...string) string
		input  string
	}{
		{"Bold", theme.Bold.Render, "hello"},
		{"Dim", theme.Dim.Render, "muted"},
		{"Success", theme.Success.Render, "ok"},
		{"Error", theme.Error.Render, "fail"},
		{"Info", theme.Info.Render, "note"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.render(tc.input)
			if got != tc.input {
				t.Errorf("%s.Render(%q) = %q, want %q", tc.name, tc.input, got, tc.input)
			}
		})
	}
}

func TestInit_SetsGlobals(t *testing.T) {
	Init(true)

	if Stdout == nil {
		t.Fatal("Stdout should not be nil after Init")
	}

	if Stderr == nil {
		t.Fatal("Stderr should not be nil after Init")
	}
}
