package ui

import (
	"os"

	"golang.org/x/term"
)

// DefaultTerminalWidth is the fallback width when stdout is not a TTY
// (piped output, CI, tests, etc.).
const DefaultTerminalWidth = 120

// MinColumnWidth is the minimum usable width per column in table layout.
// When the available width per column drops below this threshold, the
// renderer switches to card (vertical key-value) layout.
const MinColumnWidth = 8

// TerminalWidth returns the current terminal width in columns.
// It uses golang.org/x/term.GetSize on stdout's file descriptor.
// When stdout is not a TTY (piped, redirected, or in tests) it returns
// DefaultTerminalWidth.
func TerminalWidth() int {
	w, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || w <= 0 {
		return DefaultTerminalWidth
	}

	return w
}
