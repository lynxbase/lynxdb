package ui

import (
	"os"
	"sync/atomic"

	"golang.org/x/term"
)

// DefaultTerminalWidth is the fallback width when stdout is not a TTY
// (piped output, CI, tests, etc.).
const DefaultTerminalWidth = 120

// MinColumnWidth is the minimum usable width per column in table layout.
// When the available width per column drops below this threshold, the
// renderer switches to card (vertical key-value) layout.
const MinColumnWidth = 8

// testTermWidth holds a test override for terminal width.
// A value of 0 means "no override — use real detection".
var testTermWidth atomic.Int32

// TerminalWidth returns the current terminal width in columns.
// It uses golang.org/x/term.GetSize on stdout's file descriptor.
// When stdout is not a TTY (piped, redirected, or in tests) it returns
// DefaultTerminalWidth unless overridden via SetTerminalWidthForTest.
func TerminalWidth() int {
	if w := int(testTermWidth.Load()); w > 0 {
		return w
	}

	w, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || w <= 0 {
		return DefaultTerminalWidth
	}

	return w
}

// SetTerminalWidthForTest overrides the terminal width for deterministic tests.
// Pass 0 to clear the override and restore real detection.
func SetTerminalWidthForTest(w int) {
	testTermWidth.Store(int32(w))
}
