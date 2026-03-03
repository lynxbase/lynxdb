package ui

import "testing"

func TestTerminalWidth_Default(t *testing.T) {
	// Clear any override from other tests.
	SetTerminalWidthForTest(0)

	w := TerminalWidth()
	if w <= 0 {
		t.Errorf("TerminalWidth() = %d, want > 0", w)
	}
}

func TestTerminalWidth_Override(t *testing.T) {
	SetTerminalWidthForTest(42)
	defer SetTerminalWidthForTest(0)

	if got := TerminalWidth(); got != 42 {
		t.Errorf("TerminalWidth() = %d, want 42", got)
	}
}

func TestTerminalWidth_ClearOverride(t *testing.T) {
	SetTerminalWidthForTest(99)
	SetTerminalWidthForTest(0)

	// Should fall back to real detection or DefaultTerminalWidth.
	w := TerminalWidth()
	if w <= 0 {
		t.Errorf("TerminalWidth() after clear = %d, want > 0", w)
	}
}
