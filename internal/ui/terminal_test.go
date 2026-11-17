package ui

import "testing"

func TestTerminalWidth_Default(t *testing.T) {
	w := TerminalWidth()
	if w <= 0 {
		t.Errorf("TerminalWidth() = %d, want > 0", w)
	}
}
