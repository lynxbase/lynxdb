package ui

import (
	"fmt"
	"os"
	"strings"
)

// Confirm prompts the user for a yes/no confirmation on the theme's writer,
// reading from stdin. Returns false if stdin is not a TTY.
func (t *Theme) Confirm(prompt string) bool {
	if !isStdinTTY() {
		return false
	}

	fmt.Fprintf(t.w, "  %s  %s [y/N]: ", t.IconWarn(), prompt)

	var response string
	if _, err := fmt.Fscanln(os.Stdin, &response); err != nil {
		return false
	}

	response = strings.TrimSpace(strings.ToLower(response))

	return response == "y" || response == "yes"
}

// ConfirmDestructive prompts for typed confirmation of a destructive action.
// The user must type the exact confirmValue to proceed. Returns false if stdin
// is not a TTY.
func (t *Theme) ConfirmDestructive(message, confirmValue string) bool {
	if !isStdinTTY() {
		return false
	}

	fmt.Fprintf(t.w, "\n  %s  %s\n\n", t.IconWarn(), message)
	fmt.Fprintf(t.w, "  Type %s to confirm: ", t.Bold.Render("'"+confirmValue+"'"))

	var response string
	if _, err := fmt.Fscanln(os.Stdin, &response); err != nil {
		return false
	}

	return strings.TrimSpace(response) == confirmValue
}

// isStdinTTY reports whether stdin is connected to a terminal.
func isStdinTTY() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}

	return (fi.Mode() & os.ModeCharDevice) != 0
}
