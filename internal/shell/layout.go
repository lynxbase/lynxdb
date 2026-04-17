package shell

import (
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/lynxbase/lynxdb/internal/ui"
)

// Layout breakpoints and sidebar sizing constants.
const (
	sidebarDefaultWidth = 30 // default sidebar width in characters
	sidebarMinWidth     = 24 // minimum before auto-collapse

	// Three responsive breakpoints:
	//   < breakpointHideSidebar  → sidebar fully hidden
	//   < breakpointCompact      → compact mode (section headers only)
	//   >= breakpointCompact     → full sidebar
	breakpointHideSidebar = 60
	breakpointCompact     = 100
)

// sidebarLayout holds computed layout dimensions.
type sidebarLayout struct {
	sidebarW    int
	mainW       int
	separatorW  int
	compactMode bool
}

// computeSidebarLayout calculates sidebar and main panel widths
// based on terminal width and whether the sidebar is open.
func computeSidebarLayout(termW int, sidebarOpen bool) sidebarLayout {
	if !sidebarOpen || termW < breakpointHideSidebar {
		return sidebarLayout{mainW: termW}
	}

	var sl sidebarLayout
	sl.separatorW = 1

	if termW >= breakpointCompact {
		// Full sidebar.
		sl.sidebarW = sidebarDefaultWidth
		if maxW := termW / 3; sl.sidebarW > maxW {
			sl.sidebarW = maxW
		}
		if sl.sidebarW < sidebarMinWidth {
			sl.sidebarW = sidebarMinWidth
		}
	} else {
		// Compact mode — section headers only.
		sl.sidebarW = sidebarMinWidth
		sl.compactMode = true
	}

	sl.mainW = termW - sl.sidebarW - sl.separatorW

	return sl
}

// renderVerticalSeparator renders a thin vertical border column.
func renderVerticalSeparator(height int) string {
	if height <= 0 {
		return ""
	}

	style := lipgloss.NewStyle().Foreground(ui.ColorDark())
	line := style.Render("│")
	lines := make([]string, height)

	for i := range lines {
		lines[i] = line
	}

	return strings.Join(lines, "\n")
}
