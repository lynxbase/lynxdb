package shell

import (
	"fmt"
	"io"
	"strings"

	"github.com/lynxbase/lynxdb/internal/ui"
)

func renderShellExit(w io.Writer, m Model) {
	if w == nil {
		return
	}

	ui.WriteLynxMark(w, ui.Stdout, ui.LynxAlert, true)
	fmt.Fprintln(w, "bye-bye")
	fmt.Fprintln(w)
	fmt.Fprintln(w, shellExitSummary(m))
}

func shellExitSummary(m Model) string {
	if m.session == nil {
		return "summary: shell closed"
	}

	target := m.session.Server
	if m.session.Mode == "file" {
		target = m.session.File
	}
	if strings.TrimSpace(target) == "" {
		target = m.session.Mode
	}

	parts := []string{
		m.session.Mode,
		target,
		fmt.Sprintf("queries %d", m.session.QueryCount),
	}
	if m.session.Mode == "file" && m.session.Events > 0 {
		parts = append(parts, fmt.Sprintf("events %d", m.session.Events))
	}

	if m.session.QueryCount > 0 {
		if m.session.LastError != "" {
			parts = append(parts, "last error")
		} else {
			parts = append(parts, fmt.Sprintf("last %d rows", len(m.session.LastRows)))
		}
		if m.session.LastElapsed > 0 {
			parts = append(parts, formatElapsedShell(m.session.LastElapsed))
		}
	}

	return strings.Join(parts, " | ")
}
