// Package shell implements the full-screen Bubble Tea v2 interactive shell
// for LynxDB, replacing the former readline-based REPL.
package shell

import (
	tea "charm.land/bubbletea/v2"
	zone "github.com/lrstanley/bubblezone/v2"

	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/storage"
)

// RunOpts configures the shell session.
type RunOpts struct {
	Server string
	Client *client.Client
	Engine *storage.Engine
	Since  string
	File   string
	Events int // event count for file mode header
}

func Run(mode string, opts RunOpts) error {
	zone.NewGlobal()

	m := NewModel(mode, opts)

	// Prepend welcome banner to results viewport.
	m.results.AppendText(welcomeBanner())

	p := tea.NewProgram(m)
	_, err := p.Run()

	zone.Close()

	return err
}
