package shell

import (
	"context"
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/key"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
)

const preflightAnimationInterval = 160 * time.Millisecond

type preflightState int

const (
	preflightConnecting preflightState = iota
	preflightFailed
	preflightReady
)

type preflightModel struct {
	client *client.Client
	server string
	width  int
	height int
	frame  int
	state  preflightState
	err    error
	keys   keyMap
}

type preflightTickMsg struct{}

type preflightResultMsg struct {
	err error
}

func runServerPreflight(c *client.Client, server string) error {
	m := preflightModel{
		client: c,
		server: server,
		state:  preflightConnecting,
		keys:   defaultKeyMap(),
	}

	p := tea.NewProgram(m)
	final, err := p.Run()
	if err != nil {
		return err
	}

	if m, ok := final.(preflightModel); ok && m.state == preflightReady {
		return nil
	}

	return errPreflightQuit
}

var errPreflightQuit = fmt.Errorf("preflight quit")

func (m preflightModel) Init() tea.Cmd {
	return tea.Batch(preflightCheckCmd(m.client), preflightTickCmd())
}

func (m preflightModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

		return m, nil

	case preflightTickMsg:
		m.frame++

		return m, preflightTickCmd()

	case preflightResultMsg:
		if msg.err != nil {
			m.err = msg.err
			m.state = preflightFailed

			return m, nil
		}

		m.state = preflightReady

		return m, tea.Quit

	case tea.KeyPressMsg:
		switch {
		case key.Matches(msg, m.keys.Quit), key.Matches(msg, m.keys.FocusBack):
			return m, tea.Quit
		case m.state == preflightFailed && msg.String() == "r":
			m.err = nil
			m.state = preflightConnecting

			return m, preflightCheckCmd(m.client)
		}
	}

	return m, nil
}

func (m preflightModel) View() tea.View {
	t := ui.Stdout
	width := m.width
	if width <= 0 {
		width = 80
	}
	height := m.height
	if height <= 0 {
		height = 24
	}

	var lines []string
	lines = append(lines, m.lynxFrame())
	lines = append(lines, "")
	lines = append(lines, t.Bold.Render("LynxDB shell"))
	lines = append(lines, "")

	switch m.state {
	case preflightFailed:
		lines = append(lines,
			t.Error.Render("Cannot connect to LynxDB server"),
			t.Dim.Render("Endpoint: ")+t.Accent.Render(m.server),
			t.Dim.Render("Cause: ")+m.err.Error(),
			"",
			t.Info.Render("Press r to retry or q to quit."),
		)
	default:
		lines = append(lines,
			t.Info.Render("Connecting to LynxDB server..."),
			t.Dim.Render("Endpoint: ")+t.Accent.Render(m.server),
		)
	}

	block := strings.Join(lines, "\n")
	blockW := lipgloss.Width(block)
	left := 0
	if width > blockW {
		left = (width - blockW) / 2
	}
	top := 0
	blockH := strings.Count(block, "\n") + 1
	if height > blockH {
		top = (height - blockH) / 2
	}

	body := lipgloss.NewStyle().
		Width(width).
		Height(height).
		PaddingTop(top).
		PaddingLeft(left).
		Render(block)

	v := tea.NewView(body)
	v.AltScreen = true

	return v
}

func (m preflightModel) lynxFrame() string {
	mood := ui.LynxAlert
	if m.state == preflightFailed {
		mood = ui.LynxSad
	}
	frames := ui.LynxFrames(mood)
	if len(frames) == 0 {
		return ""
	}

	return strings.TrimRight(ui.RenderLynxFrame(ui.Stdout, frames[m.frame%len(frames)]), "\n")
}

func preflightCheckCmd(c *client.Client) tea.Cmd {
	return func() tea.Msg {
		if c == nil {
			return preflightResultMsg{err: fmt.Errorf("missing server client")}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := c.Status(ctx)

		return preflightResultMsg{err: err}
	}
}

func preflightTickCmd() tea.Cmd {
	return tea.Tick(preflightAnimationInterval, func(time.Time) tea.Msg {
		return preflightTickMsg{}
	})
}
