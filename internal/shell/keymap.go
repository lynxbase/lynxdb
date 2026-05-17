package shell

import "charm.land/bubbles/v2/key"

type keyMap struct {
	Submit        key.Binding
	Quit          key.Binding
	Cancel        key.Binding
	ClearScr      key.Binding
	HistPrev      key.Binding
	HistNext      key.Binding
	AcceptSugg    key.Binding
	CompletePopup key.Binding
	InsertNewline key.Binding
	ScrollUp      key.Binding
	ScrollDn      key.Binding
	FocusBack     key.Binding
	ToggleSidebar key.Binding
	CopyResults   key.Binding
	CopyResultsMD key.Binding
}

func defaultKeyMap() keyMap {
	return keyMap{
		Submit: key.NewBinding(
			key.WithKeys("enter"),
			key.WithHelp("enter", "run"),
		),
		Quit: key.NewBinding(
			key.WithKeys("ctrl+d"),
			key.WithHelp("ctrl+d", "quit"),
		),
		Cancel: key.NewBinding(
			key.WithKeys("ctrl+c"),
			key.WithHelp("ctrl+c", "cancel"),
		),
		ClearScr: key.NewBinding(
			key.WithKeys("ctrl+l"),
			key.WithHelp("ctrl+l", "clear"),
		),
		HistPrev: key.NewBinding(
			key.WithKeys("ctrl+p"),
			key.WithHelp("ctrl+p", "previous"),
		),
		HistNext: key.NewBinding(
			key.WithKeys("ctrl+n"),
			key.WithHelp("ctrl+n", "next"),
		),
		AcceptSugg: key.NewBinding(
			key.WithKeys("tab"),
			key.WithHelp("tab", "complete"),
		),
		CompletePopup: key.NewBinding(
			key.WithKeys("ctrl+space", "ctrl+ "),
			key.WithHelp("ctrl+space", "suggest"),
		),
		InsertNewline: key.NewBinding(
			key.WithKeys("shift+enter"),
			key.WithHelp("shift+enter", "newline"),
		),
		ScrollUp: key.NewBinding(
			key.WithKeys("pgup"),
			key.WithHelp("pgup", "scroll up"),
		),
		ScrollDn: key.NewBinding(
			key.WithKeys("pgdown"),
			key.WithHelp("pgdn", "scroll down"),
		),
		FocusBack: key.NewBinding(
			key.WithKeys("esc"),
			key.WithHelp("esc", "back"),
		),
		ToggleSidebar: key.NewBinding(
			key.WithKeys("f2"),
			key.WithHelp("f2", "sidebar"),
		),
		CopyResults: key.NewBinding(
			key.WithKeys("y"),
			key.WithHelp("y", "copy"),
		),
		CopyResultsMD: key.NewBinding(
			key.WithKeys("Y"),
			key.WithHelp("Y", "copy md"),
		),
	}
}
