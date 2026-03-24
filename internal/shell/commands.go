package shell

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/lynxbase/lynxdb/internal/output"
	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/storage"
)

// Session provides the execution context for slash commands.
type Session struct {
	Mode      string
	Server    string
	Client    *client.Client
	Engine    *storage.Engine
	Since     string
	Format    output.Format            // default: output.FormatTable
	Timing    bool                     // default: true (show elapsed after results)
	LastQuery string                   // most recently executed query (for .save without arg)
	LastRows  []map[string]interface{} // result of most recent query (for _ reference)
}

// ExecuteSlashCommand dispatches a slash command and returns the output text,
// an optional async tea.Cmd, and whether the shell should quit.
func ExecuteSlashCommand(input string, sess *Session, history *History) (output string, asyncCmd tea.Cmd, quit bool) {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return "", nil, false
	}

	cmd := strings.ToLower(parts[0])

	switch cmd {
	case "/help", "/h":
		return helpText(), nil, false

	case "/quit", "/exit", "/q":
		return "", nil, true

	case "/clear", "/cls":
		// Handled in model.go as a special case.
		return "", nil, false

	case "/history":
		return formatHistory(history), nil, false

	case "/fields":
		if sess.Mode == "file" {
			return "  /fields is not available in file mode.", nil, false
		}

		return "", fetchFieldsForDisplay(sess.Client), false

	case "/sources":
		if sess.Mode == "file" {
			return "  /sources is not available in file mode.", nil, false
		}

		return "", fetchSourcesCmd(sess.Client), false

	case "/explain":
		if len(parts) < 2 {
			return "  Usage: /explain <query>", nil, false
		}

		if sess.Mode == "file" {
			return "  /explain is not available in file mode.", nil, false
		}

		query := strings.Join(parts[1:], " ")

		return "", explainCmd(sess.Client, query), false

	case "/set":
		if len(parts) < 3 {
			return fmt.Sprintf("  Usage: /set <key> <value>\n  Current: since=%s", sess.Since), nil, false
		}

		return setOption(sess, parts[1], parts[2]), nil, false

	case "/format":
		if len(parts) < 2 {
			return fmt.Sprintf("  Current format: %s\n  Usage: /format [table|json|csv|raw]", sess.Format), nil, false
		}

		return setFormat(sess, parts[1]), nil, false

	case "/timing":
		if len(parts) < 2 {
			sess.Timing = !sess.Timing
		} else {
			switch strings.ToLower(parts[1]) {
			case "on", "true", "1":
				sess.Timing = true
			case "off", "false", "0":
				sess.Timing = false
			default:
				return "  Usage: /timing [on|off]", nil, false
			}
		}

		state := "off"
		if sess.Timing {
			state = "on"
		}

		return fmt.Sprintf("  Set timing=%s", state), nil, false

	case "/server":
		if sess.Mode == "file" {
			return "  /server is not available in file mode.", nil, false
		}

		return "", fetchServerInfoCmd(sess.Client), false

	case "/since":
		if len(parts) < 2 {
			return fmt.Sprintf("  Current since: %s\n  Usage: /since <duration>", sess.Since), nil, false
		}

		return setOption(sess, "since", parts[1]), nil, false

	case "/queries":
		if sess.Mode == "file" {
			return "  /queries is not available in file mode.", nil, false
		}

		return "", fetchSavedQueriesCmd(sess.Client), false

	case "/save":
		if sess.Mode == "file" {
			return "  /save is not available in file mode.", nil, false
		}

		if len(parts) < 2 {
			return "  Usage: /save <name> [query]", nil, false
		}

		name := parts[1]
		query := sess.LastQuery

		if len(parts) >= 3 {
			query = strings.Join(parts[2:], " ")
		}

		if query == "" {
			return "  No query to save. Run a query first or specify one.", nil, false
		}

		return "", saveQueryCmd(sess.Client, name, query), false

	case "/run":
		if sess.Mode == "file" {
			return "  /run is not available in file mode.", nil, false
		}

		if len(parts) < 2 {
			return "  Usage: /run <name>", nil, false
		}

		return "", fetchAndRunSavedQueryCmd(sess.Client, parts[1]), false

	case "/tail":
		if sess.Mode == "file" {
			return "  /tail is not available in file mode.", nil, false
		}

		if len(parts) < 2 {
			return "  Usage: /tail <query>", nil, false
		}

		query := strings.Join(parts[1:], " ")

		return "", func() tea.Msg { return tailStartMsg{query: query} }, false

	default:
		return fmt.Sprintf("  Unknown command %q. Type /help for available commands.", cmd), nil, false
	}
}

func helpText() string {
	t := ui.Stdout

	var b strings.Builder
	fmt.Fprintf(&b, "\n  %s\n\n", t.Bold.Render("Shell Commands:"))
	b.WriteString("  /help              Show this help\n")
	b.WriteString("  /quit              Exit the shell (or Ctrl+D)\n")
	b.WriteString("  /clear             Clear the screen\n")
	b.WriteString("  /history           Show query history\n")
	b.WriteString("  /fields            List known fields\n")
	b.WriteString("  /sources           List event sources\n")
	b.WriteString("  /explain <query>   Show query execution plan\n")
	b.WriteString("  /set since <val>   Change default time range\n")
	b.WriteString("  /format <fmt>      Set output format (table|json|csv|raw)\n")
	b.WriteString("  /timing [on|off]   Toggle elapsed time display\n")
	b.WriteString("  /server            Show server version, health, uptime\n")
	b.WriteString("  /since <duration>  Shorthand for /set since <val>\n")
	b.WriteString("  /save <name> [q]   Save last query (or specified query)\n")
	b.WriteString("  /run <name>        Run a saved query by name\n")
	b.WriteString("  /queries           List all saved queries\n")
	b.WriteString("  /tail <query>      Start live tail with SPL2 filter\n")
	fmt.Fprintf(&b, "\n  %s\n\n", t.Bold.Render("Tips:"))
	b.WriteString("  - Tab for completion (SPL2 commands, fields)\n")
	b.WriteString("  - Shift+Enter for multiline input (or end line with |)\n")
	b.WriteString("  - Ctrl+P / Ctrl+N for history recall\n")
	b.WriteString("  - Arrow keys to navigate within editor\n")
	b.WriteString("  - Ctrl+A/E for line start/end, Alt+Left/Right for word nav\n")
	b.WriteString("  - Ctrl+C cancels current input\n")
	b.WriteString("  - PgUp/PgDn to scroll results\n")

	return b.String()
}

func formatHistory(h *History) string {
	entries := h.Entries(20)
	if len(entries) == 0 {
		return "  No history yet."
	}

	var b strings.Builder
	b.WriteByte('\n')

	offset := len(h.entries) - len(entries)
	for i, e := range entries {
		fmt.Fprintf(&b, "  %3d  %s\n", offset+i+1, e)
	}

	return b.String()
}

func setOption(sess *Session, key, value string) string {
	switch strings.ToLower(key) {
	case "since":
		sess.Since = value

		return fmt.Sprintf("  Set since=%s", value)
	default:
		return fmt.Sprintf("  Unknown option %q. Available: since", key)
	}
}

func setFormat(sess *Session, value string) string {
	switch strings.ToLower(value) {
	case "table":
		sess.Format = output.FormatTable
	case "json":
		sess.Format = output.FormatJSON
	case "csv":
		sess.Format = output.FormatCSV
	case "raw":
		sess.Format = output.FormatRaw
	default:
		return fmt.Sprintf("  Unknown format %q. Available: table, json, csv, raw", value)
	}

	return fmt.Sprintf("  Set format=%s", strings.ToLower(value))
}

func fetchServerInfoCmd(c *client.Client) tea.Cmd {
	return func() tea.Msg {
		status, err := c.Status(context.Background())
		if err != nil {
			return slashCommandMsg{output: fmt.Sprintf("  Error: %v", err)}
		}

		t := ui.Stdout

		var b strings.Builder
		b.WriteByte('\n')
		fmt.Fprintf(&b, "  %s  %s\n", t.Dim.Render("Version:"), status.Version)
		fmt.Fprintf(&b, "  %s   %s\n", t.Dim.Render("Health:"), status.Health)
		fmt.Fprintf(&b, "  %s   %s\n", t.Dim.Render("Uptime:"), formatUptimeShell(status.UptimeSeconds))
		fmt.Fprintf(&b, "  %s   %s\n", t.Dim.Render("Events:"), formatCountShell(status.Events.Total))
		fmt.Fprintf(&b, "  %s  %d active\n", t.Dim.Render("Queries:"), status.Queries.Active)

		return slashCommandMsg{output: b.String()}
	}
}

func formatUptimeShell(seconds int) string {
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	}

	if seconds < 3600 {
		return fmt.Sprintf("%dm %ds", seconds/60, seconds%60)
	}

	hours := seconds / 3600
	mins := (seconds % 3600) / 60

	return fmt.Sprintf("%dh %dm", hours, mins)
}

// Async commands that return tea.Msg via tea.Cmd.

func fetchFieldsForDisplay(c *client.Client) tea.Cmd {
	return func() tea.Msg {
		fields, err := c.Fields(context.Background())
		if err != nil || len(fields) == 0 {
			return slashCommandMsg{output: "  No fields found."}
		}

		t := ui.Stdout
		tbl := ui.NewTable(t).SetColumns("FIELD", "TYPE", "COVERAGE")

		for _, f := range fields {
			tbl.AddRow(f.Name, f.Type, fmt.Sprintf("%.0f%%", f.Coverage))
		}

		return slashCommandMsg{output: "\n" + tbl.String()}
	}
}

func fetchSourcesCmd(c *client.Client) tea.Cmd {
	return func() tea.Msg {
		sources, err := c.Sources(context.Background())
		if err != nil || len(sources) == 0 {
			return slashCommandMsg{output: "  No sources found."}
		}

		t := ui.Stdout
		tbl := ui.NewTable(t).SetColumns("SOURCE", "EVENTS")

		for _, src := range sources {
			tbl.AddRow(src.Name, formatCountShell(src.EventCount))
		}

		return slashCommandMsg{output: "\n" + tbl.String()}
	}
}

func explainCmd(c *client.Client, query string) tea.Cmd {
	return func() tea.Msg {
		result, err := c.Explain(context.Background(), query)
		if err != nil {
			return slashCommandMsg{output: fmt.Sprintf("  Error: %v", err)}
		}

		b, _ := json.MarshalIndent(result, "  ", "  ")
		colorized := ui.Stdout.ColorizeJSON(string(b))

		return slashCommandMsg{output: "\n  " + colorized}
	}
}

func fetchSavedQueriesCmd(c *client.Client) tea.Cmd {
	return func() tea.Msg {
		queries, err := c.ListSavedQueries(context.Background())
		if err != nil {
			return slashCommandMsg{output: fmt.Sprintf("  Error: %v", err)}
		}

		if len(queries) == 0 {
			return slashCommandMsg{output: "  No saved queries."}
		}

		t := ui.Stdout
		tbl := ui.NewTable(t).SetColumns("NAME", "QUERY")

		for _, q := range queries {
			tbl.AddRow(q.Name, q.Q)
		}

		return slashCommandMsg{output: "\n" + tbl.String()}
	}
}

func saveQueryCmd(c *client.Client, name, query string) tea.Cmd {
	return func() tea.Msg {
		_, err := c.CreateSavedQuery(context.Background(), client.SavedQueryInput{
			Name: name,
			Q:    query,
		})
		if err != nil {
			return slashCommandMsg{output: fmt.Sprintf("  Error: %v", err)}
		}

		return slashCommandMsg{output: fmt.Sprintf("  Saved query %q.", name)}
	}
}

func fetchAndRunSavedQueryCmd(c *client.Client, name string) tea.Cmd {
	return func() tea.Msg {
		queries, err := c.ListSavedQueries(context.Background())
		if err != nil {
			return savedQueryRunMsg{name: name, err: err}
		}

		for _, q := range queries {
			if strings.EqualFold(q.Name, name) {
				return savedQueryRunMsg{name: q.Name, query: q.Q}
			}
		}

		return savedQueryRunMsg{
			name: name,
			err:  fmt.Errorf("saved query %q not found", name),
		}
	}
}
