package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/buildinfo"
	"github.com/lynxbase/lynxdb/internal/output"
	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage"
	"github.com/lynxbase/lynxdb/pkg/timerange"
)

func init() {
	rootCmd.AddCommand(newShellCmd())
}

func newShellCmd() *cobra.Command {
	var (
		file  string
		since string
	)

	cmd := &cobra.Command{
		Use:   "shell",
		Short: "Start an interactive SPL2 shell (REPL)",
		Long: `Starts an interactive shell for running SPL2 queries.

Supports query history (arrow keys), dot commands (.help, .quit),
and both server and file modes.`,
		Example: `  lynxdb shell                          Connect to server
  lynxdb shell --file access.log        Query a local file
  lynxdb shell --since 1h               Default time range`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if file != "" {
				return runShellFile(file, since)
			}

			return runShellServer(since)
		},
	}

	cmd.Flags().StringVarP(&file, "file", "f", "", "File mode (load file into ephemeral engine)")
	cmd.Flags().StringVarP(&since, "since", "s", "15m", "Default time range for queries")

	return cmd
}

// spl2Commands lists SPL2 command keywords for tab completion.
var spl2Commands = []string{
	"FROM", "SEARCH", "WHERE", "EVAL", "STATS", "SORT", "TABLE", "FIELDS",
	"RENAME", "HEAD", "TAIL", "DEDUP", "REX", "BIN", "TIMECHART", "TOP",
	"RARE", "STREAMSTATS", "EVENTSTATS", "JOIN", "APPEND", "MULTISEARCH",
	"TRANSACTION", "XYSERIES", "FILLNULL", "LIMIT",
}

// spl2AggFunctions lists SPL2 aggregation functions for tab completion.
var spl2AggFunctions = []string{
	"count", "sum", "avg", "min", "max", "dc", "values",
	"stdev", "perc50", "perc75", "perc90", "perc95", "perc99",
	"earliest", "latest",
}

// shellSession holds state for an interactive shell session.
type shellSession struct {
	server       string
	engine       *storage.Engine // non-nil in file mode
	since        string
	mode         string // "server" or "file"
	prompt       string
	cachedFields []string // cached field names for tab completion
	multiLine    string   // accumulated multi-line input
}

func runShellServer(since string) error {
	t := ui.Stdout
	fmt.Println()
	fmt.Printf("  \U0001F43E %s — Interactive Shell\n", t.Bold.Render(fmt.Sprintf("LynxDB %s", buildinfo.Version)))
	fmt.Printf("  Connected to %s\n", t.Accent.Render(globalServer))
	fmt.Printf("  Type %s for commands, %s to exit.\n\n",
		t.Accent.Render(".help"), t.Accent.Render("Ctrl+D"))

	s := &shellSession{
		server: globalServer,
		since:  since,
		mode:   "server",
		prompt: "lynxdb> ",
	}

	return s.run()
}

func runShellFile(file, since string) error {
	matches, err := filepath.Glob(file)
	if err != nil {
		return fmt.Errorf("invalid file pattern: %w", err)
	}
	if len(matches) == 0 {
		return fmt.Errorf("no files matching: %s", file)
	}

	eng := storage.NewEphemeralEngine()

	totalEvents := 0

	for _, path := range matches {
		f, err := os.Open(path)
		if err != nil {
			eng.Close()

			return fmt.Errorf("open %s: %w", path, err)
		}

		n, err := eng.IngestReader(f, storage.IngestOpts{Source: path})
		f.Close()

		if err != nil {
			eng.Close()

			return fmt.Errorf("ingest %s: %w", path, err)
		}

		totalEvents += n
	}

	t := ui.Stdout
	fmt.Println()
	fmt.Printf("  \U0001F43E %s — File Mode (%s, %s events loaded)\n",
		t.Bold.Render(fmt.Sprintf("LynxDB %s", buildinfo.Version)),
		file, formatCount(int64(totalEvents)))
	fmt.Printf("  Type %s for commands, %s to exit.\n\n",
		t.Accent.Render(".help"), t.Accent.Render("Ctrl+D"))

	s := &shellSession{
		engine: eng,
		since:  since,
		mode:   "file",
		prompt: "lynxdb[file]> ",
	}

	defer eng.Close()

	return s.run()
}

// Do implements readline.AutoCompleter for dynamic tab completion.
func (s *shellSession) Do(line []rune, pos int) ([][]rune, int) {
	left := string(line[:pos])
	lastSpace := strings.LastIndexByte(left, ' ')

	var prefix string
	if lastSpace >= 0 {
		prefix = left[lastSpace+1:]
	} else {
		prefix = left
	}

	lowerPrefix := strings.ToLower(prefix)

	var candidates [][]rune
	for _, cmd := range spl2Commands {
		if strings.HasPrefix(strings.ToLower(cmd), lowerPrefix) {
			suffix := cmd[len(prefix):]
			candidates = append(candidates, []rune(suffix))
		}
	}
	for _, fn := range spl2AggFunctions {
		if strings.HasPrefix(fn, lowerPrefix) {
			suffix := fn[len(prefix):]
			candidates = append(candidates, []rune(suffix))
		}
	}

	if s.cachedFields == nil && s.mode == "server" {
		s.cachedFields = fetchFieldNames()
	}
	for _, f := range s.cachedFields {
		if strings.HasPrefix(strings.ToLower(f), lowerPrefix) {
			suffix := f[len(prefix):]
			candidates = append(candidates, []rune(suffix))
		}
	}

	return candidates, len(prefix)
}

func (s *shellSession) run() error {
	historyPath := shellHistoryPath()

	rl, err := readline.NewEx(&readline.Config{
		Prompt:            s.prompt,
		HistoryFile:       historyPath,
		HistoryLimit:      1000,
		InterruptPrompt:   "^C",
		EOFPrompt:         "exit",
		HistorySearchFold: true,
		AutoComplete:      s,
		Listener:          s,
	})
	if err != nil {
		return fmt.Errorf("init readline: %w", err)
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err != nil {
			if errors.Is(err, readline.ErrInterrupt) {
				// Ctrl+C — if in multi-line mode, cancel it.
				if s.multiLine != "" {
					s.multiLine = ""
					rl.SetPrompt(s.prompt)
					fmt.Println("  (multi-line canceled)")

					continue
				}

				fmt.Println("  (Ctrl+D to exit)")

				continue
			}
			// Ctrl+D or other EOF — exit cleanly.
			fmt.Println()

			return nil
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if s.multiLine == "" && strings.HasPrefix(line, ".") {
			if s.handleDotCommand(line) {
				return nil // .quit
			}

			continue
		}

		if strings.HasSuffix(line, "|") {
			s.multiLine += line + " "
			rl.SetPrompt("    ...> ")

			continue
		}

		fullQuery := s.multiLine + line
		s.multiLine = ""
		rl.SetPrompt(s.prompt)

		s.executeQuery(fullQuery)
	}
}

// OnChange implements readline.Listener for dynamic completion updates.
func (s *shellSession) OnChange(line []rune, pos int, key rune) ([]rune, int, bool) {
	return line, pos, false
}

// handleDotCommand handles shell meta-commands. Returns true if session should exit.
func (s *shellSession) handleDotCommand(line string) bool {
	parts := strings.Fields(line)
	cmd := strings.ToLower(parts[0])

	switch cmd {
	case ".help", ".h":
		s.printHelp()
	case ".quit", ".exit", ".q":
		return true
	case ".clear", ".cls":
		fmt.Print("\033[H\033[2J")
	case ".history":
		s.printHistory()
	case ".fields":
		s.showFields()
	case ".sources":
		s.showSources()
	case ".explain":
		if len(parts) < 2 {
			fmt.Println("  Usage: .explain <query>")
		} else {
			s.explainQuery(strings.Join(parts[1:], " "))
		}
	case ".set":
		if len(parts) < 3 {
			fmt.Println("  Usage: .set <key> <value>")
			fmt.Printf("  Current: since=%s\n", s.since)
		} else {
			s.setOption(parts[1], parts[2])
		}
	default:
		fmt.Printf("  Unknown command %q. Type .help for available commands.\n\n", cmd)
	}

	return false
}

func (s *shellSession) printHelp() {
	t := ui.Stdout
	fmt.Println()
	fmt.Printf("  %s\n\n", t.Bold.Render("Shell Commands:"))
	fmt.Println("  .help              Show this help")
	fmt.Println("  .quit              Exit the shell (or Ctrl+D)")
	fmt.Println("  .clear             Clear the screen")
	fmt.Println("  .history           Show query history")
	fmt.Println("  .fields            List known fields")
	fmt.Println("  .sources           List event sources")
	fmt.Println("  .explain <query>   Show query execution plan")
	fmt.Println("  .set since <val>   Change default time range")
	fmt.Println()
	fmt.Printf("  %s\n\n", t.Bold.Render("Tips:"))
	fmt.Println("  - Tab for completion (SPL2 commands, fields)")
	fmt.Println("  - End a line with | for multi-line input")
	fmt.Println("  - Arrow Up/Down for history recall")
	fmt.Println("  - Ctrl+C cancels current input")
	fmt.Println("  - Queries are sent to the connected server")
	fmt.Println()
}

func (s *shellSession) printHistory() {
	path := shellHistoryPath()

	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("  No history yet.")

		return
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	start := 0

	if len(lines) > 20 {
		start = len(lines) - 20
	}

	fmt.Println()

	for i := start; i < len(lines); i++ {
		fmt.Printf("  %3d  %s\n", i+1, lines[i])
	}

	fmt.Println()
}

func (s *shellSession) showFields() {
	if s.mode == "file" {
		fmt.Println("  .fields is not available in file mode.")

		return
	}

	ctx := context.Background()

	fields, err := apiClient().Fields(ctx)
	if err != nil || len(fields) == 0 {
		fmt.Println("  No fields found.")

		return
	}

	fmt.Println()

	for _, f := range fields {
		fmt.Printf("  %-20s %-10s %.0f%%\n", f.Name, f.Type, f.Coverage)
	}

	fmt.Println()
}

func (s *shellSession) showSources() {
	if s.mode == "file" {
		fmt.Println("  .sources is not available in file mode.")

		return
	}

	ctx := context.Background()

	sources, err := apiClient().Sources(ctx)
	if err != nil || len(sources) == 0 {
		fmt.Println("  No sources found.")

		return
	}

	fmt.Println()

	for _, src := range sources {
		fmt.Printf("  %-20s %s events\n", src.Name, formatCount(src.EventCount))
	}

	fmt.Println()
}

func (s *shellSession) explainQuery(query string) {
	if s.mode == "file" {
		fmt.Println("  .explain is not available in file mode.")

		return
	}

	ctx := context.Background()

	result, err := apiClient().Explain(ctx, query)
	if err != nil {
		fmt.Printf("  Error: %v\n\n", err)

		return
	}

	b, _ := json.MarshalIndent(result, "  ", "  ")
	fmt.Printf("\n  %s\n\n", string(b))
}

func (s *shellSession) setOption(key, value string) {
	switch strings.ToLower(key) {
	case "since":
		s.since = value
		fmt.Printf("  Set since=%s\n\n", value)
	default:
		fmt.Printf("  Unknown option %q. Available: since\n\n", key)
	}
}

func (s *shellSession) executeQuery(query string) {
	start := time.Now()

	if s.mode == "file" {
		s.executeQueryFile(query, start)
	} else {
		s.executeQueryServer(query, start)
	}
}

func (s *shellSession) executeQueryFile(query string, start time.Time) {
	normalizedQuery := ensureFromClause(query)

	if hints := spl2.DetectCompatHints(normalizedQuery); len(hints) > 0 {
		fmt.Fprint(os.Stderr, spl2.FormatCompatHints(hints))
	}

	ctx := context.Background()

	result, _, err := s.engine.Query(ctx, normalizedQuery, storage.QueryOpts{})
	if err != nil {
		fmt.Printf("\n  %s %s\n\n", ui.Stdout.Error.Render("Error:"), spl2.FormatParseError(err, normalizedQuery))

		return
	}

	elapsed := time.Since(start)

	fmt.Println()
	s.printResults(result.Rows, elapsed)
}

func (s *shellSession) executeQueryServer(query string, start time.Time) {
	ctx := context.Background()

	var from, to string
	if s.since != "" {
		tr, err := timerange.FromSince(strings.TrimPrefix(s.since, "-"), time.Now())
		if err == nil {
			from = tr.Earliest.Format(time.RFC3339Nano)
			to = tr.Latest.Format(time.RFC3339Nano)
		}
	}

	result, err := apiClient().QuerySync(ctx, query, from, to)
	if err != nil {
		if isConnectionError(err) {
			fmt.Printf("\n  %s Cannot connect to %s\n\n", ui.Stdout.Error.Render("Error:"), s.server)
		} else {
			fmt.Printf("\n  %s %v\n\n", ui.Stdout.Error.Render("Error:"), err)
		}

		return
	}

	rows := queryResultToRows(result)
	elapsed := time.Since(start)

	fmt.Println()
	s.printResults(rows, elapsed)
}

func (s *shellSession) printResults(rows []map[string]interface{}, elapsed time.Duration) {
	t := ui.Stdout

	if len(rows) == 0 {
		fmt.Printf("  %s\n\n", t.Dim.Render("No results."))

		return
	}

	// Use table format for the shell for readability.
	f := output.DetectFormat("table", rows)
	if err := f.Format(os.Stdout, rows); err != nil {
		// Fallback to JSON.
		for _, row := range rows {
			b, _ := json.Marshal(row)
			fmt.Printf("  %s\n", string(b))
		}
	}

	word := "rows"
	if len(rows) == 1 {
		word = "row"
	}

	fmt.Printf("\n  %s\n\n", t.Dim.Render(fmt.Sprintf("%d %s — %s", len(rows), word, formatElapsed(elapsed))))
}

// shellHistoryPath returns the path for the shell history file.
func shellHistoryPath() string {
	dataDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}

	dir := filepath.Join(dataDir, ".local", "share", "lynxdb")

	_ = os.MkdirAll(dir, 0o755)

	return filepath.Join(dir, "history")
}
