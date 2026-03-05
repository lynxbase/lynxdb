package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
)

func init() {
	rootCmd.AddCommand(newLastCmd())
}

func newLastCmd() *cobra.Command {
	var since string

	cmd := &cobra.Command{
		Use:   "last",
		Short: "Re-run the last query",
		Long:  `Repeats the most recently executed query. Optionally override the time range or output format.`,
		Example: `  lynxdb last                    Repeat last query
  lynxdb last --since 24h        Same query, wider time range
  lynxdb last -F csv             Same query, CSV output`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLast(since)
		},
	}

	cmd.Flags().StringVarP(&since, "since", "s", "", "Override time range")

	return cmd
}

// savedQuery stores the last query for replay.
type savedQuery struct {
	Query  string `json:"query"`
	Since  string `json:"since,omitempty"`
	From   string `json:"from,omitempty"`
	To     string `json:"to,omitempty"`
	Server string `json:"server,omitempty"`
}

func lastQueryPath() string {
	dataDir := os.Getenv("XDG_DATA_HOME")
	if dataDir == "" {
		home, _ := os.UserHomeDir()
		dataDir = filepath.Join(home, ".local", "share")
	}

	return filepath.Join(dataDir, "lynxdb", "last_query.json")
}

// SaveLastQuery persists the last query for `lynxdb last`.
func SaveLastQuery(query, since, from, to string) {
	sq := savedQuery{
		Query:  query,
		Since:  since,
		From:   from,
		To:     to,
		Server: globalServer,
	}

	data, err := json.Marshal(sq)
	if err != nil {
		return
	}

	path := lastQueryPath()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}

	_ = os.WriteFile(path, data, 0o600)
}

func loadLastQuery() (*savedQuery, error) {
	path := lastQueryPath()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("no previous query found. Run a query first")
		}

		return nil, fmt.Errorf("read last query: %w", err)
	}

	var sq savedQuery
	if err := json.Unmarshal(data, &sq); err != nil {
		return nil, fmt.Errorf("parse last query: %w", err)
	}

	if sq.Query == "" {
		return nil, fmt.Errorf("no previous query found. Run a query first")
	}

	return &sq, nil
}

func runLast(sinceOverride string) error {
	sq, err := loadLastQuery()
	if err != nil {
		return err
	}

	since := sq.Since
	if sinceOverride != "" {
		since = sinceOverride
	}

	if !globalQuiet {
		t := ui.Stderr
		fmt.Fprintf(os.Stderr, "  %s %s\n", t.Dim.Render("Replaying:"), sq.Query)

		if since != "" {
			fmt.Fprintf(os.Stderr, "  %s last %s\n", t.Dim.Render("Time range:"), since)
		}

		fmt.Fprintln(os.Stderr)
	}

	var cmdArgs []string
	cmdArgs = append(cmdArgs, "query")

	if since != "" {
		cmdArgs = append(cmdArgs, "--since", since)
	} else {
		if sq.From != "" {
			cmdArgs = append(cmdArgs, "--from", sq.From)
		}

		if sq.To != "" {
			cmdArgs = append(cmdArgs, "--to", sq.To)
		}
	}

	cmdArgs = append(cmdArgs, sq.Query)

	rootCmd.SetArgs(cmdArgs)

	return rootCmd.Execute()
}
