package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
)

func init() {
	rootCmd.AddCommand(newSavedCmd())
	rootCmd.AddCommand(newSaveCmd())
	rootCmd.AddCommand(newRunCmd())
}

// lynxdb saved (subcommand group)

func newSavedCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "saved",
		Short: "Manage saved queries",
		Long:  `List, create, run, and delete saved queries.`,
		Example: `  lynxdb saved                                  List all saved queries
  lynxdb saved create "5xx-rate" 'status>=500 | stats count by uri'
  lynxdb saved run 5xx-rate --since 1h
  lynxdb saved delete 5xx-rate`,
		RunE: runSavedList,
	}

	createCmd := &cobra.Command{
		Use:   "create <name> <query>",
		Short: "Save a new query",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			return runSavedCreate(args[0], args[1])
		},
	}

	var since, from, to string

	runCmd := &cobra.Command{
		Use:               "run <name>",
		Short:             "Execute a saved query",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeSavedQueryNames,
		RunE: func(_ *cobra.Command, args []string) error {
			return runSavedRun(args[0], since, from, to)
		},
	}
	runCmd.Flags().StringVarP(&since, "since", "s", "", "Relative time range (e.g., 15m, 1h)")
	runCmd.Flags().StringVar(&from, "from", "", "Absolute start time (ISO 8601)")
	runCmd.Flags().StringVar(&to, "to", "", "Absolute end time (ISO 8601)")

	var forceFlag bool

	deleteCmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a saved query",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runSavedDelete(args[0], forceFlag)
		},
	}
	deleteCmd.Flags().BoolVar(&forceFlag, "force", false, "Skip confirmation prompt")

	cmd.AddCommand(createCmd, runCmd, deleteCmd)

	return cmd
}

// lynxdb save (shortcut alias)

func newSaveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "save <name> <query>",
		Short: "Save a query (shortcut for 'saved create')",
		Args:  cobra.ExactArgs(2),
		Example: `  lynxdb save "5xx-rate" '_source=nginx status>=500 | stats count by uri | sort -count'
  lynxdb save "error-by-host" 'level=error | stats count by host | sort -count'`,
		RunE: func(_ *cobra.Command, args []string) error {
			return runSavedCreate(args[0], args[1])
		},
	}
}

// lynxdb run (shortcut alias)

func newRunCmd() *cobra.Command {
	var (
		since string
		from  string
		to    string
	)

	cmd := &cobra.Command{
		Use:   "run <name>",
		Short: "Execute a saved query (shortcut for 'saved run')",
		Args:  cobra.ExactArgs(1),
		Example: `  lynxdb run 5xx-rate
  lynxdb run 5xx-rate --since 24h
  lynxdb run 5xx-rate --format csv > report.csv`,
		ValidArgsFunction: completeSavedQueryNames,
		RunE: func(_ *cobra.Command, args []string) error {
			return runSavedRun(args[0], since, from, to)
		},
	}

	cmd.Flags().StringVarP(&since, "since", "s", "", "Relative time range (e.g., 15m, 1h)")
	cmd.Flags().StringVar(&from, "from", "", "Absolute start time (ISO 8601)")
	cmd.Flags().StringVar(&to, "to", "", "Absolute end time (ISO 8601)")

	return cmd
}

// Implementation

func runSavedList(_ *cobra.Command, _ []string) error {
	ctx := context.Background()

	queries, err := apiClient().ListSavedQueries(ctx)
	if err != nil {
		return err
	}

	if isJSONFormat() {
		for _, q := range queries {
			b, _ := json.Marshal(q)
			fmt.Println(string(b))
		}

		return nil
	}

	if len(queries) == 0 {
		fmt.Println("No saved queries.")
		printNextSteps(
			"lynxdb save <name> <query>   Save a query",
		)

		return nil
	}

	t := ui.Stdout
	tbl := ui.NewTable(t).
		SetColumns("NAME", "QUERY", "CREATED")

	for _, q := range queries {
		created := formatRelativeTime(q.CreatedAt)
		tbl.AddRow(q.Name, q.Q, created)
	}

	fmt.Print(tbl.String())
	fmt.Printf("\n%s\n", t.Dim.Render(fmt.Sprintf("%d saved queries", len(queries))))

	return nil
}

func runSavedCreate(name, query string) error {
	ctx := context.Background()

	if _, err := apiClient().CreateSavedQuery(ctx, client.SavedQueryInput{
		Name: name,
		Q:    query,
	}); err != nil {
		return err
	}

	printSuccess("Saved query %q", name)
	printNextSteps(
		fmt.Sprintf("lynxdb run %s               Execute the saved query", name),
		"lynxdb saved                    List all saved queries",
	)

	return nil
}

func runSavedRun(name, since, from, to string) error {
	// Look up the saved query by name.
	sq, err := findSavedQueryByName(name)
	if err != nil {
		return err
	}

	// Execute the query using the standard query flow.
	query := sq.Q
	SaveLastQuery(query, since, from, to)

	return runQueryServer(query, since, from, to, "", false, "")
}

func runSavedDelete(name string, force bool) error {
	// Look up the saved query by name to get the ID.
	sq, err := findSavedQueryByName(name)
	if err != nil {
		return err
	}

	if !force {
		msg := fmt.Sprintf("Delete saved query '%s'?", name)
		if !confirmAction(msg) {
			printHint("Aborted.")

			return nil
		}
	}

	ctx := context.Background()
	if err := apiClient().DeleteSavedQuery(ctx, sq.ID); err != nil {
		return err
	}

	printSuccess("Deleted saved query %q", name)

	return nil
}

// findSavedQueryByName fetches all saved queries and finds one by name.
func findSavedQueryByName(name string) (*client.SavedQuery, error) {
	ctx := context.Background()

	queries, err := apiClient().ListSavedQueries(ctx)
	if err != nil {
		return nil, err
	}

	for i := range queries {
		if strings.EqualFold(queries[i].Name, name) {
			return &queries[i], nil
		}
	}

	// Suggest similar names.
	var names []string
	for _, q := range queries {
		names = append(names, q.Name)
	}

	if len(names) > 0 {
		return nil, fmt.Errorf("saved query %q not found. Available: %s", name, strings.Join(names, ", "))
	}

	return nil, fmt.Errorf("saved query %q not found. Save one with: lynxdb save <name> <query>", name)
}

// formatRelativeTime formats an ISO timestamp as a relative time string (e.g., "2d ago").
func formatRelativeTime(iso string) string {
	t, err := time.Parse(time.RFC3339Nano, iso)
	if err != nil {
		// Try other common formats.
		t, err = time.Parse(time.RFC3339, iso)
		if err != nil {
			return iso
		}
	}

	d := time.Since(t)

	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	case d < 30*24*time.Hour:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	default:
		return fmt.Sprintf("%dmo ago", int(d.Hours()/(24*30)))
	}
}
