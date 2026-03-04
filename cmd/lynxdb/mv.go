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
	rootCmd.AddCommand(newMVCmd())
}

func newMVCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mv",
		Short: "Manage materialized views",
		Example: `  lynxdb mv create mv_errors_5m 'level=error | stats count by source' --retention 90d
  lynxdb mv list
  lynxdb mv status mv_errors_5m
  lynxdb mv backfill mv_errors_5m
  lynxdb mv pause mv_errors_5m
  lynxdb mv resume mv_errors_5m
  lynxdb mv drop mv_errors_5m`,
	}

	var retention string

	createCmd := &cobra.Command{
		Use:   "create <name> <query>",
		Short: "Create a materialized view",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			return runMVCreate(args[0], args[1], retention)
		},
	}
	createCmd.Flags().StringVar(&retention, "retention", "", "Retention period (e.g., 30d)")

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List all materialized views",
		RunE:  runMVList,
	}
	statusCmd := &cobra.Command{
		Use:               "status <name>",
		Short:             "Show detailed view status",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeMVNames,
		RunE: func(_ *cobra.Command, args []string) error {
			return runMVStatus(args[0])
		},
	}

	var forceFlag bool
	var dryRunFlag bool

	dropCmd := &cobra.Command{
		Use:               "drop <name>",
		Short:             "Drop a materialized view",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeMVNames,
		RunE: func(_ *cobra.Command, args []string) error {
			return runMVDrop(args[0], forceFlag, dryRunFlag)
		},
	}
	dropCmd.Flags().BoolVar(&forceFlag, "force", false, "Skip confirmation prompt")
	dropCmd.Flags().BoolVar(&dryRunFlag, "dry-run", false, "Show what would be deleted without applying")

	pauseCmd := &cobra.Command{
		Use:               "pause <name>",
		Short:             "Pause a materialized view pipeline",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeMVNames,
		RunE: func(_ *cobra.Command, args []string) error {
			return runMVPause(args[0])
		},
	}

	resumeCmd := &cobra.Command{
		Use:               "resume <name>",
		Short:             "Resume a paused materialized view pipeline",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeMVNames,
		RunE: func(_ *cobra.Command, args []string) error {
			return runMVResume(args[0])
		},
	}

	backfillCmd := &cobra.Command{
		Use:               "backfill <name>",
		Short:             "Manually trigger a backfill for a materialized view",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeMVNames,
		RunE: func(_ *cobra.Command, args []string) error {
			return runMVBackfill(args[0])
		},
	}

	cmd.AddCommand(createCmd, listCmd, statusCmd, dropCmd, pauseCmd, resumeCmd, backfillCmd)

	return cmd
}

func runMVCreate(name, query, retention string) error {
	ctx := context.Background()

	// Pre-validate query so parse errors get caret display.
	if _, err := apiClient().Explain(ctx, query); err != nil {
		if client.IsInvalidQuery(err) {
			return &queryError{inner: err, query: query}
		}
		// Non-parse errors — proceed to create and let the server report them.
	}

	input := client.ViewInput{
		Name: name,
		Q:    query,
	}
	if retention != "" {
		input.Retention = retention
	}

	if _, err := apiClient().CreateView(ctx, input); err != nil {
		return err
	}

	printSuccess("Created materialized view %q", name)
	printNextSteps(
		fmt.Sprintf("lynxdb mv status %s        Track backfill progress", name),
		"lynxdb mv list                  List all views",
	)

	return nil
}

func runMVList(_ *cobra.Command, _ []string) error {
	ctx := context.Background()

	views, err := apiClient().ListViews(ctx)
	if err != nil {
		return err
	}

	if isJSONFormat() {
		for _, v := range views {
			b, _ := json.Marshal(v)
			fmt.Println(string(b))
		}

		return nil
	}

	if len(views) == 0 {
		fmt.Println("No materialized views.")
		printNextSteps(
			"lynxdb mv create <name> <query>   Create a new view",
		)

		return nil
	}

	t := ui.Stdout
	tbl := ui.NewTable(t).
		SetColumns("NAME", "STATUS", "QUERY")

	for _, v := range views {
		displayStatus := mvStatusColored(t, v.Status)
		tbl.AddRow(v.Name, displayStatus, v.Query)
	}

	fmt.Print(tbl.String())
	fmt.Printf("\n%s\n", t.Dim.Render(fmt.Sprintf("%d views total", len(views))))

	return nil
}

func runMVStatus(name string) error {
	ctx := context.Background()

	view, err := apiClient().GetView(ctx, name)
	if err != nil {
		return err
	}

	if isJSONFormat() {
		b, _ := json.MarshalIndent(view, "", "  ")
		fmt.Println(string(b))

		return nil
	}

	t := ui.Stdout

	fmt.Println()
	fmt.Printf("  %s\n\n", t.Bold.Render(view.Name))
	fmt.Println(t.KeyValue("Status", mvStatusColored(t, view.Status)))

	if lower := strings.ToLower(view.Status); lower == "backfill" || lower == "backfilling" {
		if view.Backfill != nil {
			elapsed := time.Duration(view.Backfill.ElapsedMS * float64(time.Millisecond))
			fmt.Println(t.KeyValue("Progress", fmt.Sprintf("%s — %d/%d segments, %s rows scanned (%s)",
				view.Backfill.Phase,
				view.Backfill.SegmentsScanned, view.Backfill.SegmentsTotal,
				formatCountHuman(view.Backfill.RowsScanned),
				formatElapsed(elapsed))))
		} else {
			fmt.Println(t.KeyValue("Progress", t.Dim.Render("starting...")))
		}
	}

	fmt.Println(t.KeyValue("Query", view.Query))

	if len(view.Columns) > 0 {
		names := make([]string, 0, len(view.Columns))
		for _, c := range view.Columns {
			names = append(names, c.Name)
		}

		fmt.Println(t.KeyValue("Columns", strings.Join(names, ", ")))
	}

	fmt.Println(t.KeyValue("Retention", view.Retention))
	fmt.Println(t.KeyValue("Created", view.CreatedAt))
	fmt.Println()

	lower := strings.ToLower(view.Status)
	switch lower {
	case "backfill":
		printNextSteps(
			fmt.Sprintf("lynxdb mv status %s         Check backfill progress", name),
			fmt.Sprintf("lynxdb query '| from %s'    Query the view (partial results during backfill)", name),
		)
	default:
		printNextSteps(
			fmt.Sprintf("lynxdb mv pause %s          Pause the pipeline", name),
			fmt.Sprintf("lynxdb query '| from %s'    Query the view", name),
		)
	}

	return nil
}

func runMVDrop(name string, force, dryRun bool) error {
	if dryRun {
		t := ui.Stdout
		fmt.Printf("  %s\n", t.Bold.Render("Would delete:"))
		fmt.Println(t.KeyValue("View", name))
		fmt.Printf("\n  %s\n", t.Dim.Render("Run without --dry-run to delete."))

		return nil
	}

	if !force {
		msg := fmt.Sprintf("This will permanently delete materialized view '%s' and all its data.", name)
		if !confirmDestructive(msg, name) {
			if !isStdinTTY() {
				return fmt.Errorf("destructive action requires confirmation; use --force in non-interactive mode")
			}

			printHint("Aborted.")

			return nil
		}
	}

	ctx := context.Background()
	if err := apiClient().DeleteView(ctx, name); err != nil {
		return err
	}

	printSuccess("Dropped materialized view %q", name)

	return nil
}

// runMVBackfill triggers a manual backfill for a materialized view.
func runMVBackfill(name string) error {
	ctx := context.Background()

	if err := apiClient().TriggerBackfill(ctx, name); err != nil {
		return err
	}

	printSuccess("Backfill triggered for materialized view %q", name)
	printNextSteps(
		fmt.Sprintf("lynxdb mv status %s         Track backfill progress", name),
		fmt.Sprintf("lynxdb query '| from %s'    Query the view", name),
	)

	return nil
}

// runMVPause pauses a materialized view pipeline.
func runMVPause(name string) error {
	return patchMVPaused(name, true)
}

// runMVResume resumes a paused materialized view pipeline.
func runMVResume(name string) error {
	return patchMVPaused(name, false)
}

// patchMVPaused sends a PATCH request to pause or resume a materialized view.
func patchMVPaused(name string, paused bool) error {
	ctx := context.Background()

	if _, err := apiClient().PatchView(ctx, name, client.ViewPatchInput{
		Paused: &paused,
	}); err != nil {
		return err
	}

	if paused {
		printSuccess("Paused materialized view %q", name)
		printNextSteps(
			fmt.Sprintf("lynxdb mv resume %s   Resume the pipeline", name),
			fmt.Sprintf("lynxdb mv status %s   Check current status", name),
		)
	} else {
		printSuccess("Resumed materialized view %q", name)
		printNextSteps(
			fmt.Sprintf("lynxdb mv status %s   Check current status", name),
		)
	}

	return nil
}

// mvStatusColored returns a colored status string for TTY display.
func mvStatusColored(t *ui.Theme, status string) string {
	lower := strings.ToLower(status)
	switch {
	case lower == "active":
		return t.Success.Render(status)
	case lower == "backfilling" || lower == "backfill":
		return t.Warning.Render(status)
	case lower == "paused":
		return t.Dim.Render(status)
	case lower == "error" || strings.HasPrefix(lower, "err"):
		return t.Error.Render(status)
	default:
		return status
	}
}
