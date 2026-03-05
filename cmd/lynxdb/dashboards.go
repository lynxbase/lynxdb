package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
)

func init() {
	rootCmd.AddCommand(newDashboardsCmd())
}

func newDashboardsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "dashboards [id]",
		Aliases: []string{"dash"},
		Short:   "Manage dashboards",
		Long:    `List, create, export, open, and delete dashboards.`,
		Example: `  lynxdb dashboards                              List all dashboards
  lynxdb dashboards <id>                         Show dashboard details
  lynxdb dashboards create --file dash.json      Create from JSON file
  lynxdb dashboards <id> open                    Open in Web UI
  lynxdb dashboards <id> export > dash.json      Export as JSON
  lynxdb dashboards <id> delete                  Delete a dashboard`,
		RunE: func(_ *cobra.Command, args []string) error {
			if len(args) > 0 {
				return runDashboardDetail(args[0])
			}

			return runDashboardsList()
		},
	}

	var (
		dashFile  string
		forceFlag bool
	)

	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create a dashboard from a JSON file",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDashboardCreate(dashFile)
		},
	}
	createCmd.Flags().StringVar(&dashFile, "file", "", "Path to dashboard JSON file (required)")
	_ = createCmd.MarkFlagRequired("file")

	openCmd := &cobra.Command{
		Use:   "open <id>",
		Short: "Open a dashboard in the Web UI",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runDashboardOpen(args[0])
		},
	}

	exportCmd := &cobra.Command{
		Use:   "export <id>",
		Short: "Export a dashboard as JSON",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runDashboardExport(args[0])
		},
	}

	deleteCmd := &cobra.Command{
		Use:   "delete <id>",
		Short: "Delete a dashboard",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runDashboardDelete(args[0], forceFlag)
		},
	}
	deleteCmd.Flags().BoolVar(&forceFlag, "force", false, "Skip confirmation prompt")

	cmd.AddCommand(createCmd, openCmd, exportCmd, deleteCmd)

	return cmd
}

func runDashboardsList() error {
	ctx := context.Background()

	dashboards, err := apiClient().ListDashboards(ctx)
	if err != nil {
		return err
	}

	if isJSONFormat() {
		for _, d := range dashboards {
			b, _ := json.Marshal(d)
			fmt.Println(string(b))
		}

		return nil
	}

	if len(dashboards) == 0 {
		fmt.Println("No dashboards found.")
		printNextSteps(
			"lynxdb dashboards create --file dash.json   Create a dashboard",
		)

		return nil
	}

	t := ui.Stdout
	tbl := ui.NewTable(t).
		SetColumns("ID", "NAME", "PANELS", "UPDATED")

	for _, d := range dashboards {
		updated := ""
		if d.UpdatedAt != "" {
			updated = formatRelativeTime(d.UpdatedAt)
		}

		tbl.AddRow(d.ID, d.Name, fmt.Sprint(len(d.Panels)), updated)
	}

	fmt.Print(tbl.String())
	fmt.Printf("\n%s\n", t.Dim.Render(fmt.Sprintf("%d dashboards total", len(dashboards))))

	return nil
}

func runDashboardDetail(id string) error {
	ctx := context.Background()

	dash, err := apiClient().GetDashboard(ctx, id)
	if err != nil {
		return err
	}

	if isJSONFormat() {
		b, _ := json.MarshalIndent(dash, "", "  ")
		fmt.Println(string(b))

		return nil
	}

	t := ui.Stdout
	fmt.Println()
	fmt.Printf("  %s\n\n", t.Bold.Render(dash.Name))
	fmt.Printf("  ID:         %s\n", dash.ID)
	fmt.Printf("  Panels:     %d\n", len(dash.Panels))

	for i, p := range dash.Panels {
		fmt.Printf("    %d. %s (%s)\n", i+1, p.Title, p.Type)
	}

	if len(dash.Variables) > 0 {
		fmt.Printf("  Variables:  %d\n", len(dash.Variables))
	}

	fmt.Println()
	printNextSteps(
		fmt.Sprintf("lynxdb dashboards %s open      Open in browser", id),
		fmt.Sprintf("lynxdb dashboards %s export    Export as JSON", id),
	)

	return nil
}

func runDashboardCreate(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	var input client.DashboardInput
	if err := json.Unmarshal(data, &input); err != nil {
		return fmt.Errorf("invalid JSON in %s: %w", filePath, err)
	}

	ctx := context.Background()
	if _, err := apiClient().CreateDashboard(ctx, input); err != nil {
		return err
	}

	printSuccess("Created dashboard %q", input.Name)
	printNextSteps(
		"lynxdb dashboards              List all dashboards",
	)

	return nil
}

func runDashboardOpen(id string) error {
	u := globalServer + "/dashboards/" + id
	printHint("Opening %s in browser...", u)

	if err := openBrowser(u); err != nil {
		return fmt.Errorf("could not open browser: %w\n  Open manually: %s", err, u)
	}

	return nil
}

func runDashboardExport(id string) error {
	ctx := context.Background()

	dash, err := apiClient().GetDashboard(ctx, id)
	if err != nil {
		return err
	}

	b, err := json.MarshalIndent(dash, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	fmt.Println(string(b))

	return nil
}

func runDashboardDelete(id string, force bool) error {
	if !force {
		msg := fmt.Sprintf("Delete dashboard '%s'?", id)
		if !confirmAction(msg) {
			printHint("Aborted.")

			return nil
		}
	}

	ctx := context.Background()
	if err := apiClient().DeleteDashboard(ctx, id); err != nil {
		return err
	}

	printSuccess("Deleted dashboard %s", id)

	return nil
}
