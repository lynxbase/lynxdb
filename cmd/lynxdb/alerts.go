package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
)

func init() {
	rootCmd.AddCommand(newAlertsCmd())
}

func newAlertsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "alerts [id]",
		Short: "Manage alerts",
		Long:  `List, create, test, enable, disable, and delete alerts.`,
		Example: `  lynxdb alerts                                List all alerts
  lynxdb alerts create --name "High errors" --query 'level=error | stats count as errors | where errors > 100' --interval 5m
  lynxdb alerts <id>                           Show alert details
  lynxdb alerts <id> test                      Test alert without notifying
  lynxdb alerts <id> test-channels             Send test notification
  lynxdb alerts <id> enable                    Enable an alert
  lynxdb alerts <id> disable                   Disable an alert
  lynxdb alerts <id> delete                    Delete an alert`,
		RunE: func(_ *cobra.Command, args []string) error {
			if len(args) > 0 {
				return runAlertDetail(args[0])
			}

			return runAlertsList()
		},
	}

	var (
		alertName     string
		alertQuery    string
		alertInterval string
		forceFlag     bool
	)

	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new alert",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runAlertCreate(alertName, alertQuery, alertInterval)
		},
	}
	createCmd.Flags().StringVar(&alertName, "name", "", "Alert name (required)")
	createCmd.Flags().StringVar(&alertQuery, "query", "", "SPL2 query (required)")
	createCmd.Flags().StringVar(&alertInterval, "interval", "5m", "Check interval")
	_ = createCmd.MarkFlagRequired("name")
	_ = createCmd.MarkFlagRequired("query")

	testCmd := &cobra.Command{
		Use:   "test <id>",
		Short: "Test alert evaluation without sending notifications",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runAlertTest(args[0])
		},
	}

	testChannelsCmd := &cobra.Command{
		Use:   "test-channels <id>",
		Short: "Send a test notification to all configured channels",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runAlertTestChannels(args[0])
		},
	}

	enableCmd := &cobra.Command{
		Use:   "enable <id>",
		Short: "Enable an alert",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runAlertSetEnabled(args[0], true)
		},
	}

	disableCmd := &cobra.Command{
		Use:   "disable <id>",
		Short: "Disable an alert",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runAlertSetEnabled(args[0], false)
		},
	}

	deleteCmd := &cobra.Command{
		Use:   "delete <id>",
		Short: "Delete an alert",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runAlertDelete(args[0], forceFlag)
		},
	}
	deleteCmd.Flags().BoolVar(&forceFlag, "force", false, "Skip confirmation prompt")

	cmd.AddCommand(createCmd, testCmd, testChannelsCmd, enableCmd, disableCmd, deleteCmd)

	return cmd
}

func runAlertsList() error {
	ctx := context.Background()

	alerts, err := apiClient().ListAlerts(ctx)
	if err != nil {
		return err
	}

	if isJSONFormat() {
		for _, a := range alerts {
			b, _ := json.Marshal(a)
			fmt.Println(string(b))
		}

		return nil
	}

	if len(alerts) == 0 {
		fmt.Println("No alerts configured.")
		printNextSteps(
			"lynxdb alerts create --name <name> --query <query>   Create an alert",
		)

		return nil
	}

	t := ui.Stdout
	tbl := ui.NewTable(t).
		SetColumns("ID", "NAME", "STATUS", "INTERVAL", "QUERY")

	for _, a := range alerts {
		status := "enabled"
		if !a.Enabled {
			status = "disabled"
		}

		tbl.AddRow(a.ID, a.Name, status, a.Interval, a.Q)
	}

	fmt.Print(tbl.String())
	fmt.Printf("\n%s\n", t.Dim.Render(fmt.Sprintf("%d alerts total", len(alerts))))

	return nil
}

func runAlertDetail(id string) error {
	ctx := context.Background()

	alert, err := apiClient().GetAlert(ctx, id)
	if err != nil {
		return err
	}

	if isJSONFormat() {
		b, _ := json.MarshalIndent(alert, "", "  ")
		fmt.Println(string(b))

		return nil
	}

	t := ui.Stdout
	fmt.Println()
	fmt.Printf("  %s\n\n", t.Bold.Render(alert.Name))
	fmt.Printf("  ID:         %s\n", alert.ID)
	fmt.Printf("  Query:      %s\n", alert.Q)
	fmt.Printf("  Interval:   %s\n", alert.Interval)

	status := "enabled"
	if !alert.Enabled {
		status = "disabled"
	}

	fmt.Printf("  Status:     %s\n", status)

	if len(alert.Channels) > 0 {
		fmt.Printf("  Channels:   %d configured\n", len(alert.Channels))
	}

	fmt.Println()

	return nil
}

func runAlertCreate(name, query, interval string) error {
	ctx := context.Background()

	_, err := apiClient().CreateAlert(ctx, client.AlertInput{
		Name:     name,
		Q:        query,
		Interval: interval,
	})
	if err != nil {
		return err
	}

	printSuccess("Created alert %q", name)
	printNextSteps(
		"lynxdb alerts                  List all alerts",
	)

	return nil
}

func runAlertTest(id string) error {
	ctx := context.Background()

	result, err := apiClient().TestAlert(ctx, id)
	if err != nil {
		return err
	}

	if isJSONFormat() {
		b, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(b))

		return nil
	}

	printSuccess("Alert test completed for %s", id)

	return nil
}

func runAlertTestChannels(id string) error {
	ctx := context.Background()

	if _, err := apiClient().TestAlertChannels(ctx, id); err != nil {
		return err
	}

	printSuccess("Test notifications sent for alert %s", id)

	return nil
}

func runAlertSetEnabled(id string, enabled bool) error {
	ctx := context.Background()

	if _, err := apiClient().PatchAlert(ctx, id, client.AlertPatchInput{
		Enabled: &enabled,
	}); err != nil {
		return err
	}

	if enabled {
		printSuccess("Enabled alert %s", id)
	} else {
		printSuccess("Disabled alert %s", id)
	}

	return nil
}

func runAlertDelete(id string, force bool) error {
	if !force {
		msg := fmt.Sprintf("Delete alert '%s'?", id)
		if !confirmAction(msg) {
			printHint("Aborted.")

			return nil
		}
	}

	ctx := context.Background()
	if err := apiClient().DeleteAlert(ctx, id); err != nil {
		return err
	}

	printSuccess("Deleted alert %s", id)

	return nil
}
