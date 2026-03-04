package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/timerange"
)

func init() {
	rootCmd.AddCommand(newSampleCmd())
}

func newSampleCmd() *cobra.Command {
	var since string

	cmd := &cobra.Command{
		Use:   "sample [count] [filter]",
		Short: "Show a sample of recent events",
		Long:  `Fetch a small sample of recent events, useful for exploring data structure and field names.`,
		Example: `  lynxdb sample                         5 random events (default)
  lynxdb sample 10                      10 events
  lynxdb sample 5 'source=nginx'        5 nginx events
  lynxdb sample 3 --format json | jq .  JSON for inspecting structure`,
		RunE: func(cmd *cobra.Command, args []string) error {
			count := 5
			var filter string

			if len(args) > 0 {
				if n, err := strconv.Atoi(args[0]); err == nil {
					count = n
					if len(args) > 1 {
						filter = strings.Join(args[1:], " ")
					}
				} else {
					filter = strings.Join(args, " ")
				}
			}

			return runSample(count, filter, since)
		},
	}

	cmd.Flags().StringVarP(&since, "since", "s", "", "Relative time range (e.g., 15m, 1h, 7d)")

	return cmd
}

func runSample(count int, filter, since string) error {
	query := "FROM main"
	if filter != "" {
		query += " | " + filter
	}

	query += fmt.Sprintf(" | head %d", count)

	var from, to string
	if since != "" {
		tr, err := timerange.FromSince(since, time.Now())
		if err != nil {
			return fmt.Errorf("invalid --since: %w", err)
		}

		from = tr.Earliest.Format(time.RFC3339Nano)
		to = tr.Latest.Format(time.RFC3339Nano)
	}

	ctx := context.Background()

	result, err := apiClient().QuerySync(ctx, query, from, to)
	if err != nil {
		return err
	}

	rows := queryResultToRows(result)
	if len(rows) == 0 {
		fmt.Println("No events found.")
		printHint("Try a broader search or widen the time range with --since 1h")

		return nil
	}

	// When --format is explicitly set (not "auto"), use the unified formatter.
	if globalFormat != "auto" {
		return printFormattedRows(rows)
	}

	// Human-readable numbered output.
	for i, row := range rows {
		printSampleEvent(i+1, row)
		if i < len(rows)-1 {
			fmt.Println()
		}
	}

	printMeta("\n%d events sampled", len(rows))

	return nil
}

func printSampleEvent(idx int, row map[string]interface{}) {
	t := ui.Stdout

	// Extract well-known fields for the header line.
	ts, _ := row["_time"].(string)
	if ts == "" {
		ts, _ = row["_timestamp"].(string)
	}

	fmt.Printf("  %s  %s\n", t.Bold.Render(fmt.Sprintf("#%d", idx)), t.Dim.Render(ts))

	// Print key=value pairs for all fields.
	for k, v := range row {
		if k == "_time" || k == "_timestamp" {
			continue
		}

		val := fmt.Sprint(v)
		if len(val) > 120 {
			val = val[:117] + "..."
		}

		fmt.Printf("      %s=%s\n", t.Info.Render(k), val)
	}
}
