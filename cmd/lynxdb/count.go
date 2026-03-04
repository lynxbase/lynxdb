package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/timerange"
)

func init() {
	rootCmd.AddCommand(newCountCmd())
}

func newCountCmd() *cobra.Command {
	var since string

	cmd := &cobra.Command{
		Use:   "count [filter]",
		Short: "Quick event count",
		Long:  `Count events matching an optional filter. Faster than 'query ... | stats count'.`,
		Example: `  lynxdb count                      Count all events
  lynxdb count 'level=error'        Count errors
  lynxdb count --since 1h           Count events in last hour`,
		RunE: func(cmd *cobra.Command, args []string) error {
			filter := ""
			if len(args) > 0 {
				filter = strings.Join(args, " ")
			}

			return runCount(filter, since)
		},
	}

	cmd.Flags().StringVarP(&since, "since", "s", "", "Relative time range (e.g., 15m, 1h, 7d)")

	return cmd
}

func runCount(filter, since string) error {
	query := "FROM main | stats count"
	if filter != "" {
		query = "FROM main | " + filter + " | stats count"
	}

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

	// Extract count from the result.
	count := extractCount(result)

	// When --format is explicitly set (not "auto"), use the unified formatter.
	if globalFormat != "auto" {
		return printFormattedRows([]map[string]interface{}{{"count": count}})
	}

	fmt.Printf("%s\n", formatValueTTY(count))

	return nil
}

// extractCount pulls the count value from a query result.
func extractCount(result *client.QueryResult) interface{} {
	switch {
	case result.Aggregate != nil:
		if len(result.Aggregate.Rows) > 0 && len(result.Aggregate.Rows[0]) > 0 {
			return result.Aggregate.Rows[0][0]
		}
	case result.Events != nil:
		if len(result.Events.Events) > 0 {
			if c, ok := result.Events.Events[0]["count"]; ok {
				return c
			}
		}
	}

	return int64(0)
}
