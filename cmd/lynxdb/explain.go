package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/stats"
)

func init() {
	rootCmd.AddCommand(newExplainCmd())
}

func newExplainCmd() *cobra.Command {
	var analyze bool

	cmd := &cobra.Command{
		Use:   "explain [SPL2 query]",
		Short: "Show query execution plan without running",
		Long:  `Parses and optimizes the query, then prints the execution plan, optimizer rules applied, and estimated cost.`,
		Example: `  lynxdb explain 'level=error | stats count by source'
  lynxdb explain 'status>=500 | top 10 uri' --format json
  lynxdb explain --analyze 'level=error | stats count'`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if analyze {
				return runExplainAnalyze(strings.Join(args, " "))
			}

			return runExplain(strings.Join(args, " "))
		},
	}

	cmd.Flags().BoolVar(&analyze, "analyze", false, "Execute query and show plan with actual execution stats")

	return cmd
}

func runExplainAnalyze(query string) error {
	ctx := context.Background()
	c := apiClient()
	start := time.Now()

	explainResult, err := c.Explain(ctx, query)
	if err != nil {
		var apiErr *client.APIError
		if errors.As(err, &apiErr) && apiErr.Code == client.ErrCodeInvalidQuery {
			renderQueryError(query, -1, 0, apiErr.Message, apiErr.Suggestion)

			return err
		}

		return err
	}

	// Execute the query with full profiling.
	qResult, err := c.Query(ctx, client.QueryRequest{
		Q:       query,
		Profile: "full",
	})
	if err != nil {
		return fmt.Errorf("execute for EXPLAIN ANALYZE: %w", err)
	}

	rows := queryResultToRows(qResult)
	st := buildQueryStatsFromMeta(qResult.Meta, int64(len(rows)), time.Since(start))
	st.Recommendations = stats.GenerateRecommendations(st)

	if isJSONFormat() {
		combined := map[string]interface{}{
			"explain": explainResult,
			"profile": st,
		}
		b, _ := json.MarshalIndent(combined, "", "  ")
		fmt.Println(string(b))

		return nil
	}

	// Human-readable output: plan then profile.
	t := ui.Stdout
	if explainResult.Parsed != nil {
		if len(explainResult.Parsed.Pipeline) > 0 {
			var stages []string
			for _, s := range explainResult.Parsed.Pipeline {
				stages = append(stages, s.Command)
			}

			fmt.Printf("%s\n  %s\n\n", t.Bold.Render("Plan:"), strings.Join(stages, " → "))
		}
		if explainResult.Parsed.EstimatedCost != "" {
			fmt.Printf("%s %s\n\n", t.Bold.Render("Estimated cost:"), explainResult.Parsed.EstimatedCost)
		}
	}

	// Print the profile from actual execution.
	stats.FormatProfile(os.Stdout, st)

	return nil
}

func runExplain(query string) error {
	ctx := context.Background()

	result, err := apiClient().Explain(ctx, query)
	if err != nil {
		var apiErr *client.APIError
		if errors.As(err, &apiErr) && apiErr.Code == client.ErrCodeInvalidQuery {
			renderQueryError(query, -1, 0, apiErr.Message, apiErr.Suggestion)

			return err
		}

		return err
	}

	if isJSONFormat() {
		b, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(b))

		return nil
	}

	// Human-readable plan output.
	t := ui.Stdout

	if result.Parsed != nil {
		// Show pipeline stages as the plan.
		if len(result.Parsed.Pipeline) > 0 {
			var stages []string
			for _, s := range result.Parsed.Pipeline {
				stages = append(stages, s.Command)
			}

			fmt.Printf("%s\n  %s\n\n", t.Bold.Render("Plan:"), strings.Join(stages, " → "))
		}

		if result.Parsed.EstimatedCost != "" {
			fmt.Printf("%s %s\n\n", t.Bold.Render("Estimated cost:"), result.Parsed.EstimatedCost)
		}

		if len(result.Parsed.FieldsRead) > 0 {
			fmt.Printf("%s %s\n\n", t.Bold.Render("Fields read:"),
				strings.Join(result.Parsed.FieldsRead, ", "))
		}
	}

	if result.Acceleration != nil && result.Acceleration.Available {
		fmt.Printf("%s view=%s speedup=%s\n\n",
			t.Bold.Render("MV acceleration:"),
			result.Acceleration.View,
			result.Acceleration.EstimatedSpeedup)
	}

	if len(result.Errors) > 0 {
		fmt.Println(t.Bold.Render("Errors:"))
		for _, e := range result.Errors {
			fmt.Printf("  • %s\n", e.Message)
			if e.Suggestion != "" {
				fmt.Printf("    %s\n", t.Dim.Render(e.Suggestion))
			}
		}

		fmt.Println()
	}

	return nil
}
