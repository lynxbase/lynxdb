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

	if err := renderExplainReport(os.Stdout, explainResult, explainReportOptions{
		Analyze: true,
		Plain:   explainPlainMode(),
		Theme:   ui.Stdout,
	}); err != nil {
		return err
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

	return renderExplainReport(os.Stdout, result, explainReportOptions{
		Plain: explainPlainMode(),
		Theme: ui.Stdout,
	})
}
