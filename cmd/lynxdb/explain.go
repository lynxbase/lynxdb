package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/stats"
)

func init() {
	rootCmd.AddCommand(newExplainCmd())
}

func newExplainCmd() *cobra.Command {
	var analyze bool

	cmd := &cobra.Command{
		Use:   "explain [PPL query]",
		Short: "Show the query plan without running the query",
		Long: `Compiles the query on the server and prints its logical IR plan, the
optimized plan, the optimizer rules that fired and the storage hints the scan uses.`,
		Example: `  lynxdb explain 'source=main | where level = "error" | stats count() by host'
  lynxdb explain 'source=main | where status >= 500 | top 10 uri' --format json
  lynxdb explain --analyze 'source=main | where level = "error" | stats count()'`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExplain(strings.Join(args, " "), analyze)
		},
	}

	cmd.Flags().BoolVar(&analyze, "analyze", false, "Run the query and add its execution profile")

	return cmd
}

func runExplain(query string, analyze bool) error {
	ctx := context.Background()
	c := apiClient()
	start := time.Now()

	result, err := c.Explain(ctx, query)
	if err != nil {
		var apiErr *client.APIError
		if errors.As(err, &apiErr) && client.IsInvalidQuery(apiErr) {
			renderQueryError(query, -1, 0, apiErr.Message, apiErr.Suggestion)
		}

		return err
	}

	var st *stats.QueryStats
	if analyze && result.IsValid {
		qResult, err := c.Query(ctx, client.QueryRequest{Q: query})
		if err != nil {
			return &queryError{inner: err, query: query}
		}
		rows := queryResultToRows(qResult)
		st = buildQueryStatsFromMeta(qResult.Meta, int64(len(rows)), time.Since(start))
	}

	if isJSONFormat() {
		out := map[string]interface{}{"explain": result}
		if st != nil {
			out["profile"] = st
		}
		b, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(b))

		return nil
	}

	if !result.IsValid {
		for _, e := range result.Errors {
			col := 0
			if e.Position != nil {
				col = e.Position.Column - 1
			}
			renderQueryError(query, col, 1, e.Message, e.Suggestion)
		}

		return fmt.Errorf("invalid query")
	}

	writeExplain(os.Stdout, result.Parsed)
	if st != nil {
		stats.FormatProfile(os.Stdout, st)
	}

	return nil
}

func writeExplain(w io.Writer, p *client.ExplainParsed) {
	if p == nil {
		return
	}
	fmt.Fprintln(w, "Logical plan:")
	fmt.Fprint(w, p.LogicalPlan)
	fmt.Fprintln(w, "\nOptimized plan:")
	fmt.Fprint(w, p.OptimizedPlan)
	if len(p.OptimizerRules) > 0 {
		fmt.Fprintln(w, "\nOptimizer rules:")
		for _, r := range p.OptimizerRules {
			fmt.Fprintf(w, "  %s x%d\n", r.Name, r.Count)
		}
	}
	fmt.Fprintln(w, "\nScan:")
	fmt.Fprintf(w, "  result type   %s\n", p.ResultType)
	fmt.Fprintf(w, "  cost          %s\n", p.EstimatedCost)
	if len(p.SearchTerms) > 0 {
		fmt.Fprintf(w, "  search terms  %s\n", strings.Join(p.SearchTerms, ", "))
	}
	if len(p.FieldsRead) > 0 {
		fmt.Fprintf(w, "  fields read   %s\n", strings.Join(p.FieldsRead, ", "))
	}
	if p.SourceScope != nil {
		fmt.Fprintf(w, "  sources       %s %s%s\n", p.SourceScope.Type, strings.Join(p.SourceScope.Sources, ", "), p.SourceScope.Pattern)
	}
	if p.UsesFullScan {
		fmt.Fprintln(w, "  full scan: add a time range or search terms to skip data")
	}
}
