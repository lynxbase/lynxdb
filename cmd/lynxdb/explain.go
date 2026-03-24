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

		// Render optimizer narration.
		narration := narrateExplain(result.Parsed)
		if narration != "" {
			fmt.Print(narration)
		}
	}

	if result.Acceleration != nil && result.Acceleration.Available {
		accelLine := fmt.Sprintf("  view=%s", result.Acceleration.View)
		if result.Acceleration.EstimatedSpeedup != "" {
			accelLine += fmt.Sprintf(" speedup=%s", result.Acceleration.EstimatedSpeedup)
		}

		fmt.Printf("%s%s\n\n", t.Bold.Render("MV acceleration:"), accelLine)
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

// narrateExplain renders optimizer details, rule applications, physical plan
// decisions, and warnings as human-readable prose.
func narrateExplain(parsed *client.ExplainParsed) string {
	if parsed == nil {
		return ""
	}

	var b strings.Builder
	t := ui.Stdout

	// Optimizer messages — what the optimizer did.
	if len(parsed.OptimizerMessages) > 0 {
		fmt.Fprintf(&b, "%s\n", t.Bold.Render("Optimizer notes:"))
		for _, msg := range parsed.OptimizerMessages {
			fmt.Fprintf(&b, "  %s %s\n", t.Dim.Render("✓"), msg)
		}
		b.WriteByte('\n')
	}

	// Rules applied — which optimizer rules fired.
	if len(parsed.OptimizerRules) > 0 {
		totalFirings := 0
		for _, rd := range parsed.OptimizerRules {
			totalFirings += rd.Count
		}
		fmt.Fprintf(&b, "%s (%d rules, %d firings)\n",
			t.Bold.Render("Rules applied"), len(parsed.OptimizerRules), totalFirings)

		var parts []string
		for _, rd := range parsed.OptimizerRules {
			if rd.Count > 1 {
				parts = append(parts, fmt.Sprintf("%s ×%d", rd.Name, rd.Count))
			} else {
				parts = append(parts, rd.Name)
			}
		}
		fmt.Fprintf(&b, "  %s\n\n", strings.Join(parts, ", "))
	}

	// Physical plan narration.
	if parsed.PhysicalPlan != nil {
		var planLines []string
		pp := parsed.PhysicalPlan
		if pp.PartialAgg {
			planLines = append(planLines, "Partial aggregation enabled — per-segment results merged at coordinator")
		}
		if pp.CountStarOnly {
			planLines = append(planLines, "Count(*) optimization — reading row count from metadata (no event scan)")
		}
		if pp.TopKAgg {
			planLines = append(planLines, fmt.Sprintf("TopK heap optimization — tracking top %d during aggregation (avoids full sort)", pp.TopK))
		}
		if pp.JoinStrategy != "" {
			planLines = append(planLines, fmt.Sprintf("Join strategy: %s", pp.JoinStrategy))
		}

		if len(planLines) > 0 {
			fmt.Fprintf(&b, "%s\n", t.Bold.Render("Execution strategy:"))
			for _, line := range planLines {
				fmt.Fprintf(&b, "  • %s\n", line)
			}
			b.WriteByte('\n')
		}
	}

	// Cost explanation with time bounds and scan info.
	if parsed.HasTimeBounds || parsed.UsesFullScan || parsed.SourceScope != nil {
		fmt.Fprintf(&b, "%s\n", t.Bold.Render("Scan details:"))
		if parsed.HasTimeBounds {
			fmt.Fprintf(&b, "  Time range: bounded (query uses time filters)\n")
		} else if parsed.UsesFullScan {
			fmt.Fprintf(&b, "  Time range: unbounded (full scan — consider adding time filters)\n")
		}
		if parsed.SourceScope != nil {
			scope := parsed.SourceScope
			if scope.Type == "single" && len(scope.Sources) > 0 {
				fmt.Fprintf(&b, "  Source: %s\n", scope.Sources[0])
			} else if scope.Type == "multi" && len(scope.Sources) > 0 {
				fmt.Fprintf(&b, "  Sources: %s (%d)\n", strings.Join(scope.Sources, ", "), len(scope.Sources))
			} else if scope.Type == "glob" && scope.Pattern != "" {
				fmt.Fprintf(&b, "  Source pattern: %s", scope.Pattern)
				if scope.TotalSourcesAvailable > 0 {
					fmt.Fprintf(&b, " (%d sources available)", scope.TotalSourcesAvailable)
				}
				b.WriteByte('\n')
			}
		}
		b.WriteByte('\n')
	}

	// Optimizer warnings.
	if len(parsed.OptimizerWarnings) > 0 {
		fmt.Fprintf(&b, "%s\n", t.Bold.Render("Warnings:"))
		for _, w := range parsed.OptimizerWarnings {
			fmt.Fprintf(&b, "  %s %s\n", t.Warning.Render("!"), w)
		}
		b.WriteByte('\n')
	}

	// Timing.
	if parsed.ParseMS > 0 || parsed.OptimizeMS > 0 {
		fmt.Fprintf(&b, "  %s\n", t.Dim.Render(
			fmt.Sprintf("parse: %.2fms  optimize: %.2fms", parsed.ParseMS, parsed.OptimizeMS)))
		b.WriteByte('\n')
	}

	return b.String()
}
