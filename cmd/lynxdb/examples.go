package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
)

func init() {
	rootCmd.AddCommand(newExamplesCmd())
}

func newExamplesCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "examples",
		Aliases: []string{"cookbook"},
		Short:   "Show SPL2 query examples",
		Long:    `A cookbook of common SPL2 query patterns for log analysis.`,
		RunE:    runExamples,
	}
}

func runExamples(_ *cobra.Command, _ []string) error {
	t := ui.Stdout

	fmt.Printf("\n  %s\n\n", t.Bold.Render("SPL2 Query Cookbook"))

	sections := []struct {
		title    string
		examples []struct{ query, desc string }
	}{
		{
			title: "Search & Filter",
			examples: []struct{ query, desc string }{
				{"level=error", "Find all error events"},
				{`level=error source=nginx`, "Errors from nginx"},
				{`status>=500`, "Server errors by status code"},
				{`search "connection refused"`, "Full-text search"},
				{`| where duration_ms > 1000`, "Slow requests"},
			},
		},
		{
			title: "Aggregation",
			examples: []struct{ query, desc string }{
				{`level=error | stats count`, "Count errors"},
				{`level=error | stats count by source`, "Errors per source"},
				{`| stats avg(duration_ms), p99(duration_ms) by endpoint`, "Latency stats"},
				{`status>=500 | top 10 uri`, "Top failing URIs"},
				{`| stats dc(user_id) as unique_users`, "Distinct users"},
			},
		},
		{
			title: "Time Analysis",
			examples: []struct{ query, desc string }{
				{`level=error | timechart count span=5m`, "Error rate over time"},
				{`| timechart avg(duration_ms) by service span=1h`, "Latency by service"},
				{`| bin _time span=1h | stats count by _time, level`, "Hourly breakdown"},
			},
		},
		{
			title: "Transformation",
			examples: []struct{ query, desc string }{
				{`| eval is_slow=if(duration_ms>1000, "yes", "no")`, "Computed field"},
				{`| rex field=_raw "user=(?P<user>\\w+)"`, "Extract with regex"},
				{`| rename status AS http_status`, "Rename fields"},
				{`| table _time, source, level, message`, "Select columns"},
			},
		},
		{
			title: "Local File Queries",
			examples: []struct{ query, desc string }{
				{`lynxdb query --file app.log '| stats count by level'`, "Query local file"},
				{`cat app.json | lynxdb query '| where level="ERROR"'`, "Pipe from stdin"},
				{`lynxdb query --file '*.log' '| stats count by source'`, "Glob pattern"},
			},
		},
	}

	for _, s := range sections {
		fmt.Printf("  %s\n", t.Bold.Render(s.title))
		for _, ex := range s.examples {
			fmt.Printf("    %s %s\n", t.Info.Render(fmt.Sprintf("%-55s", ex.query)), t.Dim.Render(ex.desc))
		}
		fmt.Println()
	}

	printWarning("Splunk SPL1 syntax is auto-detected and suggestions are shown.")
	printHint("Run 'lynxdb query --help' for all query options.")
	fmt.Println()

	return nil
}
