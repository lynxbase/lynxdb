package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
)

func init() {
	rootCmd.AddCommand(newExplainErrorCmd())
}

func newExplainErrorCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "explain-error [ERROR_CODE]",
		Short: "Show detailed explanation for a LynxDB error code",
		Long:  `Displays a detailed explanation with examples for the given error code (e.g., LF-E101).`,
		Example: `  lynxdb explain-error LF-E101
  lynxdb explain-error LF-E201`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExplainError(args[0])
		},
	}
}

func runExplainError(code string) error {
	info, ok := errorExplanations[code]
	if !ok {
		ui.Stderr.RenderError(fmt.Errorf("unknown error code %q. Run 'lynxdb explain-error LF-E101' for an example.", code))

		return nil
	}

	t := ui.Stdout

	fmt.Printf("\n  %s\n", t.Bold.Render(info.Title))
	fmt.Printf("  Code: %s\n\n", t.Info.Render(code))
	fmt.Printf("  %s\n\n", info.Description)

	if len(info.Causes) > 0 {
		fmt.Printf("  %s\n", t.Bold.Render("Common causes:"))
		for _, c := range info.Causes {
			fmt.Printf("    • %s\n", c)
		}
		fmt.Println()
	}

	if len(info.Examples) > 0 {
		fmt.Printf("  %s\n", t.Bold.Render("Examples:"))
		for _, ex := range info.Examples {
			fmt.Printf("    %s %s\n", t.StatusErr.Render("✗"), ex.Bad)
			fmt.Printf("    %s %s\n", t.StatusOK.Render("✔"), ex.Good)
			fmt.Println()
		}
	}

	if info.MoreInfo != "" {
		fmt.Printf("  %s\n", t.Dim.Render(info.MoreInfo))
	}

	fmt.Println()

	return nil
}

type errorExplanation struct {
	Title       string
	Description string
	Causes      []string
	Examples    []example
	MoreInfo    string
}

type example struct {
	Bad  string
	Good string
}

var errorExplanations = map[string]errorExplanation{
	"LF-E101": {
		Title:       "Unknown command",
		Description: "The parser encountered a token that doesn't match any known command.",
		Causes: []string{
			"Typo in command name (e.g., \"staats\" instead of \"stats\")",
			"Missing pipe (|) before a command",
			"Using a field name without the \"search\" keyword",
		},
		Examples: []example{
			{Bad: "level=error | staats count", Good: "level=error | stats count"},
			{Bad: "level=error stats count", Good: "level=error | stats count"},
		},
		MoreInfo: "See also: LF-E104 (missing pipe), LF-E103 (unknown field)",
	},
	"LF-E102": {
		Title:       "Unknown function",
		Description: "The parser encountered a function name that isn't recognized.",
		Causes: []string{
			"Typo in function name (e.g., \"avgerage\" instead of \"avg\")",
			"Using a function that doesn't exist in LynxDB",
		},
		Examples: []example{
			{Bad: "| eval x = avgerage(y)", Good: "| eval x = avg(y)"},
			{Bad: "| eval x = SUBSTR(y, 1)", Good: "| eval x = substr(y, 1)"},
		},
		MoreInfo: "Run 'lynxdb fields' to see available functions.",
	},
	"LF-E103": {
		Title:       "Unknown field",
		Description: "The query references a field that doesn't exist in the data.",
		Causes: []string{
			"Typo in field name",
			"Field doesn't exist in this data source",
			"Field exists in some events but not others (schema-on-read)",
		},
		Examples: []example{
			{Bad: "where stauts >= 500", Good: "where status >= 500"},
			{Bad: "where duration > 1000", Good: "where duration_ms > 1000"},
		},
		MoreInfo: "Run 'lynxdb fields' or use '| glimpse' to discover available fields.",
	},
	"LF-E104": {
		Title:       "Missing pipe",
		Description: "A command name was found without a preceding pipe (|) operator.",
		Causes: []string{
			"Forgot the pipe character before a command",
			"Two commands written without a pipe between them",
		},
		Examples: []example{
			{Bad: "level=error stats count by source", Good: "level=error | stats count by source"},
			{Bad: "stats count sort -count", Good: "stats count | sort -count"},
		},
	},
	"LF-E201": {
		Title:       "Type mismatch",
		Description: "The expression attempts to compare or operate on incompatible types.",
		Causes: []string{
			"Comparing a string field to a number without conversion",
			"Arithmetic on a string field",
			"Inconsistent field types across events (schema-on-read)",
		},
		Examples: []example{
			{Bad: "where status >= 500  (when status is stored as string)", Good: "where tonumber(status) >= 500"},
			{Bad: "eval x = duration + 1  (when duration is a string)", Good: "eval x = tonumber(duration) + 1"},
		},
		MoreInfo: "LynxDB is schema-on-read — fields may have mixed types. Use tonumber(), tostring(), or ?? for null coalescing.",
	},
	"LF-E301": {
		Title:       "Syntax error",
		Description: "The query doesn't follow SPL2 syntax rules.",
		Causes: []string{
			"Invalid operator or expression",
			"Missing required clause (e.g., 'by' after 'stats')",
			"Unexpected token in the current context",
		},
		Examples: []example{
			{Bad: "stats count source  (missing 'by')", Good: "stats count by source"},
			{Bad: "sort count", Good: "sort -count or sort +count"},
		},
	},
	"LF-E302": {
		Title:       "Unterminated string",
		Description: "A string literal was opened with a quote but never closed.",
		Causes: []string{
			"Missing closing double-quote",
			"Quote inside a string not properly escaped",
		},
		Examples: []example{
			{Bad: `where message = "hello`, Good: `where message = "hello"`},
			{Bad: `where msg = "say \"hi"`, Good: `where msg = "say \"hi\""`},
		},
	},
	"LF-E303": {
		Title:       "Unmatched parenthesis",
		Description: "A parenthesis was opened but never closed, or closed without a matching open.",
		Causes: []string{
			"Missing closing ')' in a function call or expression",
			"Extra closing ')' without a matching '('",
		},
		Examples: []example{
			{Bad: `eval x = (a + b`, Good: `eval x = (a + b)`},
			{Bad: `where (status >= 500 AND uri = "/api"`, Good: `where (status >= 500 AND uri = "/api")`},
		},
	},
	"LF-E304": {
		Title:       "Unmatched bracket",
		Description: "A bracket was opened but never closed, or closed without a matching open.",
		Causes: []string{
			"Missing closing ']' in a source time range like from nginx[-1h",
			"Extra closing ']' without a matching '['",
		},
		Examples: []example{
			{Bad: `from nginx[-1h`, Good: `from nginx[-1h]`},
			{Bad: `from nginx[-7d..-1d`, Good: `from nginx[-7d..-1d]`},
		},
	},
	"LF-E305": {
		Title:       "Empty pipeline",
		Description: "The query ends with a pipe (|) but no command follows it.",
		Causes: []string{
			"Trailing pipe with nothing after it",
			"Incomplete query construction",
		},
		Examples: []example{
			{Bad: "from nginx |", Good: "from nginx | stats count"},
			{Bad: "from nginx | where status=200 |", Good: "from nginx | where status=200 | head 10"},
		},
	},
	"LF-E306": {
		Title:       "Clause used as command",
		Description: "A clause keyword (like BY, AS, SPAN) was used as a standalone command, but these keywords only make sense inside their parent commands.",
		Causes: []string{
			"Using 'by' without a preceding 'stats' or 'timechart'",
			"Using 'as' as a standalone command instead of inside eval/stats",
		},
		Examples: []example{
			{Bad: "| by source", Good: "| stats count by source"},
			{Bad: "| as myfield", Good: "| eval myfield = value"},
		},
	},
	"LF-E401": {
		Title:       "Missing aggregation",
		Description: "The 'stats' command requires at least one aggregation function.",
		Causes: []string{
			"Wrote 'stats' without specifying an aggregation like count(), avg(), sum()",
			"Typo in the aggregation function name",
		},
		Examples: []example{
			{Bad: "stats by source", Good: "stats count by source"},
			{Bad: "stats avg by source", Good: "stats avg(duration) by source"},
		},
	},
	"LF-E402": {
		Title:       "Missing BY keyword",
		Description: "A field name follows an aggregation without the required 'by' keyword.",
		Causes: []string{
			"Forgot 'by' between aggregation and group-by field",
		},
		Examples: []example{
			{Bad: "stats count source", Good: "stats count by source"},
			{Bad: "stats avg(duration) uri", Good: "stats avg(duration) by uri"},
		},
	},
	"LF-E501": {
		Title:       "Query too complex",
		Description: "The expression nesting depth exceeds the maximum allowed (128 levels).",
		Causes: []string{
			"Deeply nested boolean expressions",
			"Complex sub-expressions in eval",
		},
		Examples: []example{
			{Bad: "where (a AND (b AND (c AND ...)))  (128+ levels)", Good: "Simplify the expression or break into multiple queries"},
		},
	},
	"LF-E601": {
		Title:       "Parse format error",
		Description: "The 'parse' command was used with an invalid or unrecognized format specification.",
		Causes: []string{
			"Missing parentheses around parse arguments",
			"Unknown format name in parse command",
			"Missing required arguments for the parse format",
		},
		Examples: []example{
			{Bad: "| parse json _raw", Good: "| parse json(_raw)"},
			{Bad: "| parse combined", Good: "| parse combined(_raw)"},
		},
		MoreInfo: "Known formats: json, logfmt, syslog, combined, clf, regex, pattern, kv, docker",
	},
	"LF-E602": {
		Title:       "Missing compute clause",
		Description: "The 'group' or 'every' command requires a 'compute' clause with aggregation functions.",
		Causes: []string{
			"Using 'group by field' without 'compute count()'",
			"Using 'every 5m' without 'compute count()'",
		},
		Examples: []example{
			{Bad: "| group by source", Good: "| group by source compute count() as n"},
			{Bad: "| every 5m", Good: "| every 5m compute count() as n"},
		},
	},
}
