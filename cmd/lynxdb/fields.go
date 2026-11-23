package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/output"
	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
)

func init() {
	rootCmd.AddCommand(newFieldsCmd())
}

func newFieldsCmd() *cobra.Command {
	var (
		values bool
		since  string
		from   string
		to     string
		source string
		prefix string
	)

	cmd := &cobra.Command{
		Use:     "fields [name]",
		Aliases: []string{"f"},
		Short:   "Show field catalog from server",
		Long: `Lists the fields the indexes hold and the indexes that hold each.

When a field name is provided, shows the indexes that hold it and its top values.
Use --values to see top values for a field, optionally within a time range.`,
		Example: `  lynxdb fields                            All fields
  lynxdb fields status                     Detail for 'status' field
  lynxdb fields status --values            Top values for 'status'
  lynxdb fields status --values --since 1h Top values in the last hour
  lynxdb fields --source nginx             Fields of the index nginx
  lynxdb fields --prefix sta               Autocomplete helper
  lynxdb fields --format json              JSON output for scripting`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeFieldNames,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 && values {
				return runFieldValues(args[0], since, from, to, source)
			}
			if since != "" || from != "" || to != "" {
				return fmt.Errorf("--since, --from and --to apply to --values only")
			}
			if len(args) == 1 {
				return runFieldDetail(args[0], source)
			}

			return runFieldsList(source, prefix)
		},
	}

	f := cmd.Flags()
	f.BoolVar(&values, "values", false, "Show top values for the specified field")
	f.StringVarP(&since, "since", "s", "", "Restrict values to a time range (e.g., 15m, 1h, 7d)")
	f.StringVar(&from, "from", "", "Absolute start time of values (ISO 8601)")
	f.StringVar(&to, "to", "", "Absolute end time of values (ISO 8601)")
	f.StringVar(&source, "source", "", "Restrict to one index")
	f.StringVar(&prefix, "prefix", "", "Filter fields by name prefix")

	return cmd
}

type fieldEntry struct {
	Name    string   `json:"name"`
	Indexes []string `json:"indexes"`
}

var (
	fieldColumns      = []output.Column{{Key: "name", Header: "FIELD"}, {Key: "indexes", Header: "INDEXES"}}
	fieldValueColumns = []output.Column{{Key: "value", Header: "VALUE"}, {Key: "count", Header: "COUNT"}}
)

// runFieldsList lists all fields, with optional filtering.
func runFieldsList(source, prefix string) error {
	ctx := context.Background()

	clientFields, err := apiClient().FieldsFiltered(ctx, client.FieldsOpts{
		Source: source,
		Prefix: prefix,
	})
	if err != nil {
		return err
	}

	fields := clientFieldsToEntries(clientFields)

	if jsonOutput() {
		for _, f := range fields {
			if err := printJSON(f); err != nil {
				return err
			}
		}

		return nil
	}

	if len(fields) == 0 {
		if !humanOutputActive() {
			return renderTabular(os.Stdout, fieldColumns, nil, ui.Stdout)
		}
		fmt.Println(ui.Stdout.EmptyState("No fields found.",
			"lynxdb ingest <file>           Ingest a log file",
		))

		return nil
	}

	t := ui.Stdout
	rows := make([][]any, 0, len(fields))
	for _, f := range fields {
		rows = append(rows, []any{f.Name, strings.Join(f.Indexes, ", ")})
	}

	if err := renderTabular(os.Stdout, fieldColumns, rows, t); err != nil {
		return err
	}

	if humanOutputActive() {
		summary := fmt.Sprintf("%s fields total", formatCount(int64(len(fields))))
		if source != "" {
			summary += fmt.Sprintf(" (source=%s)", source)
		}

		fmt.Printf("\n%s\n", t.Dim.Render(summary))
	}

	return nil
}

// runFieldDetail shows the indexes that hold a field and its top values.
func runFieldDetail(name, source string) error {
	ctx := context.Background()

	clientFields, err := apiClient().FieldsFiltered(ctx, client.FieldsOpts{Source: source})
	if err != nil {
		return err
	}

	fields := clientFieldsToEntries(clientFields)

	var field *fieldEntry

	for i := range fields {
		if strings.EqualFold(fields[i].Name, name) {
			field = &fields[i]

			break
		}
	}

	if field == nil {
		suggestions := findSimilarFields(name, fields)
		if len(suggestions) > 0 {
			return fmt.Errorf("unknown field %q. Did you mean: %s?\n\nRun 'lynxdb fields' to see all known fields",
				name, strings.Join(suggestions, ", "))
		}

		return fmt.Errorf("unknown field %q. Run 'lynxdb fields' to see all known fields", name)
	}

	if jsonOutput() {
		return printJSON(field)
	}

	if !humanOutputActive() {
		return renderKeyValues(os.Stdout, [][2]any{
			{"name", field.Name},
			{"indexes", strings.Join(field.Indexes, ",")},
		}, ui.Stdout)
	}

	t := ui.Stdout
	fmt.Println()
	fmt.Printf("  %s\n\n", t.Bold.Render(field.Name))
	fmt.Printf("  Indexes:    %s\n", strings.Join(field.Indexes, ", "))

	result, err := apiClient().FieldValuesFiltered(ctx, field.Name, client.FieldValuesOpts{Limit: 20, Index: source})
	if err == nil && len(result.Values) > 0 {
		fmt.Printf("\n  %s\n", t.Bold.Render("Top Values:"))
		if err := renderTabular(os.Stdout, fieldValueColumns, fieldValueRows(result.Values), t); err != nil {
			return err
		}
	}

	fmt.Println()
	printNextSteps(
		fmt.Sprintf("lynxdb fields %s --values   Full value list", name),
		fmt.Sprintf("lynxdb query 'source=main | where %s = \"<value>\"'    Search by this field", name),
	)

	return nil
}

func fieldValueRows(values []client.FieldValue) [][]any {
	rows := make([][]any, 0, len(values))
	for _, v := range values {
		rows = append(rows, []any{fmt.Sprint(v.Value), v.Count})
	}

	return rows
}

// runFieldValues shows top values for a named field.
func runFieldValues(name, since, from, to, source string) error {
	ctx := context.Background()

	result, err := apiClient().FieldValuesFiltered(ctx, name, client.FieldValuesOpts{
		Limit: 50,
		Since: since,
		From:  from,
		To:    to,
		Index: source,
	})
	if err != nil {
		return err
	}

	if jsonOutput() {
		for _, v := range result.Values {
			if err := printJSON(v); err != nil {
				return err
			}
		}

		return nil
	}

	if len(result.Values) == 0 {
		if !humanOutputActive() {
			return renderTabular(os.Stdout, fieldValueColumns, nil, ui.Stdout)
		}
		fmt.Printf("No values found for field %q.\n", name)

		return nil
	}

	t := ui.Stdout
	fmt.Println()
	fmt.Printf("  %s — top values\n\n", t.Bold.Render(name))

	return renderTabular(os.Stdout, fieldValueColumns, fieldValueRows(result.Values), t)
}

// clientFieldsToEntries converts []client.FieldInfo to []fieldEntry.
func clientFieldsToEntries(fields []client.FieldInfo) []fieldEntry {
	entries := make([]fieldEntry, 0, len(fields))
	for _, f := range fields {
		entries = append(entries, fieldEntry{Name: f.Name, Indexes: f.Indexes})
	}

	return entries
}

// findSimilarFields returns field names similar to the given name (Levenshtein distance <= 2).
func findSimilarFields(name string, fields []fieldEntry) []string {
	lowerName := strings.ToLower(name)
	var suggestions []string

	for _, f := range fields {
		lowerField := strings.ToLower(f.Name)
		if levenshteinDistance(lowerName, lowerField) <= 2 {
			suggestions = append(suggestions, f.Name)
		}
	}

	if len(suggestions) > 3 {
		suggestions = suggestions[:3]
	}

	return suggestions
}

// levenshteinDistance computes the edit distance between two strings.
func levenshteinDistance(a, b string) int {
	if a == "" {
		return len(b)
	}
	if b == "" {
		return len(a)
	}

	// Use single-row DP for memory efficiency.
	row := make([]int, len(b)+1)
	for j := range row {
		row[j] = j
	}

	for i := 1; i <= len(a); i++ {
		prev := row[0]
		row[0] = i

		for j := 1; j <= len(b); j++ {
			old := row[j]
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}

			ins := row[j] + 1
			del := row[j-1] + 1
			sub := prev + cost

			best := ins
			if del < best {
				best = del
			}
			if sub < best {
				best = sub
			}

			row[j] = best
			prev = old
		}
	}

	return row[len(b)]
}
