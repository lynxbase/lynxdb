package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

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
		Long: `Lists all known fields with their types, coverage, and top values.

When a field name is provided, shows details for that specific field.
Use --values to see top values for a field.`,
		Example: `  lynxdb fields                            All fields
  lynxdb fields status                     Detail for 'status' field
  lynxdb fields status --values            Top values for 'status'
  lynxdb fields --source nginx             Fields seen from nginx
  lynxdb fields --prefix sta               Autocomplete helper
  lynxdb fields --since 1h                 Fields seen in last hour
  lynxdb fields --format json              JSON output for scripting`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completeFieldNames,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				if values {
					return runFieldValues(args[0], since, from, to)
				}

				return runFieldDetail(args[0], since, from, to, source)
			}

			return runFieldsList(since, from, to, source, prefix)
		},
	}

	f := cmd.Flags()
	f.BoolVar(&values, "values", false, "Show top values for the specified field")
	f.StringVarP(&since, "since", "s", "", "Restrict stats to time range (e.g., 15m, 1h, 7d)")
	f.StringVar(&from, "from", "", "Absolute start time (ISO 8601)")
	f.StringVar(&to, "to", "", "Absolute end time (ISO 8601)")
	f.StringVar(&source, "source", "", "Filter by source")
	f.StringVar(&prefix, "prefix", "", "Filter fields by name prefix")

	return cmd
}

type fieldEntry struct {
	Name       string           `json:"name"`
	Type       string           `json:"type"`
	Coverage   float64          `json:"coverage"`
	TotalCount int64            `json:"total_count"`
	TopValues  []fieldValuePair `json:"top_values"`
}

type fieldValuePair struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

// runFieldsList lists all fields, with optional filtering.
func runFieldsList(since, from, to, source, prefix string) error {
	ctx := context.Background()

	clientFields, err := apiClient().FieldsFiltered(ctx, client.FieldsOpts{
		Since:  since,
		From:   from,
		To:     to,
		Source: source,
		Prefix: prefix,
	})
	if err != nil {
		return err
	}

	fields := clientFieldsToEntries(clientFields)

	if prefix != "" {
		fields = filterFieldsByPrefix(fields, prefix)
	}

	if isJSONFormat() {
		for _, f := range fields {
			b, _ := json.Marshal(f)
			fmt.Println(string(b))
		}

		return nil
	}

	if len(fields) == 0 {
		fmt.Println("No fields found. Ingest some data first.")
		printNextSteps(
			"lynxdb ingest <file>           Ingest a log file",
			"lynxdb demo                    Run demo with sample data",
		)

		return nil
	}

	t := ui.Stdout
	tbl := ui.NewTable(t).
		SetColumns("FIELD", "TYPE", "COVERAGE", "TOP VALUES")

	for _, f := range fields {
		topStr := formatTopValues(f.TopValues, f.TotalCount)
		tbl.AddRow(f.Name, f.Type, fmt.Sprintf("%.0f%%", f.Coverage), topStr)
	}

	fmt.Print(tbl.String())

	summary := fmt.Sprintf("%s fields total", formatCount(int64(len(fields))))
	if source != "" {
		summary += fmt.Sprintf(" (source=%s)", source)
	}
	if since != "" {
		summary += fmt.Sprintf(" (last %s)", since)
	}

	fmt.Printf("\n%s\n", t.Dim.Render(summary))

	return nil
}

// runFieldDetail shows details for a single field.
func runFieldDetail(name, since, from, to, source string) error {
	ctx := context.Background()

	clientFields, err := apiClient().FieldsFiltered(ctx, client.FieldsOpts{
		Since:  since,
		From:   from,
		To:     to,
		Source: source,
	})
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

	if isJSONFormat() {
		b, _ := json.MarshalIndent(field, "", "  ")
		fmt.Println(string(b))

		return nil
	}

	topValues := fetchFieldTopValuesDetailed(name)

	t := ui.Stdout
	fmt.Println()
	fmt.Printf("  %s\n\n", t.Bold.Render(field.Name))
	fmt.Printf("  Type:       %s\n", field.Type)
	fmt.Printf("  Coverage:   %.1f%%\n", field.Coverage)
	fmt.Printf("  Count:      %s\n", formatCount(field.TotalCount))

	if len(topValues) > 0 {
		fmt.Printf("\n  %s\n", t.Bold.Render("Top Values:"))

		tbl := ui.NewTable(t).
			SetColumns("VALUE", "COUNT", "%%")

		for _, v := range topValues {
			tbl.AddRow(v.Value, formatCount(int64(v.Count)), fmt.Sprintf("%.1f%%", v.Percent))
		}

		fmt.Print(tbl.String())
	} else if len(field.TopValues) > 0 {
		fmt.Printf("\n  %s\n", t.Bold.Render("Top Values:"))

		tbl := ui.NewTable(t).
			SetColumns("VALUE", "COUNT", "%%")

		for _, v := range field.TopValues {
			pct := float64(0)
			if field.TotalCount > 0 {
				pct = float64(v.Count) / float64(field.TotalCount) * 100
			}

			tbl.AddRow(v.Value, formatCount(v.Count), fmt.Sprintf("%.1f%%", pct))
		}

		fmt.Print(tbl.String())
	}

	fmt.Println()
	printNextSteps(
		fmt.Sprintf("lynxdb fields %s --values   Full value list", name),
		fmt.Sprintf("lynxdb query '%s=<value>'    Search by this field", name),
	)

	return nil
}

// fieldValueDetailed holds a field value entry from the API.
type fieldValueDetailed struct {
	Value   string  `json:"value"`
	Count   int     `json:"count"`
	Percent float64 `json:"percent"`
}

// fetchFieldTopValuesDetailed fetches detailed top values for a field from the API.
func fetchFieldTopValuesDetailed(fieldName string) []fieldValueDetailed {
	ctx := context.Background()

	result, err := apiClient().FieldValuesFiltered(ctx, fieldName, client.FieldValuesOpts{
		Limit: 20,
	})
	if err != nil {
		return nil
	}

	values := make([]fieldValueDetailed, 0, len(result.Values))
	for _, v := range result.Values {
		values = append(values, fieldValueDetailed{
			Value:   fmt.Sprint(v.Value),
			Count:   int(v.Count),
			Percent: v.Percent,
		})
	}

	return values
}

// runFieldValues shows top values for a named field.
func runFieldValues(name, since, from, to string) error {
	ctx := context.Background()

	result, err := apiClient().FieldValuesFiltered(ctx, name, client.FieldValuesOpts{
		Limit: 50,
		Since: since,
		From:  from,
		To:    to,
	})
	if err != nil {
		return err
	}

	if isJSONFormat() {
		b, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(b))

		return nil
	}

	if len(result.Values) == 0 {
		fmt.Printf("No values found for field %q.\n", name)

		return nil
	}

	t := ui.Stdout
	fmt.Println()
	fmt.Printf("  %s — top values\n\n", t.Bold.Render(name))

	tbl := ui.NewTable(t).
		SetColumns("VALUE", "COUNT", "%%")

	for _, v := range result.Values {
		tbl.AddRow(fmt.Sprint(v.Value), formatCount(v.Count), fmt.Sprintf("%.1f%%", v.Percent))
	}

	fmt.Print(tbl.String())

	summary := fmt.Sprintf("%s unique values, %s total occurrences",
		formatCount(int64(result.UniqueCount)),
		formatCount(int64(result.TotalCount)))
	fmt.Printf("\n%s\n", t.Dim.Render(summary))

	return nil
}

// clientFieldsToEntries converts []client.FieldInfo to []fieldEntry.
func clientFieldsToEntries(fields []client.FieldInfo) []fieldEntry {
	entries := make([]fieldEntry, 0, len(fields))
	for _, f := range fields {
		entry := fieldEntry{
			Name:       f.Name,
			Type:       f.Type,
			Coverage:   f.Coverage,
			TotalCount: f.Count,
		}
		for _, tv := range f.TopValues {
			entry.TopValues = append(entry.TopValues, fieldValuePair{
				Value: fmt.Sprint(tv.Value),
				Count: tv.Count,
			})
		}

		entries = append(entries, entry)
	}

	return entries
}

// filterFieldsByPrefix filters fields whose name starts with the given prefix.
func filterFieldsByPrefix(fields []fieldEntry, prefix string) []fieldEntry {
	lowerPrefix := strings.ToLower(prefix)
	var result []fieldEntry

	for _, f := range fields {
		if strings.HasPrefix(strings.ToLower(f.Name), lowerPrefix) {
			result = append(result, f)
		}
	}

	return result
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

func formatTopValues(values []fieldValuePair, total int64) string {
	if len(values) == 0 || total == 0 {
		return ""
	}

	parts := make([]string, 0, len(values))

	for _, v := range values {
		pct := float64(v.Count) / float64(total) * 100
		if pct >= 1 {
			parts = append(parts, fmt.Sprintf("%s(%.0f%%)", truncateStr(v.Value, 20), pct))
		} else {
			parts = append(parts, truncateStr(v.Value, 20))
		}
	}

	return strings.Join(parts, ", ")
}
