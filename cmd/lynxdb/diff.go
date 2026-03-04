package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
)

func init() {
	rootCmd.AddCommand(newDiffCmd())
}

func newDiffCmd() *cobra.Command {
	var period string

	cmd := &cobra.Command{
		Use:   "diff [SPL2 query]",
		Short: "Compare query results across two time periods",
		Long:  `Runs the same query for two consecutive time periods and shows the difference. Useful for spotting trends and anomalies.`,
		Example: `  lynxdb diff 'level=error | stats count by source'
  lynxdb diff 'level=error | stats count by source' --period 1h
  lynxdb diff 'status>=500 | stats count by uri' --period 24h`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			query := strings.Join(args, " ")

			return runDiff(query, period)
		},
	}

	cmd.Flags().StringVar(&period, "period", "1h", "Compare last N vs previous N (e.g., 1h, 6h, 24h)")

	return cmd
}

func runDiff(query, period string) error {
	dur, err := time.ParseDuration(period)
	if err != nil {
		return fmt.Errorf("invalid --period: %w", err)
	}

	now := time.Now()
	fullQuery := ensureFromClause(query)

	// Period 1: now-dur .. now (current).
	curRows, err := executeDiffQuery(fullQuery, now.Add(-dur), now)
	if err != nil {
		return fmt.Errorf("current period query: %w", err)
	}

	// Period 2: now-2*dur .. now-dur (previous).
	prevRows, err := executeDiffQuery(fullQuery, now.Add(-2*dur), now.Add(-dur))
	if err != nil {
		return fmt.Errorf("previous period query: %w", err)
	}

	if isJSONFormat() {
		return printDiffJSON(curRows, prevRows, period)
	}

	return printDiffTable(curRows, prevRows, period)
}

func executeDiffQuery(query string, from, to time.Time) ([]map[string]interface{}, error) {
	ctx := context.Background()

	result, err := apiClient().QuerySync(ctx, query,
		from.Format(time.RFC3339Nano),
		to.Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}

	return queryResultToRows(result), nil
}

func printDiffJSON(curRows, prevRows []map[string]interface{}, period string) error {
	output := map[string]interface{}{
		"period":   period,
		"current":  curRows,
		"previous": prevRows,
	}

	b, _ := json.MarshalIndent(output, "", "  ")
	fmt.Println(string(b))

	return nil
}

func printDiffTable(curRows, prevRows []map[string]interface{}, period string) error {
	t := ui.Stdout
	fmt.Printf("\n  %s last %s vs previous %s\n\n",
		t.Bold.Render("Comparing:"), period, period)

	if len(curRows) == 0 && len(prevRows) == 0 {
		fmt.Println("  No results in either period.")
		printNextSteps(
			fmt.Sprintf("lynxdb diff '...' --period %s   Widen the comparison period", suggestWiderDiffPeriod(period)),
			"lynxdb query '| stats count'            Verify data exists",
			"lynxdb status                            Check server status",
		)

		return nil
	}

	// Collect all columns from both result sets.
	cols := collectDiffColumns(curRows, prevRows)
	numericCols, groupCols := classifyColumns(cols, curRows, prevRows)

	// Build lookup for previous period rows.
	prevLookup := make(map[string]map[string]interface{}, len(prevRows))

	for _, row := range prevRows {
		key := diffRowKey(row, groupCols)
		prevLookup[key] = row
	}

	type diffRow struct {
		group  []string
		now    []string
		prev   []string
		change []string
	}

	var dRows []diffRow

	for _, row := range curRows {
		key := diffRowKey(row, groupCols)
		prevRow := prevLookup[key]
		dr := buildDiffRow(row, prevRow, groupCols, numericCols)
		dRows = append(dRows, dr)
		delete(prevLookup, key)
	}

	// Rows that exist in prev but not current.
	for _, prevRow := range prevLookup {
		dr := buildDiffRow(nil, prevRow, groupCols, numericCols)
		dRows = append(dRows, dr)
	}

	// Build table with lipgloss/table.
	headers := append(groupCols, "NOW", "PREV", "CHANGE")
	tbl := ui.NewTable(t).
		SetColumns(headers...)

	for _, dr := range dRows {
		row := make([]string, 0, len(headers))
		row = append(row, append(dr.group,
			strings.Join(dr.now, ", "),
			strings.Join(dr.prev, ", "),
			strings.Join(dr.change, ", "),
		)...)
		tbl.AddRow(row...)
	}

	fmt.Print(tbl.String())

	// Total summary.
	curTotal, prevTotal := sumNumericColumns(curRows, numericCols), sumNumericColumns(prevRows, numericCols)
	if curTotal > 0 || prevTotal > 0 {
		change := formatDiffPct(curTotal, prevTotal)
		fmt.Printf("\n  Total: %.0f vs %.0f (%s)\n", curTotal, prevTotal, change)
	}

	fmt.Println()

	return nil
}

func collectDiffColumns(curRows, prevRows []map[string]interface{}) []string {
	seen := make(map[string]bool)
	var cols []string

	for _, rows := range [][]map[string]interface{}{curRows, prevRows} {
		for _, row := range rows {
			for k := range row {
				if !seen[k] {
					seen[k] = true
					cols = append(cols, k)
				}
			}
		}
	}

	return cols
}

func classifyColumns(cols []string, curRows, prevRows []map[string]interface{}) (numeric, group []string) {
	allRows := make([]map[string]interface{}, 0, len(curRows)+len(prevRows))
	allRows = append(allRows, curRows...)
	allRows = append(allRows, prevRows...)

	for _, col := range cols {
		isNumeric := false

		for _, row := range allRows {
			if v, ok := row[col]; ok {
				if _, fOK := v.(float64); fOK {
					isNumeric = true

					break
				}
			}
		}

		if isNumeric {
			numeric = append(numeric, col)
		} else {
			group = append(group, col)
		}
	}

	return numeric, group
}

func diffRowKey(row map[string]interface{}, groupCols []string) string {
	var parts []string

	for _, col := range groupCols {
		parts = append(parts, fmt.Sprintf("%v", row[col]))
	}

	return strings.Join(parts, "|")
}

func buildDiffRow(curRow, prevRow map[string]interface{}, groupCols, numericCols []string) struct {
	group  []string
	now    []string
	prev   []string
	change []string
} {
	var dr struct {
		group  []string
		now    []string
		prev   []string
		change []string
	}

	// Group columns from whichever row exists.
	srcRow := curRow
	if srcRow == nil {
		srcRow = prevRow
	}

	for _, col := range groupCols {
		dr.group = append(dr.group, fmt.Sprint(srcRow[col]))
	}

	// Numeric columns.
	for _, col := range numericCols {
		var curVal, prevVal float64

		if curRow != nil {
			if v, ok := curRow[col].(float64); ok {
				curVal = v
			}
		}

		if prevRow != nil {
			if v, ok := prevRow[col].(float64); ok {
				prevVal = v
			}
		}

		if curVal == float64(int64(curVal)) {
			dr.now = append(dr.now, fmt.Sprintf("%.0f", curVal))
		} else {
			dr.now = append(dr.now, fmt.Sprintf("%.2f", curVal))
		}

		if prevVal == float64(int64(prevVal)) {
			dr.prev = append(dr.prev, fmt.Sprintf("%.0f", prevVal))
		} else {
			dr.prev = append(dr.prev, fmt.Sprintf("%.2f", prevVal))
		}

		dr.change = append(dr.change, formatDiffPct(curVal, prevVal))
	}

	return dr
}

func formatDiffPct(cur, prev float64) string {
	if prev == 0 && cur == 0 {
		return "0%"
	}

	if prev == 0 {
		return "+∞"
	}

	pctChange := (cur - prev) / math.Abs(prev) * 100
	sign := "+"

	if pctChange < 0 {
		sign = ""
	}

	if math.Abs(pctChange) < 0.1 {
		return "0%"
	}

	return fmt.Sprintf("%s%.1f%%", sign, pctChange)
}

func sumNumericColumns(rows []map[string]interface{}, numericCols []string) float64 {
	total := 0.0

	for _, row := range rows {
		for _, col := range numericCols {
			if v, ok := row[col].(float64); ok {
				total += v
			}
		}
	}

	return total
}

// suggestWiderDiffPeriod suggests a wider period for diff comparisons.
func suggestWiderDiffPeriod(current string) string {
	const defaultWider = "24h"

	switch current {
	case "1h":
		return "6h"
	case "6h":
		return defaultWider
	case defaultWider:
		return "7d"
	default:
		return defaultWider
	}
}
