package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/buildinfo"
	"github.com/lynxbase/lynxdb/internal/ui"
)

func init() {
	rootCmd.AddCommand(newStatusCmd())
	rootCmd.AddCommand(newHealthCmd())
	rootCmd.AddCommand(newIndexesCmd())
	rootCmd.AddCommand(newCacheCmd())
}

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "status",
		Aliases: []string{"st"},
		Short:   "Show server status",
		Example: `  lynxdb status
  lynxdb status --format json`,
		RunE: runStatus,
	}
}

func newHealthCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "health",
		Short: "Check server health",
		RunE:  runHealth,
	}
}

func newIndexesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "indexes",
		Short: "List all indexes",
		RunE:  runIndexes,
	}
}

func newCacheCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cache",
		Short: "Cache management",
	}
	cmd.AddCommand(newCacheClearCmd(), newCacheStatsCmd())

	return cmd
}

func newCacheClearCmd() *cobra.Command {
	var force bool

	cmd := &cobra.Command{
		Use:   "clear",
		Short: "Clear the query cache",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCacheClear(force)
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Skip confirmation prompt")

	return cmd
}

func newCacheStatsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stats",
		Short: "Show cache statistics",
		RunE:  runCacheStats,
	}
}

func runStatus(_ *cobra.Command, _ []string) error {
	ctx := context.Background()
	c := apiClient()

	stats, err := c.Stats(ctx)
	if err != nil {
		return err
	}

	healthStatus := "unknown"
	if h, err := c.Health(ctx); err == nil {
		healthStatus = h.Status
	}

	if isJSONFormat() {
		out := map[string]interface{}{
			"uptime_seconds":  stats.UptimeSeconds,
			"storage_bytes":   stats.StorageBytes,
			"total_events":    stats.TotalEvents,
			"events_today":    stats.EventsToday,
			"index_count":     stats.IndexCount,
			"segment_count":   stats.SegmentCount,
			"buffered_events": stats.BufferedEvents,
			"oldest_event":    stats.OldestEvent,
			"health":          healthStatus,
		}
		if len(stats.Sources) > 0 {
			out["sources"] = stats.Sources
		}

		b, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(b))

		return nil
	}

	t := ui.Stdout

	uptimeStr := formatDuration(int64(stats.UptimeSeconds))

	fmt.Printf("\n  %s — uptime %s — %s\n\n",
		t.Bold.Render(fmt.Sprintf("LynxDB %s", buildinfo.Version)),
		uptimeStr,
		colorizeHealthStatus(t, healthStatus))

	fmt.Println(t.KeyValue("Storage", formatBytes(stats.StorageBytes)))

	fmt.Println(t.KeyValue("Events",
		fmt.Sprintf("%s total    %s today",
			formatCountHuman(stats.TotalEvents),
			formatCountHuman(stats.EventsToday))))

	fmt.Println(t.KeyValue("Segments",
		fmt.Sprintf("%s    Buffer: %s events",
			formatCount(int64(stats.SegmentCount)),
			formatCount(int64(stats.BufferedEvents)))))

	if len(stats.Sources) > 0 {
		parts := make([]string, 0, len(stats.Sources))
		for _, s := range stats.Sources {
			if stats.TotalEvents > 0 {
				pct := float64(s.Count) / float64(stats.TotalEvents) * 100
				parts = append(parts, fmt.Sprintf("%s (%.0f%%)", s.Name, pct))
			} else {
				parts = append(parts, s.Name)
			}
		}

		fmt.Println(t.KeyValue("Sources", strings.Join(parts, ", ")))
	}

	if stats.OldestEvent != "" {
		fmt.Println(t.KeyValue("Oldest", t.Dim.Render(stats.OldestEvent)))
	}

	fmt.Println(t.KeyValue("Indexes", formatCount(int64(stats.IndexCount))))

	statusData, statusErr := c.Status(ctx)
	if statusErr == nil && statusData.MemoryPool != nil {
		mp := statusData.MemoryPool
		fmt.Println()
		fmt.Printf("  %s\n", t.Bold.Render("Memory Pool"))
		fmt.Println(t.KeyValue("  Total", formatBytes(mp.TotalBytes)))
		fmt.Println(t.KeyValue("  Queries", formatBytes(mp.QueryAllocated)))
		fmt.Println(t.KeyValue("  Cache", formatBytes(mp.CacheAllocated)))
		fmt.Println(t.KeyValue("  Free", formatBytes(mp.FreeBytes)))

		if mp.CacheEvictions > 0 {
			fmt.Println(t.KeyValue("  Evictions",
				fmt.Sprintf("%s (%s freed)",
					formatCount(mp.CacheEvictions),
					formatBytes(mp.CacheEvictedBytes))))
		}

		if mp.QueryRejections > 0 {
			fmt.Println(t.KeyValue("  Rejections", formatCount(mp.QueryRejections)))
		}
	}

	fmt.Println()

	if stats.TotalEvents == 0 {
		printNextSteps(
			"lynxdb demo                              Generate sample data",
			"lynxdb ingest <file>                     Ingest a log file",
			"cat app.log | lynxdb ingest              Pipe from stdin",
		)
	}

	return nil
}

// colorizeHealthStatus applies color to the health status string.
func colorizeHealthStatus(t *ui.Theme, status string) string {
	switch status {
	case "healthy":
		return t.Success.Render(status)
	case "degraded":
		return t.Warning.Render(status)
	case "unhealthy":
		return t.Error.Render(status)
	default:
		return t.Dim.Render(status)
	}
}

func runHealth(_ *cobra.Command, _ []string) error {
	ctx := context.Background()

	result, err := apiClient().Health(ctx)
	if err != nil {
		return err
	}

	if isJSONFormat() {
		b, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(b))

		return nil
	}

	if result.Status == "healthy" {
		printSuccess("Server is healthy")
	} else {
		t := ui.Stdout
		fmt.Println(t.KeyValue("Status", colorizeHealthStatus(t, result.Status)))
	}

	return nil
}

func runIndexes(_ *cobra.Command, _ []string) error {
	ctx := context.Background()

	indexes, err := apiClient().Indexes(ctx)
	if err != nil {
		return err
	}

	if len(indexes) == 0 {
		fmt.Println("No indexes found.")
		printNextSteps(
			"lynxdb ingest <file>                     Ingest data to create an index",
			"lynxdb demo                              Generate sample data",
		)

		return nil
	}

	t := ui.Stdout
	tbl := ui.NewTable(t).SetColumns("NAME", "RETENTION", "REPLICATION")
	for _, idx := range indexes {
		tbl.AddRow(idx.Name, idx.RetentionPeriod, fmt.Sprintf("%d", idx.ReplicationFactor))
	}
	fmt.Print(tbl.String())
	fmt.Printf("\n%s\n", t.Dim.Render(fmt.Sprintf("%d indexes", len(indexes))))

	return nil
}

func runCacheClear(force bool) error {
	if !force {
		if !confirmAction("Clear the entire query cache?") {
			printHint("Aborted.")

			return nil
		}
	}

	ctx := context.Background()
	if err := apiClient().CacheClear(ctx); err != nil {
		return err
	}

	printSuccess("Cache cleared")

	return nil
}

func runCacheStats(_ *cobra.Command, _ []string) error {
	ctx := context.Background()

	stats, err := apiClient().CacheStats(ctx)
	if err != nil {
		return err
	}

	if isJSONFormat() {
		b, _ := json.MarshalIndent(stats, "", "  ")
		fmt.Println(string(b))

		return nil
	}

	t := ui.Stdout
	fmt.Printf("\n  %s\n\n", t.Bold.Render("Cache Statistics"))

	if v, ok := stats["hits"].(float64); ok {
		fmt.Println(t.KeyValue("Hits", formatCount(int64(v))))
	}
	if v, ok := stats["misses"].(float64); ok {
		fmt.Println(t.KeyValue("Misses", formatCount(int64(v))))
	}
	if v, ok := stats["hit_rate"]; ok {
		if rate, ok := v.(float64); ok {
			fmt.Println(t.KeyValue("Hit Rate", fmt.Sprintf("%.1f%%", rate*100)))
		}
	}
	if v, ok := stats["entries"].(float64); ok {
		fmt.Println(t.KeyValue("Entries", formatCount(int64(v))))
	}
	if v, ok := stats["size_bytes"].(float64); ok {
		fmt.Println(t.KeyValue("Size", formatBytes(int64(v))))
	}
	if v, ok := stats["evictions"].(float64); ok {
		fmt.Println(t.KeyValue("Evictions", formatCount(int64(v))))
	}

	fmt.Println()

	return nil
}

// isJSONFormat reports whether the current output format is JSON or NDJSON.
// Use this for commands with structured (non-row) output where the only sensible
// machine-readable format is JSON (e.g., status, health, cache stats). For commands
// that produce tabular row data, use printFormattedRows instead — it honors all
// format values including table, csv, tsv, and raw.
func isJSONFormat() bool {
	return globalFormat == formatJSON || globalFormat == formatNDJSON
}
