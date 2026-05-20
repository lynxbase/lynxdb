package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

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
	stopAnimation := startStatusLynxAnimation()
	defer stopAnimation()

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

	stopAnimation()

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

	if !humanOutputActive() {
		rows := [][2]any{
			{"uptime_seconds", stats.UptimeSeconds},
			{"storage_bytes", stats.StorageBytes},
			{"total_events", stats.TotalEvents},
			{"events_today", stats.EventsToday},
			{"index_count", stats.IndexCount},
			{"segment_count", stats.SegmentCount},
			{"buffered_events", stats.BufferedEvents},
			{"oldest_event", stats.OldestEvent},
			{"health", healthStatus},
		}
		return renderKeyValues(os.Stdout, rows, ui.Stdout)
	}

	t := ui.Stdout

	uptimeStr := formatDuration(int64(stats.UptimeSeconds))

	fmt.Printf("\n  %s — uptime %s — %s\n\n",
		t.Bold.Render(fmt.Sprintf("LynxDB %s", buildinfo.Version)),
		uptimeStr,
		colorizeHealthStatus(t, healthStatus))

	metrics := []ui.Metric{
		{Label: "Storage", Value: formatBytes(stats.StorageBytes)},
		{
			Label: "Events",
			Value: formatCountHuman(stats.TotalEvents) + " total",
			Hint:  formatCountHuman(stats.EventsToday) + " today",
		},
		{
			Label: "Segments",
			Value: formatCount(int64(stats.SegmentCount)),
			Hint:  "buffer: " + formatCount(int64(stats.BufferedEvents)) + " events",
		},
		{Label: "Indexes", Value: formatCount(int64(stats.IndexCount))},
	}
	fmt.Println(t.MetricGrid(metrics, globalCompact))

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

	statusData, statusErr := c.Status(ctx)
	if statusErr == nil && statusData.MemoryPool != nil {
		mp := statusData.MemoryPool
		fmt.Println()
		fmt.Println(t.Section("Memory Pool"))
		memMetrics := []ui.Metric{
			{Label: "Total", Value: formatBytes(mp.TotalBytes)},
			{Label: "Queries", Value: formatBytes(mp.QueryAllocated)},
			{Label: "Cache", Value: formatBytes(mp.CacheAllocated)},
			{Label: "Free", Value: formatBytes(mp.FreeBytes)},
		}
		fmt.Println(t.MetricGrid(memMetrics, globalCompact))

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

var statusLynxFrames = []string{
	"  .        \n /\\_/\\     \n( o.o )    \n > ^ <     ",
	"    .      \n /\\_/\\     \n( o.- )    \n > ^ <     ",
	"      .    \n /\\_/\\     \n( o.o )    \n > v <     ",
	"    .      \n /\\_/\\     \n( -.o )    \n > ^ <     ",
}

func startStatusLynxAnimation() func() {
	if globalQuiet || !isTTY() || !humanOutputActive() {
		return func() {}
	}

	done := make(chan struct{})
	started := make(chan struct{})
	var wg sync.WaitGroup
	var once sync.Once
	start := time.Now()

	wg.Add(1)
	go func() {
		defer wg.Done()

		t := ui.Stdout
		frameLines := strings.Count(statusLynxFrames[0], "\n") + 2
		ticker := time.NewTicker(140 * time.Millisecond)
		defer ticker.Stop()

		render := func(frame string) {
			lines := strings.Split(frame, "\n")
			for i, line := range lines {
				switch i {
				case 1:
					lines[i] = t.Success.Render(line)
				case 2:
					lines[i] = t.Info.Render(line)
				case 3:
					lines[i] = t.Accent.Render(line)
				default:
					lines[i] = t.Dim.Render(line)
				}
			}

			fmt.Fprint(os.Stdout, strings.Join(lines, "\n"))
			fmt.Fprintf(os.Stdout, "\n%s\n", t.Dim.Render("Checking LynxDB status..."))
		}

		clear := func() {
			fmt.Fprintf(os.Stdout, "\x1b[%dA", frameLines)
			for i := 0; i < frameLines; i++ {
				fmt.Fprint(os.Stdout, "\x1b[2K\r")
				if i < frameLines-1 {
					fmt.Fprint(os.Stdout, "\n")
				}
			}
			fmt.Fprintf(os.Stdout, "\x1b[%dA", frameLines-1)
		}

		render(statusLynxFrames[0])
		close(started)
		frame := 1

		for {
			select {
			case <-done:
				clear()

				return
			case <-ticker.C:
				clear()
				render(statusLynxFrames[frame%len(statusLynxFrames)])
				frame++
			}
		}
	}()

	return func() {
		once.Do(func() {
			<-started
			if remaining := 700*time.Millisecond - time.Since(start); remaining > 0 {
				time.Sleep(remaining)
			}
			close(done)
			wg.Wait()
		})
	}
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

	if !humanOutputActive() {
		return renderKeyValues(os.Stdout, [][2]any{{"status", result.Status}}, ui.Stdout)
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
		if !humanOutputActive() {
			return renderTabular(os.Stdout, []string{"NAME", "RETENTION", "REPLICATION"}, nil, ui.Stdout)
		}
		fmt.Println("No indexes found.")
		printNextSteps(
			"lynxdb ingest <file>                     Ingest data to create an index",
			"lynxdb demo                              Generate sample data",
		)

		return nil
	}

	t := ui.Stdout
	rows := make([][]any, 0, len(indexes))
	for _, idx := range indexes {
		rows = append(rows, []any{idx.Name, idx.RetentionPeriod, idx.ReplicationFactor})
	}
	if err := renderTabular(os.Stdout, []string{"NAME", "RETENTION", "REPLICATION"}, rows, t); err != nil {
		return err
	}
	if humanOutputActive() {
		fmt.Printf("\n%s\n", t.Dim.Render(fmt.Sprintf("%d indexes", len(indexes))))
	}

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

	if !humanOutputActive() {
		rows := [][2]any{}
		for _, key := range []string{"hits", "misses", "hit_rate", "entries", "size_bytes", "evictions"} {
			if value, ok := stats[key]; ok {
				rows = append(rows, [2]any{key, value})
			}
		}
		return renderKeyValues(os.Stdout, rows, ui.Stdout)
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
// Commands keep their richer structured JSON paths for these formats; row-like
// machine formats are handled by renderTabular/renderKeyValues.
func isJSONFormat() bool {
	return globalFormat == formatJSON || globalFormat == formatNDJSON
}
