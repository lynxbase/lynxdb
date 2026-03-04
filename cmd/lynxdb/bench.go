package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/storage"
)

var (
	flagBenchEvents int
)

var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "Run performance benchmark",
	Long:  `Generates synthetic events and measures ingest and query performance.`,
	RunE:  runBench,
}

func init() {
	benchCmd.Flags().IntVar(&flagBenchEvents, "events", 100000, "Number of events to generate")
	rootCmd.AddCommand(benchCmd)
}

func runBench(cmd *cobra.Command, args []string) error {
	n := flagBenchEvents
	if n <= 0 {
		return fmt.Errorf("--events must be a positive integer (got %d)", n)
	}
	if n > 100_000_000 {
		return fmt.Errorf("--events exceeds maximum (100,000,000)")
	}

	t := ui.Stdout
	fmt.Printf("\n  %s — %s events\n", t.Bold.Render("LynxDB Benchmark"), formatCount(int64(n)))
	fmt.Printf("  %s\n", t.Rule.Render(strings.Repeat("━", 60)))

	// Generate events.
	fmt.Print("  Generating events... ")
	lines := generateBenchLines(n)
	fmt.Printf("%s lines\n", formatCount(int64(len(lines))))

	// Ingest.
	eng := storage.NewEphemeralEngine()
	defer eng.Close()
	ingestStart := time.Now()
	count, err := eng.IngestLines(lines, storage.IngestOpts{
		Source:     "bench",
		SourceType: "bench",
	})
	if err != nil {
		return fmt.Errorf("ingest: %w", err)
	}
	ingestDur := time.Since(ingestStart)
	eps := float64(count) / ingestDur.Seconds()
	fmt.Printf("\n  %s  %s events in %s (%s events/sec)\n\n",
		t.Success.Render("Ingest:"),
		formatCount(int64(count)),
		formatElapsed(ingestDur),
		formatCount(int64(eps)))

	// Benchmark queries.
	queries := []struct {
		name  string
		query string
	}{
		{"Filtered aggregate", `FROM main | where level="ERROR" | stats count`},
		{"Full scan aggregate", `FROM main | stats count by host`},
		{"Full-text search", `FROM main | search "connection refused"`},
		{"Range filter + top", `FROM main | where status>=500 | top 10 path`},
		{"Time bucketed", `FROM main | timechart count span=5m`},
	}

	tbl := ui.NewTable(t).
		SetColumns("QUERY", "RESULTS", "TIME")

	ctx := context.Background()
	for _, q := range queries {
		start := time.Now()
		result, _, err := eng.Query(ctx, q.query, storage.QueryOpts{})
		dur := time.Since(start)
		if err != nil {
			tbl.AddRow(q.name, "ERROR", formatElapsed(dur))

			continue
		}
		tbl.AddRow(q.name, formatCount(int64(len(result.Rows))), formatElapsed(dur))
	}

	fmt.Print("  ")
	fmt.Println(tbl.String())
	printSuccess("Done.")

	return nil
}

func generateBenchLines(n int) []string {
	hosts := []string{"web-01", "web-02", "web-03", "api-01", "api-02"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	paths := []string{
		"/api/v1/users", "/api/v1/orders", "/api/v1/products",
		"/api/v1/auth/login", "/api/v1/auth/logout",
		"/api/v1/search", "/api/v1/health", "/static/app.js",
	}
	statuses := []int{200, 200, 200, 200, 200, 201, 204, 301, 400, 401, 403, 404, 500, 502, 503}
	levels := []string{"INFO", "INFO", "INFO", "INFO", "WARN", "ERROR"}
	users := []string{"alice", "bob", "charlie", "diana", "eve", "frank"}
	messages := []string{
		"request completed", "connection refused", "timeout exceeded",
		"cache miss", "rate limited", "auth failed",
	}

	rng := rand.New(rand.NewSource(42))
	base := time.Date(2025, 1, 15, 8, 0, 0, 0, time.UTC)

	lines := make([]string, n)
	for i := 0; i < n; i++ {
		t := base.Add(time.Duration(i) * 500 * time.Millisecond)
		t = t.Add(time.Duration(rng.Intn(200)) * time.Millisecond)

		host := hosts[rng.Intn(len(hosts))]
		method := methods[rng.Intn(len(methods))]
		path := paths[rng.Intn(len(paths))]
		status := statuses[rng.Intn(len(statuses))]
		level := levels[rng.Intn(len(levels))]
		user := users[rng.Intn(len(users))]
		respTime := rng.Intn(800) + 10
		bytes := rng.Intn(50000) + 100
		msg := messages[rng.Intn(len(messages))]

		if status >= 500 {
			level = "ERROR"
		} else if status >= 400 {
			level = "WARN"
		}

		lines[i] = fmt.Sprintf("%s host=%s level=%s method=%s path=%s status=%d user=%s response_time=%d bytes=%d msg=%q",
			t.Format(time.RFC3339Nano), host, level, method, path, status, user, respTime, bytes, msg)
	}

	return lines
}
