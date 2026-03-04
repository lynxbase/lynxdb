package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/storage"
)

var (
	flagDemoRate int
)

var demoCmd = &cobra.Command{
	Use:   "demo",
	Short: "Run a demo with live-generated logs",
	Long:  `Starts an in-process engine and continuously generates realistic logs from multiple sources.`,
	RunE:  runDemo,
}

func init() {
	demoCmd.Flags().IntVar(&flagDemoRate, "rate", 200, "Events per second")
	rootCmd.AddCommand(demoCmd)
}

func runDemo(cmd *cobra.Command, args []string) error {
	if flagDemoRate <= 0 {
		return fmt.Errorf("--rate must be a positive integer (got %d)", flagDemoRate)
	}
	if flagDemoRate > 1_000_000 {
		return fmt.Errorf("--rate exceeds maximum (1,000,000 events/sec)")
	}

	eng := storage.NewEphemeralEngine()
	defer eng.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	t := ui.Stdout
	fmt.Printf("\n  %s\n", t.Bold.Render("LynxDB Demo Mode"))
	fmt.Printf("  %s\n\n", t.Rule.Render(strings.Repeat("─", 40)))
	fmt.Printf("  Generating %s events/sec from 4 sources\n\n",
		t.Success.Render(fmt.Sprintf("%d", flagDemoRate)))
	fmt.Printf("  %s nginx, api-gateway, postgres, redis\n\n", t.Bold.Render("Sources:"))
	fmt.Printf("  %s\n", t.Dim.Render("Try these queries in another terminal:"))
	fmt.Printf("    %s\n", t.Info.Render("lynxdb query 'source=nginx | stats count by status'"))
	fmt.Printf("    %s\n", t.Info.Render("lynxdb query 'level=ERROR | stats count by host' --since 5m"))
	fmt.Printf("    %s\n", t.Info.Render("lynxdb query 'source=nginx | top 10 path'"))
	fmt.Printf("    %s\n\n", t.Info.Render("lynxdb tail 'level=ERROR'"))
	fmt.Printf("  %s\n\n", t.Dim.Render("Press Ctrl+C to stop."))

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	interval := time.Second / time.Duration(flagDemoRate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	generated := 0
	start := time.Now()
	lastReport := start

	for {
		select {
		case <-sigCh:
			fmt.Printf("\n\n  %s Generated %s events in %s\n",
				t.IconOK(), formatCount(int64(generated)), time.Since(start).Round(time.Second))

			return nil
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			line := generateDemoLine(rng, time.Now())
			_, err := eng.IngestLines([]string{line}, storage.IngestOpts{})
			if err != nil {
				continue
			}
			generated++

			if time.Since(lastReport) >= 5*time.Second {
				elapsed := time.Since(start)
				eps := float64(generated) / elapsed.Seconds()
				fmt.Printf("  %s %s events generated (%s/sec)\n",
					t.Dim.Render("▸"),
					formatCount(int64(generated)),
					t.Dim.Render(fmt.Sprintf("%.0f", eps)))
				lastReport = time.Now()
			}
		}
	}
}

func generateDemoLine(rng *rand.Rand, t time.Time) string {
	source := rng.Intn(4)
	switch source {
	case 0:
		return generateNginxLine(rng, t)
	case 1:
		return generateAPILine(rng, t)
	case 2:
		return generatePostgresLine(rng, t)
	default:
		return generateRedisLine(rng, t)
	}
}

func generateNginxLine(rng *rand.Rand, t time.Time) string {
	ips := []string{"10.0.1.5", "10.0.1.12", "10.0.2.8", "203.0.113.50", "192.168.1.100"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	paths := []string{"/", "/api/users", "/api/orders", "/static/app.js", "/login", "/dashboard", "/api/search"}
	statuses := []int{200, 200, 200, 200, 201, 301, 304, 400, 401, 403, 404, 500, 502, 503}

	ip := ips[rng.Intn(len(ips))]
	method := methods[rng.Intn(len(methods))]
	path := paths[rng.Intn(len(paths))]
	status := statuses[rng.Intn(len(statuses))]
	bytes := rng.Intn(50000) + 100
	rt := float64(rng.Intn(500)+5) / 1000.0

	return fmt.Sprintf("%s source=nginx host=web-01 %s - - [%s] \"%s %s HTTP/1.1\" %d %d \"-\" \"Mozilla/5.0\" rt=%.3f",
		t.Format(time.RFC3339Nano), ip, t.Format("02/Jan/2006:15:04:05 -0700"),
		method, path, status, bytes, rt)
}

func generateAPILine(rng *rand.Rand, t time.Time) string {
	services := []string{"user-service", "order-service", "payment-service", "auth-service"}
	levels := []string{"INFO", "INFO", "INFO", "WARN", "ERROR"}
	messages := []string{"request handled", "cache miss", "timeout", "rate limited", "connection reset"}
	traceIDs := []string{"abc123", "def456", "ghi789", "jkl012"}

	service := services[rng.Intn(len(services))]
	level := levels[rng.Intn(len(levels))]
	msg := messages[rng.Intn(len(messages))]
	traceID := traceIDs[rng.Intn(len(traceIDs))]
	duration := rng.Intn(2000) + 1

	return fmt.Sprintf("%s source=api-gateway host=%s level=%s trace_id=%s duration=%d msg=%q",
		t.Format(time.RFC3339Nano), service, level, traceID, duration, msg)
}

func generatePostgresLine(rng *rand.Rand, t time.Time) string {
	queries := []string{
		"SELECT * FROM users WHERE id = $1",
		"INSERT INTO orders (user_id, total) VALUES ($1, $2)",
		"UPDATE sessions SET last_active = NOW() WHERE token = $1",
		"SELECT COUNT(*) FROM events WHERE created_at > $1",
		"DELETE FROM logs WHERE created_at < $1",
	}
	durations := []int{1, 2, 5, 15, 50, 200, 1500, 5000}

	query := queries[rng.Intn(len(queries))]
	dur := durations[rng.Intn(len(durations))]
	level := "LOG"
	if dur > 1000 {
		level = "WARNING"
	}

	return fmt.Sprintf("%s source=postgres host=db-01 level=%s duration=%d statement=%q",
		t.Format(time.RFC3339Nano), level, dur, query)
}

func generateRedisLine(rng *rand.Rand, t time.Time) string {
	ops := []string{"GET", "SET", "DEL", "HGET", "HSET", "LPUSH", "RPOP", "EXPIRE"}
	keys := []string{"session:abc", "cache:users:123", "rate:10.0.1.5", "queue:orders", "lock:payment"}

	op := ops[rng.Intn(len(ops))]
	key := keys[rng.Intn(len(keys))]
	hit := rng.Float32() < 0.7
	latencyUs := rng.Intn(500) + 10

	hitStr := "miss"
	if hit {
		hitStr = "hit"
	}

	return fmt.Sprintf("%s source=redis host=redis-01 op=%s key=%s result=%s latency_us=%d",
		t.Format(time.RFC3339Nano), op, key, hitStr, latencyUs)
}
