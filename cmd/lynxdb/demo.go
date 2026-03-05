package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/api/rest"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
)

var (
	flagDemoRate int
	flagDemoAddr string
)

var demoCmd = &cobra.Command{
	Use:   "demo",
	Short: "Run a demo with live-generated logs",
	Long: `Starts a LynxDB server in demo mode with continuously generated realistic logs.

The server runs in-memory and listens on localhost:3100 (override with --addr).
All normal commands (query, tail, ingest) and the REST API work against it.`,
	RunE: runDemo,
}

func init() {
	demoCmd.Flags().IntVar(&flagDemoRate, "rate", 200, "Events per second")
	demoCmd.Flags().StringVar(&flagDemoAddr, "addr", "localhost:3100", "Listen address for demo server")
	rootCmd.AddCommand(demoCmd)
}

func runDemo(cmd *cobra.Command, args []string) error {
	if flagDemoRate <= 0 {
		return fmt.Errorf("--rate must be a positive integer (got %d)", flagDemoRate)
	}
	if flagDemoRate > 1_000_000 {
		return fmt.Errorf("--rate exceeds maximum (1,000,000 events/sec)")
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	srv, err := rest.NewServer(rest.Config{
		Addr:    flagDemoAddr,
		DataDir: "", // in-memory, no persistence
		Logger:  logger,
	})
	if err != nil {
		return fmt.Errorf("demo: failed to create server: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverErr := make(chan error, 1)
	go func() { serverErr <- srv.Start(ctx) }()

	readyCh := make(chan struct{})
	go func() { srv.WaitReady(); close(readyCh) }()
	select {
	case <-readyCh:
		// Server is ready to accept requests.
	case err := <-serverErr:
		return fmt.Errorf("demo: failed to start on %s: %w\n\n  Hint: Try --addr localhost:3101", flagDemoAddr, err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	t := ui.Stdout
	scheme := "http"
	fmt.Printf("\n  %s\n", t.Bold.Render("LynxDB Demo Mode"))
	fmt.Printf("  %s\n\n", t.HRule(40))
	fmt.Println(t.KeyValue("Server", scheme+"://"+srv.Addr()))
	fmt.Println(t.KeyValue("Data", "(in-memory)"))
	fmt.Println(t.KeyValue("Rate", fmt.Sprintf("%d events/sec", flagDemoRate)))
	fmt.Println(t.KeyValue("Sources", "nginx, api-gateway, postgres, redis"))
	fmt.Println()
	fmt.Printf("  %s\n", t.Dim.Render("Try in another terminal:"))
	fmt.Printf("    %s\n", t.Info.Render("lynxdb query '_source=nginx | stats count by status'"))
	fmt.Printf("    %s\n", t.Info.Render("lynxdb query 'level=ERROR | stats count by host' --since 5m"))
	fmt.Printf("    %s\n", t.Info.Render("lynxdb tail 'level=ERROR'"))
	fmt.Println()
	fmt.Printf("  %s\n", t.Dim.Render("REST API:"))
	fmt.Printf("    %s\n", t.Info.Render(
		fmt.Sprintf("curl -s %s://%s/api/v1/query -d '{\"q\":\"| stats count by source\"}' | jq .", scheme, srv.Addr())))
	fmt.Println()
	fmt.Printf("  %s\n\n", t.Dim.Render("Press Ctrl+C to stop."))

	pipe := pipeline.DefaultPipeline()
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

			cancel()
			<-serverErr

			return nil
		case err := <-serverErr:
			// Server died unexpectedly.
			return fmt.Errorf("demo: server exited unexpectedly: %w", err)
		case <-ticker.C:
			line := generateDemoLine(rng, time.Now())
			ev := event.NewEvent(time.Time{}, line)
			processed, pErr := pipe.Process([]*event.Event{ev})
			if pErr != nil {
				continue
			}
			if iErr := srv.Engine().Ingest(processed); iErr != nil {
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
