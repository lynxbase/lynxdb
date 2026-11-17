// genbench writes testdata/bench/json_100k.log and text_100k.log, the
// benchmark and profiling fixtures, from a seeded PRNG so that every run
// writes the same bytes.
//
// Usage, from the repository root:
//
//	go run ./scripts/genbench
package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

var (
	sources = []string{
		"api-gw", "auth-svc", "user-svc", "order-svc", "payment-svc",
		"search-svc", "cache-svc", "notification-svc", "analytics-svc", "cdn-edge",
	}

	levels = []string{"info", "info", "info", "info", "warn", "error"}

	methods = []string{"GET", "GET", "GET", "POST", "PUT", "DELETE"}

	jsonPaths = []string{
		"/api/v1/users", "/api/v1/orders", "/api/v1/products",
		"/api/v1/auth/login", "/api/v1/auth/logout",
		"/api/v1/search", "/api/v1/health",
	}

	// ~5% of these contain "/user_" for selectivity testing.
	msgTemplates = []string{
		"connection refused to /user_service/health",
		"timeout connecting to /user_profile/api",
		"failed to reach /user_settings/preferences",
		"request completed successfully",
		"response sent to client",
		"database query executed",
		"cache hit for key lookup",
		"cache miss, fetching from origin",
		"rate limit exceeded for client",
		"authentication token validated",
		"request payload validated",
		"retry attempt on upstream",
		"circuit breaker tripped",
		"connection pool exhausted",
		"TLS handshake completed",
		"DNS lookup completed",
		"upstream health check passed",
		"load balancer routing decision",
		"compression applied to response",
		"CORS preflight handled",
	}

	// Syslog-style text message templates.
	textTemplates = []string{
		"%s %s %s[%d]: connection refused to /user_service/health from %s",
		"%s %s %s[%d]: timeout connecting to /user_profile/api from %s",
		"%s %s %s[%d]: failed to reach /user_settings/preferences from %s",
		"%s %s %s[%d]: request completed status=%d duration=%dms from %s",
		"%s %s %s[%d]: response sent status=%d bytes=%d from %s",
		"%s %s %s[%d]: database query completed rows=%d duration=%dms",
		"%s %s %s[%d]: cache lookup key=%s result=hit latency=%dns",
		"%s %s %s[%d]: cache lookup key=%s result=miss latency=%dns",
		"%s %s %s[%d]: rate limit bucket=%s tokens=%d",
		"%s %s %s[%d]: auth token validated user=%s",
		"%s %s %s[%d]: payload validated size=%d bytes",
		"%s %s %s[%d]: upstream retry attempt=%d target=%s",
		"%s %s %s[%d]: circuit breaker state=%s threshold=%d",
		"%s %s %s[%d]: connection pool active=%d idle=%d max=%d",
		"%s %s %s[%d]: TLS handshake completed protocol=%s",
		"%s %s %s[%d]: DNS resolved host=%s addr=%s ttl=%ds",
		"%s %s %s[%d]: health check target=%s status=%s",
		"%s %s %s[%d]: load balance algo=round_robin target=%s",
		"%s %s %s[%d]: compression applied ratio=%.2f",
		"%s %s %s[%d]: CORS preflight origin=%s allowed=%t",
	}

	hosts = []string{"prod-web-01", "prod-web-02", "prod-app-01", "prod-app-02", "prod-db-01"}
)

func main() {
	dir := "testdata/bench"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}

	const events = 100000
	writeJSON(filepath.Join(dir, "json_100k.log"), events)
	writeText(filepath.Join(dir, "text_100k.log"), events)

	fmt.Println("done")
}

func writeJSON(path string, n int) {
	rng := rand.New(rand.NewSource(42))
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", path, err)
		os.Exit(1)
	}
	defer closeOutput(f, path)

	base := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
	userCount := 0

	for i := 0; i < n; i++ {
		ts := base.Add(time.Duration(i) * 100 * time.Millisecond)
		ts = ts.Add(time.Duration(rng.Intn(90)) * time.Millisecond)

		source := sources[rng.Intn(len(sources))]
		level := levels[rng.Intn(len(levels))]
		method := methods[rng.Intn(len(methods))]
		urlPath := jsonPaths[rng.Intn(len(jsonPaths))]
		status := 200 + rng.Intn(5)*100 // 200,300,400,500,600(clamp)
		if status > 503 {
			status = 200
		}
		duration := rng.Intn(5000) + 1
		traceID := fmt.Sprintf("trace-%08x", rng.Uint32())

		// Select message: first 3 templates contain "/user_" (~5% target).
		var msg string
		r := rng.Intn(100)
		if r < 5 {
			msg = msgTemplates[rng.Intn(3)] // one of the /user_ messages
			userCount++
		} else {
			msg = msgTemplates[3+rng.Intn(len(msgTemplates)-3)]
		}

		if level == "error" || status >= 500 {
			level = "error"
		}

		if _, err := fmt.Fprintf(f,
			`{"timestamp":"%s","level":"%s","source":"%s","msg":"%s","status":%d,"duration":%d,"trace_id":"%s","method":"%s","path":"%s"}`+"\n",
			ts.Format(time.RFC3339Nano), level, source, msg, status, duration, traceID, method, urlPath,
		); err != nil {
			fmt.Fprintf(os.Stderr, "write %s: %v\n", path, err)
			os.Exit(1)
		}
	}

	fmt.Printf("wrote %s: %d lines, ~%.1f%% /user_ (%d)\n", path, n, float64(userCount)/float64(n)*100, userCount)
}

func writeText(path string, n int) {
	rng := rand.New(rand.NewSource(123))
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", path, err)
		os.Exit(1)
	}
	defer closeOutput(f, path)

	base := time.Date(2026, 2, 14, 0, 0, 0, 0, time.UTC)
	userCount := 0

	for i := 0; i < n; i++ {
		ts := base.Add(time.Duration(i) * 100 * time.Millisecond)
		ts = ts.Add(time.Duration(rng.Intn(90)) * time.Millisecond)

		host := hosts[rng.Intn(len(hosts))]
		source := sources[rng.Intn(len(sources))]
		pid := 1000 + rng.Intn(9000)
		clientIP := fmt.Sprintf("10.%d.%d.%d", rng.Intn(256), rng.Intn(256), rng.Intn(256))

		r := rng.Intn(100)
		tsStr := ts.Format("Jan 02 15:04:05")

		if r < 5 {
			// /user_ templates (indices 0-2)
			tmpl := textTemplates[rng.Intn(3)]
			if _, err := fmt.Fprintf(f, tmpl+"\n", tsStr, host, source, pid, clientIP); err != nil {
				fmt.Fprintf(os.Stderr, "write %s: %v\n", path, err)
				os.Exit(1)
			}
			userCount++
		} else if r < 30 {
			// Status-bearing templates (indices 3-4)
			status := 200 + rng.Intn(5)*100
			if status > 503 {
				status = 200
			}
			dur := rng.Intn(5000) + 1
			tmpl := textTemplates[3+rng.Intn(2)]
			if _, err := fmt.Fprintf(f, tmpl+"\n", tsStr, host, source, pid, status, dur, clientIP); err != nil {
				fmt.Fprintf(os.Stderr, "write %s: %v\n", path, err)
				os.Exit(1)
			}
		} else if r < 50 {
			// Database/cache templates (indices 5-7)
			key := fmt.Sprintf("session:%08x", rng.Uint32())
			rows := rng.Intn(1000)
			dur := rng.Intn(100)
			tmpl := textTemplates[5+rng.Intn(3)]
			fmt.Fprintf(f, tmpl+"\n", tsStr, host, source, pid, key, rows, dur)
		} else {
			// Other misc templates (indices 8+)
			idx := 8 + rng.Intn(len(textTemplates)-8)
			switch idx {
			case 8: // rate limit
				writeFormatted(path, f, textTemplates[idx]+"\n", tsStr, host, source, pid, "api-default", rng.Intn(100))
			case 9: // auth
				users := []string{"alice", "bob", "charlie", "diana", "eve"}
				writeFormatted(path, f, textTemplates[idx]+"\n", tsStr, host, source, pid, users[rng.Intn(len(users))])
			case 10: // payload
				writeFormatted(path, f, textTemplates[idx]+"\n", tsStr, host, source, pid, rng.Intn(100000))
			case 11: // retry
				writeFormatted(path, f, textTemplates[idx]+"\n", tsStr, host, source, pid, rng.Intn(5)+1, sources[rng.Intn(len(sources))])
			case 12: // circuit breaker
				states := []string{"closed", "open", "half-open"}
				writeFormatted(path, f, textTemplates[idx]+"\n", tsStr, host, source, pid, states[rng.Intn(len(states))], rng.Intn(100))
			case 13: // conn pool
				active := rng.Intn(50)
				idle := rng.Intn(20)
				writeFormatted(path, f, textTemplates[idx]+"\n", tsStr, host, source, pid, active, idle, 100)
			case 14: // TLS
				protos := []string{"TLSv1.2", "TLSv1.3"}
				writeFormatted(path, f, textTemplates[idx]+"\n", tsStr, host, source, pid, protos[rng.Intn(len(protos))])
			case 15: // DNS
				writeFormatted(path, f, textTemplates[idx]+"\n", tsStr, host, source, pid, "api.example.com", clientIP, rng.Intn(300))
			case 16: // health check
				statuses := []string{"healthy", "unhealthy", "degraded"}
				writeFormatted(path, f, textTemplates[idx]+"\n", tsStr, host, source, pid, sources[rng.Intn(len(sources))], statuses[rng.Intn(len(statuses))])
			case 17: // load balance
				writeFormatted(path, f, textTemplates[idx]+"\n", tsStr, host, source, pid, sources[rng.Intn(len(sources))])
			case 18: // compression
				writeFormatted(path, f, textTemplates[idx]+"\n", tsStr, host, source, pid, 0.1+rng.Float64()*0.9)
			case 19: // CORS
				origins := []string{"https://app.example.com", "https://admin.example.com"}
				writeFormatted(path, f, textTemplates[idx]+"\n", tsStr, host, source, pid, origins[rng.Intn(len(origins))], rng.Intn(2) == 0)
			}
		}
	}

	fmt.Printf("wrote %s: %d lines, ~%.1f%% /user_ (%d)\n", path, n, float64(userCount)/float64(n)*100, userCount)
}

func closeOutput(f *os.File, path string) {
	if err := f.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close %s: %v\n", path, err)
		os.Exit(1)
	}
}

func writeFormatted(path string, f *os.File, format string, args ...interface{}) {
	if _, err := fmt.Fprintf(f, format, args...); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", path, err)
		os.Exit(1)
	}
}
