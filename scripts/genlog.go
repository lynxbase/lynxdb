package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

func main() {
	hosts := []string{"web-01", "web-02", "web-03", "api-01", "api-02"}
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	paths := []string{
		"/api/v1/users", "/api/v1/orders", "/api/v1/products",
		"/api/v1/auth/login", "/api/v1/auth/logout",
		"/api/v1/search", "/api/v1/health", "/static/app.js",
		"/static/style.css", "/index.html",
	}
	statuses := []int{200, 200, 200, 200, 200, 201, 204, 301, 400, 401, 403, 404, 500, 502, 503}
	levels := []string{"INFO", "INFO", "INFO", "INFO", "WARN", "ERROR"}
	users := []string{"alice", "bob", "charlie", "diana", "eve", "frank", "grace"}

	rng := rand.New(rand.NewSource(42))
	base := time.Date(2025, 1, 15, 8, 0, 0, 0, time.UTC)

	n := 1000
	if len(os.Args) > 1 {
		_, _ = fmt.Sscanf(os.Args[1], "%d", &n)
	}

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

		if status >= 500 {
			level = "ERROR"
		} else if status >= 400 {
			level = "WARN"
		}

		fmt.Printf("%s host=%s level=%s method=%s path=%s status=%d user=%s response_time=%d bytes=%d msg=\"%s %s completed\"\n",
			t.Format(time.RFC3339Nano), host, level, method, path, status, user, respTime, bytes, method, path)
	}
}
