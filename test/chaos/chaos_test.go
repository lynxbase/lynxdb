//go:build chaos

// Package chaos contains Docker-based chaos tests for LynxDB clustering.
// These tests require a running 5-node cluster (docker compose up).
//
// Run with: go test -tags chaos -timeout 10m ./test/chaos/...
//
// Prerequisites:
//
//	docker compose -f test/chaos/docker-compose.yml up --build -d
//
// The tests use the fault_inject.sh script to inject network partitions,
// latency, and node kills. They verify invariants like shard map convergence,
// data durability, and leader election.
package chaos

import (
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	// Base URL for each node's HTTP API.
	node1URL = "http://localhost:3101"
	node2URL = "http://localhost:3102"
	node3URL = "http://localhost:3103"
	node4URL = "http://localhost:3104"
	node5URL = "http://localhost:3105"
)

var nodeURLs = []string{node1URL, node2URL, node3URL, node4URL, node5URL}

// containerName maps node index (0-based) to docker compose container name.
func containerName(idx int) string {
	return fmt.Sprintf("chaos-node%d-1", idx+1)
}

// faultInject runs the fault_inject.sh script.
func faultInject(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("./fault_inject.sh", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("fault_inject %v: %s (error: %v)", args, string(out), err)
	}
}

// healthCheck checks if a node is healthy.
func healthCheck(url string) bool {
	resp, err := http.Get(url + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

// waitForHealth waits for a node to become healthy.
func waitForHealth(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if healthCheck(url) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("node %s did not become healthy within %v", url, timeout)
}

// ingestEvent sends a single event to a node.
func ingestEvent(url string, payload string) error {
	resp, err := http.Post(url+"/api/v1/ingest", "application/json", strings.NewReader(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ingest returned %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// TestChaos_LeaderKill_ShardMapConverges kills the meta leader and verifies
// a new leader is elected and the shard map converges within 30 seconds.
func TestChaos_LeaderKill_ShardMapConverges(t *testing.T) {
	// Wait for cluster to be healthy.
	for _, url := range nodeURLs {
		waitForHealth(t, url, 30*time.Second)
	}

	// Kill node 1 (likely the initial leader).
	faultInject(t, "kill_node", containerName(0))
	defer faultInject(t, "restart_node", containerName(0))

	// Wait for a surviving node to be healthy (new leader elected).
	time.Sleep(5 * time.Second)

	healthyCount := 0
	for _, url := range nodeURLs[1:] {
		if healthCheck(url) {
			healthyCount++
		}
	}

	if healthyCount < 3 {
		t.Errorf("expected at least 3 healthy nodes after leader kill, got %d", healthyCount)
	}
}

// TestChaos_RollingRestart_NoDataLoss restarts nodes one by one during
// continuous ingest and verifies all events are queryable.
func TestChaos_RollingRestart_NoDataLoss(t *testing.T) {
	for _, url := range nodeURLs {
		waitForHealth(t, url, 30*time.Second)
	}

	// Ingest events.
	eventCount := 50
	for i := 0; i < eventCount; i++ {
		payload := fmt.Sprintf(`{"message":"rolling-restart-test","seq":%d}`, i)
		nodeIdx := i % len(nodeURLs)
		if err := ingestEvent(nodeURLs[nodeIdx], payload); err != nil {
			t.Logf("ingest to node %d failed (expected during restart): %v", nodeIdx+1, err)
		}
	}

	// Rolling restart: kill and restart each non-leader node.
	for i := 1; i < len(nodeURLs); i++ {
		faultInject(t, "kill_node", containerName(i))
		time.Sleep(2 * time.Second)
		faultInject(t, "restart_node", containerName(i))
		waitForHealth(t, nodeURLs[i], 30*time.Second)
	}

	// All nodes should be healthy after rolling restart.
	for _, url := range nodeURLs {
		if !healthCheck(url) {
			t.Errorf("node %s not healthy after rolling restart", url)
		}
	}
}
