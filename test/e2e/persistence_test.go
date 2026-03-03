//go:build e2e

package e2e

import (
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/client"
)

// TestE2E_Persistence_DataSurvivesRestart ingests data to disk, restarts the
// server, and verifies that all queries return identical results.
//
// BUG: This test exposes a real data loss bug in the persistence layer.
// After restart, SSH events drop from 2000 to ~1000 and OpenStack events
// drop from 2000 to 0. This means either:
// (a) graceful shutdown does not flush the memtable to disk, or
// (b) WAL recovery does not replay all entries, or
// (c) segment files are not written/read correctly for all indexes.
//
// The application code must be fixed — do not modify this test to pass.
func TestE2E_Persistence_DataSurvivesRestart(t *testing.T) {
	h := NewHarness(t, WithDisk())
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")
	h.IngestFile("idx_openstack", "testdata/logs/OpenStack_2k.log")

	// Collect pre-restart reference results.
	type queryCase struct {
		name  string
		query string
	}
	queries := []queryCase{
		{"SSH_Count", `FROM idx_ssh | STATS count`},
		{"OpenStack_Count", `FROM idx_openstack | STATS count`},
		{"SSH_TopIPs", `FROM idx_ssh | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip) | STATS count BY ip | SORT - count | HEAD 3`},
		{"OpenStack_StatusCounts", `FROM idx_openstack | REX "status: (?<status>\d+)" | WHERE isnotnull(status) | STATS count BY status | SORT status`},
		{"SSH_HourlyBuckets", `FROM idx_ssh | BIN _time span=1h AS hour | STATS count BY hour | SORT hour`},
		{"SSH_FailedCount", `FROM idx_ssh | EVAL has_failed = IF(match(_raw, "Failed password"), 1, 0) | STATS sum(has_failed) AS failed_count`},
		// Note: count AS attempts in compound STATS (with BY) uses single-agg path where alias is broken.
		// We use count BY user directly.
		{"SSH_TopAttackers", `FROM idx_ssh | REX "Failed password for (?:invalid user )?(?<user>\w+) from (?<ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(user) | STATS count BY user | SORT - count | HEAD 3`},
		{"SSH_UniqueIPs", `FROM idx_ssh | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip) | DEDUP ip | STATS count`},
		{"OpenStack_APILatency", `FROM idx_openstack | REX "\"(?<method>GET|POST|DELETE) (?<url_path>/[^\s]+) HTTP" | REX "status: (?<status>\d+) len: (?<resp_len>\d+) time: (?<resp_time>[0-9.]+)" | WHERE isnotnull(method) | EVAL resp_ms = round(tonumber(resp_time) * 1000, 2) | STATS count AS requests, avg(resp_ms) AS avg_latency BY method | SORT method`},
		// Wildcard persistence — validates bloom filter tokenization after flush.
		{"Wildcard_InvalidUserFrom", `FROM idx_ssh | search "Invalid user*from" | STATS count`},
		{"Wildcard_Password", `FROM idx_ssh | search "*password*" | STATS count`},
		{"Wildcard_FailedFromPort", `FROM idx_ssh | search "Failed*from*port" | STATS count`},
	}

	preResults := make(map[string]*client.QueryResult, len(queries))
	for _, q := range queries {
		preResults[q.name] = h.MustQuery(q.query)
	}

	// Restart.
	h.RestartServer()

	// Verify post-restart results match.
	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			post := h.MustQuery(q.query)
			assertQueryResultsEqual(t, q.name, preResults[q.name], post)
		})
	}

	// Additional: verify both indexes are still queryable.
	t.Run("BothIndexes_Queryable", func(t *testing.T) {
		requireAggValue(t, h.MustQuery(`FROM idx_ssh | STATS count`), "count", 2000)
		requireAggValue(t, h.MustQuery(`FROM idx_openstack | STATS count`), "count", 2000)
	})
}
