//go:build e2e

package e2e

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
)

// TestE2E_Persistence_DataSurvivesRestart ingests data to disk, restarts the
// server, and verifies that all queries return identical results.
//
// Regression test for persistence bug (fixed). Verifies that batcher flush
// and atomic part writes preserve all events across restart for multiple indexes.
func TestE2E_Persistence_DataSurvivesRestart(t *testing.T) {
	h := NewHarness(t, WithDisk())
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")
	h.IngestFile("idx_openstack", "testdata/logs/OpenStack_2k.log")
	h.FlushBatcher()

	// Collect pre-restart reference results.
	type queryCase struct {
		name  string
		query string
	}
	queries := []queryCase{
		{"SSH_Count", `source=idx_ssh | stats count() as count`},
		{"OpenStack_Count", `source=idx_openstack | stats count() as count`},
		{"SSH_TopIPs", `source=idx_ssh | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip) | stats count() as count by ip | sort -count, ip | head 3`},
		{"OpenStack_StatusCounts", `source=idx_openstack | rex field=_raw "status: (?<status>\d+)" | where isnotnull(status) | stats count() as count by status | sort status`},
		{"SSH_HourlyBuckets", `source=idx_ssh | bin _time span=1h as hour | stats count() as count by hour | sort hour`},
		{"SSH_FailedCount", `source=idx_ssh | eval has_failed = if(regexp_match(_raw, "Failed password"), 1, 0) | stats sum(has_failed) as failed_count`},
		{"SSH_TopAttackers", `source=idx_ssh | rex field=_raw "Failed password for (?:invalid user )?(?<user>\w+) from (?<ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(user) | stats count() as count by user | sort -count, user | head 3`},
		{"SSH_UniqueIPs", `source=idx_ssh | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip) | dedup ip | stats count() as count`},
		{"OpenStack_APILatency", `source=idx_openstack | rex field=_raw "(?<method>GET|POST|DELETE) (?<url_path>/[^\s]+) HTTP" | rex field=_raw "status: (?<status>\d+) len: (?<resp_len>\d+) time: (?<resp_time>[0-9.]+)" | where isnotnull(method) | eval resp_ms = round(cast(resp_time as double) * 1000, 2) | stats count() as requests, avg(resp_ms) as avg_latency by method | sort method`},
		// Term-index lookups after the flush exercise the persisted tokenization.
		{"Search_FailedPassword", `search source=idx_ssh "Failed password" | stats count() as count`},
		{"Search_FailedPrefixRoot", `search source=idx_ssh Failed* root | stats count() as count`},
		{"Wildcard_InvalidUserFrom", `source=idx_ssh | where regexp_match(_raw, "Invalid user.*from") | stats count() as count`},
		{"Wildcard_Password", `source=idx_ssh | where _raw contains "password" | stats count() as count`},
		{"Wildcard_FailedFromPort", `source=idx_ssh | where regexp_match(_raw, "Failed.*from.*port") | stats count() as count`},
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
		requireAggValue(t, h.MustQuery(`source=idx_ssh | stats count() as count`), "count", 2000)
		requireAggValue(t, h.MustQuery(`source=idx_openstack | stats count() as count`), "count", 2000)
	})
}
