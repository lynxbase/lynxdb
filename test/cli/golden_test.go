//go:build clitest

package cli_test

import (
	"path/filepath"
	"testing"
)

// TestGolden_File discovers .test files under testdata/cli/file/ and runs each
// as a serverless (--file) query against a local log file.
func TestGolden_File(t *testing.T) {
	testDir := filepath.Join(projectRoot, "testdata", "cli", "file")
	tests := discoverTests(t, testDir)

	if len(tests) == 0 {
		t.Fatal("no .test files found in testdata/cli/file/")
	}

	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Skip != "" {
				t.Skip(tc.Skip)
			}

			if tc.File == "" {
				t.Fatalf("test %s: file mode requires '# file:' header", tc.Name)
			}

			logPath := testdataLog(tc.File)

			args := []string{
				"query",
				"--file", logPath,
				"--format", tc.Format,
				tc.Query,
			}

			result := runLynxDB(t, args...)
			assertGolden(t, tc, result)
		})
	}
}

// TestGolden_Server starts ONE shared server, ingests all test log files into
// named indexes, then discovers and runs .test files from testdata/cli/server/.
func TestGolden_Server(t *testing.T) {
	testDir := filepath.Join(projectRoot, "testdata", "cli", "server")
	tests := discoverTests(t, testDir)

	if len(tests) == 0 {
		t.Fatal("no .test files found in testdata/cli/server/")
	}

	// Start one shared server for all server-mode tests.
	srv := startServer(t)

	// Ingest all 5 log files into named indexes.
	logIndexes := map[string]string{
		"backend_server.log":     "backend",
		"nginx_access.log":       "nginx",
		"frontend_console.log":   "frontend",
		"audit_security.log":     "audit_security",
		"audit_transactions.log": "audit_transactions",
	}

	for logFile, index := range logIndexes {
		ingestFileWithIndex(t, srv, testdataLog(logFile), index)
	}

	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Skip != "" {
				t.Skip(tc.Skip)
			}

			args := []string{
				"--server", srv.BaseURL,
				"query",
				"--format", tc.Format,
				tc.Query,
			}

			result := runLynxDB(t, args...)
			assertGolden(t, tc, result)
		})
	}
}

// ============================================================================
// Known bugs discovered by golden tests (skipped tests)
// ============================================================================
//
// BUG 1: SelectCommand not implemented in pipeline builder
//   Affected tests:
//     - file/backend_eventstats_pct
//     - file/backend_lynxflow_enrich_outlier
//   Description: The SPL2 parser recognizes `| select field1, field2` (LynxFlow
//   syntax) and produces *spl2.SelectCommand, but pkg/engine/pipeline/pipeline.go
//   has no case for it and returns "unsupported command type: *spl2.SelectCommand".
//   Fix: Add SelectCommand handling in pipeline.go (should behave like fields/table
//   with explicit column ordering).
//
// BUG 2: Transaction duration is non-deterministic
//   Affected tests:
//     - file/backend_transaction_user
//     - file/backend_transaction_maxspan
//     - server/backend_transaction_user
//   Description: The `transaction` command produces different `duration` values
//   across runs (sub-ms fluctuations). Duration should be computed from event
//   timestamps (deterministic), not wall-clock time.
//
// BUG 3 (FIXED): Multisearch sub-queries return 0 rows
//   Source inheritance fix: pipeline.go now propagates defaultSource to sub-queries.
//   Ordering fix: ConcurrentUnionIterator.OrderConcurrent provides deterministic
//   output in child index order while running all branches concurrently.
//   Previously affected: file/backend_multisearch_*, server/multisearch_cross_*.
//
// BUG 4: timechart uses system clock instead of event timestamps
//   Affected tests:
//     - file/backend_timechart_hourly
//     - file/backend_timechart_by_level
//     - file/backend_timechart_avg_duration
//     - server/backend_timechart_hourly
//   Description: `| timechart count span=1h` on events spanning 08:12–10:45
//   produces 1 bucket at the current system time with count missing and
//   avg(duration_ms)=null. The timechart command uses `_time` (ingestion time)
//   instead of the event's `timestamp` field. Workaround: use
//   `| bin timestamp span=30m as bucket | stats count by bucket` which correctly
//   buckets by the event timestamp and produces 6+ rows.
//   See: backend_timeseries_* tests for the working equivalents.
