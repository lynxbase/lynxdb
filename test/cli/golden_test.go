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
			t.Parallel()

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
			t.Parallel()

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
// Known bugs discovered by golden tests (all resolved — retained for history)
// ============================================================================
//
// BUG 1 (FIXED): SelectCommand not implemented in pipeline builder.
//   Fixed: pipeline.go:726 now handles *spl2.SelectCommand as project + rename.
//
// BUG 2 (FIXED): Transaction duration was non-deterministic.
//   Fixed: TimestampNormalizer now correctly parses event timestamps from raw
//   JSON, so _time reflects the event timestamp and duration is deterministic.
//
// BUG 3 (FIXED): Multisearch sub-queries return 0 rows.
//   Fixed: pipeline.go propagates defaultSource; ConcurrentUnionIterator
//   provides deterministic child-index-ordered output.
//
// BUG 4 (FIXED): timechart used system clock instead of event timestamps.
//   Fixed: TimestampNormalizer correctly parses event timestamps from raw JSON,
//   so _time reflects the event timestamp and timechart produces correct buckets.
