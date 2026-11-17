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

			result := runLynxDB(t, "query", "--file", logPath, "--format", tc.Format, tc.Query)
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

	querySlots := make(chan struct{}, 8)

	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			if tc.Skip != "" {
				t.Skip(tc.Skip)
			}

			querySlots <- struct{}{}
			defer func() { <-querySlots }()

			result := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", tc.Format, tc.Query)
			assertGolden(t, tc, result)
		})
	}
}
