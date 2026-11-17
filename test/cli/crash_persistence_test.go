//go:build clitest

package cli_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestServer_ForcedCrash_FlushedPartSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	srv := startServerWithDataDir(t, dataDir)

	ingestFile(t, srv, testdataLog("backend_server.log"))
	assertServerCount(t, srv, 26)

	indexDir := filepath.Join(dataDir, "indexes", "main")
	partPath := waitForFinalPart(t, indexDir, 5*time.Second)
	// A part file that was being written when the process died ends in .tmp.
	staleTmpPath := filepath.Join(indexDir, "forced_crash_cleanup.part.tmp")
	if err := os.WriteFile(staleTmpPath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale tmp file: %v", err)
	}

	if err := srv.hardKill(10 * time.Second); err != nil {
		t.Fatalf("hard kill server: %v", err)
	}

	restarted := startServerWithDataDir(t, dataDir)

	assertServerCount(t, restarted, 26)

	if _, err := os.Stat(staleTmpPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale tmp file still present after restart: err=%v", err)
	}
	if _, err := os.Stat(partPath); err != nil {
		t.Fatalf("final part missing after restart: %v", err)
	}
}

func assertServerCount(t *testing.T, srv *Server, want int) {
	t.Helper()

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`source=main | stats count() as count`)
	if r.ExitCode != 0 {
		t.Fatalf("query exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}
	if got := jsonCount(t, r.Stdout); got != want {
		t.Fatalf("count = %d, want %d", got, want)
	}
}

func waitForFinalPart(t *testing.T, indexDir string, timeout time.Duration) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	backoff := 10 * time.Millisecond
	const maxBackoff = 500 * time.Millisecond

	for {
		partPath, err := findFinalPart(indexDir)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("scan for final part: %v", err)
		}
		if partPath != "" {
			return partPath
		}
		if ctx.Err() != nil {
			t.Fatalf("final part not visible within %s under %s", timeout, indexDir)
		}

		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			t.Fatalf("final part not visible within %s under %s", timeout, indexDir)
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func findFinalPart(root string) (string, error) {
	var partPath string

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if d.Name() == "lost+found" {
				return filepath.SkipDir
			}

			return nil
		}
		if !strings.HasSuffix(d.Name(), ".part") {
			return nil
		}

		partPath = path

		return filepath.SkipAll
	})
	if err != nil {
		return "", err
	}

	return partPath, nil
}
