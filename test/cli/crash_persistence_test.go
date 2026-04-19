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

	partPath := waitForFinalPart(t, dataDir, 15*time.Second)
	partDir := filepath.Dir(partPath)
	staleTmpPath := filepath.Join(partDir, "tmp_forced_crash_cleanup.lsg")
	if err := os.WriteFile(staleTmpPath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale tmp file: %v", err)
	}

	if err := srv.hardKill(10 * time.Second); err != nil {
		t.Fatalf("hard kill server: %v", err)
	}

	restarted := startServerWithDataDir(t, dataDir)

	r := runLynxDB(t, "--server", restarted.BaseURL, "query", "--format", "json",
		`FROM main | stats count`)
	if r.ExitCode != 0 {
		t.Fatalf("restart query exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}
	if got := jsonCount(t, r.Stdout); got != 26 {
		t.Fatalf("restart count = %d, want 26", got)
	}

	if _, err := os.Stat(staleTmpPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale tmp file still present after restart: err=%v", err)
	}
	if _, err := os.Stat(partPath); err != nil {
		t.Fatalf("final part missing after restart: %v", err)
	}
}

func waitForFinalPart(t *testing.T, dataDir string, timeout time.Duration) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	hotDir := filepath.Join(dataDir, "segments", "hot")
	backoff := 10 * time.Millisecond
	const maxBackoff = 500 * time.Millisecond

	for {
		partPath, err := findFinalPart(hotDir)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("scan for final part: %v", err)
		}
		if partPath != "" {
			return partPath
		}
		if ctx.Err() != nil {
			t.Fatalf("final part not visible within %s under %s", timeout, hotDir)
		}

		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			t.Fatalf("final part not visible within %s under %s", timeout, hotDir)
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
			return nil
		}
		if filepath.Ext(d.Name()) != ".lsg" {
			return nil
		}
		if strings.HasPrefix(d.Name(), "tmp_") {
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
