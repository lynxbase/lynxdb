package upgrade

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Install extracts the binary from the archive and replaces the current
// executable with an atomic rename sequence:
//  1. Resolve the current executable path (follow symlinks)
//  2. Extract the archive to a temp directory
//  3. Copy the extracted binary to execPath.new (same filesystem for atomic rename)
//  4. Rename current binary to execPath.old (backup)
//  5. Rename execPath.new to execPath (install)
//  6. Set permissions to 0755
//  7. Remove execPath.old (cleanup)
//
// On failure during step 5, the function attempts to restore the backup.
func Install(archivePath string) error {
	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("upgrade.Install: determine executable path: %w", err)
	}
	execPath, err = filepath.EvalSymlinks(execPath)
	if err != nil {
		return fmt.Errorf("upgrade.Install: resolve symlinks: %w", err)
	}

	// Extract to temp directory.
	tmpDir, err := os.MkdirTemp("", "lynxdb-extract-*")
	if err != nil {
		return fmt.Errorf("upgrade.Install: create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	binaryPath, err := extractArchive(archivePath, tmpDir)
	if err != nil {
		return fmt.Errorf("upgrade.Install: %w", err)
	}

	// Copy the extracted binary to execPath.new so it's on the same
	// filesystem as the target. This ensures os.Rename is atomic.
	newExecPath := execPath + ".new"
	oldExecPath := execPath + ".old"

	if err := copyFile(binaryPath, newExecPath); err != nil {
		return fmt.Errorf("upgrade.Install: stage new binary: %w", err)
	}
	// Clean up .new on failure.
	defer func() {
		if err != nil {
			os.Remove(newExecPath)
		}
	}()

	if err := os.Chmod(newExecPath, 0o755); err != nil {
		return fmt.Errorf("upgrade.Install: chmod new binary: %w", err)
	}

	// Atomic swap: backup current, install new.
	_ = os.Remove(oldExecPath) // Remove leftover from previous upgrade.

	if err := os.Rename(execPath, oldExecPath); err != nil {
		return fmt.Errorf("upgrade.Install: backup current binary: %w", err)
	}

	if err := os.Rename(newExecPath, execPath); err != nil {
		// Attempt to restore the old binary.
		if restoreErr := os.Rename(oldExecPath, execPath); restoreErr != nil {
			return fmt.Errorf("upgrade.Install: install failed (%v) and restore failed (%v)", err, restoreErr)
		}
		return fmt.Errorf("upgrade.Install: install new binary: %w", err)
	}

	// Final permissions set (in case umask changed during rename).
	if err := os.Chmod(execPath, 0o755); err != nil {
		return fmt.Errorf("upgrade.Install: set permissions: %w", err)
	}

	// Cleanup backup.
	_ = os.Remove(oldExecPath)

	return nil
}

// copyFile copies src to dst, creating dst if needed.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}

	return out.Close()
}
