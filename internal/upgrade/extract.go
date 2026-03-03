package upgrade

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// maxExtractSize limits the total extracted size to 256 MB to prevent decompression bombs.
const maxExtractSize = 256 << 20

// extractArchive extracts the binary from the given archive into destDir.
// It dispatches based on file extension (.tar.gz or .zip).
// Returns the path to the extracted binary.
func extractArchive(archivePath, destDir string) (string, error) {
	switch {
	case strings.HasSuffix(archivePath, ".tar.gz") || strings.HasSuffix(archivePath, ".tgz"):
		return extractTarGz(archivePath, destDir)
	case strings.HasSuffix(archivePath, ".zip"):
		return extractZip(archivePath, destDir)
	default:
		return "", fmt.Errorf("upgrade.extractArchive: unsupported archive format: %s", filepath.Base(archivePath))
	}
}

// extractTarGz extracts a .tar.gz archive looking for the lynxdb binary.
func extractTarGz(archivePath, destDir string) (string, error) {
	f, err := os.Open(archivePath)
	if err != nil {
		return "", fmt.Errorf("upgrade.extractTarGz: %w", err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return "", fmt.Errorf("upgrade.extractTarGz: gzip: %w", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	var binaryPath string
	var totalSize int64

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("upgrade.extractTarGz: %w", err)
		}

		// Security: reject paths with ".." to prevent zip-slip.
		if err := validateArchivePath(header.Name); err != nil {
			return "", err
		}

		// Only extract regular files.
		if header.Typeflag != tar.TypeReg {
			continue
		}

		totalSize += header.Size
		if totalSize > maxExtractSize {
			return "", fmt.Errorf("upgrade.extractTarGz: archive exceeds maximum size (%d bytes)", maxExtractSize)
		}

		base := filepath.Base(header.Name)
		if !isBinaryName(base) {
			continue
		}

		destPath := filepath.Join(destDir, base)
		if err := extractFile(destPath, tr, header.Size, os.FileMode(header.Mode)); err != nil {
			return "", err
		}
		binaryPath = destPath
	}

	if binaryPath == "" {
		return "", fmt.Errorf("upgrade.extractTarGz: lynxdb binary not found in archive")
	}

	return binaryPath, nil
}

// extractZip extracts a .zip archive looking for the lynxdb binary.
func extractZip(archivePath, destDir string) (string, error) {
	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return "", fmt.Errorf("upgrade.extractZip: %w", err)
	}
	defer r.Close()

	var binaryPath string
	var totalSize int64

	for _, f := range r.File {
		// Security: reject paths with ".." to prevent zip-slip.
		if err := validateArchivePath(f.Name); err != nil {
			return "", err
		}

		if f.FileInfo().IsDir() {
			continue
		}

		totalSize += int64(f.UncompressedSize64)
		if totalSize > maxExtractSize {
			return "", fmt.Errorf("upgrade.extractZip: archive exceeds maximum size (%d bytes)", maxExtractSize)
		}

		base := filepath.Base(f.Name)
		if !isBinaryName(base) {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			return "", fmt.Errorf("upgrade.extractZip: open %s: %w", f.Name, err)
		}

		destPath := filepath.Join(destDir, base)
		if err := extractFile(destPath, rc, int64(f.UncompressedSize64), f.Mode()); err != nil {
			rc.Close()
			return "", err
		}
		rc.Close()
		binaryPath = destPath
	}

	if binaryPath == "" {
		return "", fmt.Errorf("upgrade.extractZip: lynxdb binary not found in archive")
	}

	return binaryPath, nil
}

// isBinaryName checks if the filename is the lynxdb binary.
func isBinaryName(name string) bool {
	return name == "lynxdb" || name == "lynxdb.exe"
}

// validateArchivePath rejects path traversal attacks.
func validateArchivePath(name string) error {
	if strings.Contains(name, "..") {
		return fmt.Errorf("upgrade: archive contains path traversal: %q", name)
	}
	return nil
}

// extractFile writes the content from reader to destPath with the given mode.
func extractFile(destPath string, reader io.Reader, size int64, mode os.FileMode) error {
	if mode == 0 {
		mode = 0o755
	}

	out, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("upgrade: create %s: %w", destPath, err)
	}
	defer out.Close()

	// Use LimitReader as an additional safeguard.
	if _, err := io.Copy(out, io.LimitReader(reader, size+1)); err != nil {
		return fmt.Errorf("upgrade: write %s: %w", destPath, err)
	}

	return nil
}
