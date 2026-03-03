package upgrade

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/internal/buildinfo"
)

// ProgressFunc is called periodically during download to report progress.
// downloaded is the number of bytes downloaded so far, total is the
// expected total size (0 if unknown).
type ProgressFunc func(downloaded, total int64)

// Download fetches the artifact to a temp file, verifies the SHA256 checksum,
// and returns the path to the verified archive.
func Download(ctx context.Context, artifact *Artifact) (string, error) {
	return DownloadWithProgress(ctx, artifact, nil)
}

// DownloadWithProgress fetches the artifact to a temp file with progress
// reporting, verifies the SHA256 checksum, and returns the path to the
// verified archive.
func DownloadWithProgress(ctx context.Context, artifact *Artifact, progress ProgressFunc) (archivePath string, err error) {
	tmpFile, err := os.CreateTemp("", "lynxdb-upgrade-*")
	if err != nil {
		return "", fmt.Errorf("upgrade.Download: create temp file: %w", err)
	}
	defer func() {
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
		}
	}()

	client := &http.Client{Timeout: 5 * time.Minute}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, artifact.URL, nil)
	if err != nil {
		return "", fmt.Errorf("upgrade.Download: create request: %w", err)
	}
	req.Header.Set("User-Agent", buildinfo.UserAgent())

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("upgrade.Download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("upgrade.Download: HTTP %d", resp.StatusCode)
	}

	// Determine total size for progress reporting.
	total := resp.ContentLength
	if total <= 0 {
		total = artifact.Size
	}

	// Stream to file while computing SHA256 hash.
	hasher := sha256.New()
	var reader io.Reader = resp.Body

	if progress != nil {
		reader = &progressReader{
			reader:   resp.Body,
			total:    total,
			callback: progress,
		}
	}

	writer := io.MultiWriter(tmpFile, hasher)
	if _, err := io.Copy(writer, reader); err != nil {
		return "", fmt.Errorf("upgrade.Download: download interrupted: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return "", fmt.Errorf("upgrade.Download: close temp file: %w", err)
	}

	// Verify checksum.
	actualHash := hex.EncodeToString(hasher.Sum(nil))
	if !strings.EqualFold(actualHash, artifact.SHA256) {
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf(
			"%w: expected %s, got %s",
			ErrChecksumMismatch, artifact.SHA256, actualHash,
		)
	}

	return tmpFile.Name(), nil
}

// progressReader wraps an io.Reader and calls a progress callback
// every progressReportInterval bytes.
type progressReader struct {
	reader     io.Reader
	total      int64
	downloaded int64
	callback   ProgressFunc
	lastReport int64
}

const progressReportInterval = 64 * 1024 // 64 KB

func (r *progressReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.downloaded += int64(n)

	if r.downloaded-r.lastReport >= progressReportInterval || err == io.EOF {
		r.callback(r.downloaded, r.total)
		r.lastReport = r.downloaded
	}

	return n, err
}
