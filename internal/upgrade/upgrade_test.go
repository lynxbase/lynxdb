package upgrade

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// semver tests

func TestParseSemver(t *testing.T) {
	tests := []struct {
		input   string
		want    semver
		wantErr bool
	}{
		{"v1.2.3", semver{1, 2, 3, ""}, false},
		{"1.2.3", semver{1, 2, 3, ""}, false},
		{"v0.0.0", semver{0, 0, 0, ""}, false},
		{"v1.2.3-rc.1", semver{1, 2, 3, "rc.1"}, false},
		{"v1.2.3-beta.2", semver{1, 2, 3, "beta.2"}, false},
		{"v10.20.30", semver{10, 20, 30, ""}, false},
		{"", semver{}, true},
		{"v1.2", semver{}, true},
		{"v1.2.x", semver{}, true},
		{"vx.y.z", semver{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseSemver(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseSemver(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parseSemver(%q) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestCompareVersions(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		// Equal versions.
		{"v0.5.0", "v0.5.0", 0},
		{"v1.0.0", "1.0.0", 0},

		// Newer/older.
		{"v0.6.0", "v0.5.0", 1},
		{"v0.5.0", "v0.6.0", -1},
		{"v1.0.0", "v0.99.99", 1},
		{"v0.5.1", "v0.5.0", 1},

		// Pre-release ordering.
		{"v0.5.0", "v0.5.0-rc.1", 1},         // stable > pre-release
		{"v0.5.0-rc.1", "v0.5.0", -1},        // pre-release < stable
		{"v0.5.0-rc.2", "v0.5.0-rc.1", 1},    // rc.2 > rc.1
		{"v0.5.0-rc.1", "v0.5.0-rc.2", -1},   // rc.1 < rc.2
		{"v0.5.0-beta.1", "v0.5.0-rc.1", -1}, // beta < rc (lexicographic)
		{"v0.5.0-rc.1", "v0.5.0-beta.1", 1},  // rc > beta

		// v prefix handling.
		{"v1.0.0", "v1.0.0", 0},
		{"1.0.0", "v1.0.0", 0},
		{"v1.0.0", "1.0.0", 0},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_vs_%s", tt.a, tt.b), func(t *testing.T) {
			got := CompareVersions(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("CompareVersions(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// PlatformKey tests

func TestPlatformKey(t *testing.T) {
	key := PlatformKey()
	expected := runtime.GOOS + "-" + runtime.GOARCH
	if key != expected {
		t.Errorf("PlatformKey() = %q, want %q", key, expected)
	}
}

// Manifest parsing tests

func TestParseManifest(t *testing.T) {
	data := `{
		"version": "v0.5.0",
		"channel": "stable",
		"released_at": "2025-06-15T10:30:00Z",
		"changelog_url": "https://github.com/lynxbase/lynxdb/releases/tag/v0.5.0",
		"artifacts": {
			"linux-amd64": {
				"url": "https://dl.lynxdb.org/v0.5.0/lynxdb-v0.5.0-linux-amd64.tar.gz",
				"sha256": "abc123",
				"size": 15728640,
				"filename": "lynxdb-v0.5.0-linux-amd64.tar.gz"
			},
			"darwin-arm64": {
				"url": "https://dl.lynxdb.org/v0.5.0/lynxdb-v0.5.0-darwin-arm64.tar.gz",
				"sha256": "def456",
				"size": 15466496,
				"filename": "lynxdb-v0.5.0-darwin-arm64.tar.gz"
			}
		},
		"notices": ["Test notice"]
	}`

	var m Manifest
	if err := json.Unmarshal([]byte(data), &m); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}

	if m.Version != "v0.5.0" {
		t.Errorf("Version = %q, want %q", m.Version, "v0.5.0")
	}
	if m.Channel != "stable" {
		t.Errorf("Channel = %q, want %q", m.Channel, "stable")
	}
	if len(m.Artifacts) != 2 {
		t.Errorf("len(Artifacts) = %d, want 2", len(m.Artifacts))
	}
	if m.Artifacts["linux-amd64"].SHA256 != "abc123" {
		t.Errorf("linux-amd64 SHA256 = %q, want %q", m.Artifacts["linux-amd64"].SHA256, "abc123")
	}
	if len(m.Notices) != 1 || m.Notices[0] != "Test notice" {
		t.Errorf("Notices = %v, want [Test notice]", m.Notices)
	}
}

// FetchManifest tests

func TestFetchManifest(t *testing.T) {
	manifest := Manifest{
		Version: "v0.5.0",
		Channel: "stable",
		Artifacts: map[string]Artifact{
			"linux-amd64": {URL: "https://example.com/file.tar.gz", SHA256: "abc"},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(manifest)
	}))
	defer srv.Close()

	// Override constants not possible directly, so test fetchManifestFromEndpoints.
	ctx := context.Background()
	got, err := fetchManifestFromEndpoints(ctx, srv.URL, srv.URL)
	if err != nil {
		t.Fatalf("fetchManifestFromEndpoints: %v", err)
	}

	if got.Version != "v0.5.0" {
		t.Errorf("Version = %q, want %q", got.Version, "v0.5.0")
	}
}

func TestFetchManifestFallback(t *testing.T) {
	manifest := Manifest{
		Version: "v0.5.0",
		Channel: "stable",
		Artifacts: map[string]Artifact{
			"linux-amd64": {URL: "https://example.com/file.tar.gz", SHA256: "abc"},
		},
	}

	// Primary fails, fallback succeeds.
	primaryFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer primaryFail.Close()

	fallbackOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(manifest)
	}))
	defer fallbackOK.Close()

	ctx := context.Background()
	got, err := fetchManifestFromEndpoints(ctx, primaryFail.URL, fallbackOK.URL)
	if err != nil {
		t.Fatalf("fetchManifestFromEndpoints (fallback): %v", err)
	}

	if got.Version != "v0.5.0" {
		t.Errorf("Version = %q, want %q", got.Version, "v0.5.0")
	}
}

// Check tests

func TestCheckUpdateAvailable(t *testing.T) {
	platformKey := PlatformKey()
	manifest := &Manifest{
		Version: "v0.6.0",
		Channel: "stable",
		Artifacts: map[string]Artifact{
			platformKey: {URL: "https://example.com/file.tar.gz", SHA256: "abc"},
		},
	}

	result, err := checkAgainstManifest(manifest, "v0.5.0")
	if err != nil {
		t.Fatalf("checkAgainstManifest: %v", err)
	}

	if !result.UpdateAvail {
		t.Error("UpdateAvail = false, want true")
	}
	if result.LatestVersion != "v0.6.0" {
		t.Errorf("LatestVersion = %q, want %q", result.LatestVersion, "v0.6.0")
	}
	if result.Artifact == nil {
		t.Error("Artifact is nil, want non-nil")
	}
}

func TestCheckAlreadyLatest(t *testing.T) {
	platformKey := PlatformKey()
	manifest := &Manifest{
		Version: "v0.5.0",
		Channel: "stable",
		Artifacts: map[string]Artifact{
			platformKey: {URL: "https://example.com/file.tar.gz", SHA256: "abc"},
		},
	}

	result, err := checkAgainstManifest(manifest, "v0.5.0")
	if err != nil {
		t.Fatalf("checkAgainstManifest: %v", err)
	}

	if result.UpdateAvail {
		t.Error("UpdateAvail = true, want false")
	}
}

func TestCheckOlderManifest(t *testing.T) {
	// When current version is newer than manifest (e.g. running pre-release).
	platformKey := PlatformKey()
	manifest := &Manifest{
		Version: "v0.5.0",
		Channel: "stable",
		Artifacts: map[string]Artifact{
			platformKey: {URL: "https://example.com/file.tar.gz", SHA256: "abc"},
		},
	}

	result, err := checkAgainstManifest(manifest, "v0.6.0")
	if err != nil {
		t.Fatalf("checkAgainstManifest: %v", err)
	}

	if result.UpdateAvail {
		t.Error("UpdateAvail = true, want false (current is newer)")
	}
}

// Checksum verification tests

func TestChecksumVerification(t *testing.T) {
	content := []byte("hello lynxdb binary content")
	hash := sha256.Sum256(content)
	hexHash := hex.EncodeToString(hash[:])

	tmpFile, err := os.CreateTemp(t.TempDir(), "checksum-test-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tmpFile.Write(content); err != nil {
		tmpFile.Close()
		t.Fatal(err)
	}
	tmpFile.Close()

	// Correct checksum should pass.
	h := sha256.New()
	f, _ := os.Open(tmpFile.Name())
	io.Copy(h, f)
	f.Close()
	actual := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(actual, hexHash) {
		t.Errorf("checksum mismatch: got %s, want %s", actual, hexHash)
	}

	// Wrong checksum should not match.
	wrongHash := "0000000000000000000000000000000000000000000000000000000000000000"
	if strings.EqualFold(actual, wrongHash) {
		t.Error("checksum unexpectedly matched wrong hash")
	}
}

// Extract tests

func TestExtractTarGz(t *testing.T) {
	dir := t.TempDir()
	archivePath := filepath.Join(dir, "test.tar.gz")
	binaryContent := []byte("#!/bin/sh\necho lynxdb")

	// Create a tar.gz with a lynxdb binary inside.
	createTarGz(t, archivePath, "lynxdb-v0.5.0-linux-amd64/lynxdb", binaryContent)

	extractDir := filepath.Join(dir, "extract")
	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		t.Fatal(err)
	}

	binPath, err := extractTarGz(archivePath, extractDir)
	if err != nil {
		t.Fatalf("extractTarGz: %v", err)
	}

	if filepath.Base(binPath) != "lynxdb" {
		t.Errorf("extracted binary name = %q, want 'lynxdb'", filepath.Base(binPath))
	}

	got, err := os.ReadFile(binPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(binaryContent) {
		t.Errorf("extracted content = %q, want %q", got, binaryContent)
	}
}

func TestExtractZip(t *testing.T) {
	dir := t.TempDir()
	archivePath := filepath.Join(dir, "test.zip")
	binaryContent := []byte("lynxdb.exe binary content")

	// Create a zip with a lynxdb.exe binary inside.
	createZip(t, archivePath, "lynxdb-v0.5.0-windows-amd64/lynxdb.exe", binaryContent)

	extractDir := filepath.Join(dir, "extract")
	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		t.Fatal(err)
	}

	binPath, err := extractZip(archivePath, extractDir)
	if err != nil {
		t.Fatalf("extractZip: %v", err)
	}

	if filepath.Base(binPath) != "lynxdb.exe" {
		t.Errorf("extracted binary name = %q, want 'lynxdb.exe'", filepath.Base(binPath))
	}

	got, err := os.ReadFile(binPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(binaryContent) {
		t.Errorf("extracted content = %q, want %q", got, binaryContent)
	}
}

func TestExtractTarGz_PathTraversal(t *testing.T) {
	dir := t.TempDir()
	archivePath := filepath.Join(dir, "malicious.tar.gz")

	createTarGz(t, archivePath, "../../../etc/passwd", []byte("root:x:0:0"))

	extractDir := filepath.Join(dir, "extract")
	os.MkdirAll(extractDir, 0o755)

	_, err := extractTarGz(archivePath, extractDir)
	if err == nil {
		t.Fatal("expected error for path traversal, got nil")
	}
	if !strings.Contains(err.Error(), "path traversal") {
		t.Errorf("error = %q, want path traversal error", err.Error())
	}
}

func TestExtractArchiveDispatch(t *testing.T) {
	dir := t.TempDir()

	// tar.gz should work
	tgzPath := filepath.Join(dir, "test.tar.gz")
	createTarGz(t, tgzPath, "lynxdb", []byte("binary"))
	extractDir1 := filepath.Join(dir, "ext1")
	os.MkdirAll(extractDir1, 0o755)
	if _, err := extractArchive(tgzPath, extractDir1); err != nil {
		t.Errorf("extractArchive(.tar.gz) failed: %v", err)
	}

	// zip should work
	zipPath := filepath.Join(dir, "test.zip")
	createZip(t, zipPath, "lynxdb.exe", []byte("binary"))
	extractDir2 := filepath.Join(dir, "ext2")
	os.MkdirAll(extractDir2, 0o755)
	if _, err := extractArchive(zipPath, extractDir2); err != nil {
		t.Errorf("extractArchive(.zip) failed: %v", err)
	}

	// Unknown extension should fail
	unknownPath := filepath.Join(dir, "test.rar")
	os.WriteFile(unknownPath, []byte("data"), 0o644)
	if _, err := extractArchive(unknownPath, dir); err == nil {
		t.Error("extractArchive(.rar) should have failed")
	}
}

// Atomic swap tests

func TestAtomicSwap(t *testing.T) {
	dir := t.TempDir()

	// Create a fake "current binary".
	currentPath := filepath.Join(dir, "lynxdb")
	if err := os.WriteFile(currentPath, []byte("old-binary-v0.4.0"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Create a fake archive with a new binary.
	archivePath := filepath.Join(dir, "upgrade.tar.gz")
	createTarGz(t, archivePath, "lynxdb", []byte("new-binary-v0.5.0"))

	// Extract and simulate the swap logic (without calling Install which
	// uses os.Executable()).
	extractDir := filepath.Join(dir, "extract")
	os.MkdirAll(extractDir, 0o755)

	binaryPath, err := extractArchive(archivePath, extractDir)
	if err != nil {
		t.Fatalf("extractArchive: %v", err)
	}

	// Copy to .new
	newPath := currentPath + ".new"
	oldPath := currentPath + ".old"

	if err := copyFile(binaryPath, newPath); err != nil {
		t.Fatalf("copyFile: %v", err)
	}

	// Swap
	_ = os.Remove(oldPath)
	if err := os.Rename(currentPath, oldPath); err != nil {
		t.Fatalf("backup rename: %v", err)
	}
	if err := os.Rename(newPath, currentPath); err != nil {
		t.Fatalf("install rename: %v", err)
	}
	_ = os.Remove(oldPath)

	// Verify the new binary is in place.
	got, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new-binary-v0.5.0" {
		t.Errorf("binary content = %q, want %q", got, "new-binary-v0.5.0")
	}
}

// Download tests

func TestDownloadWithProgress(t *testing.T) {
	content := []byte("this is the archive content for download testing")
	hash := sha256.Sum256(content)
	hexHash := hex.EncodeToString(hash[:])

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
		w.Write(content)
	}))
	defer srv.Close()

	artifact := &Artifact{
		URL:    srv.URL + "/lynxdb.tar.gz",
		SHA256: hexHash,
		Size:   int64(len(content)),
	}

	var progressCalled bool
	progressFn := func(downloaded, total int64) {
		progressCalled = true
	}

	ctx := context.Background()
	path, err := DownloadWithProgress(ctx, artifact, progressFn)
	if err != nil {
		t.Fatalf("DownloadWithProgress: %v", err)
	}
	defer os.Remove(path)

	// Verify content.
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(content) {
		t.Errorf("downloaded content mismatch")
	}

	if !progressCalled {
		t.Error("progress callback was not called")
	}
}

func TestDownloadChecksumMismatch(t *testing.T) {
	content := []byte("some content")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(content)
	}))
	defer srv.Close()

	artifact := &Artifact{
		URL:    srv.URL + "/lynxdb.tar.gz",
		SHA256: "0000000000000000000000000000000000000000000000000000000000000000",
		Size:   int64(len(content)),
	}

	ctx := context.Background()
	_, err := Download(ctx, artifact)
	if err == nil {
		t.Fatal("expected checksum mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "checksum verification failed") {
		t.Errorf("error = %q, want checksum mismatch", err.Error())
	}
}

// helpers

func createTarGz(t *testing.T, archivePath, entryName string, content []byte) {
	t.Helper()

	f, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	gw := gzip.NewWriter(f)
	defer gw.Close()

	tw := tar.NewWriter(gw)
	defer tw.Close()

	if err := tw.WriteHeader(&tar.Header{
		Name: entryName,
		Size: int64(len(content)),
		Mode: 0o755,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := tw.Write(content); err != nil {
		t.Fatal(err)
	}
}

func createZip(t *testing.T, archivePath, entryName string, content []byte) {
	t.Helper()

	f, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	zw := zip.NewWriter(f)
	defer zw.Close()

	w, err := zw.Create(entryName)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := w.Write(content); err != nil {
		t.Fatal(err)
	}
}
