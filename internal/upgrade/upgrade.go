package upgrade

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/internal/buildinfo"
)

const (
	manifestURL         = "https://dl.lynxdb.org/manifest.json"
	manifestFallbackURL = "https://raw.githubusercontent.com/OrlovEvgeny/Lynxdb/main/dist/manifest.json"
	httpTimeout         = 30 * time.Second

	// maxManifestSize limits the manifest body to prevent abuse (1 MB).
	maxManifestSize = 1 << 20
)

// Manifest represents the release manifest served from CDN.
type Manifest struct {
	Version      string              `json:"version"`
	Channel      string              `json:"channel"`
	ReleasedAt   string              `json:"released_at"`
	ChangelogURL string              `json:"changelog_url"`
	Artifacts    map[string]Artifact `json:"artifacts"`
	Notices      []string            `json:"notices"`
}

// Artifact represents a platform-specific binary archive.
type Artifact struct {
	URL      string `json:"url"`
	SHA256   string `json:"sha256"`
	Size     int64  `json:"size"`
	Filename string `json:"filename"`
}

// CheckResult is returned by Check.
type CheckResult struct {
	CurrentVersion string
	LatestVersion  string
	UpdateAvail    bool
	Artifact       *Artifact
	ChangelogURL   string
	Notices        []string
}

// PlatformKey returns the manifest key for the current platform.
// Examples: "linux-amd64", "darwin-arm64".
func PlatformKey() string {
	return runtime.GOOS + "-" + runtime.GOARCH
}

// FetchManifest downloads and parses the release manifest from the CDN,
// falling back to GitHub if the primary endpoint fails.
func FetchManifest(ctx context.Context) (*Manifest, error) {
	return fetchManifestFromEndpoints(ctx, manifestURL, manifestFallbackURL)
}

// FetchVersionedManifest downloads and parses a manifest for a specific version,
// falling back to GitHub if the primary CDN endpoint fails.
func FetchVersionedManifest(ctx context.Context, version string) (*Manifest, error) {
	primary := fmt.Sprintf("https://dl.lynxdb.org/%s/manifest.json", version)
	fallback := fmt.Sprintf("https://raw.githubusercontent.com/OrlovEvgeny/Lynxdb/main/dist/manifest.json")
	return fetchManifestFromEndpoints(ctx, primary, fallback)
}

func fetchManifestFromEndpoints(ctx context.Context, primary, fallback string) (*Manifest, error) {
	client := &http.Client{Timeout: httpTimeout}

	manifest, err := fetchManifestFrom(ctx, client, primary)
	if err == nil {
		return manifest, nil
	}

	manifest, err2 := fetchManifestFrom(ctx, client, fallback)
	if err2 != nil {
		return nil, fmt.Errorf("upgrade.FetchManifest: primary=%v, fallback=%v", err, err2)
	}

	return manifest, nil
}

func fetchManifestFrom(ctx context.Context, client *http.Client, url string) (*Manifest, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", buildinfo.UserAgent())

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxManifestSize))
	if err != nil {
		return nil, err
	}

	var m Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("invalid manifest JSON: %w", err)
	}

	return &m, nil
}

// Check fetches the manifest and checks if an update is available
// by comparing the current version against the manifest's latest version
// using semantic versioning.
func Check(ctx context.Context, currentVersion string) (*CheckResult, error) {
	manifest, err := FetchManifest(ctx)
	if err != nil {
		return nil, err
	}

	return checkAgainstManifest(manifest, currentVersion)
}

// CheckAgainstManifest checks if an update is available using a pre-fetched manifest.
// Exported for use when a versioned manifest has already been fetched.
func CheckAgainstManifest(manifest *Manifest, currentVersion string) (*CheckResult, error) {
	return checkAgainstManifest(manifest, currentVersion)
}

func checkAgainstManifest(manifest *Manifest, currentVersion string) (*CheckResult, error) {
	result := &CheckResult{
		CurrentVersion: currentVersion,
		LatestVersion:  manifest.Version,
		ChangelogURL:   manifest.ChangelogURL,
		Notices:        manifest.Notices,
	}

	// Use semver comparison: update available only if latest > current.
	if CompareVersions(manifest.Version, currentVersion) > 0 {
		key := PlatformKey()
		artifact, ok := manifest.Artifacts[key]
		if !ok {
			return nil, fmt.Errorf("%w: %s in %s", ErrPlatformNotFound, key, manifest.Version)
		}
		result.UpdateAvail = true
		result.Artifact = &artifact
	}

	return result, nil
}
