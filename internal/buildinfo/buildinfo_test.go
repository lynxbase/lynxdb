package buildinfo

import (
	"runtime"
	"strings"
	"testing"
)

func TestDefaults(t *testing.T) {
	if Version != "dev" {
		t.Errorf("default Version = %q, want %q", Version, "dev")
	}
	if Commit != "unknown" {
		t.Errorf("default Commit = %q, want %q", Commit, "unknown")
	}
	if Date != "unknown" {
		t.Errorf("default Date = %q, want %q", Date, "unknown")
	}
}

func TestIsDev(t *testing.T) {
	// Default should be dev.
	if !IsDev() {
		t.Error("IsDev() = false for default Version, want true")
	}

	// Override and check.
	orig := Version
	t.Cleanup(func() { Version = orig })

	Version = "v0.5.0"
	if IsDev() {
		t.Error("IsDev() = true for Version=v0.5.0, want false")
	}
}

func TestFullVersion(t *testing.T) {
	got := FullVersion()
	// Should contain "lynxdb", the version, and both commit/date markers.
	for _, want := range []string{"lynxdb", Version, "commit:", "built:"} {
		if !strings.Contains(got, want) {
			t.Errorf("FullVersion() = %q, missing %q", got, want)
		}
	}
}

func TestUserAgent(t *testing.T) {
	got := UserAgent()
	want := "lynxdb/dev"
	if got != want {
		t.Errorf("UserAgent() = %q, want %q", got, want)
	}

	orig := Version
	t.Cleanup(func() { Version = orig })

	Version = "v1.2.3"
	if got := UserAgent(); got != "lynxdb/v1.2.3" {
		t.Errorf("UserAgent() = %q, want %q", got, "lynxdb/v1.2.3")
	}
}

func TestInfo(t *testing.T) {
	info := Info()

	requiredKeys := []string{"version", "commit", "date", "go", "os", "arch"}
	for _, key := range requiredKeys {
		if _, ok := info[key]; !ok {
			t.Errorf("Info() missing key %q", key)
		}
	}

	if info["version"] != Version {
		t.Errorf("Info()[version] = %q, want %q", info["version"], Version)
	}
	if info["go"] != runtime.Version() {
		t.Errorf("Info()[go] = %q, want %q", info["go"], runtime.Version())
	}
	if info["os"] != runtime.GOOS {
		t.Errorf("Info()[os] = %q, want %q", info["os"], runtime.GOOS)
	}
	if info["arch"] != runtime.GOARCH {
		t.Errorf("Info()[arch] = %q, want %q", info["arch"], runtime.GOARCH)
	}
}

func TestShort(t *testing.T) {
	got := Short()
	if !strings.Contains(got, "LynxDB") {
		t.Errorf("Short() = %q, missing 'LynxDB'", got)
	}
	if !strings.Contains(got, Version) {
		t.Errorf("Short() = %q, missing version %q", got, Version)
	}
}

func TestRuntime(t *testing.T) {
	got := Runtime()
	if !strings.Contains(got, runtime.Version()) {
		t.Errorf("Runtime() = %q, missing Go version", got)
	}
	if !strings.Contains(got, runtime.GOOS) {
		t.Errorf("Runtime() = %q, missing GOOS", got)
	}
}
