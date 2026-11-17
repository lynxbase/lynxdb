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

func TestRuntime(t *testing.T) {
	got := Runtime()
	if !strings.Contains(got, runtime.Version()) {
		t.Errorf("Runtime() = %q, missing Go version", got)
	}
	if !strings.Contains(got, runtime.GOOS) {
		t.Errorf("Runtime() = %q, missing GOOS", got)
	}
}
