package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEntriesDefaultsOnly(t *testing.T) {
	t.Setenv("LYNXDB_CONFIG", "")

	// Change to temp dir so no ./lynxdb.yaml is found.
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	entries := Entries("")
	if len(entries) == 0 {
		t.Fatal("expected entries")
	}
	for _, e := range entries {
		if e.Source != "default" {
			t.Errorf("key %q has source %q, want default", e.Key, e.Source)
		}
	}
}

func TestEntriesWithFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(`listen: "0.0.0.0:3200"`), 0o644)

	entries := Entries(cfgPath)

	found := false
	for _, e := range entries {
		if e.Key == "listen" {
			found = true
			if e.Source != "config file" {
				t.Errorf("listen source: got %q, want config file", e.Source)
			}
			if e.Value != "0.0.0.0:3200" {
				t.Errorf("listen value: got %q, want 0.0.0.0:3200", e.Value)
			}
		}
	}
	if !found {
		t.Error("listen entry not found")
	}
}

func TestEntriesWithEnv(t *testing.T) {
	t.Setenv("LYNXDB_CONFIG", "")
	t.Setenv("LYNXDB_LISTEN", "0.0.0.0:4000")

	// Change to temp dir so no ./lynxdb.yaml is found.
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	entries := Entries("")

	found := false
	for _, e := range entries {
		if e.Key == "listen" {
			found = true
			if e.Source != "env var" {
				t.Errorf("listen source: got %q, want env var", e.Source)
			}
			if e.Value != "0.0.0.0:4000" {
				t.Errorf("listen value: got %q, want 0.0.0.0:4000", e.Value)
			}
		}
	}
	if !found {
		t.Error("listen entry not found")
	}
}

func TestEntriesWithFileAndEnv(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(`
listen: "0.0.0.0:3200"
storage:
  compression: "zstd"
`), 0o644)

	t.Setenv("LYNXDB_LISTEN", "0.0.0.0:5000")

	entries := Entries(cfgPath)

	for _, e := range entries {
		switch e.Key {
		case "listen":
			// Env overrides file.
			if e.Source != "env var" {
				t.Errorf("listen source: got %q, want env var", e.Source)
			}
			if e.Value != "0.0.0.0:5000" {
				t.Errorf("listen value: got %q", e.Value)
			}
		case "storage.compression":
			if e.Source != "config file" {
				t.Errorf("storage.compression source: got %q, want config file", e.Source)
			}
			if e.Value != "zstd" {
				t.Errorf("storage.compression value: got %q", e.Value)
			}
		}
	}
}

func TestEntriesWithCLI_Override(t *testing.T) {
	t.Setenv("LYNXDB_CONFIG", "")

	// Change to temp dir so no ./lynxdb.yaml is found.
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	cli := []CLIOverride{
		{Key: "data_dir", Value: "./testdata/db", Flag: "--data-dir"},
		{Key: "listen", Value: "0.0.0.0:9000", Flag: "--addr"},
	}

	entries := EntriesWithCLI("", cli)

	for _, e := range entries {
		switch e.Key {
		case "data_dir":
			if e.Source != "--data-dir" {
				t.Errorf("data_dir source: got %q, want --data-dir", e.Source)
			}
			if e.Value != "./testdata/db" {
				t.Errorf("data_dir value: got %q, want ./testdata/db", e.Value)
			}
		case "listen":
			if e.Source != "--addr" {
				t.Errorf("listen source: got %q, want --addr", e.Source)
			}
			if e.Value != "0.0.0.0:9000" {
				t.Errorf("listen value: got %q, want 0.0.0.0:9000", e.Value)
			}
		case "log_level":
			// Not overridden by CLI — should be default.
			if e.Source != "default" {
				t.Errorf("log_level source: got %q, want default", e.Source)
			}
		}
	}
}

func TestEntriesWithCLI_OverridesEnvAndFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(`listen: "0.0.0.0:3200"`), 0o644)

	t.Setenv("LYNXDB_LISTEN", "0.0.0.0:5000")

	// CLI overrides both env and file.
	cli := []CLIOverride{
		{Key: "listen", Value: "0.0.0.0:9999", Flag: "--addr"},
	}
	entries := EntriesWithCLI(cfgPath, cli)

	for _, e := range entries {
		if e.Key == "listen" {
			if e.Source != "--addr" {
				t.Errorf("listen source: got %q, want --addr (CLI should win over env and file)", e.Source)
			}
			if e.Value != "0.0.0.0:9999" {
				t.Errorf("listen value: got %q, want 0.0.0.0:9999", e.Value)
			}
		}
	}
}

func TestEntriesWithCLI_AllFieldsHaveSource(t *testing.T) {
	t.Setenv("LYNXDB_CONFIG", "")

	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	entries := EntriesWithCLI("", nil)
	if len(entries) == 0 {
		t.Fatal("expected entries")
	}
	for _, e := range entries {
		if e.Source == "" {
			t.Errorf("key %q has empty source", e.Key)
		}
		if e.Key == "" {
			t.Error("entry has empty key")
		}
	}
}

func TestEntriesContainsAllFields(t *testing.T) {
	t.Setenv("LYNXDB_CONFIG", "")

	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	entries := Entries("")

	// Build set of keys returned by Entries().
	entryKeys := make(map[string]bool)
	for _, e := range entries {
		entryKeys[e.Key] = true
	}

	// Every leaf config key from KnownKeyNames() should be present in Entries(),
	// except map-type keys (profiles) which have dynamic children.
	for _, key := range KnownKeyNames() {
		// Skip profiles — they are dynamic, not enumerated in Entries().
		if key == "profiles" {
			continue
		}
		if !entryKeys[key] {
			t.Errorf("missing entry for config key %q", key)
		}
	}
}
