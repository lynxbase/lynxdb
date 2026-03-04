package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	// No config file, no env vars — should use defaults.
	t.Setenv("LYNXDB_CONFIG", "")

	// Change to temp dir so no ./lynxdb.yaml is found.
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)        // prevents ~/.config/lynxdb/config.yaml
	t.Setenv("XDG_CONFIG_HOME", "") // prevents XDG override
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	cfg, path, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if path != "" {
		t.Errorf("expected empty path (no file), got %q", path)
	}
	if cfg.Listen != "localhost:3100" {
		t.Errorf("Listen: got %q, want localhost:3100", cfg.Listen)
	}
}

func TestLoadExplicitEnvPathMissing(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("LYNXDB_CONFIG", filepath.Join(tmpDir, "nonexistent.yaml"))

	_, _, err := Load("")
	if err == nil {
		t.Fatal("expected error for LYNXDB_CONFIG pointing to nonexistent file")
	}
}

func TestLoadFromFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(`
listen: "0.0.0.0:3200"
retention: "30d"
storage:
  compression: "zstd"
  row_group_size: 32768
query:
  max_concurrent: 20
`), 0o644)

	cfg, path, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if path != cfgPath {
		t.Errorf("path: got %q, want %q", path, cfgPath)
	}
	if cfg.Listen != "0.0.0.0:3200" {
		t.Errorf("Listen: got %q", cfg.Listen)
	}
	if cfg.Retention != Duration(30*24*time.Hour) {
		t.Errorf("Retention: got %v", cfg.Retention)
	}
	if cfg.Storage.Compression != "zstd" {
		t.Errorf("Compression: got %q", cfg.Storage.Compression)
	}
	if cfg.Storage.RowGroupSize != 32768 {
		t.Errorf("RowGroupSize: got %d", cfg.Storage.RowGroupSize)
	}
	if cfg.Query.MaxConcurrent != 20 {
		t.Errorf("MaxConcurrent: got %d", cfg.Query.MaxConcurrent)
	}
	// Fields not in file should remain default.
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel: got %q, want info", cfg.LogLevel)
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	t.Setenv("LYNXDB_CONFIG", "")

	// Change to temp dir so no ./lynxdb.yaml is found.
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	t.Setenv("LYNXDB_LISTEN", "0.0.0.0:4000")
	t.Setenv("LYNXDB_LOG_LEVEL", "debug")
	t.Setenv("LYNXDB_QUERY_MAX_CONCURRENT", "50")
	t.Setenv("LYNXDB_STORAGE_COMPRESSION", "zstd")

	cfg, _, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Listen != "0.0.0.0:4000" {
		t.Errorf("Listen: got %q", cfg.Listen)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel: got %q", cfg.LogLevel)
	}
	if cfg.Query.MaxConcurrent != 50 {
		t.Errorf("MaxConcurrent: got %d", cfg.Query.MaxConcurrent)
	}
	if cfg.Storage.Compression != "zstd" {
		t.Errorf("Compression: got %q", cfg.Storage.Compression)
	}
}

func TestLoadEnvOverridesFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(`listen: "0.0.0.0:3200"`), 0o644)

	t.Setenv("LYNXDB_LISTEN", "0.0.0.0:5000")

	cfg, _, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Env should override file.
	if cfg.Listen != "0.0.0.0:5000" {
		t.Errorf("Listen: got %q, want 0.0.0.0:5000 (env override)", cfg.Listen)
	}
}

func TestLoadExplicitPathMissing(t *testing.T) {
	_, _, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing explicit config path")
	}
}

func TestLoadFileSearchHierarchy(t *testing.T) {
	tmpDir := t.TempDir()
	// Unset LYNXDB_CONFIG so file search is used.
	t.Setenv("LYNXDB_CONFIG", "")

	// Create a config in the current directory.
	cfgPath := filepath.Join(tmpDir, "lynxdb.yaml")
	os.WriteFile(cfgPath, []byte(`listen: "from-cwd"`), 0o644)

	// Change to tmpDir so ./lynxdb.yaml is found.
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	cfg, path, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if path != "lynxdb.yaml" {
		t.Errorf("path: got %q, want lynxdb.yaml", path)
	}
	if cfg.Listen != "from-cwd" {
		t.Errorf("Listen: got %q, want from-cwd", cfg.Listen)
	}
}

func TestSaveAndReload(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "saved.yaml")
	t.Setenv("LYNXDB_CONFIG", "")

	cfg := DefaultConfig()
	cfg.Listen = "0.0.0.0:9000"
	cfg.Retention = Duration(30 * 24 * time.Hour)

	if err := Save(cfg, cfgPath); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, _, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Listen != "0.0.0.0:9000" {
		t.Errorf("Listen: got %q", loaded.Listen)
	}
	if loaded.Retention != Duration(30*24*time.Hour) {
		t.Errorf("Retention: got %v", loaded.Retention)
	}
}

func TestDefaultsTemplateEmbed(t *testing.T) {
	if len(DefaultsTemplate) == 0 {
		t.Fatal("DefaultsTemplate is empty")
	}
}

func TestLoadWithOverrides(t *testing.T) {
	t.Setenv("LYNXDB_CONFIG", "")
	t.Setenv("LYNXDB_LISTEN", "0.0.0.0:4000")
	t.Setenv("LYNXDB_STORAGE_COMPRESSION", "zstd")

	// Change to temp dir so no ./lynxdb.yaml is found.
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	cfg, _, overrides, warnings, err := LoadWithOverrides("")
	if err != nil {
		t.Fatalf("LoadWithOverrides: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("expected no warnings, got %v", warnings)
	}
	if cfg.Listen != "0.0.0.0:4000" {
		t.Errorf("Listen: got %q", cfg.Listen)
	}

	// Check overrides are tracked.
	if len(overrides) != 2 {
		t.Fatalf("expected 2 overrides, got %d: %v", len(overrides), overrides)
	}

	overrideMap := make(map[string]configOverride)
	for _, o := range overrides {
		overrideMap[o.Key] = configOverride{o.Source, o.Value}
	}
	if o, ok := overrideMap["listen"]; !ok {
		t.Error("missing 'listen' override")
	} else {
		if o.source != "LYNXDB_LISTEN" {
			t.Errorf("listen source: got %q", o.source)
		}
		if o.value != "0.0.0.0:4000" {
			t.Errorf("listen value: got %q", o.value)
		}
	}
	if o, ok := overrideMap["storage.compression"]; !ok {
		t.Error("missing 'storage.compression' override")
	} else if o.source != "LYNXDB_STORAGE_COMPRESSION" {
		t.Errorf("storage.compression source: got %q", o.source)
	}
}

type configOverride struct {
	source, value string
}

func TestLoadYAMLParseError(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "bad.yaml")
	os.WriteFile(cfgPath, []byte("listen: ok\nbad_indent\nretention: 7d\n"), 0o644)

	_, _, err := Load(cfgPath)
	if err == nil {
		t.Fatal("expected YAML parse error")
	}
	if !strings.Contains(err.Error(), "parse config") {
		t.Errorf("error %q should mention parse config", err.Error())
	}
}

func TestLoadUnknownKeys(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(`
listen: "localhost:3100"
stoage:
  compression: "lz4"
`), 0o644)

	_, _, _, warnings, err := LoadWithOverrides(cfgPath)
	if err != nil {
		t.Fatalf("LoadWithOverrides: %v", err)
	}

	if len(warnings) == 0 {
		t.Fatal("expected warnings for unknown key 'stoage'")
	}
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "stoage") {
			found = true
			if !strings.Contains(w, "storage") {
				t.Errorf("expected 'Did you mean \"storage\"' suggestion, got:\n%s", w)
			}
		}
	}
	if !found {
		t.Errorf("expected warning about 'stoage', got: %v", warnings)
	}
}

func TestLoadUnknownNestedKey(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(`
storage:
  bogus_unknown_key: "lz4"
`), 0o644)

	_, _, _, warnings, err := LoadWithOverrides(cfgPath)
	if err != nil {
		t.Fatalf("LoadWithOverrides: %v", err)
	}

	if len(warnings) == 0 {
		t.Fatal("expected warnings for unknown key 'storage.bogus_unknown_key'")
	}
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "bogus_unknown_key") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected warning about 'bogus_unknown_key', got: %v", warnings)
	}
}

func TestPIDFilePath(t *testing.T) {
	t.Run("with dataDir", func(t *testing.T) {
		dataDir := filepath.Join("var", "lib", "lynxdb")
		got := PIDFilePath(dataDir)
		want := filepath.Join(dataDir, "lynxdb.pid")
		if got != want {
			t.Errorf("PIDFilePath(%q) = %q, want %q", dataDir, got, want)
		}
	})

	t.Run("XDG_RUNTIME_DIR", func(t *testing.T) {
		xdg := t.TempDir()
		t.Setenv("XDG_RUNTIME_DIR", xdg)
		got := PIDFilePath("")
		want := filepath.Join(xdg, "lynxdb.pid")
		if got != want {
			t.Errorf("PIDFilePath('') with XDG = %q, want %q", got, want)
		}
	})

	t.Run("fallback to TempDir", func(t *testing.T) {
		t.Setenv("XDG_RUNTIME_DIR", "")
		got := PIDFilePath("")
		want := filepath.Join(os.TempDir(), "lynxdb.pid")
		if got != want {
			t.Errorf("PIDFilePath('') fallback = %q, want %q", got, want)
		}
	})
}

func TestGetValue(t *testing.T) {
	t.Setenv("LYNXDB_CONFIG", "")

	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	val, source, err := GetValue("", "listen")
	if err != nil {
		t.Fatalf("GetValue: %v", err)
	}
	if val != "localhost:3100" {
		t.Errorf("expected 'localhost:3100', got %q", val)
	}
	if source != "default" {
		t.Errorf("expected 'default' source, got %q", source)
	}
}

func TestGetValue_UnknownKey(t *testing.T) {
	_, _, err := GetValue("", "nonexistent")
	if err == nil {
		t.Fatal("expected error for unknown key")
	}
}

func TestGetValue_TypoSuggestion(t *testing.T) {
	_, _, err := GetValue("", "lisen")
	if err == nil {
		t.Fatal("expected error for typo")
	}
	if !strings.Contains(err.Error(), "did you mean") {
		t.Errorf("expected 'did you mean' suggestion, got: %v", err)
	}
}

func TestSetValueInFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte("listen: localhost:3100\n"), 0o644)

	if err := SetValueInFile(cfgPath, "listen", "0.0.0.0:9000"); err != nil {
		t.Fatalf("SetValueInFile: %v", err)
	}

	data, _ := os.ReadFile(cfgPath)
	if !strings.Contains(string(data), "0.0.0.0:9000") {
		t.Errorf("expected updated value in file, got:\n%s", string(data))
	}
}

func TestSetValueInFile_Nested(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte("storage:\n  compression: lz4\n"), 0o644)

	if err := SetValueInFile(cfgPath, "storage.compression", "zstd"); err != nil {
		t.Fatalf("SetValueInFile: %v", err)
	}

	data, _ := os.ReadFile(cfgPath)
	if !strings.Contains(string(data), "zstd") {
		t.Errorf("expected 'zstd' in file, got:\n%s", string(data))
	}
}

func TestSetValueInFile_NewFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "sub", "config.yaml")

	if err := SetValueInFile(cfgPath, "retention", "30d"); err != nil {
		t.Fatalf("SetValueInFile: %v", err)
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !strings.Contains(string(data), "30d") {
		t.Errorf("expected '30d' in file, got:\n%s", string(data))
	}
}

func TestSetValueInFile_UnknownKey(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte("listen: localhost:3100\n"), 0o644)

	err := SetValueInFile(cfgPath, "bogus", "value")
	if err == nil {
		t.Fatal("expected error for unknown key")
	}
	if !strings.Contains(err.Error(), "unknown config key") {
		t.Errorf("expected 'unknown config key' error, got: %v", err)
	}
}

func TestSetValueInFile_PreservesOtherKeys(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte("listen: localhost:3100\nretention: 7d\n"), 0o644)

	if err := SetValueInFile(cfgPath, "listen", "0.0.0.0:9000"); err != nil {
		t.Fatalf("SetValueInFile: %v", err)
	}

	data, _ := os.ReadFile(cfgPath)
	content := string(data)
	if !strings.Contains(content, "0.0.0.0:9000") {
		t.Errorf("expected updated listen, got:\n%s", content)
	}
	if !strings.Contains(content, "7d") {
		t.Errorf("expected preserved retention, got:\n%s", content)
	}
}

func TestKnownKeyNames(t *testing.T) {
	keys := KnownKeyNames()
	if len(keys) == 0 {
		t.Fatal("expected non-empty list of known keys")
	}

	// Check some expected keys are present.
	expectedKeys := []string{"listen", "data_dir", "retention", "storage.compression", "query.max_concurrent"}
	for _, ek := range expectedKeys {
		found := false
		for _, k := range keys {
			if k == ek {
				found = true

				break
			}
		}
		if !found {
			t.Errorf("expected key %q in KnownKeyNames()", ek)
		}
	}

	// Section-only keys should NOT be present.
	for _, k := range keys {
		if k == "storage" || k == "query" || k == "ingest" || k == "http" {
			t.Errorf("section key %q should not be in KnownKeyNames()", k)
		}
	}
}

func TestLoadNoUnknownKeysForValidConfig(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(`
listen: "0.0.0.0:3200"
storage:
  compression: "zstd"
query:
  max_concurrent: 20
`), 0o644)

	_, _, _, warnings, err := LoadWithOverrides(cfgPath)
	if err != nil {
		t.Fatalf("LoadWithOverrides: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("expected no warnings for valid config, got: %v", warnings)
	}
}

// F6: Env var parse errors produce warnings instead of being silently swallowed.
func TestLoadEnvParseErrorProducesWarning(t *testing.T) {
	t.Setenv("LYNXDB_CONFIG", "")
	t.Setenv("LYNXDB_STORAGE_FLUSH_THRESHOLD", "garbage")

	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	cfg, _, _, warnings, err := LoadWithOverrides("")
	if err != nil {
		t.Fatalf("LoadWithOverrides: %v", err)
	}
	// Config should still have the default value.
	if cfg.Storage.FlushThreshold != 512*MB {
		t.Errorf("FlushThreshold should be default 512mb, got %s", cfg.Storage.FlushThreshold)
	}
	// There should be a warning about the bad env var.
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "LYNXDB_STORAGE_FLUSH_THRESHOLD") && strings.Contains(w, "garbage") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected warning about bad LYNXDB_STORAGE_FLUSH_THRESHOLD, got: %v", warnings)
	}
}

// F13: Profile CRUD tests.
func TestAddProfile(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")

	if err := AddProfile(cfgPath, "staging", "https://staging:3100", "tok123"); err != nil {
		t.Fatalf("AddProfile: %v", err)
	}

	cfg, _, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, ok := cfg.Profiles["staging"]
	if !ok {
		t.Fatal("profile 'staging' not found")
	}
	if p.URL != "https://staging:3100" {
		t.Errorf("URL: got %q", p.URL)
	}
	if p.Token != "tok123" {
		t.Errorf("Token: got %q", p.Token)
	}
}

func TestAddProfile_UpdateExisting(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")

	if err := AddProfile(cfgPath, "prod", "https://old:3100", "old-tok"); err != nil {
		t.Fatalf("AddProfile: %v", err)
	}
	if err := AddProfile(cfgPath, "prod", "https://new:3100", "new-tok"); err != nil {
		t.Fatalf("AddProfile update: %v", err)
	}

	cfg, _, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p := cfg.Profiles["prod"]
	if p.URL != "https://new:3100" {
		t.Errorf("expected updated URL, got %q", p.URL)
	}
	if p.Token != "new-tok" {
		t.Errorf("expected updated Token, got %q", p.Token)
	}
}

func TestRemoveProfile(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")

	if err := AddProfile(cfgPath, "staging", "https://staging:3100", ""); err != nil {
		t.Fatalf("AddProfile: %v", err)
	}
	if err := RemoveProfile(cfgPath, "staging"); err != nil {
		t.Fatalf("RemoveProfile: %v", err)
	}

	cfg, _, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, ok := cfg.Profiles["staging"]; ok {
		t.Error("profile 'staging' should have been removed")
	}
}

func TestRemoveProfile_NotFound(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte("listen: localhost:3100\n"), 0o644)

	err := RemoveProfile(cfgPath, "nonexistent")
	if err == nil {
		t.Fatal("expected error for removing nonexistent profile")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should mention 'not found', got: %v", err)
	}
}

func TestGetProfile(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")

	if err := AddProfile(cfgPath, "prod", "https://prod:3100", "secret"); err != nil {
		t.Fatalf("AddProfile: %v", err)
	}

	p, err := GetProfile(cfgPath, "prod")
	if err != nil {
		t.Fatalf("GetProfile: %v", err)
	}
	if p.URL != "https://prod:3100" {
		t.Errorf("URL: got %q", p.URL)
	}
	if p.Token != "secret" {
		t.Errorf("Token: got %q", p.Token)
	}
}

func TestGetProfile_NotFound(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte("listen: localhost:3100\n"), 0o644)

	_, err := GetProfile(cfgPath, "missing")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should mention 'not found', got: %v", err)
	}
}

func TestGetProfile_NotFoundWithSuggestions(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")

	if err := AddProfile(cfgPath, "staging", "https://staging:3100", ""); err != nil {
		t.Fatalf("AddProfile: %v", err)
	}

	_, err := GetProfile(cfgPath, "missing")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "staging") {
		t.Errorf("error should list available profiles, got: %v", err)
	}
}

func TestListProfiles_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte("listen: localhost:3100\n"), 0o644)

	profiles, err := ListProfiles(cfgPath)
	if err != nil {
		t.Fatalf("ListProfiles: %v", err)
	}
	if len(profiles) != 0 {
		t.Errorf("expected empty profiles, got %d", len(profiles))
	}
}

func TestListProfiles_Multiple(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")

	if err := AddProfile(cfgPath, "staging", "https://staging:3100", ""); err != nil {
		t.Fatalf("AddProfile staging: %v", err)
	}
	if err := AddProfile(cfgPath, "prod", "https://prod:3100", "tok"); err != nil {
		t.Fatalf("AddProfile prod: %v", err)
	}

	profiles, err := ListProfiles(cfgPath)
	if err != nil {
		t.Fatalf("ListProfiles: %v", err)
	}
	if len(profiles) != 2 {
		t.Fatalf("expected 2 profiles, got %d", len(profiles))
	}
	if profiles["staging"].URL != "https://staging:3100" {
		t.Errorf("staging URL: got %q", profiles["staging"].URL)
	}
	if profiles["prod"].URL != "https://prod:3100" {
		t.Errorf("prod URL: got %q", profiles["prod"].URL)
	}
}

// F17: Profiles in YAML should not trigger unknown-key warnings.
func TestLoadProfilesNoUnknownKeyWarning(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(`
listen: "localhost:3100"
profiles:
  staging:
    url: "https://staging:3100"
    token: "tok123"
`), 0o644)

	_, _, _, warnings, err := LoadWithOverrides(cfgPath)
	if err != nil {
		t.Fatalf("LoadWithOverrides: %v", err)
	}
	for _, w := range warnings {
		if strings.Contains(w, "profiles") {
			t.Errorf("unexpected warning about profiles key: %s", w)
		}
	}
}
