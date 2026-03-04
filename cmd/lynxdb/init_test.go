package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/lynxbase/lynxdb/pkg/config"
)

// resetInitFlags clears the Changed state and resets values of init
// command flags so tests don't leak state to each other.
func resetInitFlags(t *testing.T) {
	t.Helper()

	flagInitDataDir = ""
	flagInitRetention = ""
	flagInitNoInteractive = false

	// Reset root persistent flags.
	rootCmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		f.Changed = false
		_ = f.Value.Set(f.DefValue)
	})
}

func TestInit_NonInteractive(t *testing.T) {
	resetInitFlags(t)
	t.Setenv("LYNXDB_CONFIG", "")

	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	cfgDir := filepath.Join(tmpDir, "config")
	cfgPath := filepath.Join(cfgDir, "config.yaml")

	// Point config init at our temp path.
	t.Setenv("XDG_CONFIG_HOME", cfgDir)

	cmd := rootCmd
	cmd.SetArgs([]string{"init", "--data-dir", dataDir, "--retention", "30d", "--no-interactive"})

	output, err := captureStdout(t, cmd.Execute)
	if err != nil {
		t.Fatalf("init: %v", err)
	}

	_ = output

	// Verify config file was created.
	cfgData, err := os.ReadFile(filepath.Join(cfgDir, "lynxdb", "config.yaml"))
	if err != nil {
		// Try direct path.
		cfgData, err = os.ReadFile(cfgPath)
		if err != nil {
			// List what was created for debugging.
			entries, _ := os.ReadDir(cfgDir)
			var names []string
			for _, e := range entries {
				names = append(names, e.Name())
			}

			t.Fatalf("config file not created; cfgDir contents: %v", names)
		}
	}

	cfgStr := string(cfgData)
	if !strings.Contains(cfgStr, "data_dir: "+dataDir) {
		t.Errorf("config should contain data_dir, got:\n%s", cfgStr)
	}
	if !strings.Contains(cfgStr, "retention: 30d") {
		t.Errorf("config should contain retention: 30d, got:\n%s", cfgStr)
	}

	// Verify data directory was created.
	info, err := os.Stat(dataDir)
	if err != nil {
		t.Fatalf("data dir not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("data dir is not a directory")
	}
}

func TestInit_NonInteractive_Defaults(t *testing.T) {
	resetInitFlags(t)

	tmpDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tmpDir)
	t.Setenv("XDG_DATA_HOME", tmpDir)
	t.Setenv("LYNXDB_CONFIG", "")

	// Change to temp dir so no ./lynxdb.yaml is found.
	origDir, _ := os.Getwd()
	_ = os.Chdir(tmpDir)
	defer func() { _ = os.Chdir(origDir) }()

	cmd := rootCmd
	cmd.SetArgs([]string{"init", "--no-interactive"})

	_, err := captureStdout(t, cmd.Execute)
	if err != nil {
		t.Fatalf("init with defaults: %v", err)
	}

	// Config file should exist under XDG_CONFIG_HOME.
	cfgPath := filepath.Join(tmpDir, "lynxdb", "config.yaml")
	cfgData, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("config not created at %s: %v", cfgPath, err)
	}

	cfgStr := string(cfgData)
	// Should contain the default retention.
	if !strings.Contains(cfgStr, "retention: 7d") {
		t.Errorf("expected default retention 7d, got:\n%s", cfgStr)
	}
}

func TestInit_NonInteractive_AlreadyExists(t *testing.T) {
	resetInitFlags(t)

	tmpDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tmpDir)
	t.Setenv("LYNXDB_CONFIG", "")

	// Pre-create the config file.
	cfgDir := filepath.Join(tmpDir, "lynxdb")
	_ = os.MkdirAll(cfgDir, 0o755)
	cfgPath := filepath.Join(cfgDir, "config.yaml")
	_ = os.WriteFile(cfgPath, []byte("existing: true\n"), 0o600)

	cmd := rootCmd
	cmd.SetArgs([]string{"init", "--no-interactive"})

	_, err := captureStdout(t, cmd.Execute)
	if err == nil {
		t.Fatal("expected error when config already exists in non-interactive mode")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("error %q should mention 'already exists'", err.Error())
	}
}

func TestInit_InvalidRetention(t *testing.T) {
	resetInitFlags(t)

	tmpDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tmpDir)
	t.Setenv("LYNXDB_CONFIG", "")

	cmd := rootCmd
	cmd.SetArgs([]string{"init", "--retention", "not-a-duration", "--no-interactive"})

	_, err := captureStdout(t, cmd.Execute)
	if err == nil {
		t.Fatal("expected error for invalid retention")
	}
	if !strings.Contains(err.Error(), "invalid retention") {
		t.Errorf("error %q should mention 'invalid retention'", err.Error())
	}
}

func TestExpandHome(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("test requires home directory: %v", err)
	}

	tests := []struct {
		input string
		want  string
	}{
		{"~/data/lynxdb", filepath.Join(home, "data", "lynxdb")},
		{"/absolute/path", "/absolute/path"},
		{"relative/path", "relative/path"},
		{"~nothome", "~nothome"},
	}

	for _, tt := range tests {
		got := expandHome(tt.input)
		if got != tt.want {
			t.Errorf("expandHome(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestWriteInitConfig(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "sub", "config.yaml")

	retDur, _ := config.ParseDuration("30d")
	err := writeInitConfig(path, "/data/lynxdb", retDur)
	if err != nil {
		t.Fatalf("writeInitConfig: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "data_dir: /data/lynxdb") {
		t.Errorf("missing data_dir, got:\n%s", content)
	}
	if !strings.Contains(content, "retention: 30d") {
		t.Errorf("missing retention, got:\n%s", content)
	}
	if !strings.Contains(content, "lynxdb init") {
		t.Errorf("missing 'lynxdb init' comment, got:\n%s", content)
	}
}
