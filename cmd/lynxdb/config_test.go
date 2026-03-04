package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/lynxbase/lynxdb/pkg/config"
)

// resetConfigFlags clears the Changed state and resets values of config
// command PersistentFlags so tests don't leak flag state to each other.
func resetConfigFlags(t *testing.T) {
	t.Helper()
	configCmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		f.Changed = false
		f.Value.Set(f.DefValue)
	})
	// Also reset root persistent flags.
	rootCmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		f.Changed = false
		f.Value.Set(f.DefValue)
	})
}

func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := fn()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	buf.ReadFrom(r)

	return buf.String(), err
}

func TestConfigShow(t *testing.T) {
	resetConfigFlags(t)
	t.Setenv("LYNXDB_CONFIG", "")

	// Change to temp dir so no ./lynxdb.yaml is found.
	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	cmd := rootCmd
	cmd.SetArgs([]string{"config"})

	output, err := captureStdout(t, cmd.Execute)
	if err != nil {
		t.Fatalf("config show: %v", err)
	}
	if !strings.Contains(output, "listen:") {
		t.Errorf("expected 'listen:' in output, got:\n%s", output)
	}
	if !strings.Contains(output, "Server") {
		t.Errorf("expected 'Server' section header, got:\n%s", output)
	}
	if !strings.Contains(output, "Storage") {
		t.Errorf("expected 'Storage' section header, got:\n%s", output)
	}
}

func TestConfigShow_WithCLIOverrides(t *testing.T) {
	resetConfigFlags(t)
	t.Setenv("LYNXDB_CONFIG", "")

	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "--data-dir", "./testdata/db", "--addr", "0.0.0.0:9000"})

	output, err := captureStdout(t, cmd.Execute)
	if err != nil {
		t.Fatalf("config show: %v", err)
	}

	// data_dir should show the CLI override value and source.
	if !strings.Contains(output, "./testdata/db") {
		t.Errorf("expected './testdata/db' in output, got:\n%s", output)
	}
	if !strings.Contains(output, "(--data-dir)") {
		t.Errorf("expected '(--data-dir)' source in output, got:\n%s", output)
	}
	// listen should show the CLI override.
	if !strings.Contains(output, "0.0.0.0:9000") {
		t.Errorf("expected '0.0.0.0:9000' in output, got:\n%s", output)
	}
	if !strings.Contains(output, "(--addr)") {
		t.Errorf("expected '(--addr)' source in output, got:\n%s", output)
	}
}

func TestConfigShow_CLIOverridesEnv(t *testing.T) {
	resetConfigFlags(t)
	t.Setenv("LYNXDB_CONFIG", "")
	t.Setenv("LYNXDB_LISTEN", "0.0.0.0:5000")

	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "--addr", "0.0.0.0:9000"})

	output, err := captureStdout(t, cmd.Execute)
	if err != nil {
		t.Fatalf("config show: %v", err)
	}

	// CLI flag should win over env var.
	if !strings.Contains(output, "0.0.0.0:9000") {
		t.Errorf("expected CLI value '0.0.0.0:9000', got:\n%s", output)
	}
	if !strings.Contains(output, "(--addr)") {
		t.Errorf("expected '(--addr)' source, not env var, got:\n%s", output)
	}
}

func TestConfigValidate_Valid(t *testing.T) {
	resetConfigFlags(t)
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(`
listen: "0.0.0.0:3200"
storage:
  compression: "zstd"
`), 0o644)

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "validate", "--config", cfgPath})

	output, err := captureStdout(t, cmd.Execute)
	if err != nil {
		t.Fatalf("config validate: %v", err)
	}
	if !strings.Contains(output, "is valid") {
		t.Errorf("expected 'is valid' in output, got:\n%s", output)
	}
}

func TestConfigValidate_Invalid(t *testing.T) {
	resetConfigFlags(t)
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(`
listen: ""
`), 0o644)

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "validate", "--config", cfgPath})
	_, err := captureStdout(t, cmd.Execute)
	if err == nil {
		t.Fatal("expected validation error for empty listen")
	}
}

func TestConfigValidate_WithCLIOverride(t *testing.T) {
	resetConfigFlags(t)
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte(`
listen: "0.0.0.0:3200"
`), 0o644)

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "validate", "--config", cfgPath, "--data-dir", "/tmp/lynxtest"})

	output, err := captureStdout(t, cmd.Execute)
	if err != nil {
		t.Fatalf("config validate: %v", err)
	}
	// Non-default values should include both the file override and CLI override.
	if !strings.Contains(output, "Non-default values") {
		t.Errorf("expected 'Non-default values' section, got:\n%s", output)
	}
	if !strings.Contains(output, "--data-dir") {
		t.Errorf("expected '--data-dir' in non-default values, got:\n%s", output)
	}
}

func TestConfigInit(t *testing.T) {
	resetConfigFlags(t)
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "sub", "config.yaml")

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "init", "--config", cfgPath})

	output, err := captureStdout(t, cmd.Execute)
	if err != nil {
		t.Fatalf("config init: %v", err)
	}
	if !strings.Contains(output, "Created config template") {
		t.Errorf("expected creation message, got:\n%s", output)
	}

	// Verify the file was created with the defaults template.
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read created config: %v", err)
	}
	if !bytes.Equal(data, config.DefaultsTemplate) {
		t.Error("created config doesn't match DefaultsTemplate")
	}
}

func TestFindServerPID_ReadsFile(t *testing.T) {
	resetConfigFlags(t)
	t.Setenv("LYNXDB_CONFIG", "")
	// Use a temp dir as dataDir so PIDFilePath points there.
	tmpDir := t.TempDir()
	// Write a minimal config with data_dir set.
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte("data_dir: "+tmpDir+"\n"), 0o600)
	flagConfigPath = cfgPath

	// Write a PID file with our own PID (guaranteed alive).
	pidPath := filepath.Join(tmpDir, "lynxdb.pid")
	os.WriteFile(pidPath, []byte(fmt.Sprintf("%d", os.Getpid())), 0o600)

	pid, err := findServerPID()
	if err != nil {
		t.Fatalf("findServerPID: %v", err)
	}
	if pid != os.Getpid() {
		t.Errorf("got PID %d, want %d", pid, os.Getpid())
	}
}

func TestFindServerPID_NoPIDFile(t *testing.T) {
	resetConfigFlags(t)
	t.Setenv("LYNXDB_CONFIG", "")
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte("data_dir: "+tmpDir+"\n"), 0o600)
	flagConfigPath = cfgPath

	// No PID file written — should fail.
	_, err := findServerPID()
	if err == nil {
		t.Fatal("expected error when no PID file exists")
	}
	if !strings.Contains(err.Error(), "no PID file found") {
		t.Errorf("error %q should mention 'no PID file found'", err.Error())
	}
}

func TestConfigShow_InvalidDuration(t *testing.T) {
	resetConfigFlags(t)
	t.Setenv("LYNXDB_CONFIG", "")

	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "--compaction-interval", "not-a-duration"})

	_, err := captureStdout(t, cmd.Execute)
	if err == nil {
		t.Fatal("expected error for invalid duration flag")
	}
	if !strings.Contains(err.Error(), "invalid --compaction-interval") {
		t.Errorf("error %q should mention 'invalid --compaction-interval'", err.Error())
	}
}

func TestConfigInit_AlreadyExists(t *testing.T) {
	resetConfigFlags(t)
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte("existing"), 0o644)

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "init", "--config", cfgPath})
	_, err := captureStdout(t, cmd.Execute)
	if err == nil {
		t.Fatal("expected error for already-existing config file")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("error %q should mention 'already exists'", err.Error())
	}
}

func TestConfigGet(t *testing.T) {
	resetConfigFlags(t)
	t.Setenv("LYNXDB_CONFIG", "")

	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "get", "listen"})

	output, err := captureStdout(t, cmd.Execute)
	if err != nil {
		t.Fatalf("config get: %v", err)
	}
	if !strings.Contains(output, "localhost:3100") {
		t.Errorf("expected default listen value, got:\n%s", output)
	}
	if !strings.Contains(output, "(default)") {
		t.Errorf("expected '(default)' source, got:\n%s", output)
	}
}

func TestConfigGet_FromFile(t *testing.T) {
	resetConfigFlags(t)
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte("listen: \"0.0.0.0:4200\"\n"), 0o644)

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "get", "listen", "--config", cfgPath})

	output, err := captureStdout(t, cmd.Execute)
	if err != nil {
		t.Fatalf("config get: %v", err)
	}
	if !strings.Contains(output, "0.0.0.0:4200") {
		t.Errorf("expected file value '0.0.0.0:4200', got:\n%s", output)
	}
	if !strings.Contains(output, "(config file)") {
		t.Errorf("expected '(config file)' source, got:\n%s", output)
	}
}

func TestConfigGet_UnknownKey(t *testing.T) {
	resetConfigFlags(t)
	t.Setenv("LYNXDB_CONFIG", "")

	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "get", "nonexistent"})

	_, err := captureStdout(t, cmd.Execute)
	if err == nil {
		t.Fatal("expected error for unknown key")
	}
	if !strings.Contains(err.Error(), "unknown config key") {
		t.Errorf("error %q should mention 'unknown config key'", err.Error())
	}
}

func TestConfigGet_TypoSuggestion(t *testing.T) {
	resetConfigFlags(t)
	t.Setenv("LYNXDB_CONFIG", "")

	tmpDir := t.TempDir()
	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "get", "lisen"})

	_, err := captureStdout(t, cmd.Execute)
	if err == nil {
		t.Fatal("expected error for typo")
	}
	if !strings.Contains(err.Error(), "did you mean") {
		t.Errorf("error %q should suggest correction", err.Error())
	}
}

func TestConfigSet(t *testing.T) {
	resetConfigFlags(t)
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte("listen: \"localhost:3100\"\n"), 0o644)

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "set", "listen", "0.0.0.0:9000", "--config", cfgPath})

	_, err := captureStdout(t, cmd.Execute)
	if err != nil {
		t.Fatalf("config set: %v", err)
	}

	// Verify the file was updated.
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !strings.Contains(string(data), "0.0.0.0:9000") {
		t.Errorf("expected updated value in file, got:\n%s", string(data))
	}
}

func TestConfigSet_NestedKey(t *testing.T) {
	resetConfigFlags(t)
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte("storage:\n  compression: lz4\n"), 0o644)

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "set", "storage.compression", "zstd", "--config", cfgPath})

	_, err := captureStdout(t, cmd.Execute)
	if err != nil {
		t.Fatalf("config set: %v", err)
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !strings.Contains(string(data), "zstd") {
		t.Errorf("expected 'zstd' in file, got:\n%s", string(data))
	}
}

func TestConfigSet_NewFile(t *testing.T) {
	resetConfigFlags(t)
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "newdir", "config.yaml")

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "set", "retention", "30d", "--config", cfgPath})

	_, err := captureStdout(t, cmd.Execute)
	if err != nil {
		t.Fatalf("config set new file: %v", err)
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !strings.Contains(string(data), "30d") {
		t.Errorf("expected '30d' in new file, got:\n%s", string(data))
	}
}

func TestConfigSet_UnknownKey(t *testing.T) {
	resetConfigFlags(t)
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")
	os.WriteFile(cfgPath, []byte("listen: localhost:3100\n"), 0o644)

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "set", "bogus_key", "value", "--config", cfgPath})

	_, err := captureStdout(t, cmd.Execute)
	if err == nil {
		t.Fatal("expected error for unknown key")
	}
	if !strings.Contains(err.Error(), "unknown config key") {
		t.Errorf("error %q should mention 'unknown config key'", err.Error())
	}
}

func TestConfigPath(t *testing.T) {
	resetConfigFlags(t)
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "path", "--config", cfgPath})

	output, err := captureStdout(t, cmd.Execute)
	if err != nil {
		t.Fatalf("config path: %v", err)
	}
	if !strings.Contains(output, cfgPath) {
		t.Errorf("expected path %q in output, got:\n%s", cfgPath, output)
	}
}

func TestConfigReset_NoFile(t *testing.T) {
	resetConfigFlags(t)
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "nonexistent.yaml")

	cmd := rootCmd
	cmd.SetArgs([]string{"config", "reset", "--config", cfgPath})

	_, err := captureStdout(t, cmd.Execute)
	if err == nil {
		t.Fatal("expected error when config file doesn't exist")
	}
	if !strings.Contains(err.Error(), "no config file") {
		t.Errorf("error %q should mention 'no config file'", err.Error())
	}
}
