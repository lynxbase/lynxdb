package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadProjectRC_Found(t *testing.T) {
	tmpDir := t.TempDir()
	rcPath := filepath.Join(tmpDir, ".lynxdbrc")
	os.WriteFile(rcPath, []byte("server: http://prod:3100\ndefault_format: table\n"), 0o644)

	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	rc, path, err := LoadProjectRC()
	if err != nil {
		t.Fatalf("LoadProjectRC: %v", err)
	}
	if rc == nil {
		t.Fatal("expected non-nil ProjectRC")
	}
	if filepath.Base(path) != ".lynxdbrc" {
		t.Errorf("path should end with .lynxdbrc, got %q", path)
	}
	if rc.Server != "http://prod:3100" {
		t.Errorf("Server: got %q", rc.Server)
	}
	if rc.DefaultFormat != "table" {
		t.Errorf("DefaultFormat: got %q", rc.DefaultFormat)
	}
}

func TestLoadProjectRC_WalksUp(t *testing.T) {
	tmpDir := t.TempDir()

	// Place .lynxdbrc in parent.
	rcPath := filepath.Join(tmpDir, ".lynxdbrc")
	os.WriteFile(rcPath, []byte("server: http://parent:3100\n"), 0o644)

	// Create a child directory and chdir to it.
	childDir := filepath.Join(tmpDir, "sub", "deep")
	os.MkdirAll(childDir, 0o755)

	origDir, _ := os.Getwd()
	os.Chdir(childDir)
	defer os.Chdir(origDir)

	rc, path, err := LoadProjectRC()
	if err != nil {
		t.Fatalf("LoadProjectRC: %v", err)
	}
	if rc == nil {
		t.Fatal("expected to find .lynxdbrc in parent")
	}
	if filepath.Base(path) != ".lynxdbrc" {
		t.Errorf("path should end with .lynxdbrc, got %q", path)
	}
	if rc.Server != "http://parent:3100" {
		t.Errorf("Server: got %q", rc.Server)
	}
}

func TestLoadProjectRC_NotFound(t *testing.T) {
	tmpDir := t.TempDir()

	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	rc, path, err := LoadProjectRC()
	if err != nil {
		t.Fatalf("LoadProjectRC: %v", err)
	}
	if rc != nil {
		t.Errorf("expected nil rc when no .lynxdbrc exists, got %+v", rc)
	}
	if path != "" {
		t.Errorf("expected empty path, got %q", path)
	}
}

func TestLoadProjectRC_AllFields(t *testing.T) {
	tmpDir := t.TempDir()
	rcPath := filepath.Join(tmpDir, ".lynxdbrc")
	os.WriteFile(rcPath, []byte(`
server: http://staging:3100
default_since: 6h
default_format: csv
default_source: my-svc
profile: staging
`), 0o644)

	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	rc, _, err := LoadProjectRC()
	if err != nil {
		t.Fatalf("LoadProjectRC: %v", err)
	}
	if rc.Server != "http://staging:3100" {
		t.Errorf("Server: got %q", rc.Server)
	}
	if rc.DefaultSince != "6h" {
		t.Errorf("DefaultSince: got %q", rc.DefaultSince)
	}
	if rc.DefaultFormat != "csv" {
		t.Errorf("DefaultFormat: got %q", rc.DefaultFormat)
	}
	if rc.DefaultSource != "my-svc" {
		t.Errorf("DefaultSource: got %q", rc.DefaultSource)
	}
	if rc.Profile != "staging" {
		t.Errorf("Profile: got %q", rc.Profile)
	}
}

func TestLoadProjectRC_InvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	rcPath := filepath.Join(tmpDir, ".lynxdbrc")
	os.WriteFile(rcPath, []byte("server: ok\nbad_indent\n  nope\n"), 0o644)

	origDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(origDir)

	_, _, err := LoadProjectRC()
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}
