package auth

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveLoadCredentials_WithFingerprint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.yaml")

	// Override CredentialsPath for the test by writing directly via helpers.
	f := &credentialsFile{
		Servers: map[string]serverEntry{
			"https://lynxdb.company.com": {
				Token:       "lynx_ak_testtoken1234567890abcdef",
				Fingerprint: "SHA-256:aa:bb:cc:dd",
			},
		},
	}

	if err := writeCredentials(path, f); err != nil {
		t.Fatalf("writeCredentials: %v", err)
	}

	// Read back.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	loaded := loadCredentialsFile(path)

	entry, ok := loaded.Servers["https://lynxdb.company.com"]
	if !ok {
		t.Fatalf("server entry not found in file content:\n%s", data)
	}

	if entry.Token != "lynx_ak_testtoken1234567890abcdef" {
		t.Errorf("token = %q, want lynx_ak_testtoken1234567890abcdef", entry.Token)
	}

	if entry.Fingerprint != "SHA-256:aa:bb:cc:dd" {
		t.Errorf("fingerprint = %q, want SHA-256:aa:bb:cc:dd", entry.Fingerprint)
	}
}

func TestSaveLoadCredentials_WithoutFingerprint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.yaml")

	// Save without fingerprint (backward compatible).
	f := &credentialsFile{
		Servers: map[string]serverEntry{
			"http://localhost:3100": {
				Token: "lynx_rk_roottoken1234567890abcdef",
			},
		},
	}

	if err := writeCredentials(path, f); err != nil {
		t.Fatalf("writeCredentials: %v", err)
	}

	loaded := loadCredentialsFile(path)

	entry, ok := loaded.Servers["http://localhost:3100"]
	if !ok {
		t.Fatal("server entry not found")
	}

	if entry.Token != "lynx_rk_roottoken1234567890abcdef" {
		t.Errorf("token = %q, want lynx_rk_roottoken1234567890abcdef", entry.Token)
	}

	if entry.Fingerprint != "" {
		t.Errorf("fingerprint = %q, want empty", entry.Fingerprint)
	}
}

func TestSaveCredentials_PreservesExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.yaml")

	// Write an initial entry.
	f := &credentialsFile{
		Servers: map[string]serverEntry{
			"http://localhost:3100": {Token: "lynx_rk_existing"},
		},
	}
	if err := writeCredentials(path, f); err != nil {
		t.Fatalf("writeCredentials: %v", err)
	}

	// Add a second entry via loadCredentialsFile + write.
	loaded := loadCredentialsFile(path)
	loaded.Servers["https://other:3100"] = serverEntry{
		Token:       "lynx_ak_other",
		Fingerprint: "SHA-256:11:22:33:44",
	}
	if err := writeCredentials(path, loaded); err != nil {
		t.Fatalf("writeCredentials 2: %v", err)
	}

	// Both entries should be present.
	result := loadCredentialsFile(path)

	if len(result.Servers) != 2 {
		t.Errorf("servers = %d, want 2", len(result.Servers))
	}

	if result.Servers["http://localhost:3100"].Token != "lynx_rk_existing" {
		t.Error("first entry lost")
	}

	if result.Servers["https://other:3100"].Fingerprint != "SHA-256:11:22:33:44" {
		t.Error("second entry fingerprint missing")
	}
}

func TestLoadCredentialsFile_MissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nonexistent.yaml")

	f := loadCredentialsFile(path)

	if f.Servers == nil {
		t.Error("Servers map should be initialized")
	}

	if len(f.Servers) != 0 {
		t.Errorf("Servers = %d, want 0", len(f.Servers))
	}
}

func TestWriteCredentials_Permissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "credentials.yaml")

	f := &credentialsFile{
		Servers: map[string]serverEntry{
			"http://localhost:3100": {Token: "test"},
		},
	}

	if err := writeCredentials(path, f); err != nil {
		t.Fatalf("writeCredentials: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("permissions = %o, want 0600", perm)
	}
}

func TestSaveFingerprint_PreservesToken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "credentials.yaml")

	// Write initial entry with token only.
	f := &credentialsFile{
		Servers: map[string]serverEntry{
			"https://server:3100": {Token: "lynx_rk_mytoken"},
		},
	}
	if err := writeCredentials(path, f); err != nil {
		t.Fatalf("writeCredentials: %v", err)
	}

	// Add fingerprint via loadCredentialsFile (simulates SaveFingerprint logic).
	loaded := loadCredentialsFile(path)
	entry := loaded.Servers["https://server:3100"]
	entry.Fingerprint = "SHA-256:ff:ee:dd"
	loaded.Servers["https://server:3100"] = entry

	if err := writeCredentials(path, loaded); err != nil {
		t.Fatalf("writeCredentials 2: %v", err)
	}

	// Both token and fingerprint should be present.
	result := loadCredentialsFile(path)
	e := result.Servers["https://server:3100"]

	if e.Token != "lynx_rk_mytoken" {
		t.Errorf("token = %q, want lynx_rk_mytoken", e.Token)
	}

	if e.Fingerprint != "SHA-256:ff:ee:dd" {
		t.Errorf("fingerprint = %q, want SHA-256:ff:ee:dd", e.Fingerprint)
	}
}
