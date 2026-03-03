package auth

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateSelfSignedCert(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tls")

	cert, err := GenerateSelfSignedCert(dir)
	if err != nil {
		t.Fatalf("GenerateSelfSignedCert: %v", err)
	}

	if len(cert.Certificate) == 0 {
		t.Fatal("expected at least one certificate in chain")
	}

	// Parse the leaf to inspect fields.
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}

	// Check subject.
	if leaf.Subject.CommonName != "lynxdb" {
		t.Errorf("CN = %q, want lynxdb", leaf.Subject.CommonName)
	}

	// Check SANs include localhost and 127.0.0.1.
	hasLocalhost := false
	for _, dns := range leaf.DNSNames {
		if dns == "localhost" {
			hasLocalhost = true
		}
	}
	if !hasLocalhost {
		t.Error("SANs missing localhost DNS name")
	}

	hasLoopback := false
	for _, ip := range leaf.IPAddresses {
		if ip.String() == "127.0.0.1" {
			hasLoopback = true
		}
	}
	if !hasLoopback {
		t.Error("SANs missing 127.0.0.1 IP")
	}

	// Check key usage.
	if leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		t.Error("missing DigitalSignature key usage")
	}

	// Check files were written.
	certPath := filepath.Join(dir, certFileName)
	keyPath := filepath.Join(dir, keyFileName)

	if _, err := os.Stat(certPath); err != nil {
		t.Errorf("cert file not written: %v", err)
	}
	if _, err := os.Stat(keyPath); err != nil {
		t.Errorf("key file not written: %v", err)
	}

	// Check key file permissions (owner-only).
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("stat key: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("key permissions = %o, want 0600", perm)
	}
}

func TestCertFingerprint(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tls")

	cert, err := GenerateSelfSignedCert(dir)
	if err != nil {
		t.Fatalf("GenerateSelfSignedCert: %v", err)
	}

	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}

	fp := CertFingerprint(leaf)

	// Must start with "SHA-256:".
	if !strings.HasPrefix(fp, "SHA-256:") {
		t.Errorf("fingerprint = %q, want SHA-256: prefix", fp)
	}

	// Must have 32 hex pairs separated by colons after the prefix.
	// "SHA-256:" = 7 chars, then 32 * 2 hex chars + 31 colons = 95 chars.
	// Total = 7 + 95 = 102 (for SHA-256 without trailing separator issues).
	parts := strings.Split(strings.TrimPrefix(fp, "SHA-256:"), ":")
	if len(parts) != 32 {
		t.Errorf("fingerprint has %d hex pairs, want 32", len(parts))
	}

	for i, part := range parts {
		if len(part) != 2 {
			t.Errorf("hex pair %d = %q, want 2 chars", i, part)
		}
	}

	// Same cert should produce same fingerprint.
	fp2 := CertFingerprint(leaf)
	if fp != fp2 {
		t.Error("fingerprint not deterministic")
	}
}

func TestLoadOrGenerateCert_Reuse(t *testing.T) {
	dataDir := t.TempDir()

	// First call: generate.
	_, fp1, err := LoadOrGenerateCert(dataDir)
	if err != nil {
		t.Fatalf("first LoadOrGenerateCert: %v", err)
	}

	// Second call: load existing.
	_, fp2, err := LoadOrGenerateCert(dataDir)
	if err != nil {
		t.Fatalf("second LoadOrGenerateCert: %v", err)
	}

	// Same fingerprint means same cert was reused.
	if fp1 != fp2 {
		t.Errorf("fingerprints differ:\n  first:  %s\n  second: %s", fp1, fp2)
	}
}

func TestLoadCertificate_Valid(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tls")

	// Generate a cert first.
	_, err := GenerateSelfSignedCert(dir)
	if err != nil {
		t.Fatalf("GenerateSelfSignedCert: %v", err)
	}

	certPath := filepath.Join(dir, certFileName)
	keyPath := filepath.Join(dir, keyFileName)

	// LoadCertificate should succeed for a valid pair.
	cert, err := LoadCertificate(certPath, keyPath)
	if err != nil {
		t.Fatalf("LoadCertificate: %v", err)
	}

	if len(cert.Certificate) == 0 {
		t.Error("expected at least one certificate")
	}
}

func TestLoadCertificate_InvalidPair(t *testing.T) {
	dir1 := filepath.Join(t.TempDir(), "tls1")
	dir2 := filepath.Join(t.TempDir(), "tls2")

	// Generate two different certs.
	_, err := GenerateSelfSignedCert(dir1)
	if err != nil {
		t.Fatalf("GenerateSelfSignedCert 1: %v", err)
	}
	_, err = GenerateSelfSignedCert(dir2)
	if err != nil {
		t.Fatalf("GenerateSelfSignedCert 2: %v", err)
	}

	// Mismatched cert from dir1, key from dir2.
	certPath := filepath.Join(dir1, certFileName)
	keyPath := filepath.Join(dir2, keyFileName)

	_, err = LoadCertificate(certPath, keyPath)
	if err == nil {
		t.Error("expected error for mismatched cert+key pair")
	}
}

func TestLoadCertificate_MissingFile(t *testing.T) {
	_, err := LoadCertificate("/nonexistent/cert.pem", "/nonexistent/key.pem")
	if err == nil {
		t.Error("expected error for missing files")
	}
}

func TestLoadOrGenerateCert_ReturnsLeaf(t *testing.T) {
	dataDir := t.TempDir()

	cert, fp, err := LoadOrGenerateCert(dataDir)
	if err != nil {
		t.Fatalf("LoadOrGenerateCert: %v", err)
	}

	// Leaf should be populated for efficient TLS handshakes.
	if cert.Leaf == nil {
		t.Error("Leaf should be populated")
	}

	if fp == "" {
		t.Error("fingerprint should not be empty")
	}

	// Verify the cert can be used in a TLS config without error.
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if len(tlsCfg.Certificates) != 1 {
		t.Error("expected one certificate in TLS config")
	}
}
