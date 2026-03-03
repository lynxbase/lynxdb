package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// TLSDir is the subdirectory under dataDir for auto-generated TLS files.
	tlsDir = "tls"
	// CertFileName is the filename for the auto-generated certificate.
	certFileName = "server.crt"
	// KeyFileName is the filename for the auto-generated private key.
	keyFileName = "server.key"
	// SelfSignedValidity is how long an auto-generated self-signed cert is valid.
	selfSignedValidity = 10 * 365 * 24 * time.Hour // ~10 years
)

// GenerateSelfSignedCert creates an ECDSA P-256 self-signed certificate
// and writes it to dir/server.crt and dir/server.key.
//
// The certificate includes SANs for localhost, 127.0.0.1, ::1, and the
// system hostname (if resolvable). Validity is 10 years.
//
// Returns the loaded tls.Certificate ready for use in a TLS listener.
func GenerateSelfSignedCert(dir string) (tls.Certificate, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return tls.Certificate{}, fmt.Errorf("auth.GenerateSelfSignedCert: create dir: %w", err)
	}

	// Generate ECDSA P-256 private key.
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("auth.GenerateSelfSignedCert: generate key: %w", err)
	}

	// Build serial number.
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("auth.GenerateSelfSignedCert: generate serial: %w", err)
	}

	// SANs: localhost, 127.0.0.1, ::1, plus system hostname.
	dnsNames := []string{"localhost"}
	ipAddresses := []net.IP{
		net.ParseIP("127.0.0.1"),
		net.ParseIP("::1"),
	}

	if hostname, hostErr := os.Hostname(); hostErr == nil && hostname != "" {
		// Add hostname as DNS SAN if it's not already localhost.
		if hostname != "localhost" {
			dnsNames = append(dnsNames, hostname)
		}
	}

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "lynxdb",
		},
		NotBefore:             now,
		NotAfter:              now.Add(selfSignedValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              dnsNames,
		IPAddresses:           ipAddresses,
	}

	// Self-sign: issuer == subject.
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("auth.GenerateSelfSignedCert: create certificate: %w", err)
	}

	// PEM-encode certificate.
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	// PEM-encode private key.
	keyDER, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("auth.GenerateSelfSignedCert: marshal key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	// Write files — both with 0600 permissions for security.
	certPath := filepath.Join(dir, certFileName)
	keyPath := filepath.Join(dir, keyFileName)

	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		return tls.Certificate{}, fmt.Errorf("auth.GenerateSelfSignedCert: write cert: %w", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		return tls.Certificate{}, fmt.Errorf("auth.GenerateSelfSignedCert: write key: %w", err)
	}

	// Load the written cert+key pair.
	tlsCert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("auth.GenerateSelfSignedCert: load pair: %w", err)
	}

	return tlsCert, nil
}

// LoadCertificate loads a TLS certificate and key from PEM files.
// Returns an error if the files cannot be read or the cert+key pair is invalid.
func LoadCertificate(certFile, keyFile string) (tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("auth.LoadCertificate: %w", err)
	}

	return cert, nil
}

// CertFingerprint computes the SHA-256 fingerprint of an X.509 certificate.
// The format matches SSH and browser conventions (e.g. "SHA-256:2f:4a:8b:c1:...").
func CertFingerprint(cert *x509.Certificate) string {
	hash := sha256.Sum256(cert.Raw)

	var sb strings.Builder
	sb.WriteString("SHA-256:")

	for i, b := range hash {
		if i > 0 {
			sb.WriteByte(':')
		}
		fmt.Fprintf(&sb, "%02x", b)
	}

	return sb.String()
}

// LoadOrGenerateCert loads an existing self-signed certificate from
// {dataDir}/tls/ or generates a new one if none exists.
//
// Returns the TLS certificate, its SHA-256 fingerprint string, and any error.
func LoadOrGenerateCert(dataDir string) (tls.Certificate, string, error) {
	dir := filepath.Join(dataDir, tlsDir)
	certPath := filepath.Join(dir, certFileName)
	keyPath := filepath.Join(dir, keyFileName)

	var tlsCert tls.Certificate
	var err error

	// Try to load existing cert+key.
	if fileExists(certPath) && fileExists(keyPath) {
		tlsCert, err = tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return tls.Certificate{}, "", fmt.Errorf("auth.LoadOrGenerateCert: load existing: %w", err)
		}
	} else {
		// Generate new self-signed cert.
		tlsCert, err = GenerateSelfSignedCert(dir)
		if err != nil {
			return tls.Certificate{}, "", err
		}
	}

	// Parse the leaf certificate to compute the fingerprint.
	leaf, parseErr := x509.ParseCertificate(tlsCert.Certificate[0])
	if parseErr != nil {
		return tls.Certificate{}, "", fmt.Errorf("auth.LoadOrGenerateCert: parse leaf: %w", parseErr)
	}
	tlsCert.Leaf = leaf

	fingerprint := CertFingerprint(leaf)

	return tlsCert, fingerprint, nil
}

// fileExists reports whether the named file exists and is not a directory.
func fileExists(path string) bool {
	info, err := os.Stat(path)

	return err == nil && !info.IsDir()
}
