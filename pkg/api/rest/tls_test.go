package rest

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/auth"
)

// startTLSServer starts a server with a self-signed TLS certificate.
// Returns the server, the leaf certificate for fingerprint verification, and a cleanup function.
func startTLSServer(t *testing.T) (*Server, *x509.Certificate, func()) {
	t.Helper()

	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	tlsCert, fp, err := auth.LoadOrGenerateCert(dir)
	if err != nil {
		t.Fatalf("generate cert: %v", err)
	}

	if fp == "" {
		t.Fatal("fingerprint should not be empty")
	}

	// Parse the leaf for tests that need it.
	leaf, err := x509.ParseCertificate(tlsCert.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf cert: %v", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS12,
	}

	srv, err := NewServer(Config{
		Addr:      "127.0.0.1:0",
		DataDir:   dir,
		TLSConfig: tlsCfg,
		Logger:    logger,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go srv.Start(ctx)
	srv.WaitReady()

	return srv, leaf, func() {
		cancel()
		time.Sleep(50 * time.Millisecond)
	}
}

func TestTLS_ServerListensHTTPS(t *testing.T) {
	srv, _, cleanup := startTLSServer(t)
	defer cleanup()

	if !srv.TLSEnabled() {
		t.Fatal("TLSEnabled() should be true")
	}

	// Plain HTTP to a TLS server should fail or return a non-200 response.
	// The server rejects the plain-text HTTP request during TLS handshake.
	resp, err := http.Get(fmt.Sprintf("http://%s/health", srv.Addr()))
	if err == nil {
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			t.Fatal("plain HTTP request to TLS server should not succeed with 200")
		}
	}

	// HTTPS with InsecureSkipVerify should succeed (self-signed cert).
	httpsClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	resp, err = httpsClient.Get(fmt.Sprintf("https://%s/health", srv.Addr()))
	if err != nil {
		t.Fatalf("HTTPS request failed: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestTLS_FingerprintVerification(t *testing.T) {
	srv, leaf, cleanup := startTLSServer(t)
	defer cleanup()

	// Build a client that verifies the server cert fingerprint (TOFU model).
	expectedFP := auth.CertFingerprint(leaf)

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
					if len(rawCerts) == 0 {
						return fmt.Errorf("no certificates presented")
					}

					cert, parseErr := x509.ParseCertificate(rawCerts[0])
					if parseErr != nil {
						return parseErr
					}

					gotFP := auth.CertFingerprint(cert)
					if gotFP != expectedFP {
						return fmt.Errorf("fingerprint mismatch: got %s, want %s", gotFP, expectedFP)
					}

					return nil
				},
				MinVersion: tls.VersionTLS12,
			},
		},
	}

	resp, err := client.Get(fmt.Sprintf("https://%s/health", srv.Addr()))
	if err != nil {
		t.Fatalf("fingerprint-verified request failed: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestTLS_WrongFingerprint_Rejected(t *testing.T) {
	srv, _, cleanup := startTLSServer(t)
	defer cleanup()

	// Build a client that expects a wrong fingerprint.
	wrongFP := "SHA-256:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00"

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
					if len(rawCerts) == 0 {
						return fmt.Errorf("no certificates presented")
					}

					cert, parseErr := x509.ParseCertificate(rawCerts[0])
					if parseErr != nil {
						return parseErr
					}

					gotFP := auth.CertFingerprint(cert)
					if gotFP != wrongFP {
						return fmt.Errorf("fingerprint mismatch: got %s, want %s", gotFP, wrongFP)
					}

					return nil
				},
				MinVersion: tls.VersionTLS12,
			},
		},
	}

	_, err := client.Get(fmt.Sprintf("https://%s/health", srv.Addr()))
	if err == nil {
		t.Fatal("request with wrong fingerprint should fail")
	}
}

func TestTLS_WithAuth(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// Generate TLS cert.
	tlsCert, _, err := auth.LoadOrGenerateCert(dir)
	if err != nil {
		t.Fatalf("generate cert: %v", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS12,
	}

	// Set up auth.
	ks, err := auth.OpenKeyStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	root, err := ks.CreateKey("root", true)
	if err != nil {
		t.Fatal(err)
	}

	srv, err := NewServer(Config{
		Addr:      "127.0.0.1:0",
		DataDir:   dir,
		TLSConfig: tlsCfg,
		KeyStore:  ks,
		Logger:    logger,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		time.Sleep(50 * time.Millisecond)
	}()

	go srv.Start(ctx)
	srv.WaitReady()

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	// Health should work without auth.
	resp, err := httpClient.Get(fmt.Sprintf("https://%s/health", srv.Addr()))
	if err != nil {
		t.Fatalf("health: %v", err)
	}

	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("health status = %d, want 200", resp.StatusCode)
	}

	// Stats without token should return 401.
	resp, err = httpClient.Get(fmt.Sprintf("https://%s/api/v1/stats", srv.Addr()))
	if err != nil {
		t.Fatalf("stats no-auth: %v", err)
	}

	resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("stats no-auth status = %d, want 401", resp.StatusCode)
	}

	// Stats with valid token should succeed.
	req, _ := http.NewRequest("GET", fmt.Sprintf("https://%s/api/v1/stats", srv.Addr()), http.NoBody)
	req.Header.Set("Authorization", "Bearer "+root.Token)

	resp, err = httpClient.Do(req)
	if err != nil {
		t.Fatalf("stats with-auth: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("stats with-auth status = %d, want 200", resp.StatusCode)
	}
}

func TestTLS_IngestAndQuery(t *testing.T) {
	srv, _, cleanup := startTLSServer(t)
	defer cleanup()

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	// Ingest an event over TLS.
	ingestBody := `[{"event":"test TLS event","level":"error","source":"tls-test"}]`
	resp, err := httpClient.Post(
		fmt.Sprintf("https://%s/api/v1/ingest", srv.Addr()),
		"application/json",
		io.NopCloser(
			io.Reader(
				&readerFromString{s: ingestBody},
			),
		),
	)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}

	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("ingest status = %d, want 200", resp.StatusCode)
	}

	// Give the engine time to process.
	time.Sleep(100 * time.Millisecond)

	// Query over TLS.
	queryBody := `{"q": "level=error"}`
	resp, err = httpClient.Post(
		fmt.Sprintf("https://%s/api/v1/query", srv.Addr()),
		"application/json",
		io.NopCloser(
			io.Reader(
				&readerFromString{s: queryBody},
			),
		),
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("query status = %d, want 200", resp.StatusCode)
	}

	var result struct {
		Data json.RawMessage `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode query response: %v", err)
	}

	if len(result.Data) == 0 {
		t.Error("expected non-empty query results")
	}
}

// readerFromString is a simple io.Reader wrapping a string.
type readerFromString struct {
	s string
	i int
}

func (r *readerFromString) Read(p []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}

	n := copy(p, r.s[r.i:])
	r.i += n

	return n, nil
}

func TestTLS_DisabledByDefault(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	if srv.TLSEnabled() {
		t.Fatal("TLSEnabled() should be false for plain server")
	}

	// Plain HTTP should work.
	resp, err := http.Get(fmt.Sprintf("http://%s/health", srv.Addr()))
	if err != nil {
		t.Fatalf("plain HTTP: %v", err)
	}

	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}
