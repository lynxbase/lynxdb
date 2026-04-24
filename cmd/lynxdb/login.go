package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/client"
)

var flagLoginToken string

var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Authenticate to a LynxDB server",
	Example: `  lynxdb login
  lynxdb login --server https://lynxdb.company.com
  lynxdb login --token "$LYNXDB_TOKEN"`,
	RunE: runLogin,
}

func init() {
	loginCmd.Flags().StringVar(&flagLoginToken, "token", "", "API key (non-interactive)")
	rootCmd.AddCommand(loginCmd)
}

func runLogin(_ *cobra.Command, _ []string) error {
	// Handle TLS TOFU before anything else.
	fingerprint, clientOpts, err := handleLoginTLS()
	if err != nil {
		return err
	}

	token := flagLoginToken

	// Interactive prompt if no --token flag and stdin is a TTY.
	if token == "" && isStdinTTY() {
		fmt.Fprintf(os.Stderr, "LynxDB server: %s\n", globalServer)
		fmt.Fprint(os.Stderr, "API key: ")

		raw, readErr := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr) // Newline after hidden input.

		if readErr != nil {
			return fmt.Errorf("read API key: %w", readErr)
		}

		token = string(raw)
	}

	if token == "" {
		return fmt.Errorf("no API key provided; use --token or run interactively")
	}

	opts := append([]client.Option{
		client.WithBaseURL(globalServer),
		client.WithAuthToken(token),
	}, clientOpts...)
	c := client.NewClient(opts...)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	status, statusErr := c.Status(ctx)
	if statusErr != nil {
		if client.IsAuthRequired(statusErr) {
			return fmt.Errorf("invalid API key")
		}

		return fmt.Errorf("connect to %s: %w", globalServer, statusErr)
	}

	// Check if auth is even enabled — if we got 200 without needing a token,
	// the server doesn't require auth.
	if token == "" && status != nil {
		printMeta("Auth is not enabled on this server. No login needed.")

		// Still save fingerprint if we have one (TLS without auth).
		if fingerprint != "" {
			if saveErr := auth.SaveFingerprint(globalServer, fingerprint); saveErr != nil {
				return fmt.Errorf("save TLS fingerprint: %w", saveErr)
			}

			printMeta("TLS fingerprint saved.")
		}

		return nil
	}

	// Save token and optional fingerprint to credentials file.
	if saveErr := auth.SaveCredentials(globalServer, token, fingerprint); saveErr != nil {
		return fmt.Errorf("save credentials: %w", saveErr)
	}

	if warning := auth.CheckPermissions(); warning != "" {
		printWarning("%s", warning)
	}

	version := ""
	events := ""

	if status != nil {
		version = status.Version
		events = formatCountHuman(status.Events.Total)
	}

	printSuccess("Authenticated to %s (v%s, %s events)", globalServer, version, events)

	if fingerprint != "" {
		printMeta("TLS fingerprint saved.")
	}

	printMeta("Credentials saved.")
	printNextSteps(
		fmt.Sprintf("lynxdb query 'level=error | stats count' --server %s", globalServer),
	)

	return nil
}

// handleLoginTLS handles the TOFU TLS flow for HTTPS servers during login.
// Returns the fingerprint to save (empty for HTTP or CA-signed certs) and
// any additional client options needed for the connection.
func handleLoginTLS() (string, []client.Option, error) {
	if !strings.HasPrefix(globalServer, "https://") {
		return "", nil, nil
	}

	// Skip all verification if requested.
	if globalTLSSkipVerify {
		fmt.Fprintln(os.Stderr, "Warning: TLS verification disabled. Connection is encrypted but not authenticated.")

		return "", []client.Option{client.WithInsecureSkipVerify()}, nil
	}

	// Probe the server cert to determine if it's CA-signed or self-signed.
	serverCert, isCASigned, err := probeServerCert(globalServer)
	if err != nil {
		return "", nil, fmt.Errorf("TLS: connect to %s: %w", globalServer, err)
	}

	// CA-signed: no TOFU needed, use standard verification.
	if isCASigned {
		return "", nil, nil
	}

	// Self-signed: TOFU flow.
	gotFP := auth.CertFingerprint(serverCert)

	_, savedFP, loadErr := auth.LoadCredentials(globalServer)
	if loadErr != nil {
		return "", nil, fmt.Errorf("load credentials: %w", loadErr)
	}

	if savedFP != "" {
		// Saved fingerprint exists — verify it matches.
		if savedFP != gotFP {
			return "", nil, fmt.Errorf(
				"TLS certificate fingerprint mismatch for %s!\n\n"+
					"  Expected: %s\n"+
					"  Got:      %s\n\n"+
					"This could indicate a man-in-the-middle attack, or the server's\n"+
					"certificate was regenerated. If expected, run:\n"+
					"  lynxdb logout --server %s\n"+
					"  lynxdb login --server %s",
				globalServer, savedFP, gotFP, globalServer, globalServer)
		}

		// Fingerprint matches — trust established.
		return savedFP, []client.Option{client.WithInsecureSkipVerify()}, nil
	}

	// No saved fingerprint — prompt for TOFU trust.
	if !isStdinTTY() {
		return "", nil, fmt.Errorf(
			"unknown TLS certificate for %s (non-interactive mode)\n"+
				"Use --tls-skip-verify or run interactively to trust the certificate",
			globalServer)
	}

	fmt.Fprintf(os.Stderr, "\nWarning: Unknown TLS certificate for %s:\n", globalServer)
	fmt.Fprintf(os.Stderr, "  Fingerprint: %s\n\n", gotFP)

	if !confirmAction("Trust this certificate?") {
		return "", nil, fmt.Errorf("certificate not trusted, aborting")
	}

	return gotFP, []client.Option{client.WithInsecureSkipVerify()}, nil
}

// probeServerCert connects to the server to retrieve its TLS certificate
// and determine whether it is trusted by the system CA store.
func probeServerCert(serverURL string) (*x509.Certificate, bool, error) {
	// Extract host from URL.
	host := strings.TrimPrefix(serverURL, "https://")
	if !strings.Contains(host, ":") {
		host += ":443"
	}

	// First try with standard verification (system CAs).
	conn, err := tls.DialWithDialer(
		&net.Dialer{Timeout: 10 * time.Second},
		"tcp", host,
		&tls.Config{MinVersion: tls.VersionTLS12},
	)
	if err == nil {
		// CA-signed cert — trusted by system.
		state := conn.ConnectionState()
		conn.Close()

		if len(state.PeerCertificates) == 0 {
			return nil, false, fmt.Errorf("server presented no certificates")
		}

		return state.PeerCertificates[0], true, nil
	}

	// Standard verification failed — try again without verification to get the cert.
	conn, err = tls.DialWithDialer(
		&net.Dialer{Timeout: 10 * time.Second},
		"tcp", host,
		&tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // Intentional: we need the cert to display its fingerprint for TOFU.
			MinVersion:         tls.VersionTLS12,
		},
	)
	if err != nil {
		return nil, false, err
	}
	defer conn.Close()

	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return nil, false, fmt.Errorf("server presented no certificates")
	}

	return state.PeerCertificates[0], false, nil
}
