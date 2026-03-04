package main

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/buildinfo"
	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/config"
)

// Output format constants to satisfy goconst.
const (
	formatJSON   = "json"
	formatNDJSON = "ndjson"
)

// Global flags accessible by all subcommands.
var (
	flagConfigPath      string
	globalServer        string
	globalToken         string
	globalFormat        string
	globalProfile       string
	globalQuiet         bool
	globalVerbose       bool
	globalNoStats       bool
	globalNoColor       bool
	globalDebug         bool
	globalTLSSkipVerify bool
)

var rootCmd = &cobra.Command{
	Use:           "lynxdb",
	Short:         "LynxDB — log analytics in a single binary",
	Long:          `LynxDB is a columnar log storage and search engine with SPL2 query language.`,
	RunE:          runWelcome,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func init() {
	rootCmd.SuggestionsMinimumDistance = 2
	// SetHelpTemplate controls `--help` output. We use a custom layout that
	// puts Examples before Usage for better discoverability.
	// SetUsageTemplate controls the shorter output shown on argument errors.
	// Splitting these avoids the Long description being printed twice — the
	// default help template prints Long then calls UsageString, so if the
	// usage template also contains Long it appears twice.
	rootCmd.SetHelpTemplate(helpTemplate)
	rootCmd.SetUsageTemplate(usageTemplate)
	cobra.OnInitialize(initTheme)

	pf := rootCmd.PersistentFlags()
	pf.StringVar(&flagConfigPath, "config", "", "Path to config file")
	pf.StringVar(&globalServer, "server", envOrDefault("LYNXDB_SERVER", "http://localhost:3100"), "LynxDB server address")
	pf.StringVar(&globalToken, "token", envOrDefault("LYNXDB_TOKEN", ""), "API key for authentication")
	pf.StringVarP(&globalProfile, "profile", "p", envOrDefault("LYNXDB_PROFILE", ""), "Connection profile name")
	pf.StringVarP(&globalFormat, "format", "F", "auto", "Output format: auto, json, ndjson, table, csv, tsv, raw")
	pf.BoolVarP(&globalQuiet, "quiet", "q", false, "Suppress non-data output")
	pf.BoolVarP(&globalVerbose, "verbose", "v", false, "Show extra detail")
	pf.BoolVar(&globalNoStats, "no-stats", false, "Suppress query statistics")
	pf.BoolVar(&globalNoColor, "no-color", false, "Disable colored output")
	pf.BoolVar(&globalDebug, "debug", false, "Enable debug logging to stderr")
	pf.BoolVar(&globalTLSSkipVerify, "tls-skip-verify", envBool("LYNXDB_TLS_SKIP_VERIFY"), "Skip TLS certificate verification")
}

func initTheme() {
	ui.Init(globalNoColor)
	applyProjectRC()
}

// applyProjectRC loads the nearest .lynxdbrc and applies its values
// as defaults — only if the corresponding CLI flag was not explicitly set.
func applyProjectRC() {
	rc, _, err := config.LoadProjectRC()
	if err != nil || rc == nil {
		return
	}

	pf := rootCmd.PersistentFlags()

	if rc.Server != "" && !pf.Changed("server") {
		globalServer = rc.Server
	}
	if rc.DefaultFormat != "" && !pf.Changed("format") {
		globalFormat = rc.DefaultFormat
	}
	if rc.Profile != "" && !pf.Changed("profile") {
		globalProfile = rc.Profile
	}

	// Resolve profile → server URL.
	applyProfile()
}

// applyProfile resolves the --profile flag to a server URL.
func applyProfile() {
	if globalProfile == "" {
		return
	}

	pf := rootCmd.PersistentFlags()

	// Don't override if --server was explicitly set.
	if pf.Changed("server") {
		return
	}

	p, err := config.GetProfile(flagConfigPath, globalProfile)
	if err != nil {
		// Best-effort: don't fail init, the error will show at command time.
		return
	}

	if p.URL != "" {
		globalServer = p.URL
	}
}

func runWelcome(_ *cobra.Command, _ []string) error {
	fmt.Fprintf(os.Stdout, "\n  \U0001F43E %s — log analytics in a single binary\n\n",
		ui.Stdout.Bold.Render(fmt.Sprintf("LynxDB %s", buildinfo.Version)))
	fmt.Fprint(os.Stdout, `  Quick start:
    lynxdb demo                                  Run demo with sample data
    lynxdb query --file app.log 'level=error'    Query a local file
    lynxdb server                                Start server on :3100

  Run 'lynxdb --help' for all commands.
  Run 'lynxdb <command> --help' for command details.
  Run 'lynxdb examples' for a query cookbook.
`)

	return nil
}

// Execute runs the root command and exits with an appropriate code.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		// Ensure the UI theme is initialized. cobra.OnInitialize callbacks
		// only run when a command successfully resolves and starts executing.
		// If flag parsing fails (e.g. unknown flag), initTheme was never
		// called and ui.Stderr is nil — which would panic in renderError.
		ensureThemeInit()

		// noResultsError is a clean exit with a specific code; already printed.
		var noRes noResultsError
		if errors.As(err, &noRes) {
			os.Exit(exitNoResults)
		}

		code := exitGeneral
		switch {
		case isConnectionError(err):
			code = exitConnection
		case isQueryParseError(err):
			code = exitQueryParse
		case isTimeoutError(err):
			code = exitQueryTimeout
		case isAuthError(err):
			code = exitAuth
		}
		if !globalQuiet {
			renderError(err)
		}
		os.Exit(code)
	}
}

// ensureThemeInit initializes ui.Stdout/ui.Stderr if they haven't been set
// yet. This covers error paths that fire before cobra's OnInitialize runs
// (e.g. unknown flags, flag-parse errors).
func ensureThemeInit() {
	if ui.Stderr == nil {
		ui.Init(globalNoColor || os.Getenv("NO_COLOR") != "")
	}
}

// globalHTTPClient is the lazily-initialized shared API client.
var globalHTTPClient *client.Client

// apiClient returns the shared *client.Client, initializing it on first call.
// Must be called after cobra flag parsing (globalServer is finalized).
// Token resolution order: --token flag > LYNXDB_TOKEN env > credentials file > none.
func apiClient() *client.Client {
	if globalHTTPClient == nil {
		token := resolveToken()
		opts := []client.Option{
			client.WithBaseURL(globalServer),
			client.WithAuthToken(token),
		}

		if globalTLSSkipVerify {
			opts = append(opts, client.WithInsecureSkipVerify())
			fmt.Fprintln(os.Stderr, "Warning: TLS verification disabled. Connection is encrypted but not authenticated.")
		} else if strings.HasPrefix(globalServer, "https://") {
			if tc := buildTOFUTLSConfig(globalServer); tc != nil {
				opts = append(opts, client.WithTLSConfig(tc))
			}
		}

		globalHTTPClient = client.NewClient(opts...)
	}

	return globalHTTPClient
}

// resolveToken determines the auth token using the precedence cascade:
// --token flag > LYNXDB_TOKEN env > credentials.yaml (by server URL) > empty.
func resolveToken() string {
	// Explicit --token flag (already populated from LYNXDB_TOKEN env as default).
	if globalToken != "" {
		return globalToken
	}

	// Credentials file lookup by server URL.
	token, err := auth.LoadToken(globalServer)
	if err != nil {
		if globalDebug {
			fmt.Fprintf(os.Stderr, "debug: credentials load: %v\n", err)
		}

		return ""
	}

	return token
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}

// envBool returns true if the environment variable is set to a truthy value.
func envBool(key string) bool {
	v := strings.ToLower(os.Getenv(key))

	return v == "true" || v == "1" || v == "yes"
}

// buildTOFUTLSConfig builds a TLS config that verifies the server certificate
// against a saved fingerprint from the credentials file (TOFU model).
// Returns nil if no fingerprint is saved (falls back to system CA verification).
func buildTOFUTLSConfig(serverURL string) *tls.Config {
	_, fingerprint, err := auth.LoadCredentials(serverURL)
	if err != nil || fingerprint == "" {
		return nil
	}

	// Custom TLS config that verifies the peer cert fingerprint.
	return &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // TOFU: we verify the fingerprint ourselves below.
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return fmt.Errorf("tls: server presented no certificates")
			}

			cert, parseErr := x509.ParseCertificate(rawCerts[0])
			if parseErr != nil {
				return fmt.Errorf("tls: parse server certificate: %w", parseErr)
			}

			got := certFingerprintFromRaw(cert.Raw)
			if got != fingerprint {
				return fmt.Errorf(
					"TLS certificate fingerprint mismatch for %s\n\n"+
						"  Expected: %s\n"+
						"  Got:      %s\n\n"+
						"This could indicate a man-in-the-middle attack, or the server's\n"+
						"certificate was regenerated. If expected, run:\n"+
						"  lynxdb logout --server %s\n"+
						"  lynxdb login --server %s",
					serverURL, fingerprint, got, serverURL, serverURL)
			}

			return nil
		},
		MinVersion: tls.VersionTLS12,
	}
}

// certFingerprintFromRaw computes the SHA-256 fingerprint of DER-encoded cert bytes.
func certFingerprintFromRaw(raw []byte) string {
	hash := sha256.Sum256(raw)

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

// helpTemplate is used for `--help` output. It prints Long once, then
// Examples before Usage for better discoverability.
// Cobra's default help template prints {{.Long}} and then calls
// {{.UsageString}}, causing Long to appear twice. The replacement template
// prints Long exactly once.
const helpTemplate = `{{with (or .Long .Short)}}{{. | trimTrailingWhitespaces}}
{{end}}{{if .HasExample}}
Examples:
{{.Example}}
{{end}}{{if .Runnable}}Usage:
  {{.UseLine}}
{{end}}{{if .HasAvailableSubCommands}}  {{.CommandPath}} [command]
{{end}}{{if gt (len .Aliases) 0}}
Aliases:
  {{.NameAndAliases}}
{{end}}{{if .HasAvailableSubCommands}}{{$cmds := .Commands}}{{if eq (len .Groups) 0}}
Available Commands:{{range $cmds}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{else}}{{range $group := .Groups}}

{{.Title}}{{range $cmds}}{{if (and (eq .GroupID $group.ID) (or .IsAvailableCommand (eq .Name "help")))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if not .AllChildCommandsHaveGroup}}

Additional Commands:{{range $cmds}}{{if (and (eq .GroupID "") (or .IsAvailableCommand (eq .Name "help")))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}

Exit Codes: 0=ok 1=error 2=usage 3=connection 4=parse 5=timeout 6=empty 7=auth 10=aborted 130=interrupted
`

// usageTemplate is the shorter form shown on argument/flag errors.
// It intentionally omits Long and Examples to keep error output concise.
const usageTemplate = `{{if .Runnable}}Usage:
  {{.UseLine}}
{{end}}{{if .HasAvailableSubCommands}}  {{.CommandPath}} [command]
{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`
